use crate::events3::SeriesInfo;
use crate::events3::ks::lsp_fwd_msp_single::LspFwdMspSingleStream;
use crate::events3::mspfwd::ReadMspFwdStream;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueue;
use crate::worker::ScyllaQueueCluster;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::streamitem::sitem2_data;
use items_0::timebin::BinningggContainerEventsDyn;
use netpod::RangeExcl;
use netpod::TsMs;
use netpod::TsNano;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use netpod::hpp::HaveProgressPending;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

fn _keep() {
    error!("");
    warn!("");
    info!("");
    debug!("");
    trace!("");
}

autoerr::create_error_v1!(
    name(Error, "LspFwdMspMulti"),
    enum variants {
        MspFwd(#[from] crate::events3::mspfwd::Error),
        LspFwd(#[from] crate::events3::lspfwd::Error),
        FindNextTsWithoutBuf,
        FindNextTsOnEmptyBuf,
        LoopTooMany,
        MspBck(#[from] crate::events3::mspbck::Error),
        LspFwdSingle(#[from] crate::events3::ks::lsp_fwd_msp_single::Error),
    },
);

#[derive(Debug, Clone)]
pub struct Opts {
    with_values: bool,
    _scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
}

impl Opts {
    pub fn new() -> Self {
        Self {
            with_values: false,
            _scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery::new(),
        }
    }

    pub fn set_values(mut self, x: bool) -> Self {
        self.with_values = x;
        self
    }

    pub fn is_values(&self) -> bool {
        self.with_values
    }
}

pub type Item = Box<dyn BinningggContainerEventsDyn>;

#[derive(Debug)]
struct Inp {
    evs: Option<LspFwdMspSingleStream>,
    buf: Option<Item>,
}

#[derive(Debug)]
struct InputIx(usize);

#[derive(Debug)]
enum NextTs {
    None,
    One(TsNano, InputIx),
    Two(TsNano, InputIx, TsNano, InputIx),
}

struct BaseRefs<'a> {
    series_info: &'a SeriesInfo,
    ks: &'a KeyspaceId,
    range: &'a ScyllaSeriesRange,
    scyqu: &'a mut ScyllaQueueCluster,
}

impl<'a> BaseRefs<'a> {
    fn clone_mut<'b>(&'b mut self) -> BaseRefs<'a>
    where
        'b: 'a,
    {
        Self {
            series_info: &self.series_info,
            ks: &self.ks,
            range: &self.range,
            scyqu: &mut self.scyqu,
        }
    }
}

#[derive(Debug)]
enum CheckInputItem {
    OpenNextMsp,
    Item(Item),
}

#[derive(Debug)]
struct Merging {
    msps: Option<ReadMspFwdStream>,
    mspbuf: VecDeque<TsMs>,
    inps: VecDeque<Inp>,
}

impl Merging {
    fn find_next_ts(self: Pin<&mut Self>) -> Result<NextTs, Error> {
        let self2 = self.get_mut();
        let mut best: Option<(TsNano, usize)> = None;
        let mut second: Option<(TsNano, usize)> = None;
        for (ix, inp) in self2.inps.iter_mut().enumerate() {
            if let Some(buf) = inp.buf.as_mut() {
                if let Some(ts) = buf.as_mergeable_dyn_mut().ts_min() {
                    match best {
                        Some((ts_1st, _)) => {
                            if ts < ts_1st {
                                second = best;
                                best = Some((ts, ix));
                            } else {
                                match second {
                                    None => {
                                        second = Some((ts, ix));
                                    }
                                    Some((ts_2nd, _)) => {
                                        if ts < ts_2nd {
                                            second = Some((ts, ix));
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            best = Some((ts, ix));
                        }
                    }
                } else {
                    return Err(Error::FindNextTsOnEmptyBuf);
                }
            } else {
                return Err(Error::FindNextTsWithoutBuf);
            }
        }
        let ret = match (best, second) {
            (None, _) => NextTs::None,
            (Some((ts1, ix1)), None) => NextTs::One(ts1, InputIx(ix1)),
            (Some((ts1, ix1)), Some((ts2, ix2))) => NextTs::Two(ts1, InputIx(ix1), ts2, InputIx(ix2)),
        };
        Ok(ret)
    }

    fn poll_all_inp(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _brefs: BaseRefs,
    ) -> Poll<Option<Sitemty2<(), Error>>> {
        use Poll::*;
        // TODO when does this loop actually terminate?
        let mut il1 = 0u32;
        'outer: loop {
            il1 += 1;
            if il1 > 100 {
                error!("LspFwdMspMulti  poll_all_inp  loop iteration too many");
                break Ready(Some(Err(Error::LoopTooMany)));
            }
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            for inp in &mut self2.inps {
                if let Some(buf) = inp.buf.as_mut() {
                    if buf.len() == 0 {
                        hpp.mark_progress();
                        inp.buf = None;
                    }
                } else {
                    if let Some(inps) = inp.evs.as_mut() {
                        match inps.poll_next_unpin(cx) {
                            Ready(Some(x)) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(x) => match x {
                                        StreamItem::DataItem(x) => match x {
                                            RangeCompletableItem::Data(x) => {
                                                if x.len() == 0 {
                                                    // TODO count for metrics
                                                } else {
                                                    inp.buf = Some(x);
                                                }
                                            }
                                            RangeCompletableItem::RangeComplete => {
                                                break 'outer Ready(Some(Ok(StreamItem::DataItem(
                                                    RangeCompletableItem::RangeComplete,
                                                ))));
                                            }
                                        },
                                        StreamItem::Log(x) => {
                                            break 'outer Ready(Some(Ok(StreamItem::Log(x))));
                                        }
                                        StreamItem::Stats(x) => {
                                            break 'outer Ready(Some(Ok(StreamItem::Stats(x))));
                                        }
                                    },
                                    Err(e) => break 'outer Ready(Some(Err(e.into()))),
                                }
                            }
                            Ready(None) => {
                                hpp.mark_progress();
                                inp.evs = None;
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        // will get removed
                    }
                }
            }
            {
                let n1 = self2.inps.len();
                self2.inps.retain(|x| x.buf.is_some() || x.evs.is_some());
                let n2 = self2.inps.len();
                if n1 != n2 {
                    hpp.mark_progress();
                }
            }
            let n_empty = self2.inps.iter().filter(|x| x.buf.is_none()).count();
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else if n_empty == 0 {
                Ready(Some(sitem2_data(())))
            } else {
                Ready(None)
            };
        }
    }

    fn check_inputs(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut brefs: BaseRefs,
        msp_next: Option<TsMs>,
    ) -> Poll<Option<Sitemty2<CheckInputItem, Error>>> {
        use Poll::*;
        let mut il1 = 0u32;
        loop {
            il1 += 1;
            if il1 > 100 {
                error!("LspFwdMspMulti  check_inputs  loop iteration too many");
                break Ready(Some(Err(Error::LoopTooMany)));
            }
            let mut hpp = HaveProgressPending::new();
            {
                // TODO fix
                // let brefs2 = brefs.clone_mut();
            }
            let brefs2 = BaseRefs {
                series_info: brefs.series_info,
                ks: brefs.ks,
                range: brefs.range,
                scyqu: brefs.scyqu,
            };
            if self.inps.len() == 0 {
                info!("LspFwdMspMulti  check_inputs  no inps");
            }
            match self.as_mut().poll_all_inp(cx, brefs2) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match x {
                        Ok(x) => {
                            match x {
                                StreamItem::DataItem(x) => match x {
                                    RangeCompletableItem::Data(()) => {
                                        match self.as_mut().find_next_ts()? {
                                            NextTs::None => {
                                                if self.inps.len() == 0 {
                                                    info!("LspFwdMspMulti  check_inputs  NextTs::None and no inps");
                                                    break Ready(Some(sitem2_data(CheckInputItem::OpenNextMsp)));
                                                } else {
                                                    error!(
                                                        "LspFwdMspMulti  check_inputs  find_next_ts  None  but inps not empty"
                                                    );
                                                    self.inps.clear();
                                                }
                                            }
                                            NextTs::One(_ts1, ix1) => {
                                                // TODO check if msp lower than final entry in ix1 buffer.
                                                // if yes, add a new stream for that msp and start over.
                                                // TODO maybe the ts finder should already return also the max to avoid unwrap.
                                                let b1 = &mut self.inps.get_mut(ix1.0).unwrap().buf;
                                                let b2 = b1.as_mut().unwrap();
                                                let ts_max = b2.ts_max().unwrap();
                                                if msp_next.map_or(false, |x| x.ns() <= ts_max) {
                                                    info!("LspFwdMspMulti  check_inputs  NextTs::One  open next msp");
                                                    break Ready(Some(sitem2_data(CheckInputItem::OpenNextMsp)));
                                                } else {
                                                    info!("TODO actually drain the events {}", b2.len());
                                                    let item = b1.take().unwrap();
                                                    break Ready(Some(sitem2_data(CheckInputItem::Item(item))));
                                                }
                                            }
                                            NextTs::Two(_ts1, ix1, ts2, _ix2) => {
                                                let b1 = &mut self.inps.get_mut(ix1.0).unwrap().buf;
                                                let b2 = b1.as_mut().unwrap();
                                                if msp_next.map_or(false, |x| x.ns() <= ts2) {
                                                    info!("LspFwdMspMulti  check_inputs  NextTs::Two  open next msp");
                                                    break Ready(Some(sitem2_data(CheckInputItem::OpenNextMsp)));
                                                } else {
                                                    // TODO drain all events with ts <= ts2.
                                                    info!("TODO actually drain the events {}", b2.len());
                                                    // TODO maybe better if api actually returns the index?
                                                    if let Some(i3) =
                                                        b2.as_mergeable_dyn_mut().find_highest_index_le(ts2)
                                                    {
                                                        use items_0::merge::DrainIntoNewDynResult;
                                                        match b2.as_mergeable_dyn_mut().drain_into_new(0..i3) {
                                                            // TODO optimizations?
                                                            DrainIntoNewDynResult::Done(c) => {
                                                                break Ready(Some(sitem2_data(CheckInputItem::Item(
                                                                    c,
                                                                ))));
                                                            }
                                                            DrainIntoNewDynResult::Partial(c) => {
                                                                break Ready(Some(sitem2_data(CheckInputItem::Item(
                                                                    c,
                                                                ))));
                                                            }
                                                            DrainIntoNewDynResult::NotCompatible => {
                                                                // should not happen because we drain into new
                                                                // TODO metrics
                                                                *b1 = None;
                                                            }
                                                        }
                                                    } else {
                                                        error!("nothing to drain?");
                                                        *b1 = None;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    RangeCompletableItem::RangeComplete => {
                                        break Ready(Some(Ok(StreamItem::DataItem(
                                            RangeCompletableItem::RangeComplete,
                                        ))));
                                    }
                                },
                                StreamItem::Log(x) => {
                                    break Ready(Some(Ok(StreamItem::Log(x))));
                                }
                                StreamItem::Stats(x) => {
                                    break Ready(Some(Ok(StreamItem::Stats(x))));
                                }
                            }
                        }
                        Err(e) => break Ready(Some(Err(e.into()))),
                    }
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            // TODO check whether we have to open next msp before we can make a decision.
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }

    fn gen_limit(&mut self) -> u32 {
        73
    }

    fn poll_state(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        brefs: BaseRefs,
    ) -> Poll<Option<Sitemty2<Item, Error>>> {
        use Poll::*;
        let mut il1 = 0u32;
        loop {
            il1 += 1;
            if il1 > 100 {
                error!("LspFwdMspMulti  poll_state  loop iteration too many");
                break Ready(Some(Err(Error::LoopTooMany)));
            }
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            let brefs2 = BaseRefs {
                series_info: brefs.series_info,
                ks: brefs.ks,
                range: brefs.range,
                scyqu: brefs.scyqu,
            };
            let msp_next_opt = if let Some(&msp_next) = self2.mspbuf.front() {
                Some(Some(msp_next))
            } else {
                if let Some(inp) = self2.msps.as_mut() {
                    match inp.poll_next_unpin(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => {
                                    if x.len() == 0 {
                                        // TODO count for metrics
                                    }
                                    self2.mspbuf.extend(x);
                                    None
                                }
                                Err(e) => break Ready(Some(Err(e.into()))),
                            }
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            self2.msps = None;
                            None
                        }
                        Pending => {
                            hpp.mark_pending();
                            None
                        }
                    }
                } else {
                    Some(None)
                }
            };
            if let Some(msp_next) = msp_next_opt {
                match Pin::new(&mut *self2).check_inputs(cx, brefs2, msp_next) {
                    Ready(Some(x)) => match x {
                        Ok(x) => match x {
                            StreamItem::DataItem(x) => match x {
                                RangeCompletableItem::Data(x) => match x {
                                    CheckInputItem::OpenNextMsp => {
                                        if let Some(msp_next) = self2.mspbuf.pop_front() {
                                            hpp.mark_progress();
                                            let msp = MspEv::from(msp_next);
                                            info!("LspFwdMspMulti  poll_state  OpenNextMsp  {msp_next}  {msp}");
                                            let stream = LspFwdMspSingleStream::new(
                                                brefs.ks.clone(),
                                                brefs.series_info.clone(),
                                                msp,
                                                brefs.range.clone(),
                                                self2.gen_limit(),
                                                brefs.scyqu.clone(),
                                            );
                                            self2.inps.push_back(Inp {
                                                evs: Some(stream),
                                                buf: None,
                                            });
                                        } else {
                                            info!("LspFwdMspMulti  poll_state  OpenNextMsp  no msp_next");
                                        }
                                    }
                                    CheckInputItem::Item(x) => {
                                        break Ready(Some(items_0::streamitem::sitem2_data(x)));
                                    }
                                },
                                RangeCompletableItem::RangeComplete => {
                                    break Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))));
                                }
                            },
                            StreamItem::Log(x) => {
                                break Ready(Some(Ok(StreamItem::Log(x))));
                            }
                            StreamItem::Stats(x) => {
                                break Ready(Some(Ok(StreamItem::Stats(x))));
                            }
                        },
                        Err(e) => break Ready(Some(Err(e.into()))),
                    },
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else {
                // nothing to do, will re-loop
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}

#[derive(Debug)]
enum State {
    Merging(Merging),
    Done,
}

#[derive(Debug)]
pub struct LspFwdMspMulti {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    range: ScyllaSeriesRange,
    _opts: Opts,
    state: State,
    scyqu: ScyllaQueueCluster,
    loop_cnt: u32,
}

impl LspFwdMspMulti {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        range: ScyllaSeriesRange,
        opts: Opts,
        scyqu: ScyllaQueueCluster,
        msps: VecDeque<MspEv>,
    ) -> Self {
        let (msp_stream_range, msp_begexcl) = if let Some(msp) = msps.back() {
            let beg = msp.to_ms().ns();
            (ScyllaSeriesRange::new(beg, range.end()), RangeExcl::Beg)
        } else {
            (range.clone(), RangeExcl::None)
        };
        let msp_stream = ReadMspFwdStream::new(
            ks.clone(),
            series_info.id(),
            msp_stream_range,
            msp_begexcl,
            3,
            scyqu.clone(),
        );
        let mspbuf = msps.into_iter().map(|m| m.to_ms()).collect();
        let state = State::Merging(Merging {
            msps: Some(msp_stream),
            mspbuf,
            inps: VecDeque::new(),
        });
        Self {
            ks,
            series_info,
            range,
            _opts: opts,
            state,
            scyqu,
            loop_cnt: 0,
        }
    }
}

impl Stream for LspFwdMspMulti {
    type Item = Sitemty2<Item, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.loop_cnt += 1;
        if self.loop_cnt > 10000 {
            error!("LspFwdMspMulti  poll_next  too many");
            self.state = State::Done;
            return Ready(Some(Err(Error::LoopTooMany)));
        }
        let mut il1 = 0u32;
        loop {
            il1 += 1;
            if il1 > 100 {
                error!("LspFwdMspMulti  poll_next  loop iteration too many");
                self.state = State::Done;
                break Ready(Some(Err(Error::LoopTooMany)));
            }
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Merging(st1) => {
                    let brefs = BaseRefs {
                        series_info: &self2.series_info,
                        ks: &self2.ks,
                        range: &self2.range,
                        scyqu: &mut self2.scyqu,
                    };
                    match Pin::new(st1).poll_state(cx, brefs) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => break Ready(Some(Ok(x))),
                                Err(e) => {
                                    self2.state = State::Done;
                                    break Ready(Some(Err(e)));
                                }
                            }
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            self2.state = State::Done;
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Done => {}
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}

#[derive(Debug)]
enum State2 {
    Run,
    MaybeRangeFinal,
    Done,
}

#[derive(Debug)]
pub struct LspFwdMspMultiOverClusters {
    series_info: SeriesInfo,
    range: ScyllaSeriesRange,
    opts: Opts,
    pending: VecDeque<(Arc<ScyllaQueueCluster>, KeyspaceId)>,
    act1: Option<
        FutDbg<(
            Result<VecDeque<TsMs>, crate::events3::mspbck::Error>,
            (Arc<ScyllaQueueCluster>, KeyspaceId),
        )>,
    >,
    active: Option<(String, KeyspaceId, LspFwdMspMulti, bool)>,
    range_final_true: u32,
    range_final_false: u32,
    state: State2,
}

impl LspFwdMspMultiOverClusters {
    pub fn new(series_info: SeriesInfo, range: ScyllaSeriesRange, opts: Opts, scyqu: ScyllaQueue) -> Self {
        let pending = scyqu
            .into_clusters()
            .flat_map(|c| {
                let ks_list: Vec<KeyspaceId> = c.keyspaces().iter().cloned().collect();
                ks_list.into_iter().map(move |ks| (c.clone(), ks))
            })
            .collect();
        Self {
            series_info,
            range,
            opts,
            pending,
            act1: None,
            active: None,
            range_final_true: 0,
            range_final_false: 0,
            state: State2::Run,
        }
    }
}

#[derive(Debug)]
pub struct LspFwdOverClusterItem {
    pub cl: String,
    pub ks: KeyspaceId,
    pub item: Box<dyn BinningggContainerEventsDyn>,
}

impl Stream for LspFwdMspMultiOverClusters {
    type Item = Sitemty2<LspFwdOverClusterItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let self2 = self.as_mut().get_mut();
            break match &mut self2.state {
                State2::Run => {
                    if let Some(fut) = self2.act1.as_mut() {
                        match fut.poll_unpin(cx) {
                            Ready((x, (cl, ks))) => {
                                self2.act1 = None;
                                match x {
                                    Ok(x) => {
                                        let msps = x.into_iter().map(|x| MspEv::from(x)).collect();
                                        let stream = LspFwdMspMulti::new(
                                            ks.clone(),
                                            self2.series_info.clone(),
                                            self2.range.clone(),
                                            self2.opts.clone(),
                                            (*cl).clone(),
                                            msps,
                                        );
                                        self2.active = Some((cl.tag().into(), ks, stream, false));
                                        continue;
                                    }
                                    Err(e) => Ready(Some(Err(e.into()))),
                                }
                            }
                            Pending => Pending,
                        }
                    } else if let Some((tag, ks, stream, range_final)) = self2.active.as_mut() {
                        match Pin::new(stream).poll_next(cx) {
                            Ready(Some(Ok(item))) => match item {
                                StreamItem::DataItem(RangeCompletableItem::Data(inner)) => {
                                    let wrapped = LspFwdOverClusterItem {
                                        cl: tag.clone(),
                                        ks: ks.clone(),
                                        item: inner,
                                    };
                                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(wrapped)))))
                                }
                                StreamItem::DataItem(RangeCompletableItem::RangeComplete) => {
                                    *range_final = true;
                                    continue;
                                }
                                StreamItem::Log(x) => Ready(Some(Ok(StreamItem::Log(x)))),
                                StreamItem::Stats(x) => Ready(Some(Ok(StreamItem::Stats(x)))),
                            },
                            Ready(Some(Err(e))) => Ready(Some(Err(e))),
                            Ready(None) => {
                                if *range_final {
                                    self2.range_final_true += 1;
                                } else {
                                    self2.range_final_false += 1;
                                }
                                self2.active = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else {
                        match self2.pending.pop_front() {
                            None => {
                                self2.state = State2::MaybeRangeFinal;
                                continue;
                            }
                            Some((cl, ks)) => {
                                let fut = crate::events3::mspbck::msp_bck(
                                    ks.clone(),
                                    self2.series_info.clone(),
                                    self2.range.beg(),
                                    cl.as_ref().clone(),
                                )
                                .map(|x| (x, (cl, ks)));
                                self2.act1 = Some(fut.box2());
                                continue;
                            }
                        }
                    }
                }
                State2::MaybeRangeFinal => {
                    self2.state = State2::Done;
                    if self2.range_final_false == 0 && self2.range_final_true != 0 {
                        Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                    } else {
                        continue;
                    }
                }
                State2::Done => Ready(None),
            };
        }
    }
}
