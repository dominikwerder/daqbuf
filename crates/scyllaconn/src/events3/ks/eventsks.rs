mod bck_events_lst;

use crate::events3::SeriesInfo;
use crate::events3::ks::lsp_fwd_msp_multi;
use crate::events3::ks::lsp_fwd_msp_multi::LspFwdMspMulti;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaOptsSubmit;
use crate::worker::ScyllaQueueCluster;
use bck_events_lst::BckLspLst;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinningggContainerEventsDyn;
use netpod::TsMs;
use netpod::TsNano;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use netpod::hpp::HaveProgressPending;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "EventsKs"),
    enum variants {
        MspFwd(#[from] crate::events3::mspfwd::Error),
        MspBck(#[from] crate::events3::mspbck::Error),
        BckLspLst(#[from] bck_events_lst::Error),
        MspBckTooMany,
        LspFwdMspMulti(#[from] lsp_fwd_msp_multi::Error),
    },
);

#[derive(Debug, Clone)]
pub struct Opts {
    one_before: bool,
    scyopts: ScyllaOptsSubmit,
    msp_limit: u32,
    lsp_limit: u32,
    msp_reserve_min: usize,
    msp_preopen_min: usize,
    lsp_single_buf_max: usize,
}

impl Opts {
    pub fn new(
        scyopts: ScyllaOptsSubmit,
        msp_limit: u32,
        lsp_limit: u32,
        msp_reserve_min: usize,
        msp_preopen_min: usize,
        lsp_single_buf_max: usize,
    ) -> Self {
        Self {
            one_before: false,
            scyopts,
            msp_limit,
            lsp_limit,
            msp_reserve_min,
            msp_preopen_min,
            lsp_single_buf_max,
        }
    }

    pub fn testing() -> Self {
        Self {
            one_before: true,
            scyopts: ScyllaOptsSubmit::no_choice(),
            msp_limit: 5,
            lsp_limit: 10,
            msp_reserve_min: 5,
            msp_preopen_min: 5,
            lsp_single_buf_max: 13,
        }
    }

    pub fn set_one_before(mut self, x: bool) -> Self {
        self.one_before = x;
        self
    }

    pub fn is_one_before(&self) -> bool {
        self.one_before
    }

    pub fn to_lsp_msp_multi_opts(&self) -> lsp_fwd_msp_multi::Opts {
        lsp_fwd_msp_multi::Opts::new(
            self.msp_limit,
            self.lsp_limit,
            self.msp_reserve_min,
            self.msp_preopen_min,
            self.lsp_single_buf_max,
        )
    }
}

#[derive(Debug)]
struct BckMsps {
    fut: FutDbg<Result<VecDeque<TsMs>, Error>>,
}

#[derive(Debug)]
enum State {
    BckMsps(BckMsps, Instant),
    BckLspLst(BckLspLst, Instant),
    BckLspCstr(BckLspLst, Instant),
    FwdEvents1(TsNano, VecDeque<MspEv>),
    FwdEvents2(LspFwdMspMulti),
    Done,
}

/// All, optionally including one-before, optionally without values, or transformed.
#[derive(Debug)]
pub struct EventsKs {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    scyqu: ScyllaQueueCluster,
    range: ScyllaSeriesRange,
    opts: Opts,
    state: State,
}

impl EventsKs {
    pub fn new(
        series_info: SeriesInfo,
        ks: KeyspaceId,
        scyqu: ScyllaQueueCluster,
        range: ScyllaSeriesRange,
        opts: Opts,
    ) -> Self {
        debug!("EventsKs  new");
        let st1 = BckMsps {
            fut: {
                let series_info = series_info.clone();
                let ks = ks.clone();
                let range = range.clone();
                let scyqu = scyqu.clone();
                let scyopts = opts.scyopts.clone();
                async move {
                    let msps = crate::events3::mspbck::msp_bck(ks, series_info, range.beg(), scyqu, scyopts).await?;
                    Ok(msps)
                }
                .box2()
            },
        };
        let state = State::BckMsps(st1, Instant::now());
        Self {
            series_info,
            ks,
            scyqu,
            range,
            opts,
            state,
        }
    }
}

type ContBox = Box<dyn BinningggContainerEventsDyn>;

impl Stream for EventsKs {
    type Item = Sitemty2<ContBox, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match &mut self.state {
                State::BckMsps(st1, tsbeg) => match st1.fut.poll_unpin(cx) {
                    Ready(x) => {
                        hpp.mark_progress();
                        match x {
                            Ok(msps) => {
                                let tsnow = Instant::now();
                                debug!(
                                    "BckMsps  Ready  dt {:.3} sec",
                                    tsnow.duration_since(*tsbeg).as_secs_f32()
                                );
                                let msps2 = msps.iter().map(|x| MspEv::from(*x)).collect();
                                let stn = bck_events_lst::BckLspLst::new(
                                    self.series_info.clone(),
                                    self.ks.clone(),
                                    msps2,
                                    None,
                                    self.opts.scyopts.clone(),
                                    self.scyqu.clone(),
                                );
                                self.state = State::BckLspLst(stn, tsnow);
                            }
                            Err(e) => {
                                self.state = State::Done;
                                break Ready(Some(Err(e)));
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::BckLspLst(st1, tsbeg) => match st1.poll_unpin(cx) {
                    Ready(x) => {
                        hpp.mark_progress();
                        match x {
                            Ok(res1) => {
                                let tsnow = Instant::now();
                                debug!(
                                    "BckLspLst  Ready  dt {:.3} sec",
                                    tsnow.duration_since(*tsbeg).as_secs_f32()
                                );
                                let msp_keep_a: Vec<_> = res1
                                    .lsps()
                                    .iter()
                                    .filter_map(|(msp, lsp)| {
                                        if let Some(lsp) = lsp {
                                            let ts = msp.to_ts(*lsp);
                                            debug!("BckLspLst  {msp}  {lsp}  {ts}");
                                            Some((*msp, *lsp, ts))
                                        } else {
                                            debug!("BckLspLst  None  {msp}");
                                            None
                                        }
                                    })
                                    .collect();
                                let by_ts: BTreeMap<_, _> =
                                    msp_keep_a.iter().map(|(msp, lsp, ts)| (*ts, (*msp, *lsp))).collect();
                                let mut it1 = by_ts.range(..self.range.beg()).rev();
                                let ts_lst_ph1 = if let Some((ts, (msp, lsp))) = it1.next() {
                                    debug!("lst-unconstr  {msp}  {lsp}  {ts}");
                                    Some(*ts)
                                } else {
                                    debug!("lst-unconstr  None");
                                    None
                                };
                                let msp_keep_b = if let Some(ts0) = ts_lst_ph1 {
                                    msp_keep_a
                                        .iter()
                                        .filter(|(_msp, _lsp, ts)| *ts >= ts0)
                                        .map(|(msp, lsp, ts)| (*msp, *lsp, *ts))
                                        .collect()
                                } else {
                                    msp_keep_a.clone()
                                };
                                for (msp, lsp, ts) in msp_keep_b.iter() {
                                    debug!("msp_keep_b    {msp}  {lsp}  {ts}");
                                }
                                let msps = msp_keep_b.iter().map(|(msp, ..)| *msp).collect();
                                let stn = bck_events_lst::BckLspLst::new(
                                    self.series_info.clone(),
                                    self.ks.clone(),
                                    msps,
                                    Some(self.range.beg()),
                                    self.opts.scyopts.clone(),
                                    self.scyqu.clone(),
                                );
                                self.state = State::BckLspCstr(stn, tsnow);
                            }
                            Err(e) => {
                                self.state = State::Done;
                                break Ready(Some(Err(e.into())));
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::BckLspCstr(st1, tsbeg) => match st1.poll_unpin(cx) {
                    Ready(x) => {
                        hpp.mark_progress();
                        match x {
                            Ok(res1) => {
                                let tsnow = Instant::now();
                                debug!(
                                    "BckLspCstr  Ready  dt {:.3} sec",
                                    tsnow.duration_since(*tsbeg).as_secs_f32()
                                );
                                let msp_keep_a: Vec<_> = res1
                                    .lsps()
                                    .iter()
                                    .filter_map(|(msp, lsp)| {
                                        if let Some(lsp) = lsp {
                                            let ts = msp.to_ts(*lsp);
                                            debug!("BckLspCstr  {msp}  {lsp}  {ts}");
                                            Some((*msp, *lsp, ts))
                                        } else {
                                            debug!("BckLspCstr  None  {msp}");
                                            None
                                        }
                                    })
                                    .collect();
                                let by_ts: BTreeMap<_, _> =
                                    msp_keep_a.iter().map(|(msp, lsp, ts)| (*ts, (*msp, *lsp))).collect();
                                let it1 = by_ts.range(..self.range.beg());
                                let ts_lst_ph2 = if let Some((ts, (msp, lsp))) = it1.rev().next() {
                                    debug!("lst-cstr      {msp}  {lsp}  {ts}");
                                    Some(*ts)
                                } else {
                                    debug!("lst-cstr      None");
                                    None
                                };
                                let range_beg_before = ts_lst_ph2.unwrap_or(self.range.beg());
                                let msps: VecDeque<_> = msp_keep_a.into_iter().map(|(msp, ..)| msp).collect();
                                let mn = msps.len();
                                debug!("FINALE  beg {range_beg_before}  msps len {mn}");
                                self.state = State::FwdEvents1(range_beg_before, msps);
                            }
                            Err(e) => {
                                self.state = State::Done;
                                break Ready(Some(Err(e.into())));
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::FwdEvents1(range_beg_before, msps) => {
                    hpp.mark_progress();
                    let msps = std::mem::replace(msps, VecDeque::new());
                    let range = ScyllaSeriesRange::new(*range_beg_before, self.range.end());
                    let opts = self.opts.to_lsp_msp_multi_opts();
                    let stream = LspFwdMspMulti::new(
                        self.ks.clone(),
                        self.series_info.clone(),
                        range,
                        opts,
                        self.opts.scyopts.clone(),
                        self.scyqu.clone(),
                        msps,
                    );
                    self.state = State::FwdEvents2(stream);
                }
                State::FwdEvents2(inp) => match inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => match x {
                        Ok(x) => match x {
                            StreamItem::DataItem(x) => match x {
                                RangeCompletableItem::Data(x) => {
                                    break Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))));
                                }
                                RangeCompletableItem::RangeComplete => {
                                    break Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))));
                                }
                            },
                            StreamItem::Log(x) => break Ready(Some(Ok(StreamItem::Log(x)))),
                            StreamItem::Stats(x) => break Ready(Some(Ok(StreamItem::Stats(x)))),
                        },
                        Err(e) => {
                            self.state = State::Done;
                            break Ready(Some(Err(e.into())));
                        }
                    },
                    Ready(None) => {
                        self.state = State::Done;
                        hpp.mark_progress();
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::Done => break Ready(None),
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
