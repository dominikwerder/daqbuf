use crate::events3::SeriesInfo;
use crate::events3::ks::lsp_fwd_msp_single::LspFwdMspSingleStream;
use crate::events3::mspfwd::ReadMspFwdStream;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaOptsSubmit;
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

pub type ContBox = Box<dyn BinningggContainerEventsDyn>;

struct BaseRefs<'a> {
    series_info: &'a SeriesInfo,
    ks: &'a KeyspaceId,
    range: &'a ScyllaSeriesRange,
    scyqu: &'a mut ScyllaQueueCluster,
}

#[derive(Debug)]
struct Reading {
    msps: Option<ReadMspFwdStream>,
    mspbuf: VecDeque<TsMs>,
    lsps: Option<(MspEv, LspFwdMspSingleStream)>,
    lsp_limit: u32,
    lsp_single_buf_max: usize,
    scyopts: ScyllaOptsSubmit,
}

impl Reading {
    fn lsp_limit(&mut self) -> u32 {
        self.lsp_limit
    }

    fn poll_state(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        brefs: BaseRefs,
    ) -> Poll<Option<Sitemty2<(MspEv, ContBox), Error>>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            if let Some((msp, inp)) = self2.lsps.as_mut() {
                match inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => match x {
                                StreamItem::DataItem(x) => match x {
                                    RangeCompletableItem::Data(x) => break Ready(Some(sitem2_data((msp.clone(), x)))),
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
                            },
                            Err(e) => break Ready(Some(Err(e.into()))),
                        }
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self2.lsps = None;
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else if let Some(msp) = self2.mspbuf.pop_front() {
                hpp.mark_progress();
                let msp = MspEv::from(msp);
                trace!("poll_state  open next msp  {msp}");
                let stream = LspFwdMspSingleStream::new(
                    brefs.ks.clone(),
                    brefs.series_info.clone(),
                    msp,
                    brefs.range.clone(),
                    self2.lsp_limit(),
                    self2.lsp_single_buf_max,
                    self2.scyopts.clone(),
                    brefs.scyqu.clone(),
                );
                self2.lsps = Some((msp, stream));
            } else if let Some(inp) = self2.msps.as_mut() {
                match inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => {
                                if x.len() == 0 {
                                    // TODO count for metrics
                                }
                                self2.mspbuf.extend(x);
                            }
                            Err(e) => break Ready(Some(Err(e.into()))),
                        }
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self2.msps = None;
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else {
                // done
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
    Reading(Reading),
    Done,
}

#[derive(Debug)]
pub struct LspFwdMspSerial {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    range: ScyllaSeriesRange,
    state: State,
    scyqu: ScyllaQueueCluster,
}

impl LspFwdMspSerial {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        range: ScyllaSeriesRange,
        scyqu: ScyllaQueueCluster,
        scyopts: ScyllaOptsSubmit,
        msps: VecDeque<MspEv>,
        msp_limit: u32,
        lsp_limit: u32,
        lsp_single_buf_max: usize,
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
            msp_limit,
            scyopts.clone(),
            scyqu.clone(),
        );
        let mspbuf = msps.into_iter().map(|m| m.to_ms()).collect();
        let state = State::Reading(Reading {
            msps: Some(msp_stream),
            mspbuf,
            lsps: None,
            lsp_limit,
            lsp_single_buf_max,
            scyopts,
        });
        Self {
            ks,
            series_info,
            range,
            state,
            scyqu,
        }
    }
}

impl Stream for LspFwdMspSerial {
    type Item = Sitemty2<(MspEv, ContBox), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Reading(st1) => {
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
pub struct LspFwdMspSerialOverClusters {
    series_info: SeriesInfo,
    range: ScyllaSeriesRange,
    pending: VecDeque<(Arc<ScyllaQueueCluster>, KeyspaceId)>,
    act1: Option<
        FutDbg<(
            Result<VecDeque<TsMs>, crate::events3::mspbck::Error>,
            (Arc<ScyllaQueueCluster>, KeyspaceId),
        )>,
    >,
    active: Option<(String, KeyspaceId, LspFwdMspSerial, bool)>,
    range_final_true: u32,
    range_final_false: u32,
    state: State2,
    msp_limit: u32,
    lsp_limit: u32,
    lsp_single_buf_max: usize,
    scyopts: ScyllaOptsSubmit,
}

impl LspFwdMspSerialOverClusters {
    pub fn new(
        series_info: SeriesInfo,
        range: ScyllaSeriesRange,
        msp_limit: u32,
        lsp_limit: u32,
        lsp_single_buf_max: usize,
        scyopts: ScyllaOptsSubmit,
        scyqu: ScyllaQueue,
    ) -> Self {
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
            pending,
            act1: None,
            active: None,
            range_final_true: 0,
            range_final_false: 0,
            state: State2::Run,
            msp_limit,
            lsp_limit,
            lsp_single_buf_max,
            scyopts,
        }
    }
}

#[derive(Debug)]
pub struct LspFwdMspSerialOverClustersItem {
    pub cl: String,
    pub ks: KeyspaceId,
    pub msp: MspEv,
    pub item: Box<dyn BinningggContainerEventsDyn>,
}

impl Stream for LspFwdMspSerialOverClusters {
    type Item = Sitemty2<LspFwdMspSerialOverClustersItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let self2 = self.as_mut().get_mut();
            break match &mut self2.state {
                State2::Run => {
                    if let Some((tag, ks, stream, range_final)) = self2.active.as_mut() {
                        match Pin::new(stream).poll_next(cx) {
                            Ready(Some(Ok(item))) => match item {
                                StreamItem::DataItem(RangeCompletableItem::Data((msp, evs))) => {
                                    let wrapped = LspFwdMspSerialOverClustersItem {
                                        cl: tag.clone(),
                                        ks: ks.clone(),
                                        msp,
                                        item: evs,
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
                            Ready(Some(Err(e))) => {
                                self2.state = State2::Done;
                                Ready(Some(Err(e)))
                            }
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
                    } else if let Some(fut) = self2.act1.as_mut() {
                        match fut.poll_unpin(cx) {
                            Ready((x, (cl, ks))) => {
                                self2.act1 = None;
                                match x {
                                    Ok(x) => {
                                        let scyopts = self2.scyopts.clone();
                                        let msps = x.into_iter().map(|x| MspEv::from(x)).collect();
                                        let stream = LspFwdMspSerial::new(
                                            ks.clone(),
                                            self2.series_info.clone(),
                                            self2.range.clone(),
                                            (*cl).clone(),
                                            scyopts,
                                            msps,
                                            self2.msp_limit,
                                            self2.lsp_limit,
                                            self2.lsp_single_buf_max,
                                        );
                                        self2.active = Some((cl.tag().into(), ks, stream, false));
                                        continue;
                                    }
                                    Err(e) => {
                                        self2.state = State2::Done;
                                        Ready(Some(Err(e.into())))
                                    }
                                }
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
                                    self2.scyopts.clone(),
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
