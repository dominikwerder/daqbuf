use crate::events3::SeriesInfo;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaOptsSubmit;
use crate::worker::ScyllaQueueCluster;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::merge::MergeableTy;
use items_0::streamitem::LogItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::streamitem::sitem2_data;
use items_0::timebin::BinningggContainerEventsDyn;
use netpod::RangeExcl;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use netpod::hpp::HaveProgressPending;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "LspFwdMspSingleStream"),
    enum variants {
        LspFwd(#[from] crate::events3::lspfwd::Error),
    },
);

type DataItem = Box<dyn BinningggContainerEventsDyn>;

#[derive(Debug)]
enum FutSt {
    Run(FutDbg<crate::events3::lspfwd::Item>),
    None,
}

#[derive(Debug)]
pub struct LspFwdMspSingleStream {
    ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    range: ScyllaSeriesRange,
    done: bool,
    begexcl: RangeExcl,
    limit: u32,
    buf_max: usize,
    fut: FutSt,
    scyopts: ScyllaOptsSubmit,
    scyqu: ScyllaQueueCluster,
    outbuf: VecDeque<Sitemty2<DataItem, Error>>,
}

impl LspFwdMspSingleStream {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        range: ScyllaSeriesRange,
        limit: u32,
        buf_max: usize,
        scyopts: ScyllaOptsSubmit,
        scyqu: ScyllaQueueCluster,
    ) -> Self {
        let limit = limit.max(1).min(70312);
        Self {
            ks,
            series_info,
            msp,
            range,
            done: false,
            // TODO maybe take already as option?
            begexcl: RangeExcl::None,
            limit,
            buf_max,
            fut: FutSt::None,
            scyopts,
            scyqu,
            outbuf: VecDeque::new(),
        }
    }

    fn trigger_done(&mut self) {
        self.done = true;
        self.range = ScyllaSeriesRange::new(self.range.end(), self.range.end());
    }

    pub fn poll_to_buffer(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.fut {
                FutSt::Run(fut) => {
                    if self2.outbuf.len() < self2.buf_max {
                        match fut.poll_unpin(cx) {
                            Ready(x) => {
                                hpp.mark_progress();
                                self2.fut = FutSt::None;
                                match x {
                                    Ok(v) => {
                                        if let Some(tsmax) = MergeableTy::ts_max(&v) {
                                            self2.range = ScyllaSeriesRange::new(tsmax, self2.range.end());
                                            self2.begexcl = RangeExcl::Beg;
                                            let item = LogItem::info(format!("cont len {n}", n = v.len()));
                                            self2.outbuf.push_back(Ok(StreamItem::Log(item)));
                                            self2.outbuf.push_back(sitem2_data(v));
                                            break Ready(Some(Ok(())));
                                        } else {
                                            self2.trigger_done();
                                        }
                                    }
                                    Err(e) => {
                                        self2.trigger_done();
                                        break Ready(Some(Err(e.into())));
                                    }
                                }
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    }
                }
                FutSt::None => {
                    if self.done == false {
                        if self.range.beg() < self.range.end() {
                            let scyqu = self.scyqu.clone();
                            let ks = self.ks.clone();
                            let series_info = self.series_info.clone();
                            let msp = self.msp;
                            let range = self.range.clone();
                            let begexcl = self.begexcl.clone();
                            let limit = self.limit;
                            let scyopts = self.scyopts.clone();
                            let fut = async move {
                                scyqu
                                    .read_03_lsp_fwd(ks, series_info, msp, range, begexcl, limit, scyopts)
                                    .await
                            };
                            hpp.mark_progress();
                            self.fut = FutSt::Run(fut.box2());
                        } else {
                        }
                    } else {
                    }
                }
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

    pub fn inp_done(&self) -> bool {
        self.done
    }

    pub fn buflen(&self) -> usize {
        self.outbuf.len()
    }
}

impl Stream for LspFwdMspSingleStream {
    type Item = Sitemty2<DataItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(x) = self.outbuf.pop_front() {
                Ready(Some(x))
            } else {
                match self.as_mut().poll_to_buffer(cx) {
                    Ready(Some(x)) => match x {
                        Ok(()) => {
                            continue;
                        }
                        Err(e) => Ready(Some(Err(e))),
                    },
                    Ready(None) => {
                        if self.outbuf.len() != 0 {
                            continue;
                        } else {
                            Ready(None)
                        }
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
