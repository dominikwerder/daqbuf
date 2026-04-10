use crate::events3::SeriesInfo;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::streamitem::LogItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::streamitem::sitem2_data;
use items_0::timebin::BinningggContainerEventsDyn;
use netpod::RangeExcl;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
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

// pub type Item = crate::events3::lspfwd::Item;

type DataItem = Box<dyn BinningggContainerEventsDyn>;

#[derive(Debug)]
pub struct LspFwdMspSingleStream {
    ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    range: ScyllaSeriesRange,
    begexcl: RangeExcl,
    limit: u32,
    fut: Option<FutDbg<crate::events3::lspfwd::Item>>,
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
        scyqu: ScyllaQueueCluster,
    ) -> Self {
        let limit = limit.max(1).min(400);
        Self {
            ks,
            series_info,
            msp,
            range,
            // TODO maybe take already as option?
            begexcl: RangeExcl::None,
            limit,
            fut: None,
            scyqu,
            outbuf: VecDeque::new(),
        }
    }

    fn trigger_done(&mut self) {
        self.range = ScyllaSeriesRange::new(self.range.end(), self.range.end());
    }
}

impl Stream for LspFwdMspSingleStream {
    type Item = Sitemty2<DataItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            if let Some(x) = self.outbuf.pop_front() {
                break Ready(Some(x));
            }
            break match &mut self.fut {
                Some(fut) => match fut.poll_unpin(cx) {
                    Ready(x) => {
                        self.fut = None;
                        match x {
                            Ok(mut v) => {
                                // TODO mut would not be necessary. Could add as_mergeable_dyn_ref
                                if let Some(tsmax) = v.as_mergeable_dyn_mut().ts_max() {
                                    self.range = ScyllaSeriesRange::new(tsmax, self.range.end());
                                    self.begexcl = RangeExcl::Beg;
                                    let item =
                                        LogItem::info(format!("LspFwdMspSingleStream  cont len {n}", n = v.len()));
                                    self.outbuf.push_back(Ok(StreamItem::Log(item)));
                                    Ready(Some(sitem2_data(v)))
                                } else {
                                    self.trigger_done();
                                    continue;
                                }
                            }
                            Err(e) => {
                                self.trigger_done();
                                Ready(Some(Err(e.into())))
                            }
                        }
                    }
                    Pending => Pending,
                },
                None => {
                    if self.range.beg() < self.range.end() {
                        let scyqu = self.scyqu.clone();
                        let ks = self.ks.clone();
                        let series_info = self.series_info.clone();
                        let msp = self.msp;
                        let range = self.range.clone();
                        let begexcl = self.begexcl.clone();
                        let limit = self.limit;
                        let fut =
                            async move { scyqu.read_03_lsp_fwd(ks, series_info, msp, range, begexcl, limit).await };
                        self.fut = Some(fut.box2());
                        continue;
                    } else {
                        Ready(None)
                    }
                }
            };
        }
    }
}
