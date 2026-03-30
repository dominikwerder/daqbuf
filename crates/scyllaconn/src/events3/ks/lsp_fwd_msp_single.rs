use crate::events3::SeriesInfo;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use futures_util::FutureExt;
use futures_util::Stream;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub type Item = crate::events3::lspfwd::Item;

pub struct LspFwdMspSingleStream {
    ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    range: ScyllaSeriesRange,
    limit: u32,
    fut: Option<Pin<Box<dyn Future<Output = Item> + Send>>>,
    scyqu: ScyllaQueueCluster,
}

impl fmt::Debug for LspFwdMspSingleStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LspFwdMspSingleStream")
            .field("ks", &self.ks)
            .field("series_info", &self.series_info)
            .field("msp", &self.msp)
            .field("range", &self.range)
            .field("limit", &self.limit)
            .finish()
    }
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
            limit,
            fut: None,
            scyqu,
        }
    }

    fn trigger_done(&mut self) {
        self.range = ScyllaSeriesRange::new(self.range.end(), self.range.end());
    }
}

impl Stream for LspFwdMspSingleStream {
    type Item = Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &mut self.fut {
                Some(fut) => match fut.poll_unpin(cx) {
                    Ready(x) => {
                        self.fut = None;
                        match x {
                            Ok(mut v) => {
                                // TODO mut would not be necessary. Could add as_mergeable_dyn_ref
                                if let Some(tsmax) = v.as_mergeable_dyn_mut().ts_max() {
                                    self.range = ScyllaSeriesRange::new(tsmax.add_ns(1), self.range.end());
                                    Ready(Some(Ok(v)))
                                } else {
                                    self.trigger_done();
                                    continue;
                                }
                            }
                            Err(e) => {
                                self.trigger_done();
                                Ready(Some(Err(e)))
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
                        let limit = self.limit;
                        let fut = async move { scyqu.read_03_lsp_fwd(ks, series_info, msp, range, limit).await };
                        self.fut = Some(Box::pin(fut));
                        continue;
                    } else {
                        Ready(None)
                    }
                }
            };
        }
    }
}
