use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use futures_util::FutureExt;
use items_0::streamitem::StreamItem;
use items_2::binning::container_bins::ContainerBins;
use netpod::DtMs;
use netpod::ttl::RetentionTime;
use query::api4::scyllaopts::ScyllaOptsQuery;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "BinnedRtMspLsps"),
    enum variants {
        FutureComplete,
        ReadJob(#[from] streams::timebin::cached::reader::Error),
    },
);

type Fut =
    Pin<Box<dyn Future<Output = Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error>> + Send>>;

struct FutW(Fut);

impl fmt::Debug for FutW {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("Fut").finish()
    }
}

pub struct BinnedRtMspLsps {
    series: SeriesId,
    rt: RetentionTime,
    msp: MspU32,
    lsps: (LspU32, LspU32),
    binlen: DtMs,
    scylla_opts: ScyllaOptsQuery,
    scyqueue: ScyllaQueue,
    fut: Option<FutW>,
}

impl BinnedRtMspLsps {
    pub fn new(
        series: SeriesId,
        rt: RetentionTime,
        msp: MspU32,
        binlen: DtMs,
        lsps: (LspU32, LspU32),
        scylla_opts: ScyllaOptsQuery,
        scyqueue: ScyllaQueue,
    ) -> Self {
        let mut ret = Self {
            series,
            rt,
            msp,
            lsps,
            binlen,
            scylla_opts,
            scyqueue,
            fut: None,
        };
        ret.fut = Some(FutW(ret.make_fut()));
        ret
    }

    fn make_fut(&mut self) -> Fut {
        let rt = self.rt.clone();
        let series = self.series.id();
        let binlen = self.binlen.clone();
        let msp = self.msp.to_u64();
        let offs = self.lsps.0.to_u32()..self.lsps.1.to_u32();
        let scyqueue = self.scyqueue.clone();
        let scylla_opts = self.scylla_opts.clone();
        let fut = async move {
            scyqueue
                .read_prebinned_f32(rt, series, binlen, msp, offs, scylla_opts)
                .await
        };
        let fut = Box::pin(fut);
        fut
    }
}

impl Future for BinnedRtMspLsps {
    type Output = Result<StreamItem<ContainerBins<f32, f32>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        if let Some(fut) = &mut self.fut {
            match Pin::new(&mut fut.0).poll_unpin(cx) {
                Ready(Ok(bins)) => Ready(Ok(StreamItem::DataItem(bins))),
                Ready(Err(e)) => Ready(Err(Error::ReadJob(e))),
                Pending => Pending,
            }
        } else {
            Ready(Err(Error::FutureComplete))
        }
    }
}
