use crate::log::*;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use crate::timebin::cached::reader::CacheReadProvider;
use crate::timebin::cached::reader::CacheReading;
use crate::timebin::cached::reader::EventsReadProvider;
use crate::timebin::cached::reader::EventsReading;
use crate::timebin::cached::reader::PrebinnedPartitioning;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_err_from_string;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinningggContainerBinsDyn;
use items_2::binning::container_bins::ContainerBins;
use items_2::channelevents::ChannelEvents;
use netpod::DtMs;
use netpod::ReqCtx;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "EventsPlainReader"),
    enum variants {
        Timebinned(#[from] crate::timebinnedjson::Error),
    },
);

type ChEvsBox = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;

enum StreamState {
    Opening(Pin<Box<dyn Future<Output = Result<ChEvsBox, Error>> + Send>>),
    Reading(ChEvsBox),
}

struct InnerStream {
    state: StreamState,
}

impl Stream for InnerStream {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &mut self.state {
                StreamState::Opening(fut) => match fut.poll_unpin(cx) {
                    Ready(Ok(x)) => {
                        self.state = StreamState::Reading(x);
                        continue;
                    }
                    Ready(Err(e)) => Ready(Some(sitem_err_from_string(e))),
                    Pending => Pending,
                },
                StreamState::Reading(fut) => match fut.poll_next_unpin(cx) {
                    Ready(Some(x)) => Ready(Some(x)),
                    Ready(None) => Ready(None),
                    Pending => Pending,
                },
            };
        }
    }
}

pub struct SfDatabufferEventReadProvider {
    ctx: Arc<ReqCtx>,
    open_bytes: OpenBoxedBytesStreamsBox,
}

impl SfDatabufferEventReadProvider {
    pub fn new(ctx: Arc<ReqCtx>, open_bytes: OpenBoxedBytesStreamsBox) -> Self {
        Self { ctx, open_bytes }
    }
}

impl EventsReadProvider for SfDatabufferEventReadProvider {
    fn read(&self, evq: EventsSubQuery) -> EventsReading {
        let range = match evq.range() {
            netpod::range::evrange::SeriesRange::TimeRange(x) => x.clone(),
            netpod::range::evrange::SeriesRange::PulseRange(_) => {
                panic!("not available for pulse range")
            }
        };
        let ctx = self.ctx.clone();
        let open_bytes = self.open_bytes.clone();
        let state = StreamState::Opening(Box::pin(async move {
            let ret = crate::timebinnedjson::timebinnable_stream_sf_databuffer_channelevents(
                range,
                evq.need_one_before_range(),
                evq.ch_conf().clone(),
                evq.transform().clone(),
                evq.settings().clone(),
                evq.log_level().into(),
                ctx,
                open_bytes,
            )
            .await;
            ret.map_err(|e| e.into()).map(|x| Box::pin(x) as _)
        }));
        let stream = InnerStream { state };
        EventsReading::new(Box::pin(stream))
    }
}

pub struct DummyCacheReadProvider {}

impl DummyCacheReadProvider {
    pub fn new() -> Self {
        Self {}
    }
}

// TODO impl
impl CacheReadProvider for DummyCacheReadProvider {
    fn read(
        &self,
        _series: u64,
        _bin_len: netpod::DtMs,
        _msp: u64,
        _offs: std::ops::Range<u32>,
    ) -> crate::timebin::cached::reader::CacheReading {
        let stream = futures_util::future::ready(Ok(None));
        crate::timebin::cached::reader::CacheReading::new(Box::pin(stream))
    }
}

pub fn test_bins_gen_dim0_f32_v00(
    bin_len: DtMs,
    msp: u64,
    offs: Range<u32>,
) -> ContainerBins<f32, f32> {
    trace!("test_bins_gen_dim0_f32_v00");
    let partt = PrebinnedPartitioning::try_from(bin_len).unwrap();
    let mut off = offs.start;
    type T = f32;
    let mut c = ContainerBins::<T, T>::new();
    loop {
        if off >= offs.end {
            break;
        }
        let ts1 = TsNano::from_ns(partt.msp_div().ns() * msp + partt.bin_len().ns() * off as u64);
        let ts2 = ts1.add_dt_nano(partt.bin_len().dt_ns());
        off += 1;
        if (ts1.ns() / 1000000000) % 5 < 2 {
            continue;
        }
        let cnt = 55;
        let min = 42.0;
        let max = 46.0;
        let agg = 44.0;
        let lst = 43.0;
        let fnl = true;
        c.push_back(ts1, ts2, cnt, min, max, agg, lst, fnl);
    }
    c
}

pub struct TestCacheReadProvider {}

impl TestCacheReadProvider {
    pub fn new() -> Self {
        Self {}
    }
}

impl CacheReadProvider for TestCacheReadProvider {
    fn read(&self, series: u64, bin_len: DtMs, msp: u64, offs: Range<u32>) -> CacheReading {
        trace!("TestCacheReadProvider  series {}", series);
        if series == 123 {
            if bin_len == DtMs::from_ms_u64(1000) {
                let bins = test_bins_gen_dim0_f32_v00(bin_len, msp, offs);
                let x: Box<dyn BinningggContainerBinsDyn> = Box::new(bins);
                let fut = futures_util::future::ready(Ok(Some(x)));
                CacheReading::new(Box::pin(fut))
            } else {
                let fut = futures_util::future::ready(Ok(None));
                CacheReading::new(Box::pin(fut))
            }
        } else {
            let fut = futures_util::future::ready(Ok(None));
            CacheReading::new(Box::pin(fut))
        }
    }
}
