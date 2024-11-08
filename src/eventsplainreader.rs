use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use crate::timebin::cached::reader::EventsReadProvider;
use crate::timebin::cached::reader::EventsReading;
use crate::timebin::CacheReadProvider;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_err_from_string;
use items_0::streamitem::Sitemty;
use items_2::channelevents::ChannelEvents;
use netpod::ReqCtx;
use query::api4::events::EventsSubQuery;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "EventsPlainReader")]
pub enum Error {
    Timebinned(#[from] crate::timebinnedjson::Error),
}

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
            netpod::range::evrange::SeriesRange::PulseRange(_) => panic!("not available for pulse range"),
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

impl CacheReadProvider for DummyCacheReadProvider {
    fn read(
        &self,
        series: u64,
        bin_len: netpod::DtMs,
        msp: u64,
        offs: std::ops::Range<u32>,
    ) -> crate::timebin::cached::reader::CacheReading {
        let stream = futures_util::future::ready(Ok(None));
        crate::timebin::cached::reader::CacheReading::new(Box::pin(stream))
    }

    fn write(&self, series: u64, bins: items_0::timebin::BinsBoxed) -> crate::timebin::cached::reader::CacheWriting {
        let fut = futures_util::future::ready(Ok(()));
        crate::timebin::cached::reader::CacheWriting::new(Box::pin(fut))
    }
}
