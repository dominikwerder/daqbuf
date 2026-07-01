use crate::api4::events::dyneventsstream::dyn_events_stream;
use netpod::ChannelTypeConfigGen;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;
use std::sync::Arc;
use streams::cbor_stream::events_stream_to_cbor_stream;
use streams::cbor_stream::CborStream;
use streams::firsterr::non_empty;
use streams::firsterr::only_first_err;
use streams::streamtimeout::StreamTimeout2;
use streams::tcprawclient::OpenBoxedBytesStreamsBox;

autoerr::create_error_v1!(
    name(Error, "PlainEventsCbor"),
    enum variants {
        Stream(#[from] crate::api4::events::dyneventsstream::Error),
    },
);

pub async fn plain_events_cbor_stream(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
    scyqu: Option<scyllaconn::worker::ScyllaQueue>,
    timeout_provider: Arc<dyn StreamTimeout2>,
) -> Result<CborStream, Error> {
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes, scyqu).await?;
    let stream = streams::logfilter::LogFilter::new(stream, netpod::log::Level::ERROR);
    let stream = events_stream_to_cbor_stream(stream, evq.timeout_content_or_default(), timeout_provider);
    let stream = non_empty(stream);
    let stream = only_first_err(stream);
    Ok(Box::pin(stream))
}
