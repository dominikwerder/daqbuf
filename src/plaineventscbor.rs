use crate::cbor_stream::events_stream_to_cbor_stream;
use crate::cbor_stream::CborStream;
use crate::firsterr::non_empty;
use crate::firsterr::only_first_err;
use crate::plaineventsstream::dyn_events_stream;
use crate::streamtimeout::StreamTimeout2;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use netpod::ChannelTypeConfigGen;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "PlainEventsCbor")]
pub enum Error {
    Stream(#[from] crate::plaineventsstream::Error),
}

pub async fn plain_events_cbor_stream(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
    timeout_provider: Box<dyn StreamTimeout2>,
) -> Result<CborStream, Error> {
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    let stream =
        events_stream_to_cbor_stream(stream, evq.timeout_content_or_default(), timeout_provider);
    let stream = non_empty(stream);
    let stream = only_first_err(stream);
    Ok(Box::pin(stream))
}
