use crate::api4::events::dyneventsstream::dyn_events_stream;
use items_2::jsonbytes::JsonBytes;
use netpod::ChannelTypeConfigGen;
use netpod::Cluster;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;
use std::sync::Arc;
use std::time::Instant;
use streams::collect::Collect;
use streams::collect::CollectResult;
use streams::firsterr::non_empty;
use streams::firsterr::only_first_err;
use streams::json_stream::events_stream_to_json_stream;
use streams::json_stream::JsonStream;
use streams::streamtimeout::StreamTimeout2;
use streams::tcprawclient::OpenBoxedBytesStreamsBox;

macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "PlainEventsJson"),
    enum variants {
        Stream(#[from] super::dyneventsstream::Error),
        Collect(#[from] streams::collect::Error),
        Json(#[from] serde_json::Error),
    },
);

pub async fn plain_events_json(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    _cluster: &Cluster,
    open_bytes: OpenBoxedBytesStreamsBox,
    timeout_provider: Arc<dyn StreamTimeout2>,
) -> Result<CollectResult<JsonBytes>, Error> {
    debug!("plain_events_json  evquery {:?}", evq);
    let deadline = Instant::now() + evq.timeout_content_or_default();
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    //let stream = PlainEventStream::new(stream);
    //let stream = EventsToTimeBinnable::new(stream);
    //let stream = TimeBinnableToCollectable::new(stream);
    debug!("plain_events_json  boxed stream created");
    // let stream = Box::pin(stream);
    let collected = Collect::new(stream, deadline, evq.events_max(), evq.bytes_max(), timeout_provider).await?;
    trace!("plain_events_json  collected  {:?}", collected);
    match collected {
        CollectResult::Some(x) => {
            let x = x.into_user_facing_api_type_box();
            let val = x.into_serializable_json();
            let jsval = serde_json::to_string(&val)?;
            debug!("plain_events_json  json serialized");
            Ok(CollectResult::Some(JsonBytes::new(jsval)))
        }
        CollectResult::Empty => {
            debug!("plain_events_json  empty");
            Ok(CollectResult::Empty)
        }
        CollectResult::Timeout => {
            debug!("plain_events_json  timeout");
            Ok(CollectResult::Timeout)
        }
    }
}

pub async fn plain_events_json_stream(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
    timeout_provider: Arc<dyn StreamTimeout2>,
) -> Result<JsonStream, Error> {
    trace!("plain_events_json_stream");
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    let stream = events_stream_to_json_stream(stream, evq.timeout_content_or_default(), timeout_provider);
    let stream = non_empty(stream);
    let stream = only_first_err(stream);
    Ok(Box::pin(stream))
}
