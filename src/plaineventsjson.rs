use crate::collect::Collect;
use crate::collect::CollectResult;
use crate::firsterr::non_empty;
use crate::firsterr::only_first_err;
use crate::json_stream::events_stream_to_json_stream;
use crate::json_stream::JsonStream;
use crate::plaineventsstream::dyn_events_stream;
use crate::streamtimeout::StreamTimeout2;
use crate::streamtimeout::TimeoutableStream;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use futures_util::StreamExt;
use items_0::collect_s::CollectableDyn;
use items_0::on_sitemty_data;
use items_2::jsonbytes::JsonBytes;
use netpod::log::*;
use netpod::ChannelTypeConfigGen;
use netpod::Cluster;
use netpod::HasTimeout;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;
use serde_json::Value as JsonValue;
use std::time::Duration;
use std::time::Instant;

autoerr::create_error_v1!(
    name(Error, "PlainEventsJson"),
    enum variants {
        Stream(#[from] crate::plaineventsstream::Error),
        Json(#[from] serde_json::Error),
        Collect(#[from] crate::collect::Error),
    },
);

pub async fn plain_events_json(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    _cluster: &Cluster,
    open_bytes: OpenBoxedBytesStreamsBox,
    timeout_provider: Box<dyn StreamTimeout2>,
) -> Result<CollectResult<JsonBytes>, Error> {
    debug!("plain_events_json  evquery {:?}", evq);
    let deadline = Instant::now() + evq.timeout_content_or_default();
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    //let stream = PlainEventStream::new(stream);
    //let stream = EventsToTimeBinnable::new(stream);
    //let stream = TimeBinnableToCollectable::new(stream);
    debug!("plain_events_json  boxed stream created");
    // let stream = Box::pin(stream);
    let collected = Collect::new(
        stream,
        deadline,
        evq.events_max(),
        evq.bytes_max(),
        Some(evq.range().clone()),
        None,
        timeout_provider,
    )
    .await?;
    warn!("plain_events_json  collected  {:?}", collected);
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
    timeout_provider: Box<dyn StreamTimeout2>,
) -> Result<JsonStream, Error> {
    trace!("plain_events_json_stream");
    let stream = dyn_events_stream(evq, ch_conf, ctx, open_bytes).await?;
    let stream =
        events_stream_to_json_stream(stream, evq.timeout_content_or_default(), timeout_provider);
    let stream = non_empty(stream);
    let stream = only_first_err(stream);
    Ok(Box::pin(stream))
}
