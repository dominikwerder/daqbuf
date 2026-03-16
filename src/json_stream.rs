use crate::streamtimeout::StreamTimeout2;
use crate::streamtimeout::TimeoutableStream;
use crate::ChannelEventsStream;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::apitypes::ToUserFacingApiType;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::jsonbytes::JsonBytes;
use netpod::log::*;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

autoerr::create_error_v1!(
    name(Error, "JsonStream"),
    enum variants {
        Json(#[from] serde_json::Error),
        Msg(String),
    },
);

pub struct ErrMsg<E>(pub E)
where
    E: ToString;

impl<E> From<ErrMsg<E>> for Error
where
    E: ToString,
{
    fn from(value: ErrMsg<E>) -> Self {
        Self::Msg(value.0.to_string())
    }
}

pub type JsonStream = Pin<Box<dyn Stream<Item = Result<JsonBytes, Error>> + Send>>;

pub fn events_stream_to_json_stream(
    stream: ChannelEventsStream,
    ivl: Duration,
    timeout_provider: Arc<dyn StreamTimeout2>,
) -> impl Stream<Item = Result<JsonBytes, Error>> {
    let stream = TimeoutableStream::new(ivl, timeout_provider, stream);
    let stream = stream.map(|x| match x {
        Some(x) => map_events(x),
        None => make_keepalive(),
    });
    stream
}

fn map_events<T>(x: Sitemty<T>) -> Result<JsonBytes, Error>
where
    T: ToUserFacingApiType,
{
    match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(evs) => {
                    let val = evs.into_user_facing_api_type();
                    let val = val.into_serializable_json();
                    let s = serde_json::to_string(&val)?;
                    let item = JsonBytes::new(s);
                    Ok(item)
                }
                RangeCompletableItem::RangeComplete => {
                    let item = serde_json::json!({
                        "rangeFinal": true,
                    });
                    let s = serde_json::to_string(&item)?;
                    let item = JsonBytes::new(s);
                    Ok(item)
                }
            },
            StreamItem::Log(item) => {
                debug!("{item:?}");
                let item = JsonBytes::new(String::new());
                Ok(item)
            }
            StreamItem::Stats(item) => {
                debug!("{item:?}");
                let item = JsonBytes::new(String::new());
                Ok(item)
            }
        },
        Err(e) => {
            let item = serde_json::json!({
                "error": e.to_string(),
            });
            let s = serde_json::to_string(&item)?;
            let item = JsonBytes::new(s);
            Ok(item)
        }
    }
}

fn make_keepalive() -> Result<JsonBytes, Error> {
    let item = serde_json::json!({
        "type": "keepalive",
    });
    let s = serde_json::to_string(&item).unwrap();
    let item = Ok(JsonBytes::new(s));
    item
}
