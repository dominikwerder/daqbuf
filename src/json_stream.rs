use crate::cbor_stream::SitemtyDynEventsStream;
use crate::streamtimeout::StreamTimeout2;
use crate::streamtimeout::TimeoutableStream;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Events;
use items_0::WithLen;
use netpod::log::*;
use std::pin::Pin;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "JsonStream")]
pub enum Error {
    Json(#[from] serde_json::Error),
    Msg(String),
}

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

pub struct JsonBytes(String);

impl JsonBytes {
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn len(&self) -> u32 {
        self.0.len() as _
    }
}

impl WithLen for JsonBytes {
    fn len(&self) -> usize {
        self.len() as usize
    }
}

impl From<JsonBytes> for String {
    fn from(value: JsonBytes) -> Self {
        value.0
    }
}

pub type JsonStream = Pin<Box<dyn Stream<Item = Result<JsonBytes, Error>> + Send>>;

pub fn events_stream_to_json_stream(
    stream: SitemtyDynEventsStream,
    timeout_provider: Box<dyn StreamTimeout2>,
) -> impl Stream<Item = Result<JsonBytes, Error>> {
    let ivl = Duration::from_millis(4000);
    let stream = TimeoutableStream::new(ivl, timeout_provider, stream);
    let stream = stream.map(|x| match x {
        Some(x) => map_events(x),
        None => make_keepalive(),
    });
    let prepend = {
        let item = make_keepalive();
        futures_util::stream::iter([item])
    };
    prepend.chain(stream)
}

fn map_events(x: Sitemty<Box<dyn Events>>) -> Result<JsonBytes, Error> {
    match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(evs) => {
                    let mut k = evs;
                    let evs = if let Some(j) = k.as_any_mut().downcast_mut::<items_2::channelevents::ChannelEvents>() {
                        use items_0::AsAnyMut;
                        match j {
                            items_2::channelevents::ChannelEvents::Events(m) => {
                                if let Some(g) = m
                                    .as_any_mut()
                                    .downcast_mut::<items_2::eventsdim0::EventsDim0<netpod::EnumVariant>>()
                                {
                                    trace!("consider container EnumVariant");
                                    let mut out = items_2::eventsdim0enum::EventsDim0Enum::new();
                                    for (&ts, val) in g.tss.iter().zip(g.values.iter()) {
                                        out.push_back(ts, val.ix(), val.name_string());
                                    }
                                    Box::new(items_2::channelevents::ChannelEvents::Events(Box::new(out)))
                                } else {
                                    trace!("consider container channel events other events  {}", k.type_name());
                                    k
                                }
                            }
                            items_2::channelevents::ChannelEvents::Status(_) => {
                                trace!("consider container channel events status  {}", k.type_name());
                                k
                            }
                        }
                    } else {
                        trace!("consider container else  {}", k.type_name());
                        k
                    };
                    let s = evs.to_json_string();
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
