use crate::plaineventsstream::ChannelEventsStream;
use crate::streamtimeout::StreamTimeout2;
use crate::streamtimeout::TimeoutableStream;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::ToCborValue;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::sitem_err_from_string;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Events;
use items_0::WithLen;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use items_2::eventsdim1::EventsDim1;
use items_2::jsonbytes::CborBytes;
use netpod::log::Level;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use std::io::Cursor;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

const FRAME_HEAD_LEN: usize = 16;
const FRAME_PAYLOAD_MAX: u32 = 1024 * 1024 * 80;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "CborStream")]
pub enum Error {
    FromSlice(#[from] std::array::TryFromSliceError),
    Msg(String),
    Ciborium(#[from] ciborium::de::Error<std::io::Error>),
    CiboriumValue(#[from] ciborium::value::Error),
}

struct ErrMsg<E>(E)
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

pub type CborStream = Pin<Box<dyn Stream<Item = Result<CborBytes, Error>> + Send>>;

pub fn events_stream_to_cbor_stream(
    stream: ChannelEventsStream,
    ivl: Duration,
    timeout_provider: Box<dyn StreamTimeout2>,
) -> impl Stream<Item = Result<CborBytes, Error>> {
    let stream = TimeoutableStream::new(ivl, timeout_provider, stream);
    let stream = stream.map(|x| match x {
        Some(x) => map_events(x),
        None => make_keepalive(),
    });
    stream
}

fn map_events(x: Sitemty<ChannelEvents>) -> Result<CborBytes, Error> {
    match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(evs) => {
                    use items_0::apitypes::ToUserFacingApiType;
                    let val = evs.to_user_facing_api_type();
                    let val = val.to_cbor_value()?;
                    let mut buf = Vec::with_capacity(64);
                    ciborium::into_writer(&val, &mut buf).map_err(|e| Error::Msg(e.to_string()))?;
                    let bytes = Bytes::from(buf);
                    let item = CborBytes::new(bytes);
                    Ok(item)
                }
                RangeCompletableItem::RangeComplete => {
                    use ciborium::cbor;
                    let val = cbor!({
                        "rangeFinal" => true,
                    })
                    .map_err(|e| Error::Msg(e.to_string()))?;
                    let mut buf = Vec::with_capacity(64);
                    ciborium::into_writer(&val, &mut buf).map_err(|e| Error::Msg(e.to_string()))?;
                    let bytes = Bytes::from(buf);
                    let item = CborBytes::new(bytes);
                    Ok(item)
                }
            },
            StreamItem::Log(item) => {
                info!("{item:?}");
                let item = CborBytes::new(Bytes::new());
                Ok(item)
            }
            StreamItem::Stats(item) => {
                info!("{item:?}");
                let item = CborBytes::new(Bytes::new());
                Ok(item)
            }
        },
        Err(e) => {
            use ciborium::cbor;
            let item = cbor!({
                "error" => e.to_string(),
            })
            .map_err(|e| Error::Msg(e.to_string()))?;
            let mut buf = Vec::with_capacity(64);
            ciborium::into_writer(&item, &mut buf).map_err(|e| Error::Msg(e.to_string()))?;
            let bytes = Bytes::from(buf);
            let item = CborBytes::new(bytes);
            Ok(item)
        }
    }
}

fn map_events_2(x: Sitemty<ChannelEvents>) -> Result<CborBytes, Error> {
    match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(evs) => {
                    let val = evs.to_cbor_value()?;
                    let mut buf = Vec::with_capacity(64);
                    ciborium::into_writer(&val, &mut buf).map_err(|e| Error::Msg(e.to_string()))?;
                    let bytes = Bytes::from(buf);
                    let item = CborBytes::new(bytes);
                    Ok(item)
                }
                RangeCompletableItem::RangeComplete => {
                    use ciborium::cbor;
                    let val = cbor!({
                        "rangeFinal" => true,
                    })
                    .map_err(|e| Error::Msg(e.to_string()))?;
                    let mut buf = Vec::with_capacity(64);
                    ciborium::into_writer(&val, &mut buf).map_err(|e| Error::Msg(e.to_string()))?;
                    let bytes = Bytes::from(buf);
                    let item = CborBytes::new(bytes);
                    Ok(item)
                }
            },
            StreamItem::Log(item) => {
                info!("{item:?}");
                let item = CborBytes::new(Bytes::new());
                Ok(item)
            }
            StreamItem::Stats(item) => {
                info!("{item:?}");
                let item = CborBytes::new(Bytes::new());
                Ok(item)
            }
        },
        Err(e) => {
            use ciborium::cbor;
            let item = cbor!({
                "error" => e.to_string(),
            })
            .map_err(|e| Error::Msg(e.to_string()))?;
            let mut buf = Vec::with_capacity(64);
            ciborium::into_writer(&item, &mut buf).map_err(|e| Error::Msg(e.to_string()))?;
            let bytes = Bytes::from(buf);
            let item = CborBytes::new(bytes);
            Ok(item)
        }
    }
}

fn make_keepalive() -> Result<CborBytes, Error> {
    use ciborium::cbor;
    let item = cbor!({
        "type" => "keepalive",
    })
    .map_err(ErrMsg)?;
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(&item, &mut buf).map_err(ErrMsg)?;
    let bytes = Bytes::from(buf);
    let item = Ok(CborBytes::new(bytes));
    item
}

pub struct FramedBytesToChannelEventsStream<S> {
    inp: S,
    scalar_type: ScalarType,
    shape: Shape,
    buf: BytesMut,
}

impl<S> FramedBytesToChannelEventsStream<S> {
    pub fn new(inp: S, scalar_type: ScalarType, shape: Shape) -> Self {
        Self {
            inp,
            scalar_type,
            shape,
            buf: BytesMut::with_capacity(1024 * 256),
        }
    }

    fn try_parse(&mut self) -> Result<Option<Sitemty<ChannelEvents>>, Error> {
        // debug!("try_parse {}", self.buf.len());
        if self.buf.len() < FRAME_HEAD_LEN {
            return Ok(None);
        }
        let n = u32::from_le_bytes(self.buf[..4].try_into()?);
        if n > FRAME_PAYLOAD_MAX {
            let e = ErrMsg(format!("frame too large {n}")).into();
            error!("{e}");
            return Err(e);
        }
        let frame_len = FRAME_HEAD_LEN + n as usize;
        let adv = (frame_len + 7) / 8 * 8;
        assert!(adv % 8 == 0);
        assert!(adv >= frame_len);
        assert!(adv < 8 + frame_len);
        if self.buf.len() < adv {
            // debug!("not enough  {}  {}", n, self.buf.len());
            return Ok(None);
        }
        let buf = &self.buf[FRAME_HEAD_LEN..frame_len];
        let val: ciborium::Value =
            ciborium::from_reader(std::io::Cursor::new(buf)).map_err(ErrMsg)?;
        debug!("decoded ciborium value {val:?}");
        let item = if let Some(map) = val.as_map() {
            let keys: Vec<&str> = map
                .iter()
                .map(|k| k.0.as_text().unwrap_or("(none)"))
                .collect();
            debug!("keys {keys:?}");
            if let Some(x) = map.get(0) {
                if let Some(y) = x.0.as_text() {
                    if y == "rangeFinal" {
                        if let Some(y) = x.1.as_bool() {
                            if y {
                                Some(StreamItem::DataItem(
                                    RangeCompletableItem::<Box<dyn Events>>::RangeComplete,
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        let item = None;
        let item = if let Some(x) = item {
            Some(x)
        } else {
            let item = decode_cbor_to_box_events(buf, &self.scalar_type, &self.shape)?;
            debug!("decoded boxed events  len {}", item.len());
            Some(StreamItem::DataItem(RangeCompletableItem::Data(item)))
        };
        self.buf.advance(adv);
        if let Some(x) = item {
            Ok(Some(Ok(x)))
        } else {
            let item = LogItem::from_node(0, Level::DEBUG, format!("decoded ciborium Value"));
            Ok(Some(Ok(StreamItem::Log(item))))
        }
    }
}

impl<S, E> Stream for FramedBytesToChannelEventsStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::error::Error,
{
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.try_parse() {
                Ok(Some(x)) => Ready(Some(x.map_err(|e| sitem_err2_from_string(e)))),
                Ok(None) => match self.inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => match x {
                        Ok(x) => {
                            self.buf.put_slice(&x);
                            continue;
                        }
                        Err(e) => Ready(Some(sitem_err_from_string(e))),
                    },
                    Ready(None) => {
                        if self.buf.len() > 0 {
                            warn!(
                                "remaining bytes in input buffer, input closed  len {}",
                                self.buf.len()
                            );
                        }
                        Ready(None)
                    }
                    Pending => Pending,
                },
                Err(e) => Ready(Some(sitem_err_from_string(e))),
            };
        }
    }
}

macro_rules! cbor_scalar {
    ($ty:ident, $buf:expr) => {{
        type T = $ty;
        type C = EventsDim0<T>;
        let item: C = ciborium::from_reader(Cursor::new($buf))?;
        Box::new(item)
    }};
}

macro_rules! cbor_wave {
    ($ty:ident, $buf:expr) => {{
        type T = $ty;
        type C = EventsDim1<T>;
        let item: C = ciborium::from_reader(Cursor::new($buf))?;
        Box::new(item)
    }};
}

fn decode_cbor_to_box_events(
    buf: &[u8],
    scalar_type: &ScalarType,
    shape: &Shape,
) -> Result<ChannelEvents, Error> {
    let item: Box<dyn Events> = match shape {
        Shape::Scalar => match scalar_type {
            ScalarType::U8 => cbor_scalar!(u8, buf),
            ScalarType::U16 => cbor_scalar!(u16, buf),
            ScalarType::U32 => cbor_scalar!(u32, buf),
            ScalarType::U64 => cbor_scalar!(u64, buf),
            ScalarType::I8 => cbor_scalar!(i8, buf),
            ScalarType::I16 => cbor_scalar!(i16, buf),
            ScalarType::I32 => cbor_scalar!(i32, buf),
            ScalarType::I64 => cbor_scalar!(i64, buf),
            ScalarType::F32 => cbor_scalar!(f32, buf),
            ScalarType::F64 => cbor_scalar!(f64, buf),
            _ => {
                return Err(ErrMsg(format!(
                    "decode_cbor_to_box_events  {:?}  {:?}",
                    scalar_type, shape
                ))
                .into())
            }
        },
        Shape::Wave(_) => match scalar_type {
            ScalarType::U8 => cbor_wave!(u8, buf),
            ScalarType::U16 => cbor_wave!(u16, buf),
            ScalarType::I64 => cbor_wave!(i64, buf),
            _ => {
                return Err(ErrMsg(format!(
                    "decode_cbor_to_box_events  {:?}  {:?}",
                    scalar_type, shape
                ))
                .into())
            }
        },
        Shape::Image(_, _) => todo!(),
    };
    // Ok(item);
    todo!()
}
