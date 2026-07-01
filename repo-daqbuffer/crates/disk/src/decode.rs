use daqbuf_err as err;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::scalar_ops::ScalarOps;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::subfr::SubFrId;
use items_0::timebin::BinningggContainerEventsDyn;
use items_0::WithLen;
use items_2::binning::container_events::ContainerEvents;
use items_2::binning::container_events::EventValueType;
use items_2::binning::container_events::PulsedVal;
use items_2::empty::empty_events_pulsed_dyn_ev;
use items_2::eventfull::EventFull;
use netpod::log::*;
use netpod::AggKind;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub trait Endianness: Send + Unpin {
    fn is_big() -> bool;
}

pub struct LittleEndian {}

pub struct BigEndian {}

impl Endianness for LittleEndian {
    fn is_big() -> bool {
        false
    }
}

impl Endianness for BigEndian {
    fn is_big() -> bool {
        true
    }
}

pub enum Endian {
    Little,
    Big,
}

pub trait ScalarValueFromBytes<STY> {
    fn convert(buf: &[u8], endian: Endian) -> Result<STY, Error>;
    fn convert_dim1(buf: &[u8], endian: Endian, n: usize) -> Result<Vec<STY>, Error>;
}

macro_rules! impl_scalar_value_from_bytes {
    ($nty:ident, $nl:expr) => {
        impl ScalarValueFromBytes<$nty> for $nty {
            // Error in data on disk:
            // Can not rely on byte order as stated in the channel config.
            // Endianness in sf-databuffer can be specified for each event.
            fn convert(buf: &[u8], endian: Endian) -> Result<$nty, Error> {
                //$nty::$ec(*arrayref::array_ref![buf, 0, $nl])
                use Endian::*;
                let ret = match endian {
                    Little => $nty::from_le_bytes(buf[..$nl].try_into()?),
                    Big => $nty::from_be_bytes(buf[..$nl].try_into()?),
                };
                Ok(ret)
            }

            fn convert_dim1(buf: &[u8], endian: Endian, n: usize) -> Result<Vec<$nty>, Error> {
                let ret = buf
                    .chunks_exact(n.min($nl))
                    .map(|b2| {
                        use Endian::*;
                        let ret = match endian {
                            Little => $nty::from_le_bytes(b2[..$nl].try_into().unwrap()),
                            Big => $nty::from_be_bytes(b2[..$nl].try_into().unwrap()),
                        };
                        ret
                    })
                    .collect();
                Ok(ret)
            }
        }
    };
}

impl_scalar_value_from_bytes!(u8, 1);
impl_scalar_value_from_bytes!(u16, 2);
impl_scalar_value_from_bytes!(u32, 4);
impl_scalar_value_from_bytes!(u64, 8);
impl_scalar_value_from_bytes!(i8, 1);
impl_scalar_value_from_bytes!(i16, 2);
impl_scalar_value_from_bytes!(i32, 4);
impl_scalar_value_from_bytes!(i64, 8);
impl_scalar_value_from_bytes!(f32, 4);
impl_scalar_value_from_bytes!(f64, 8);

impl ScalarValueFromBytes<String> for String {
    fn convert(buf: &[u8], _endian: Endian) -> Result<String, Error> {
        let s = if buf.len() >= 255 {
            String::from_utf8_lossy(&buf[..255])
        } else {
            String::from_utf8_lossy(buf)
        };
        Ok(s.into())
    }

    fn convert_dim1(buf: &[u8], _endian: Endian, _n: usize) -> Result<Vec<String>, Error> {
        let s = if buf.len() >= 255 {
            String::from_utf8_lossy(&buf[..255])
        } else {
            String::from_utf8_lossy(buf)
        };
        Ok(vec![s.into()])
    }
}

impl ScalarValueFromBytes<bool> for bool {
    fn convert(buf: &[u8], _endian: Endian) -> Result<bool, Error> {
        if buf.len() >= 1 {
            if buf[0] != 0 {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    fn convert_dim1(buf: &[u8], _endian: Endian, n: usize) -> Result<Vec<bool>, Error> {
        let nn = buf.len().min(n);
        Ok(buf.iter().take(nn).map(|&x| x != 0).collect())
    }
}

pub trait ValueFromBytes: Send {
    fn convert(
        &self,
        ts: u64,
        pulse: u64,
        buf: &[u8],
        endian: Endian,
        events: &mut dyn BinningggContainerEventsDyn,
    ) -> Result<(), Error>;
}

pub trait ValueDim0FromBytes {
    fn convert(
        &self,
        ts: u64,
        pulse: u64,
        buf: &[u8],
        endian: Endian,
        events: &mut dyn BinningggContainerEventsDyn,
    ) -> Result<(), Error>;
}

pub trait ValueDim1FromBytes {
    fn convert(
        &self,
        ts: u64,
        pulse: u64,
        buf: &[u8],
        endian: Endian,
        events: &mut dyn BinningggContainerEventsDyn,
    ) -> Result<(), Error>;
}

pub struct ValueDim0FromBytesImpl<STY>
where
    STY: ScalarOps,
{
    _m1: PhantomData<STY>,
}

impl<EVT> ValueDim0FromBytesImpl<EVT>
where
    EVT: EventValueType + SubFrId + ScalarOps + ScalarValueFromBytes<EVT>,
{
    fn boxed() -> Box<dyn ValueFromBytes> {
        Box::new(Self {
            _m1: Default::default(),
        })
    }
}

impl<EVT> ValueDim0FromBytes for ValueDim0FromBytesImpl<EVT>
where
    EVT: EventValueType + SubFrId + ScalarOps + ScalarValueFromBytes<EVT>,
{
    fn convert(
        &self,
        ts: u64,
        pulse: u64,
        buf: &[u8],
        endian: Endian,
        events: &mut dyn BinningggContainerEventsDyn,
    ) -> Result<(), Error> {
        if let Some(evs) = events.as_any_mut().downcast_mut::<ContainerEvents<PulsedVal<EVT>>>() {
            let v = <EVT as ScalarValueFromBytes<EVT>>::convert(buf, endian)?;
            evs.push_back(TsNano::from_ns(ts), PulsedVal(pulse, v));
            Ok(())
        } else {
            let self_name = std::any::type_name::<Self>();
            let events_name = events.type_name();
            error!("unexpected container  Self {}  events {}", self_name, events_name);
            Err(Error::with_msg_no_trace("unexpected container"))
        }
    }
}

impl<EVT> ValueFromBytes for ValueDim0FromBytesImpl<EVT>
where
    EVT: EventValueType + SubFrId + ScalarOps + ScalarValueFromBytes<EVT>,
{
    fn convert(
        &self,
        ts: u64,
        pulse: u64,
        buf: &[u8],
        endian: Endian,
        events: &mut dyn BinningggContainerEventsDyn,
    ) -> Result<(), Error> {
        ValueDim0FromBytes::convert(self, ts, pulse, buf, endian, events)
    }
}

pub struct ValueDim1FromBytesImpl<STY>
where
    STY: ScalarOps,
{
    shape: Shape,
    _m1: PhantomData<STY>,
}

impl<EVT> ValueDim1FromBytesImpl<EVT>
where
    EVT: EventValueType + ScalarOps + ScalarValueFromBytes<EVT>,
    Vec<EVT>: EventValueType + SubFrId,
{
    fn boxed(shape: Shape) -> Box<dyn ValueFromBytes> {
        Box::new(Self {
            shape,
            _m1: Default::default(),
        })
    }
}

impl<EVT> ValueFromBytes for ValueDim1FromBytesImpl<EVT>
where
    EVT: EventValueType + ScalarOps + ScalarValueFromBytes<EVT>,
    Vec<EVT>: EventValueType + SubFrId,
{
    fn convert(
        &self,
        ts: u64,
        pulse: u64,
        buf: &[u8],
        endian: Endian,
        events: &mut dyn BinningggContainerEventsDyn,
    ) -> Result<(), Error> {
        ValueDim1FromBytes::convert(self, ts, pulse, buf, endian, events)
    }
}

impl<EVT> ValueDim1FromBytes for ValueDim1FromBytesImpl<EVT>
where
    EVT: EventValueType + ScalarOps + ScalarValueFromBytes<EVT>,
    Vec<EVT>: EventValueType + SubFrId,
{
    fn convert(
        &self,
        ts: u64,
        pulse: u64,
        buf: &[u8],
        endian: Endian,
        events: &mut dyn BinningggContainerEventsDyn,
    ) -> Result<(), Error> {
        if let Some(evs) = events
            .as_any_mut()
            .downcast_mut::<ContainerEvents<PulsedVal<Vec<EVT>>>>()
        {
            let n = if let Shape::Wave(n) = self.shape {
                n
            } else {
                return Err(Error::with_msg_no_trace("ValueDim1FromBytesImpl bad shape"));
            };
            let v = <EVT as ScalarValueFromBytes<EVT>>::convert_dim1(buf, endian, n as _)?;
            evs.push_back(TsNano::from_ns(ts), PulsedVal(pulse, v));
            Ok(())
        } else {
            let self_name = std::any::type_name::<Self>();
            let events_name = events.type_name();
            error!("unexpected container  Self {}  events {}", self_name, events_name);
            Err(Error::with_msg_no_trace("unexpected container"))
        }
    }
}

fn make_scalar_conv(
    scalar_type: &ScalarType,
    shape: &Shape,
    agg_kind: &AggKind,
) -> Result<Box<dyn ValueFromBytes>, Error> {
    let ret = match agg_kind {
        AggKind::EventBlobs => {
            error!("make_scalar_conv  EventBlobs");
            return Err(Error::with_msg_no_trace("make_scalar_conv  EventBlobs"));
        }
        AggKind::Plain
        | AggKind::DimXBinsN(_)
        | AggKind::DimXBins1
        | AggKind::TimeWeightedScalar
        | AggKind::PulseIdDiff => match shape {
            Shape::Scalar => match scalar_type {
                ScalarType::U8 => ValueDim0FromBytesImpl::<u8>::boxed(),
                ScalarType::U16 => ValueDim0FromBytesImpl::<u16>::boxed(),
                ScalarType::U32 => ValueDim0FromBytesImpl::<u32>::boxed(),
                ScalarType::U64 => ValueDim0FromBytesImpl::<u64>::boxed(),
                ScalarType::I8 => ValueDim0FromBytesImpl::<i8>::boxed(),
                ScalarType::I16 => ValueDim0FromBytesImpl::<i16>::boxed(),
                ScalarType::I32 => ValueDim0FromBytesImpl::<i32>::boxed(),
                ScalarType::I64 => ValueDim0FromBytesImpl::<i64>::boxed(),
                ScalarType::F32 => ValueDim0FromBytesImpl::<f32>::boxed(),
                ScalarType::F64 => ValueDim0FromBytesImpl::<f64>::boxed(),
                ScalarType::BOOL => ValueDim0FromBytesImpl::<bool>::boxed(),
                ScalarType::STRING => ValueDim0FromBytesImpl::<String>::boxed(),
                ScalarType::Enum => ValueDim0FromBytesImpl::<String>::boxed(),
            },
            Shape::Wave(_) => {
                let shape = shape.clone();
                match scalar_type {
                    ScalarType::U8 => ValueDim1FromBytesImpl::<u8>::boxed(shape),
                    ScalarType::U16 => ValueDim1FromBytesImpl::<u16>::boxed(shape),
                    ScalarType::U32 => ValueDim1FromBytesImpl::<u32>::boxed(shape),
                    ScalarType::U64 => ValueDim1FromBytesImpl::<u64>::boxed(shape),
                    ScalarType::I8 => ValueDim1FromBytesImpl::<i8>::boxed(shape),
                    ScalarType::I16 => ValueDim1FromBytesImpl::<i16>::boxed(shape),
                    ScalarType::I32 => ValueDim1FromBytesImpl::<i32>::boxed(shape),
                    ScalarType::I64 => ValueDim1FromBytesImpl::<i64>::boxed(shape),
                    ScalarType::F32 => ValueDim1FromBytesImpl::<f32>::boxed(shape),
                    ScalarType::F64 => ValueDim1FromBytesImpl::<f64>::boxed(shape),
                    ScalarType::BOOL => ValueDim1FromBytesImpl::<bool>::boxed(shape),
                    ScalarType::STRING => ValueDim1FromBytesImpl::<String>::boxed(shape),
                    ScalarType::Enum => ValueDim1FromBytesImpl::<String>::boxed(shape),
                }
            }
            Shape::Image(_, _) => {
                error!("make_scalar_conv  Image");
                return Err(Error::with_msg_no_trace("make_scalar_conv  Image"));
            }
        },
    };
    Ok(ret)
}

pub struct EventsDynStream {
    scalar_type: ScalarType,
    shape: Shape,
    events_full: Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>,
    events_out: Box<dyn BinningggContainerEventsDyn>,
    scalar_conv: Box<dyn ValueFromBytes>,
    emit_threshold: usize,
    done: bool,
    complete: bool,
}

impl EventsDynStream {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(
        scalar_type: ScalarType,
        shape: Shape,
        agg_kind: AggKind,
        events_full: Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>,
    ) -> Result<Self, Error> {
        let st = &scalar_type;
        let sh = &shape;
        warn!("TODO EventsDynStream::new feed through transform");
        // TODO do we need/want the empty item from here?
        let events_out = empty_events_pulsed_dyn_ev(st, sh)?;
        let scalar_conv = make_scalar_conv(st, sh, &agg_kind)?;
        let emit_threshold = match &shape {
            Shape::Scalar => 2048,
            Shape::Wave(_) => 64,
            Shape::Image(_, _) => 1,
        };
        let ret = Self {
            scalar_type,
            shape,
            events_full,
            events_out,
            scalar_conv,
            emit_threshold,
            done: false,
            complete: false,
        };
        Ok(ret)
    }

    fn replace_events_out(&mut self) -> Result<Box<dyn BinningggContainerEventsDyn>, Error> {
        let st = &self.scalar_type;
        let sh = &self.shape;
        // error!("TODO replace_events_out feed through transform");
        // TODO do we need/want the empty item from here?
        let empty = empty_events_pulsed_dyn_ev(st, sh)?;
        let evs = mem::replace(&mut self.events_out, empty);
        Ok(evs)
    }

    fn handle_event_full(&mut self, item: EventFull) -> Result<(), Error> {
        if item.len() >= self.emit_threshold {
            info!("handle_event_full  item len {}", item.len());
        }
        for (i, ((&be, &ts), &pulse)) in item.be.iter().zip(item.tss.iter()).zip(item.pulses.iter()).enumerate() {
            let buf = item
                .data_decompressed(i)
                .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
            let endian = if be { Endian::Big } else { Endian::Little };
            self.scalar_conv
                .convert(ts, pulse, &buf, endian, self.events_out.as_mut())?;
        }
        Ok(())
    }

    fn handle_stream_item(
        &mut self,
        item: StreamItem<RangeCompletableItem<EventFull>>,
    ) -> Result<Option<Sitemty<Box<dyn BinningggContainerEventsDyn>>>, Error> {
        let ret = match item {
            StreamItem::DataItem(item) => match item {
                RangeCompletableItem::RangeComplete => {
                    Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)))
                }
                RangeCompletableItem::Data(item) => match self.handle_event_full(item) {
                    Ok(()) => {
                        // TODO collect stats.
                        if self.events_out.len() >= self.emit_threshold {
                            let evs = self.replace_events_out()?;
                            Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(evs))))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(e)),
                },
            },
            StreamItem::Log(item) => Some(Ok(StreamItem::Log(item))),
            StreamItem::Stats(item) => Some(Ok(StreamItem::Stats(item))),
        };
        Ok(ret)
    }
}

impl Stream for EventsDynStream {
    type Item = Sitemty<Box<dyn BinningggContainerEventsDyn>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("{} poll_next on complete", Self::type_name())
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                match self.events_full.poll_next_unpin(cx) {
                    Ready(Some(Ok(item))) => match self.handle_stream_item(item) {
                        Ok(Some(item)) => Ready(Some(item)),
                        Ok(None) => continue,
                        Err(e) => {
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.done = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        // Produce a last one even if it is empty.
                        match self.replace_events_out() {
                            Ok(item) => {
                                self.done = true;
                                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))
                            }
                            Err(e) => {
                                self.done = true;
                                Ready(Some(Err(e)))
                            }
                        }
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
