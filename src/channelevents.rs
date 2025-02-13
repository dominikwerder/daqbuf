use crate::binning::container_events::ContainerEvents;
use crate::binning::container_events::EventValueType;
use crate::framable::FrameType;
use crate::log::*;
use core::ops::Range;
use daqbuf_err as err;
use items_0::apitypes::ToUserFacingApiType;
use items_0::apitypes::UserApiType;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::CollectorDyn;
use items_0::container::ByteEstimate;
use items_0::framable::FrameTypeInnerStatic;
use items_0::isodate::IsoDateTime;
use items_0::merge::DrainIntoDstResult;
use items_0::merge::DrainIntoNewDynResult;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::streamitem::ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID;
use items_0::timebin::BinningggContainerEventsDyn;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Extendable;
use items_0::TypeName;
use items_0::WithLen;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::SystemTime;

// TODO maybe rename to ChannelStatus?
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConnStatus {
    Connect,
    Disconnect,
}

impl ConnStatus {
    pub fn from_ca_ingest_status_kind(k: u32) -> Self {
        match k {
            1 => Self::Connect,
            _ => Self::Disconnect,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnStatusEvent {
    pub ts: TsNano,
    #[serde(with = "humantime_serde")]
    //pub datetime: chrono::DateTime<chrono::Utc>,
    pub datetime: SystemTime,
    pub status: ConnStatus,
}

impl ConnStatusEvent {
    pub fn new(ts: TsNano, status: ConnStatus) -> Self {
        let datetime = SystemTime::UNIX_EPOCH + Duration::from_millis(ts.ms());
        Self {
            ts,
            datetime,
            status,
        }
    }
}

impl ByteEstimate for ConnStatusEvent {
    fn byte_estimate(&self) -> u64 {
        // TODO magic number, but maybe good enough
        32
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChannelStatus {
    Connect,
    Disconnect,
}

impl ChannelStatus {
    pub fn from_ca_ingest_status_kind(k: u32) -> Self {
        match k {
            1 => Self::Connect,
            _ => Self::Disconnect,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelStatusEvents {
    pub tss: VecDeque<u64>,
    pub datetimes: VecDeque<IsoDateTime>,
    pub statuses: VecDeque<ChannelStatus>,
}

impl Empty for ChannelStatusEvents {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            datetimes: VecDeque::new(),
            statuses: VecDeque::new(),
        }
    }
}

impl WithLen for ChannelStatusEvents {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl Extendable for ChannelStatusEvents {
    fn extend_from(&mut self, src: &mut Self) {
        use core::mem::replace;
        let v = replace(&mut src.tss, VecDeque::new());
        self.tss.extend(v.into_iter());
        let v = replace(&mut src.datetimes, VecDeque::new());
        self.datetimes.extend(v.into_iter());
        let v = replace(&mut src.statuses, VecDeque::new());
        self.statuses.extend(v.into_iter());
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChannelStatusEvent {
    pub ts: u64,
    #[serde(with = "humantime_serde")]
    //pub datetime: chrono::DateTime<chrono::Utc>,
    pub datetime: SystemTime,
    pub status: ChannelStatus,
}

impl ChannelStatusEvent {
    pub fn new(ts: u64, status: ChannelStatus) -> Self {
        let datetime = SystemTime::UNIX_EPOCH + Duration::from_millis(ts / 1000000);
        Self {
            ts,
            datetime,
            status,
        }
    }
}

impl ByteEstimate for ChannelStatusEvent {
    fn byte_estimate(&self) -> u64 {
        // TODO magic number, but maybe good enough
        32
    }
}

#[derive(Debug)]
pub enum ChannelEvents {
    Events(Box<dyn BinningggContainerEventsDyn>),
    Status(Option<ConnStatusEvent>),
}

impl ChannelEvents {
    pub fn is_events(&self) -> bool {
        match self {
            ChannelEvents::Events(_) => true,
            ChannelEvents::Status(_) => false,
        }
    }
}

impl<EVT> From<ContainerEvents<EVT>> for ChannelEvents
where
    EVT: EventValueType,
{
    fn from(value: ContainerEvents<EVT>) -> Self {
        Self::Events(Box::new(value))
    }
}

impl TypeName for ChannelEvents {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl FrameTypeInnerStatic for ChannelEvents {
    const FRAME_TYPE_ID: u32 = ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID;
}

impl FrameType for ChannelEvents {
    fn frame_type_id(&self) -> u32 {
        // TODO SubFrId missing, but get rid of the frame type concept anyhow.
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

impl Clone for ChannelEvents {
    fn clone(&self) -> Self {
        match self {
            Self::Events(arg0) => Self::Events(arg0.clone_dyn()),
            Self::Status(arg0) => Self::Status(arg0.clone()),
        }
    }
}

impl AsAnyRef for ChannelEvents {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl AsAnyMut for ChannelEvents {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

mod serde_channel_events {
    use super::ChannelEvents;
    use crate::binning::container_events::ContainerEvents;
    use crate::binning::container_events::PulsedVal;
    use crate::channelevents::ConnStatusEvent;
    use crate::log::*;
    use items_0::subfr::is_container_events;
    use items_0::subfr::is_pulsed_subfr;
    use items_0::subfr::is_vec_subfr;
    use items_0::subfr::subfr_scalar_type;
    use items_0::subfr::SubFrId;
    use items_0::timebin::BinningggContainerEventsDyn;
    use netpod::EnumVariant;
    use serde::de;
    use serde::de::EnumAccess;
    use serde::de::VariantAccess;
    use serde::de::Visitor;
    use serde::ser::SerializeSeq;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
    use std::cell::RefCell;
    use std::fmt;

    macro_rules! trace_serde { ($($arg:expr),*) => ( if true { trace!($($arg),*); }) }

    type C01<T> = ContainerEvents<T>;
    type C02<T> = ContainerEvents<Vec<T>>;
    type C03<T> = ContainerEvents<PulsedVal<T>>;
    type C04<T> = ContainerEvents<PulsedVal<Vec<T>>>;

    fn try_serialize<S, T>(
        v: &dyn BinningggContainerEventsDyn,
        ser: &mut <S as Serializer>::SerializeSeq,
    ) -> Result<(), <S as Serializer>::Error>
    where
        T: Serialize + 'static,
        S: Serializer,
    {
        let s = std::any::type_name::<T>();
        trace_serde!("try_serialize  T {}", s);
        if let Some(x) = v.as_any_ref().downcast_ref::<T>() {
            ser.serialize_element(x)?;
            Ok(())
        } else {
            let s = std::any::type_name::<T>();
            let e = serde::ser::Error::custom(format!("expect a {}", s));
            error!("{}", e);
            Err(e)
        }
    }

    macro_rules! ser_inner_nty {
        ($ser:expr, $cont1:ident, $nty:expr, $val:expr) => {{
            let ser = $ser;
            let nty_id = subfr_scalar_type($nty);
            let v = $val.0;
            type C<T> = $cont1<T>;
            match nty_id {
                u8::SUB => try_serialize::<S, C<u8>>(v, ser),
                u16::SUB => try_serialize::<S, C<u16>>(v, ser),
                u32::SUB => try_serialize::<S, C<u32>>(v, ser),
                u64::SUB => try_serialize::<S, C<u64>>(v, ser),
                i8::SUB => try_serialize::<S, C<i8>>(v, ser),
                i16::SUB => try_serialize::<S, C<i16>>(v, ser),
                i32::SUB => try_serialize::<S, C<i32>>(v, ser),
                i64::SUB => try_serialize::<S, C<i64>>(v, ser),
                f32::SUB => try_serialize::<S, C<f32>>(v, ser),
                f64::SUB => try_serialize::<S, C<f64>>(v, ser),
                bool::SUB => try_serialize::<S, C<bool>>(v, ser),
                String::SUB => try_serialize::<S, C<String>>(v, ser),
                EnumVariant::SUB => try_serialize::<S, C<EnumVariant>>(v, ser),
                _ => {
                    *$val.1.borrow_mut() = 1;
                    let msg = format!("serde  ser  not supported evt id 0x{:x}", nty_id);
                    error!("{}", msg);
                    Err(serde::ser::Error::custom(msg))
                }
            }
        }};
    }

    #[derive(Debug)]
    struct EvRef<'a>(&'a dyn BinningggContainerEventsDyn, RefCell<u8>);

    #[derive(Debug)]
    struct EvBox(Box<dyn BinningggContainerEventsDyn>);

    impl<'a> Serialize for EvRef<'a> {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut ser = ser.serialize_seq(Some(3))?;
            ser.serialize_element(&self.0.serde_id())?;
            ser.serialize_element(&self.0.nty_id())?;
            let nty_id = self.0.nty_id() as u16;
            if is_container_events(self.0.serde_id()) {
                if is_pulsed_subfr(nty_id) {
                    if is_vec_subfr(nty_id) {
                        ser_inner_nty!(&mut ser, C04, nty_id, self)
                    } else {
                        ser_inner_nty!(&mut ser, C03, nty_id, self)
                    }
                } else {
                    if is_vec_subfr(nty_id) {
                        ser_inner_nty!(&mut ser, C02, nty_id, self)
                    } else {
                        ser_inner_nty!(&mut ser, C01, nty_id, self)
                    }
                }
            } else {
                let msg = format!("not supported obj id {}", self.0.serde_id());
                Err(serde::ser::Error::custom(msg))
            }?;
            ser.end()
        }
    }

    struct EvBoxVis;

    impl EvBoxVis {
        fn name() -> &'static str {
            "Events"
        }
    }

    fn get_2nd_or_err<'de, T, A>(seq: &mut A) -> Result<EvBox, A::Error>
    where
        A: de::SeqAccess<'de>,
        T: Deserialize<'de> + BinningggContainerEventsDyn + 'static,
    {
        let s = std::any::type_name::<T>();
        trace_serde!("get_2nd_or_err  T {}", s);
        let obj: T = seq
            .next_element()
            .map_err(|e| {
                error!("get_2nd_or_err  next_element error {}", e);
                e
            })?
            .ok_or_else(|| de::Error::missing_field("[2] obj"))
            .map_err(|e| {
                error!("get_2nd_or_err  error {}", e);
                e
            })?;
        Ok(EvBox(Box::new(obj)))
    }

    macro_rules! de_inner_nty {
        ($seq:expr, $cont1:ident, $nty:expr) => {{
            let seq = $seq;
            let nty = subfr_scalar_type($nty);
            let cc = std::any::type_name::<$cont1<u8>>();
            trace_serde!(
                "EvBoxVis::visit_seq  de_inner_nty  nty 0x{:X}  cont1<FIX u8> {}",
                nty,
                cc
            );
            match nty {
                u8::SUB => get_2nd_or_err::<$cont1<u8>, _>(seq),
                u16::SUB => get_2nd_or_err::<$cont1<u16>, _>(seq),
                u32::SUB => get_2nd_or_err::<$cont1<u32>, _>(seq),
                u64::SUB => get_2nd_or_err::<$cont1<u64>, _>(seq),
                i8::SUB => get_2nd_or_err::<$cont1<i8>, _>(seq),
                i16::SUB => get_2nd_or_err::<$cont1<i16>, _>(seq),
                i32::SUB => get_2nd_or_err::<$cont1<i32>, _>(seq),
                i64::SUB => get_2nd_or_err::<$cont1<i64>, _>(seq),
                f32::SUB => get_2nd_or_err::<$cont1<f32>, _>(seq),
                f64::SUB => get_2nd_or_err::<$cont1<f64>, _>(seq),
                bool::SUB => get_2nd_or_err::<$cont1<bool>, _>(seq),
                String::SUB => get_2nd_or_err::<$cont1<String>, _>(seq),
                EnumVariant::SUB => get_2nd_or_err::<$cont1<EnumVariant>, _>(seq),
                netpod::UnsupEvt::SUB => get_2nd_or_err::<$cont1<netpod::UnsupEvt>, _>(seq),
                _ => {
                    let e = de::Error::custom(&format!("unknown nty 0x{:x}", nty));
                    error!("{}", e);
                    Err(e)
                }
            }
        }};
    }

    impl<'de> Visitor<'de> for EvBoxVis {
        type Value = EvBox;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "{}", Self::name())
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            trace_serde!("EvBoxVis::visit_seq");
            let cty: u32 = seq
                .next_element()?
                .ok_or_else(|| de::Error::missing_field("[0] cty"))?;
            let nty: u16 = seq
                .next_element()?
                .ok_or_else(|| de::Error::missing_field("[1] nty"))?;
            let seq = &mut seq;
            trace_serde!("EvBoxVis::visit_seq  cty 0x{:x}  nty 0x{:x}", cty, nty);
            let ret = if is_container_events(cty) {
                if is_pulsed_subfr(nty) {
                    if is_vec_subfr(nty) {
                        de_inner_nty!(seq, C04, nty)
                    } else {
                        de_inner_nty!(seq, C03, nty)
                    }
                } else {
                    if is_vec_subfr(nty) {
                        de_inner_nty!(seq, C02, nty)
                    } else {
                        de_inner_nty!(seq, C01, nty)
                    }
                }
            } else {
                error!("unsupported serde  cty 0x{:x}  nty 0x{:x}", cty, nty);
                Err(de::Error::custom(&format!("unknown cty 0x{:x}", cty)))
            };
            trace_serde!("EvBoxVis::visit_seq  ret {:?}", ret);
            ret
        }

        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            panic!("EvBoxVis visit_map");
        }
    }

    impl<'de> Deserialize<'de> for EvBox {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(EvBoxVis)
        }
    }

    impl Serialize for ChannelEvents {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let name = ChannelEventsVis::name();
            let vars = ChannelEventsVis::allowed_variants();
            match self {
                ChannelEvents::Events(obj) => {
                    let evref = EvRef(obj.as_ref(), RefCell::new(0));
                    let x = ser.serialize_newtype_variant(name, 0, vars[0], &evref);
                    let j = *evref.1.borrow();
                    if j != 0 {
                        let msg = format!("serialization failed");
                        Err(serde::ser::Error::custom(msg))
                    } else {
                        x
                    }
                }
                ChannelEvents::Status(val) => ser.serialize_newtype_variant(name, 1, vars[1], val),
            }
        }
    }

    enum VarId {
        Events,
        Status,
    }

    struct VarIdVis;

    impl<'de> Visitor<'de> for VarIdVis {
        type Value = VarId;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "variant identifier")
        }

        fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match val {
                0 => Ok(VarId::Events),
                1 => Ok(VarId::Status),
                _ => Err(de::Error::invalid_value(
                    de::Unexpected::Unsigned(val),
                    &"variant index 0..2",
                )),
            }
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let vars = ChannelEventsVis::allowed_variants();
            if val == vars[0] {
                Ok(VarId::Events)
            } else if val == vars[1] {
                Ok(VarId::Status)
            } else {
                Err(de::Error::unknown_variant(
                    val,
                    ChannelEventsVis::allowed_variants(),
                ))
            }
        }
    }

    impl<'de> Deserialize<'de> for VarId {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            de.deserialize_identifier(VarIdVis)
        }
    }

    pub struct ChannelEventsVis;

    impl ChannelEventsVis {
        fn name() -> &'static str {
            "ChannelEvents"
        }

        fn allowed_variants() -> &'static [&'static str] {
            &["Events", "Status"]
        }
    }

    impl<'de> Visitor<'de> for ChannelEventsVis {
        type Value = ChannelEvents;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "{}", Self::name())
        }

        fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
        where
            A: EnumAccess<'de>,
        {
            let (id, var) = data.variant()?;
            match id {
                VarId::Events => {
                    let x: EvBox = var.newtype_variant()?;
                    Ok(Self::Value::Events(x.0))
                }
                VarId::Status => {
                    let x: Option<ConnStatusEvent> = var.newtype_variant()?;
                    Ok(Self::Value::Status(x))
                }
            }
        }
    }

    impl<'de> Deserialize<'de> for ChannelEvents {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            de.deserialize_enum(
                ChannelEventsVis::name(),
                ChannelEventsVis::allowed_variants(),
                ChannelEventsVis,
            )
        }
    }
}

#[cfg(test)]
mod test_channel_events_serde {
    use super::ChannelEvents;
    use crate::binning::container_events::ContainerEvents;
    use crate::channelevents::ConnStatusEvent;
    use crate::framable::Framable;
    use crate::inmem::InMemoryFrame;
    use crate::log::*;
    use bincode::config::FixintEncoding;
    use bincode::config::LittleEndian;
    use bincode::config::RejectTrailing;
    use bincode::config::WithOtherEndian;
    use bincode::config::WithOtherIntEncoding;
    use bincode::config::WithOtherTrailing;
    use bincode::DefaultOptions;
    use items_0::bincode;
    use items_0::streamitem::sitem_data;
    use items_0::streamitem::Sitemty;
    use items_0::timebin::BinningggContainerEventsDyn;
    use items_0::Appendable;
    use items_0::Empty;
    use netpod::TsNano;
    use netpod::UnsupEvt;
    use serde::Deserialize;
    use serde::Serialize;
    use std::time::SystemTime;

    #[test]
    fn channel_events_unsup_evt() {
        let mut evs = ContainerEvents::new();
        evs.push_back(TsNano::from_ns(8), UnsupEvt(4));
        let item = ChannelEvents::from(evs);
        type _A<T> = items_0::streamitem::StreamItem<T>;
        type _B<T> = items_0::streamitem::RangeCompletableItem<T>;
        {
            let item = item.clone();
            let item = items_0::streamitem::RangeCompletableItem::Data(item);
            let x = crate::frame::encode_to_vec(item);
            assert_eq!(x.is_ok(), false);
        }
        let item = sitem_data(item);
        assert_eq!(crate::frame::make_frame_2(&item, 0xcafe).is_ok(), false);
        assert_eq!(item.make_frame_dyn().is_ok(), false);
    }

    #[test]
    fn channel_events() {
        let mut evs = ContainerEvents::new();
        evs.push_back(TsNano::from_ns(8), 3.0f32);
        evs.push_back(TsNano::from_ns(12), 3.2f32);
        let item = ChannelEvents::from(evs);
        let s = serde_json::to_string_pretty(&item).unwrap();
        trace!("{}", s);
        let w: ChannelEvents = serde_json::from_str(&s).unwrap();
        trace!("{:?}", w);
    }

    type OptsTy = WithOtherTrailing<
        WithOtherIntEncoding<WithOtherEndian<DefaultOptions, LittleEndian>, FixintEncoding>,
        RejectTrailing,
    >;

    fn bincode_opts() -> OptsTy {
        use bincode::Options;
        let opts = bincode::DefaultOptions::new()
            .with_little_endian()
            .with_fixint_encoding()
            .reject_trailing_bytes();
        opts
    }

    #[test]
    fn channel_events_postcard() {
        let mut evs = ContainerEvents::<f32>::new();
        evs.push_back(TsNano::from_ns(8), 3.0);
        evs.push_back(TsNano::from_ns(12), 3.2);
        let item = ChannelEvents::from(evs);
        let out = postcard::to_stdvec(&item).unwrap();
        trace!("serialized into {} bytes", out.len());
        let item: ChannelEvents = postcard::from_bytes(&out).unwrap();
        let item = if let ChannelEvents::Events(x) = item {
            x
        } else {
            panic!()
        };
        let item: &ContainerEvents<f32> = item.as_any_ref().downcast_ref().unwrap();
        use items_0::merge::MergeableTy;
        assert_eq!(item.tss_for_testing().len(), 2);
        assert_eq!(item.tss_for_testing()[1], TsNano::from_ns(12));
    }

    // TODO some unresolved issue with bincode
    #[allow(unused)]
    // #[test]
    fn channel_events_bincode() {
        let mut evs = ContainerEvents::<f32>::new();
        evs.push_back(TsNano::from_ns(8), 3.0);
        evs.push_back(TsNano::from_ns(12), 3.2);
        let item = ChannelEvents::from(evs);
        let opts = bincode_opts();
        let mut out = Vec::new();
        let mut ser = bincode::Serializer::new(&mut out, opts);
        item.serialize(&mut ser).unwrap();
        trace!("serialized into {} bytes", out.len());
        let mut de = bincode::Deserializer::from_slice(&out, opts);
        let item = <ChannelEvents as Deserialize>::deserialize(&mut de).unwrap();
        let item = if let ChannelEvents::Events(x) = item {
            x
        } else {
            panic!()
        };
        let item: &ContainerEvents<f32> = item.as_any_ref().downcast_ref().unwrap();
        use items_0::merge::MergeableTy;
        assert_eq!(item.tss_for_testing().len(), 2);
        assert_eq!(item.tss_for_testing()[1], TsNano::from_ns(12));
    }

    #[test]
    fn channel_status_bincode() {
        let mut evs = ContainerEvents::new();
        evs.push_back(TsNano::from_ns(8), 3.0f32);
        evs.push_back(TsNano::from_ns(12), 3.2f32);
        let status = ConnStatusEvent {
            ts: TsNano::from_ns(567),
            datetime: SystemTime::UNIX_EPOCH,
            status: crate::channelevents::ConnStatus::Connect,
        };
        let item = ChannelEvents::Status(Some(status));
        let opts = bincode_opts();
        let mut out = Vec::new();
        let mut ser = bincode::Serializer::new(&mut out, opts);
        item.serialize(&mut ser).unwrap();
        trace!("serialized into {} bytes", out.len());
        let mut de = bincode::Deserializer::from_slice(&out, opts);
        let item = <ChannelEvents as Deserialize>::deserialize(&mut de).unwrap();
        let item = if let ChannelEvents::Status(x) = item {
            x
        } else {
            panic!()
        };
        if let Some(item) = item {
            assert_eq!(item.ts, TsNano::from_ns(567));
        } else {
            panic!()
        }
    }
}

impl PartialEq for ChannelEvents {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Events(l0), Self::Events(r0)) => l0.eq(r0.as_ref()),
            (Self::Status(l0), Self::Status(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl WithLen for ChannelEvents {
    fn len(&self) -> usize {
        match self {
            ChannelEvents::Events(k) => k.as_ref().len(),
            ChannelEvents::Status(k) => match k {
                Some(_) => 1,
                None => 0,
            },
        }
    }
}

impl ByteEstimate for ChannelEvents {
    fn byte_estimate(&self) -> u64 {
        match self {
            ChannelEvents::Events(k) => k.byte_estimate(),
            ChannelEvents::Status(k) => match k {
                Some(k) => k.byte_estimate(),
                None => 0,
            },
        }
    }
}

impl MergeableTy for ChannelEvents {
    fn ts_min(&self) -> Option<TsNano> {
        match self {
            ChannelEvents::Events(k) => k.ts_min(),
            ChannelEvents::Status(k) => match k {
                Some(k) => Some(k.ts),
                None => None,
            },
        }
    }

    fn ts_max(&self) -> Option<TsNano> {
        match self {
            ChannelEvents::Events(k) => k.ts_max(),
            ChannelEvents::Status(k) => match k {
                Some(k) => Some(k.ts),
                None => None,
            },
        }
    }

    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) -> DrainIntoDstResult {
        match self {
            ChannelEvents::Events(k) => match dst {
                ChannelEvents::Events(j) => k.drain_into(j.as_mergeable_dyn_mut(), range),
                ChannelEvents::Status(_) => DrainIntoDstResult::NotCompatible,
            },
            ChannelEvents::Status(k) => match dst {
                ChannelEvents::Events(_) => DrainIntoDstResult::NotCompatible,
                ChannelEvents::Status(j) => match j {
                    Some(_) => DrainIntoDstResult::Partial,
                    None => {
                        if range.len() != 1 {
                            trace!("try to add empty range to status container {:?}", range);
                        }
                        if range.start != 0 {
                            trace!("weird range {:?}", range);
                        }
                        if range.end > 1 {
                            trace!("weird range {:?}", range);
                        }
                        *j = k.take();
                        DrainIntoDstResult::Done
                    }
                },
            },
        }
    }

    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewResult<Self> {
        match self {
            ChannelEvents::Events(k) => match k.drain_into_new(range) {
                DrainIntoNewDynResult::Done(x) => {
                    DrainIntoNewResult::Done(ChannelEvents::Events(x))
                }
                DrainIntoNewDynResult::Partial(x) => {
                    DrainIntoNewResult::Partial(ChannelEvents::Events(x))
                }
                DrainIntoNewDynResult::NotCompatible => DrainIntoNewResult::NotCompatible,
            },
            ChannelEvents::Status(k) => DrainIntoNewResult::Done(ChannelEvents::Status(k.clone())),
        }
    }

    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize> {
        match self {
            ChannelEvents::Events(k) => k.find_lowest_index_gt(ts),
            ChannelEvents::Status(k) => {
                if let Some(k) = k {
                    if k.ts > ts {
                        Some(0)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize> {
        match self {
            ChannelEvents::Events(k) => k.find_lowest_index_ge(ts),
            ChannelEvents::Status(k) => {
                if let Some(k) = k {
                    if k.ts >= ts {
                        Some(0)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize> {
        match self {
            ChannelEvents::Events(k) => k.find_highest_index_lt(ts),
            ChannelEvents::Status(k) => {
                if let Some(k) = k {
                    if k.ts < ts {
                        Some(0)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn tss_for_testing(&self) -> VecDeque<TsNano> {
        match self {
            ChannelEvents::Events(x) => x.tss_for_testing(),
            ChannelEvents::Status(x) => match x {
                Some(x) => [x.ts].into_iter().collect(),
                None => VecDeque::new(),
            },
        }
    }

    fn is_consistent(&self) -> bool {
        match self {
            ChannelEvents::Events(x) => x.is_consistent(),
            ChannelEvents::Status(_) => true,
        }
    }
}

impl CollectableDyn for ChannelEvents {
    fn new_collector(&self) -> Box<dyn CollectorDyn> {
        Box::new(ChannelEventsCollector::new())
    }
}

// TODO remove type
#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelEventsCollectorOutput {}

impl AsAnyRef for ChannelEventsCollectorOutput {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl AsAnyMut for ChannelEventsCollectorOutput {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl TypeName for ChannelEventsCollectorOutput {
    fn type_name(&self) -> String {
        // TODO should not be here
        any::type_name::<Self>().into()
    }
}

impl WithLen for ChannelEventsCollectorOutput {
    fn len(&self) -> usize {
        todo!()
    }
}

impl ToUserFacingApiType for ChannelEventsCollectorOutput {
    fn into_user_facing_api_type(self: Self) -> Box<dyn UserApiType> {
        todo!()
    }

    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType> {
        todo!()
    }
}

impl CollectedDyn for ChannelEventsCollectorOutput {}

#[derive(Debug)]
pub struct ChannelEventsCollector {
    coll: Option<Box<dyn CollectorDyn>>,
    range_complete: bool,
    timed_out: bool,
    tmp_warned_status: bool,
    tmp_error_unknown_type: bool,
}

impl ChannelEventsCollector {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            coll: None,
            range_complete: false,
            timed_out: false,
            tmp_warned_status: false,
            tmp_error_unknown_type: false,
        }
    }
}

impl WithLen for ChannelEventsCollector {
    fn len(&self) -> usize {
        self.coll.as_ref().map_or(0, |x| x.len())
    }
}

impl ByteEstimate for ChannelEventsCollector {
    fn byte_estimate(&self) -> u64 {
        self.coll.as_ref().map_or(0, |x| x.byte_estimate())
    }
}

impl CollectorDyn for ChannelEventsCollector {
    fn ingest(&mut self, item: &mut dyn CollectableDyn) {
        if let Some(item) = item.as_any_mut().downcast_mut::<ChannelEvents>() {
            match item {
                ChannelEvents::Events(item) => {
                    let coll = self.coll.get_or_insert_with(|| item.new_collector());
                    coll.ingest(item.as_collectable_dyn_mut());
                }
                ChannelEvents::Status(_) => {
                    // TODO decide on output format to collect also the connection status events
                    if !self.tmp_warned_status {
                        self.tmp_warned_status = true;
                        warn!("TODO  ChannelEventsCollector  ChannelEvents::Status");
                    }
                }
            }
        } else {
            if !self.tmp_error_unknown_type {
                self.tmp_error_unknown_type = true;
                error!("ChannelEventsCollector::ingest unexpected item {:?}", item);
            }
        }
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self) -> Result<Box<dyn CollectedDyn>, err::Error> {
        match self.coll.as_mut() {
            Some(coll) => {
                if self.range_complete {
                    coll.set_range_complete();
                }
                if self.timed_out {
                    debug!("ChannelEventsCollector  set_timed_out");
                    coll.set_timed_out();
                }
                let res = coll.result()?;
                Ok(res)
            }
            None => {
                let e = err::Error::with_public_msg_no_trace("nothing collected [caa8d2565]");
                error!("{}", e);
                Err(e)
            }
        }
    }
}

impl ToUserFacingApiType for ChannelEvents {
    fn into_user_facing_api_type(self) -> Box<dyn UserApiType> {
        match self {
            ChannelEvents::Events(x) => x.into_user_facing_api_type_box(),
            ChannelEvents::Status(x) => Box::new(items_0::apitypes::EmptyStruct::new()),
        }
    }

    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType> {
        let this = *self;
        this.into_user_facing_api_type()
    }
}
