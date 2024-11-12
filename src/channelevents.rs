use crate::framable::FrameType;
use crate::merger::Mergeable;
use crate::Events;
use daqbuf_err as err;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::CollectorDyn;
use items_0::container::ByteEstimate;
use items_0::framable::FrameTypeInnerStatic;
use items_0::isodate::IsoDateTime;
use items_0::streamitem::ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::EventsNonObj;
use items_0::Extendable;
use items_0::MergeError;
use items_0::TypeName;
use items_0::WithLen;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRangeEnum;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::SystemTime;

macro_rules! trace_ingest { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

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
    pub ts: u64,
    #[serde(with = "humantime_serde")]
    //pub datetime: chrono::DateTime<chrono::Utc>,
    pub datetime: SystemTime,
    pub status: ConnStatus,
}

impl ConnStatusEvent {
    pub fn new(ts: u64, status: ConnStatus) -> Self {
        let datetime = SystemTime::UNIX_EPOCH + Duration::from_millis(ts / 1000000);
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

/// Events on a channel consist not only of e.g. timestamped values, but can be also
/// connection status changes.
#[derive(Debug)]
pub enum ChannelEvents {
    Events(Box<dyn Events>),
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
    use super::Events;
    use crate::channelevents::ConnStatusEvent;
    use crate::eventsdim0::EventsDim0;
    use crate::eventsdim1::EventsDim1;
    use items_0::subfr::SubFrId;
    use netpod::log::*;
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
    use std::fmt;

    struct EvRef<'a>(&'a dyn Events);

    struct EvBox(Box<dyn Events>);

    impl<'a> Serialize for EvRef<'a> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut ser = serializer.serialize_seq(Some(3))?;
            ser.serialize_element(self.0.serde_id())?;
            ser.serialize_element(&self.0.nty_id())?;
            ser.serialize_element(self.0)?;
            ser.end()
        }
    }

    struct EvBoxVis;

    impl EvBoxVis {
        fn name() -> &'static str {
            "Events"
        }
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
            let cty: &str = seq
                .next_element()?
                .ok_or_else(|| de::Error::missing_field("[0] cty"))?;
            let nty: u32 = seq
                .next_element()?
                .ok_or_else(|| de::Error::missing_field("[1] nty"))?;
            if cty == EventsDim0::<u8>::serde_id() {
                match nty {
                    u8::SUB => {
                        let obj: EventsDim0<u8> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u16::SUB => {
                        let obj: EventsDim0<u16> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u32::SUB => {
                        let obj: EventsDim0<u32> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u64::SUB => {
                        let obj: EventsDim0<u64> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i8::SUB => {
                        let obj: EventsDim0<i8> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i16::SUB => {
                        let obj: EventsDim0<i16> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i32::SUB => {
                        let obj: EventsDim0<i32> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i64::SUB => {
                        let obj: EventsDim0<i64> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    f32::SUB => {
                        let obj: EventsDim0<f32> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    f64::SUB => {
                        let obj: EventsDim0<f64> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    bool::SUB => {
                        let obj: EventsDim0<bool> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    String::SUB => {
                        let obj: EventsDim0<String> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    EnumVariant::SUB => {
                        let obj: EventsDim0<EnumVariant> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    _ => {
                        error!("TODO serde  cty {cty}  nty {nty}");
                        Err(de::Error::custom(&format!("unknown nty {nty}")))
                    }
                }
            } else if cty == EventsDim1::<u8>::serde_id() {
                match nty {
                    u8::SUB => {
                        let obj: EventsDim1<u8> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u16::SUB => {
                        let obj: EventsDim1<u16> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u32::SUB => {
                        let obj: EventsDim1<u32> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    u64::SUB => {
                        let obj: EventsDim1<u64> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i8::SUB => {
                        let obj: EventsDim1<i8> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i16::SUB => {
                        let obj: EventsDim1<i16> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i32::SUB => {
                        let obj: EventsDim1<i32> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    i64::SUB => {
                        let obj: EventsDim1<i64> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    f32::SUB => {
                        let obj: EventsDim1<f32> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    f64::SUB => {
                        let obj: EventsDim1<f64> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    bool::SUB => {
                        let obj: EventsDim1<bool> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    String::SUB => {
                        let obj: EventsDim1<String> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::missing_field("[2] obj"))?;
                        Ok(EvBox(Box::new(obj)))
                    }
                    _ => {
                        error!("TODO serde  cty {cty}  nty {nty}");
                        Err(de::Error::custom(&format!("unknown nty {nty}")))
                    }
                }
            } else {
                error!("unsupported serde  cty {cty}  nty {nty}");
                Err(de::Error::custom(&format!("unknown cty {cty}")))
            }
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
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let name = "ChannelEvents";
            let vars = ChannelEventsVis::allowed_variants();
            match self {
                ChannelEvents::Events(obj) => {
                    serializer.serialize_newtype_variant(name, 0, vars[0], &EvRef(obj.as_ref()))
                }
                ChannelEvents::Status(val) => {
                    serializer.serialize_newtype_variant(name, 1, vars[1], val)
                }
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
    use crate::channelevents::ConnStatusEvent;
    use crate::eventsdim0::EventsDim0;
    use bincode::config::FixintEncoding;
    use bincode::config::LittleEndian;
    use bincode::config::RejectTrailing;
    use bincode::config::WithOtherEndian;
    use bincode::config::WithOtherIntEncoding;
    use bincode::config::WithOtherTrailing;
    use bincode::DefaultOptions;
    use items_0::bincode;
    use items_0::Appendable;
    use items_0::Empty;
    use serde::Deserialize;
    use serde::Serialize;
    use std::time::SystemTime;

    #[test]
    fn channel_events() {
        let mut evs = EventsDim0::empty();
        evs.push(8, 2, 3.0f32);
        evs.push(12, 3, 3.2f32);
        let item = ChannelEvents::Events(Box::new(evs));
        let s = serde_json::to_string_pretty(&item).unwrap();
        eprintln!("{s}");
        let w: ChannelEvents = serde_json::from_str(&s).unwrap();
        eprintln!("{w:?}");
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
    fn channel_events_bincode() {
        let mut evs = EventsDim0::empty();
        evs.push(8, 2, 3.0f32);
        evs.push(12, 3, 3.2f32);
        let item = ChannelEvents::Events(Box::new(evs));
        let opts = bincode_opts();
        let mut out = Vec::new();
        let mut ser = bincode::Serializer::new(&mut out, opts);
        item.serialize(&mut ser).unwrap();
        eprintln!("serialized into {} bytes", out.len());
        let mut de = bincode::Deserializer::from_slice(&out, opts);
        let item = <ChannelEvents as Deserialize>::deserialize(&mut de).unwrap();
        let item = if let ChannelEvents::Events(x) = item {
            x
        } else {
            panic!()
        };
        let item: &EventsDim0<f32> = item.as_any_ref().downcast_ref().unwrap();
        assert_eq!(item.tss().len(), 2);
        assert_eq!(item.tss()[1], 12);
    }

    #[test]
    fn channel_status_bincode() {
        let mut evs = EventsDim0::empty();
        evs.push(8, 2, 3.0f32);
        evs.push(12, 3, 3.2f32);
        let status = ConnStatusEvent {
            ts: 567,
            datetime: SystemTime::UNIX_EPOCH,
            status: crate::channelevents::ConnStatus::Connect,
        };
        let item = ChannelEvents::Status(Some(status));
        let opts = bincode_opts();
        let mut out = Vec::new();
        let mut ser = bincode::Serializer::new(&mut out, opts);
        item.serialize(&mut ser).unwrap();
        eprintln!("serialized into {} bytes", out.len());
        let mut de = bincode::Deserializer::from_slice(&out, opts);
        let item = <ChannelEvents as Deserialize>::deserialize(&mut de).unwrap();
        let item = if let ChannelEvents::Status(x) = item {
            x
        } else {
            panic!()
        };
        if let Some(item) = item {
            assert_eq!(item.ts, 567);
        } else {
            panic!()
        }
    }
}

impl PartialEq for ChannelEvents {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Events(l0), Self::Events(r0)) => l0 == r0,
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

impl Mergeable for ChannelEvents {
    fn ts_min(&self) -> Option<u64> {
        match self {
            ChannelEvents::Events(k) => Mergeable::ts_min(k),
            ChannelEvents::Status(k) => match k {
                Some(k) => Some(k.ts),
                None => None,
            },
        }
    }

    fn ts_max(&self) -> Option<u64> {
        match self {
            ChannelEvents::Events(k) => Mergeable::ts_max(k),
            ChannelEvents::Status(k) => match k {
                Some(k) => Some(k.ts),
                None => None,
            },
        }
    }

    fn new_empty(&self) -> Self {
        match self {
            ChannelEvents::Events(k) => ChannelEvents::Events(k.new_empty()),
            ChannelEvents::Status(_) => ChannelEvents::Status(None),
        }
    }

    fn clear(&mut self) {
        match self {
            ChannelEvents::Events(x) => {
                Mergeable::clear(x);
            }
            ChannelEvents::Status(x) => {
                *x = None;
            }
        }
    }

    fn drain_into(&mut self, dst: &mut Self, range: (usize, usize)) -> Result<(), MergeError> {
        match self {
            ChannelEvents::Events(k) => match dst {
                ChannelEvents::Events(j) => k.drain_into(j, range),
                ChannelEvents::Status(_) => Err(MergeError::NotCompatible),
            },
            ChannelEvents::Status(k) => match dst {
                ChannelEvents::Events(_) => Err(MergeError::NotCompatible),
                ChannelEvents::Status(j) => match j {
                    Some(_) => {
                        trace!("drain_into  merger::MergeError::Full");
                        Err(MergeError::Full)
                    }
                    None => {
                        if range.0 > 0 {
                            trace!("weird range {range:?}");
                        }
                        if range.1 > 1 {
                            trace!("weird range {range:?}");
                        }
                        if range.0 == range.1 {
                            trace!("try to add empty range to status container {range:?}");
                        }
                        *j = k.take();
                        Ok(())
                    }
                },
            },
        }
    }

    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize> {
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

    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize> {
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

    fn find_highest_index_lt(&self, ts: u64) -> Option<usize> {
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

    fn tss(&self) -> Vec<netpod::TsMs> {
        Events::tss(self)
            .iter()
            .map(|x| netpod::TsMs::from_ns_u64(*x))
            .collect()
    }
}

impl EventsNonObj for ChannelEvents {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        match *self {
            ChannelEvents::Events(k) => k.into_tss_pulses(),
            ChannelEvents::Status(_) => (VecDeque::new(), VecDeque::new()),
        }
    }
}

impl Events for ChannelEvents {
    fn verify(&self) -> bool {
        match self {
            ChannelEvents::Events(x) => Events::verify(x),
            ChannelEvents::Status(_) => panic!(),
        }
    }

    fn output_info(&self) -> String {
        todo!()
    }

    fn as_collectable_mut(&mut self) -> &mut dyn CollectableDyn {
        todo!()
    }

    fn as_collectable_with_default_ref(&self) -> &dyn CollectableDyn {
        todo!()
    }

    fn as_collectable_with_default_mut(&mut self) -> &mut dyn CollectableDyn {
        todo!()
    }

    fn ts_min(&self) -> Option<u64> {
        todo!()
    }

    fn ts_max(&self) -> Option<u64> {
        todo!()
    }

    fn take_new_events_until_ts(&mut self, _ts_end: u64) -> Box<dyn Events> {
        todo!()
    }

    fn new_empty_evs(&self) -> Box<dyn Events> {
        match self {
            ChannelEvents::Events(x) => Events::new_empty_evs(x),
            ChannelEvents::Status(_) => panic!(),
        }
    }

    fn drain_into_evs(
        &mut self,
        dst: &mut dyn Events,
        range: (usize, usize),
    ) -> Result<(), MergeError> {
        let dst2 = if let Some(x) = dst.as_any_mut().downcast_mut::<Self>() {
            // debug!("unwrapped dst ChannelEvents as well");
            x
        } else {
            panic!("dst is not ChannelEvents");
        };
        match self {
            ChannelEvents::Events(k) => match dst2 {
                ChannelEvents::Events(j) => Events::drain_into_evs(k, j, range),
                ChannelEvents::Status(_) => panic!("dst is not events"),
            },
            ChannelEvents::Status(_) => panic!("self is not events"),
        }
    }

    fn find_lowest_index_gt_evs(&self, _ts: u64) -> Option<usize> {
        todo!()
    }

    fn find_lowest_index_ge_evs(&self, _ts: u64) -> Option<usize> {
        todo!()
    }

    fn find_highest_index_lt_evs(&self, _ts: u64) -> Option<usize> {
        todo!()
    }

    fn clone_dyn(&self) -> Box<dyn Events> {
        todo!()
    }

    fn partial_eq_dyn(&self, _other: &dyn Events) -> bool {
        todo!()
    }

    fn serde_id(&self) -> &'static str {
        todo!()
    }

    fn nty_id(&self) -> u32 {
        todo!()
    }

    fn tss(&self) -> &VecDeque<u64> {
        match self {
            ChannelEvents::Events(x) => Events::tss(x),
            ChannelEvents::Status(_) => panic!(),
        }
    }

    fn pulses(&self) -> &VecDeque<u64> {
        todo!()
    }

    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        match self {
            ChannelEvents::Events(item) => {
                Box::new(ChannelEvents::Events(Events::to_min_max_avg(item)))
            }
            ChannelEvents::Status(item) => Box::new(ChannelEvents::Status(item.take())),
        }
    }

    fn to_json_string(&self) -> String {
        match self {
            ChannelEvents::Events(item) => item.to_json_string(),
            ChannelEvents::Status(_item) => {
                error!("TODO convert status to json");
                String::new()
            }
        }
    }

    fn to_json_vec_u8(&self) -> Vec<u8> {
        match self {
            ChannelEvents::Events(item) => item.to_json_vec_u8(),
            ChannelEvents::Status(_item) => {
                error!("TODO convert status to json");
                Vec::new()
            }
        }
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        match self {
            ChannelEvents::Events(item) => item.to_cbor_vec_u8(),
            ChannelEvents::Status(_item) => {
                error!("TODO convert status to cbor");
                Vec::new()
            }
        }
    }

    fn clear(&mut self) {
        match self {
            ChannelEvents::Events(x) => Events::clear(x.as_mut()),
            ChannelEvents::Status(x) => {
                *x = None;
            }
        }
    }

    fn to_dim0_f32_for_binning(&self) -> Box<dyn Events> {
        use ChannelEvents::*;
        match self {
            Events(x) => x.to_dim0_f32_for_binning(),
            Status(_x) => panic!("ChannelEvents::to_dim0_f32_for_binning"),
        }
    }

    fn to_container_events(&self) -> Box<dyn ::items_0::timebin::BinningggContainerEventsDyn> {
        panic!("should not get used")
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

impl items_0::collect_s::ToJsonResult for ChannelEventsCollectorOutput {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

impl CollectedDyn for ChannelEventsCollectorOutput {}

#[derive(Debug)]
pub struct ChannelEventsCollector {
    coll: Option<Box<dyn CollectorDyn>>,
    range_complete: bool,
    timed_out: bool,
    needs_continue_at: bool,
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
            needs_continue_at: false,
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
                    let coll = self.coll.get_or_insert_with(|| {
                        item.as_ref()
                            .as_collectable_with_default_ref()
                            .new_collector()
                    });
                    coll.ingest(item.as_collectable_with_default_mut());
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

    fn set_continue_at_here(&mut self) {
        self.needs_continue_at = true;
    }

    fn result(
        &mut self,
        range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Box<dyn CollectedDyn>, err::Error> {
        match self.coll.as_mut() {
            Some(coll) => {
                if self.needs_continue_at {
                    debug!("ChannelEventsCollector  set_continue_at_here");
                    coll.set_continue_at_here();
                }
                if self.range_complete {
                    coll.set_range_complete();
                }
                if self.timed_out {
                    debug!("ChannelEventsCollector  set_timed_out");
                    coll.set_timed_out();
                }
                let res = coll.result(range, binrange)?;
                Ok(res)
            }
            None => {
                let e = err::Error::with_public_msg_no_trace("nothing collected [caa8d2565]");
                error!("{e}");
                Err(e)
            }
        }
    }
}
