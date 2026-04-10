use super::aggregator::AggTimeWeightOutputAvg;
use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorPulsedNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::aggregator::AggregatorVecNumeric;
use super::timeweight::timeweight_events_dyn::BinnedEventsTimeweightDynbox;
use crate::apitypes::ContainerEventsApi;
use crate::log::*;
use crate::offsets::pulse_offs_from_abs;
use core::fmt;
use core::ops::Range;
use items_0::Appendable;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::TypeName;
use items_0::WithLen;
use items_0::apitypes::ToUserFacingApiType;
use items_0::apitypes::UserApiType;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::CollectorDyn;
use items_0::collect_s::CollectorTy;
use items_0::container::ByteEstimate;
use items_0::merge::DrainIntoDstResult;
use items_0::merge::DrainIntoNewDynResult;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableDyn;
use items_0::merge::MergeableTy;
use items_0::subfr::SubFrId;
use items_0::timebin::BinningggContainerEventsDyn;
use items_0::vecpreview::PreviewRange;
use items_0::vecpreview::VecPreview;
use netpod::BinnedRange;
use netpod::EnumVariant;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::cell::RefCell;
use std::collections::VecDeque;

macro_rules! trace_init { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

autoerr::create_error_v1!(
    name(Error, "ValueContainerError"),
    enum variants {
        Logic,
    },
);

autoerr::create_error_v1!(
    name(EventsContainerError, "EventsContainerError"),
    enum variants {
        Unordered,
    },
);

pub trait Container<EVT>:
    fmt::Debug + Send + Unpin + Clone + PreviewRange + Serialize + for<'a> Deserialize<'a>
where
    EVT: EventValueType,
{
    fn new() -> Self;
    fn len(&self) -> usize;
    fn push_back(&mut self, val: EVT);
    fn clear(&mut self);
    fn get_iter_ty_1(&self, pos: usize) -> Option<EVT::IterTy1<'_>>;
    fn iter_ty_1(&self) -> impl Iterator<Item = EVT::IterTy1<'_>>;
    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>);
    fn truncate_front(&mut self, len: usize);
    fn into_user_facing_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)>;
    fn into_user_facing_fields_json(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)>;
    fn byte_estimate(&self) -> u32;
}

pub trait PartialOrdEvtA<EVT> {
    fn cmp_a(&self, other: &EVT) -> Option<std::cmp::Ordering>;
}

pub trait EventValueType: fmt::Debug + Clone + PartialOrd + Send + Unpin + 'static {
    type Container: Container<Self>;
    type AggregatorTimeWeight: AggregatorTimeWeight<Self>;
    type AggTimeWeightOutputAvg: AggTimeWeightOutputAvg;
    type IterTy1<'a>: fmt::Debug + Clone + PartialOrdEvtA<Self> + Into<Self>;
    const SERDE_ID: u32;
    fn to_f32_for_binning_v01(&self) -> f32;
    fn scalar_type_name_string() -> String;
    fn byte_estimate(&self) -> u32;
}

impl<EVT> Container<EVT> for VecDeque<EVT>
where
    EVT: for<'a> EventValueType<IterTy1<'a> = EVT> + Serialize + for<'a> Deserialize<'a>,
{
    fn new() -> Self {
        trace_init!("{} as trait Container ::new", std::any::type_name::<Self>());
        VecDeque::new()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn push_back(&mut self, val: EVT) {
        self.push_back(val);
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn get_iter_ty_1(&self, pos: usize) -> Option<EVT::IterTy1<'_>> {
        self.get(pos).map(|x| x.clone())
    }

    fn iter_ty_1(&self) -> impl Iterator<Item = <EVT as EventValueType>::IterTy1<'_>> {
        self.iter().map(|x| x.clone())
    }

    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) {
        dst.extend(self.drain(range));
    }

    fn truncate_front(&mut self, len: usize) {
        if self.len() > len {
            let n = self.len() - len;
            self.drain(0..n);
        }
    }

    fn into_user_facing_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        vec![("values".into(), Box::new(self))]
    }

    fn into_user_facing_fields_json(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        vec![("values".into(), Box::new(self))]
    }

    fn byte_estimate(&self) -> u32 {
        self.iter().fold(0, |a, x| a + x.byte_estimate())
    }
}

impl Container<String> for VecDeque<String> {
    fn new() -> Self {
        VecDeque::new()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn push_back(&mut self, val: String) {
        self.push_back(val);
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn get_iter_ty_1(&self, pos: usize) -> Option<&str> {
        self.get(pos).map(|x| x.as_str())
    }

    fn iter_ty_1(&self) -> impl Iterator<Item = <String as EventValueType>::IterTy1<'_>> {
        self.iter().map(|x| x.as_str())
    }

    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) {
        dst.extend(self.drain(range))
    }

    fn truncate_front(&mut self, len: usize) {
        if self.len() > len {
            let n = self.len() - len;
            self.drain(0..n);
        }
    }

    fn into_user_facing_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        vec![("values".into(), Box::new(self))]
    }

    fn into_user_facing_fields_json(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        vec![("values".into(), Box::new(self))]
    }

    fn byte_estimate(&self) -> u32 {
        self.iter().fold(0, |a, x| a + x.bytes().len() as u32)
    }
}

macro_rules! impl_event_value_type {
    ($evt:ty, $sctname:expr) => {
        impl EventValueType for $evt {
            type Container = VecDeque<Self>;
            type AggregatorTimeWeight = AggregatorNumeric;
            type AggTimeWeightOutputAvg = f64;
            type IterTy1<'a> = $evt;
            const SERDE_ID: u32 = <$evt as SubFrId>::SUB as _;
            fn to_f32_for_binning_v01(&self) -> f32 {
                *self as _
            }
            fn scalar_type_name_string() -> String {
                $sctname.to_string()
            }
            fn byte_estimate(&self) -> u32 {
                std::mem::size_of::<$evt>() as u32
            }
        }
        impl PartialOrdEvtA<$evt> for $evt {
            fn cmp_a(&self, other: &$evt) -> Option<std::cmp::Ordering> {
                self.partial_cmp(other)
            }
        }
    };
}

impl_event_value_type!(u8, "u8");
impl_event_value_type!(u16, "u16");
impl_event_value_type!(u32, "u32");
impl_event_value_type!(u64, "u64");
impl_event_value_type!(i8, "i8");
impl_event_value_type!(i16, "i16");
impl_event_value_type!(i32, "i32");
impl_event_value_type!(i64, "i64");
// impl_event_value_type!(f32);
// impl_event_value_type!(f64);

impl PartialOrdEvtA<f32> for f32 {
    fn cmp_a(&self, other: &f32) -> Option<std::cmp::Ordering> {
        self.partial_cmp(other)
    }
}

impl PartialOrdEvtA<f64> for f64 {
    fn cmp_a(&self, other: &f64) -> Option<std::cmp::Ordering> {
        self.partial_cmp(other)
    }
}

impl PartialOrdEvtA<bool> for bool {
    fn cmp_a(&self, other: &bool) -> Option<std::cmp::Ordering> {
        self.partial_cmp(other)
    }
}

impl PartialOrdEvtA<String> for &str {
    fn cmp_a(&self, other: &String) -> Option<std::cmp::Ordering> {
        (*self).partial_cmp(other.as_str())
    }
}

impl EventValueType for f32 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f32;
    type IterTy1<'a> = f32;
    const SERDE_ID: u32 = <f32 as SubFrId>::SUB as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        *self as _
    }
    fn scalar_type_name_string() -> String {
        "f32".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        std::mem::size_of::<Self>() as u32
    }
}

impl EventValueType for f64 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = f64;
    const SERDE_ID: u32 = <f64 as SubFrId>::SUB as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        *self as _
    }
    fn scalar_type_name_string() -> String {
        "f64".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        std::mem::size_of::<Self>() as u32
    }
}

impl EventValueType for bool {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = bool;
    const SERDE_ID: u32 = <bool as SubFrId>::SUB as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        f32::from(*self)
    }
    fn scalar_type_name_string() -> String {
        "bool".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        std::mem::size_of::<Self>() as u32
    }
}

impl EventValueType for String {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = &'a str;
    const SERDE_ID: u32 = <String as SubFrId>::SUB as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        self.len() as _
    }
    fn scalar_type_name_string() -> String {
        "string".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        self.bytes().len() as u32
    }
}

macro_rules! impl_event_value_type_vec {
    ($evt:ty, $sctname:expr) => {
        impl EventValueType for Vec<$evt> {
            type Container = VecDeque<Self>;
            type AggregatorTimeWeight = AggregatorVecNumeric;
            type AggTimeWeightOutputAvg = f32;
            type IterTy1<'a> = Vec<$evt>;
            const SERDE_ID: u32 = <Vec<$evt> as SubFrId>::SUB as _;
            // TODO must use a more precise number dependent on actual elements
            fn to_f32_for_binning_v01(&self) -> f32 {
                self.iter().fold(0., |a, x| a + *x as f32)
            }
            fn scalar_type_name_string() -> String {
                $sctname.to_string()
            }
            fn byte_estimate(&self) -> u32 {
                self.iter()
                    .fold(0, |a, x| a + EventValueType::byte_estimate(x))
            }
        }

        impl PartialOrdEvtA<Vec<$evt>> for Vec<$evt> {
            fn cmp_a(&self, other: &Vec<$evt>) -> Option<core::cmp::Ordering> {
                self.partial_cmp(other)
            }
        }
    };
}

impl_event_value_type_vec!(u8, "u8");
impl_event_value_type_vec!(u16, "u16");
impl_event_value_type_vec!(u32, "u32");
impl_event_value_type_vec!(u64, "u64");
impl_event_value_type_vec!(i8, "i8");
impl_event_value_type_vec!(i16, "i16");
impl_event_value_type_vec!(i32, "i32");
impl_event_value_type_vec!(i64, "i64");
impl_event_value_type_vec!(f32, "f32");
impl_event_value_type_vec!(f64, "f64");
// impl_event_value_type_vec!(String);
// impl_event_value_type_vec!(EnumVariant);

impl EventValueType for Vec<bool> {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorVecNumeric;
    type AggTimeWeightOutputAvg = f32;
    type IterTy1<'a> = Vec<bool>;
    const SERDE_ID: u32 = <Vec<bool> as SubFrId>::SUB as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        self.iter().fold(0., |a, x| a + f32::from(*x))
    }
    fn scalar_type_name_string() -> String {
        "bool".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        self.len() as u32 * std::mem::size_of::<Self>() as u32
    }
}

impl PartialOrdEvtA<Vec<bool>> for Vec<bool> {
    fn cmp_a(&self, other: &Vec<bool>) -> Option<core::cmp::Ordering> {
        self.partial_cmp(other)
    }
}

impl EventValueType for Vec<String> {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorVecNumeric;
    type AggTimeWeightOutputAvg = f32;
    type IterTy1<'a> = Vec<String>;
    const SERDE_ID: u32 = <Vec<String> as SubFrId>::SUB as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        self.iter().fold(0., |a, x| a + x.len() as f32)
    }
    fn scalar_type_name_string() -> String {
        "string".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        self.iter()
            .fold(0, |a, x| a + EventValueType::byte_estimate(x))
    }
}

impl PartialOrdEvtA<Vec<String>> for Vec<String> {
    fn cmp_a(&self, other: &Vec<String>) -> Option<core::cmp::Ordering> {
        self.partial_cmp(other)
    }
}

impl EventValueType for Vec<EnumVariant> {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorVecNumeric;
    type AggTimeWeightOutputAvg = f32;
    type IterTy1<'a> = Vec<EnumVariant>;
    const SERDE_ID: u32 = <Vec<EnumVariant> as SubFrId>::SUB as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        self.iter().fold(0., |a, x| a + x.ix() as f32)
    }
    fn scalar_type_name_string() -> String {
        "enum".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        self.iter()
            .fold(0, |a, x| a + EventValueType::byte_estimate(x))
    }
}

impl PartialOrdEvtA<Vec<EnumVariant>> for Vec<EnumVariant> {
    fn cmp_a(&self, other: &Vec<EnumVariant>) -> Option<core::cmp::Ordering> {
        self.partial_cmp(other)
    }
}

#[derive(Debug)]
pub struct PulsedValIterTy<'a, EVT>
where
    EVT: EventValueType,
{
    pulse: u64,
    evt: EVT::IterTy1<'a>,
}

impl<'a, EVT> Clone for PulsedValIterTy<'a, EVT>
where
    EVT: EventValueType + SubFrId,
{
    fn clone(&self) -> Self {
        Self {
            pulse: self.pulse,
            evt: self.evt.clone(),
        }
    }
}

impl<'a, EVT> PartialOrdEvtA<PulsedVal<EVT>> for PulsedValIterTy<'a, EVT>
where
    EVT: EventValueType + SubFrId,
{
    fn cmp_a(&self, other: &PulsedVal<EVT>) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match self.evt.cmp_a(&other.1) {
            Some(Ordering::Less) => Some(Ordering::Less),
            Some(Ordering::Greater) => Some(Ordering::Greater),
            Some(Ordering::Equal) => Some(Ordering::Equal),
            None => None,
        }
    }
}

impl<'a, EVT> From<PulsedValIterTy<'a, EVT>> for PulsedVal<EVT>
where
    EVT: EventValueType,
{
    fn from(value: PulsedValIterTy<'a, EVT>) -> Self {
        Self(value.pulse, value.evt.into())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PulsedVal<EVT>(pub u64, pub EVT)
where
    EVT: EventValueType;

impl<EVT> fmt::Display for PulsedVal<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl<EVT> PartialOrd for PulsedVal<EVT>
where
    EVT: EventValueType,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

mod serde_pulsed_val {
    use super::EventValueType;
    use super::PulsedVal;
    use serde::Deserialize;
    use serde::Deserializer;

    impl<'de, EVT> Deserialize<'de> for PulsedVal<EVT>
    where
        EVT: EventValueType,
    {
        fn deserialize<D>(_de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            todo!("TODO mod serde_pulsed_val")
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VecDequePulsed<EVT>
where
    EVT: EventValueType,
{
    pulses: VecDeque<u64>,
    vals: EVT::Container,
}

impl<EVT> PreviewRange for VecDequePulsed<EVT>
where
    EVT: EventValueType,
{
    fn preview<'a>(&'a self) -> Box<dyn fmt::Debug + 'a> {
        let ret = items_0::vecpreview::PreviewCell {
            a: self.pulses.front(),
            b: self.pulses.back(),
        };
        Box::new(ret)
    }
}

impl<EVT> Container<PulsedVal<EVT>> for VecDequePulsed<EVT>
where
    EVT: EventValueType,
    for<'a> PulsedVal<EVT>: EventValueType<IterTy1<'a> = PulsedValIterTy<'a, EVT>>,
{
    fn new() -> Self {
        Self {
            pulses: VecDeque::new(),
            vals: <<EVT as EventValueType>::Container as Container<EVT>>::new(),
        }
    }

    fn len(&self) -> usize {
        self.vals.len()
    }

    fn push_back(&mut self, val: PulsedVal<EVT>) {
        self.pulses.push_back(val.0);
        self.vals.push_back(val.1);
    }

    fn clear(&mut self) {
        self.pulses.clear();
        self.vals.clear();
    }

    fn get_iter_ty_1(&self, pos: usize) -> Option<<PulsedVal<EVT> as EventValueType>::IterTy1<'_>> {
        if let (Some(&pulse), Some(val)) = (self.pulses.get(pos), self.vals.get_iter_ty_1(pos)) {
            let x = PulsedValIterTy { pulse, evt: val };
            Some(x)
        } else {
            None
        }
    }

    fn iter_ty_1(&self) -> impl Iterator<Item = <PulsedVal<EVT> as EventValueType>::IterTy1<'_>> {
        self.pulses
            .iter()
            .map(|&x| x)
            .zip(self.vals.iter_ty_1())
            .map(|(pulse, evt)| PulsedValIterTy { pulse, evt })
    }

    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) {
        dst.pulses.extend(self.pulses.drain(range.clone()));
        self.vals.drain_into(&mut dst.vals, range.clone());
    }

    fn truncate_front(&mut self, len: usize) {
        if self.len() > len {
            let n = self.len() - len;
            self.pulses.drain(0..n);
            self.vals.truncate_front(len);
        }
    }

    fn into_user_facing_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        vec![
            ("pulses".into(), Box::new(self.pulses)),
            ("values".into(), Box::new(self.vals)),
        ]
    }

    fn into_user_facing_fields_json(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        let (pulses_anch, pulses_offs) = pulse_offs_from_abs(&self.pulses);
        vec![
            ("pulseAnchor".into(), Box::new(pulses_anch)),
            ("pulseOff".into(), Box::new(pulses_offs)),
            ("values".into(), Box::new(self.vals)),
        ]
    }

    fn byte_estimate(&self) -> u32 {
        self.len() as u32 * 8 + self.vals.byte_estimate()
    }
}

impl<EVT> PartialOrdEvtA<PulsedVal<EVT>> for PulsedVal<EVT>
where
    EVT: EventValueType,
{
    fn cmp_a(&self, other: &PulsedVal<EVT>) -> Option<std::cmp::Ordering> {
        self.partial_cmp(other)
    }
}

impl<EVT> EventValueType for PulsedVal<EVT>
where
    EVT: EventValueType + SubFrId,
{
    type Container = VecDequePulsed<EVT>;
    type AggregatorTimeWeight = AggregatorPulsedNumeric<EVT>;
    type AggTimeWeightOutputAvg = EVT::AggTimeWeightOutputAvg;
    type IterTy1<'a> = PulsedValIterTy<'a, EVT>;
    const SERDE_ID: u32 = items_0::subfr::pulsed_subfr(<EVT as SubFrId>::SUB) as _;
    fn to_f32_for_binning_v01(&self) -> f32 {
        self.1.to_f32_for_binning_v01()
    }
    fn scalar_type_name_string() -> String {
        EVT::scalar_type_name_string()
    }
    fn byte_estimate(&self) -> u32 {
        8 + self.1.byte_estimate()
    }
}

#[derive(Debug, Clone)]
pub struct EventSingleRef<'a, EVT>
where
    EVT: EventValueType,
{
    pub ts: TsNano,
    pub val: EVT::IterTy1<'a>,
}

impl<'a, EVT> EventSingleRef<'a, EVT> where EVT: EventValueType {}

#[derive(Debug, Clone)]
pub struct EventSingle<EVT> {
    pub ts: TsNano,
    pub val: EVT,
}

impl<'a, EVT> From<EventSingleRef<'a, EVT>> for EventSingle<EVT>
where
    EVT: EventValueType,
{
    fn from(value: EventSingleRef<'a, EVT>) -> Self {
        Self {
            ts: value.ts,
            val: value.val.into(),
        }
    }
}

impl<'a, EVT> From<&EventSingleRef<'a, EVT>> for EventSingle<EVT>
where
    EVT: EventValueType,
{
    fn from(value: &EventSingleRef<'a, EVT>) -> Self {
        Self {
            ts: value.ts,
            val: value.val.clone().into(),
        }
    }
}

#[derive(Clone)]
pub struct ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    tss: VecDeque<TsNano>,
    vals: <EVT as EventValueType>::Container,
    byte_estimate: RefCell<Option<u32>>,
}

mod container_events_serde {
    use super::ContainerEvents;
    use super::EventValueType;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
    use serde::de::MapAccess;
    use serde::de::SeqAccess;
    use serde::de::Visitor;
    use serde::ser::SerializeStruct;
    use std::cell::RefCell;
    use std::fmt;
    use std::marker::PhantomData;

    macro_rules! trace_serde { ($($arg:tt)*) => ( if false { eprintln!($($arg)*); }) }

    impl<EVT> Serialize for ContainerEvents<EVT>
    where
        EVT: EventValueType,
    {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let stname = std::any::type_name::<Self>();
            let mut st = ser.serialize_struct(stname, 2)?;
            st.serialize_field("tss", &self.tss)?;
            st.serialize_field("vals", &self.vals)?;
            st.end()
        }
    }

    struct Vis<EVT> {
        _t1: PhantomData<EVT>,
    }

    impl<'de, EVT> Visitor<'de> for Vis<EVT>
    where
        EVT: EventValueType,
    {
        type Value = ContainerEvents<EVT>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            fmt.write_str("a struct with fields tss and vals")
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
        where
            S: SeqAccess<'de>,
        {
            trace_serde!("Vis ContainerEvents visit_map");
            let tss = seq
                .next_element()?
                .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
            let vals = seq
                .next_element()?
                .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
            let ret = Self::Value {
                tss,
                vals,
                byte_estimate: RefCell::new(None),
            };
            Ok(ret)
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            trace_serde!("Vis ContainerEvents visit_map");
            let mut tss = None;
            let mut vals = None;
            while let Some(key) = map.next_key::<String>()? {
                match key.as_str() {
                    "tss" => {
                        tss = Some(map.next_value()?);
                    }
                    "vals" => {
                        trace_serde!("Vis ContainerEvents visit_map");
                        vals = Some(map.next_value()?);
                    }
                    _ => {
                        use serde::de::Error;
                        return Err(Error::unknown_field(&key, &["tss", "vals"]));
                    }
                }
            }
            let ret = Self::Value {
                tss: tss.unwrap(),
                vals: vals.unwrap(),
                byte_estimate: RefCell::new(None),
            };
            Ok(ret)
        }
    }

    impl<'de, EVT> Deserialize<'de> for ContainerEvents<EVT>
    where
        EVT: EventValueType,
    {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            trace_serde!("Deserialize for ContainerEvents<EVT>");
            let stname = std::any::type_name::<Self>();
            de.deserialize_struct(stname, &["tss", "vals"], Vis { _t1: PhantomData })
        }
    }
}

impl<EVT> ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    pub fn from_constituents(
        tss: VecDeque<TsNano>,
        vals: <EVT as EventValueType>::Container,
    ) -> Self {
        Self {
            tss,
            vals,
            byte_estimate: RefCell::new(None),
        }
    }

    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            tss: VecDeque::new(),
            vals: Container::new(),
            byte_estimate: RefCell::new(None),
        }
    }

    pub fn len(&self) -> usize {
        self.tss.len()
    }

    pub fn verify(&self) -> Result<(), EventsContainerError> {
        if self
            .tss
            .iter()
            .zip(self.tss.iter().skip(1))
            .any(|(&a, &b)| a > b)
        {
            return Err(EventsContainerError::Unordered);
        }
        Ok(())
    }

    pub fn push_back(&mut self, ts: TsNano, val: EVT) {
        self.tss.push_back(ts);
        self.vals.push_back(val);
        self.byte_estimate = RefCell::new(None);
    }

    pub fn iter_zip<'a>(&'a self) -> impl Iterator<Item = (TsNano, EVT::IterTy1<'a>)> {
        self.tss.iter().map(|&x| x).zip(self.vals.iter_ty_1())
    }

    pub fn serde_id() -> u32 {
        items_0::streamitem::CONTAINER_EVENTS_TYPE_ID
    }

    pub fn clear(&mut self) {
        self.tss.clear();
        self.vals.clear();
        *self.byte_estimate.borrow_mut() = Some(0);
    }

    pub fn truncate_front(&mut self, len: usize) {
        if self.len() > len {
            let n = self.len() - len;
            self.tss.drain(0..n);
            self.vals.truncate_front(len);
        }
    }

    fn byte_estimate_mut(&self) -> u32 {
        if let Some(x) = self.byte_estimate.borrow().clone() {
            x
        } else {
            let byte_est = 8 * self.len() as u32 + self.vals.byte_estimate();
            *self.byte_estimate.borrow_mut() = Some(byte_est);
            byte_est
        }
    }
}

impl<EVT> fmt::Debug for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  {{  len: {:?},  tss: {:?},  vals {:?}  }}",
            self.len(),
            VecPreview::new(&self.tss),
            VecPreview::new(&self.vals),
        )
    }
}

impl<EVT> AsAnyRef for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn as_any_ref(&self) -> &dyn any::Any {
        self
    }
}

impl<EVT> AsAnyMut for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
}

impl<EVT> WithLen for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        self.len()
    }
}

impl<EVT> Empty for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn empty() -> Self {
        ContainerEvents::new()
    }
}

impl<EVT> Appendable<EVT> for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn push(&mut self, ts: TsNano, value: EVT) {
        self.push_back(ts, value);
    }
}

impl<EVT> ByteEstimate for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn byte_estimate(&self) -> u32 {
        self.byte_estimate_mut()
    }
}

pub struct ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    evs: &'a ContainerEvents<EVT>,
    end: usize,
    pos: usize,
    // it: Box<dyn Iterator<Item = (TsNano, EVT::IterTy1<'static>)>>,
}

impl<'a, EVT> ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn new(evs: &'a ContainerEvents<EVT>) -> Self {
        // let it = unsafe { netpod::extltref(evs) }.iter_zip();
        // let it = Box::new(it);
        Self {
            evs,
            end: evs.len(),
            pos: 0,
            // it,
        }
    }

    pub fn constrain_up_to_ts(&mut self, end: TsNano) {
        let tss = &self.evs.tss;
        let pp = tss.partition_point(|&x| x < end);
        let pp = pp.max(self.pos);
        assert!(pp <= tss.len(), "len_before  pp {}  len {}", pp, tss.len());
        assert!(pp >= self.pos);
        if pp != 0 {
            assert!(tss[pp - 1] < end);
        }
        self.end = pp;
    }

    pub fn extend_to_all(&mut self) {
        self.end = self.evs.len();
    }

    pub fn len(&self) -> usize {
        self.end - self.pos
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn ts_first(&self) -> Option<TsNano> {
        if self.pos < self.end {
            self.evs.tss.get(self.pos).cloned()
        } else {
            None
        }
    }

    pub fn next(&mut self) -> Option<EventSingleRef<'_, EVT>> {
        let evs = &self.evs;
        if self.pos < self.end {
            if let (Some(&ts), Some(val)) =
                (evs.tss.get(self.pos), evs.vals.get_iter_ty_1(self.pos))
            {
                self.pos += 1;
                let ev = EventSingleRef { ts, val };
                Some(ev)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<EVT> MergeableTy for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn ts_min(&self) -> Option<TsNano> {
        self.tss.front().copied()
    }

    fn ts_max(&self) -> Option<TsNano> {
        self.tss.back().copied()
    }

    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize> {
        let x = self.tss.partition_point(|&x| x <= ts);
        if x >= self.tss.len() { None } else { Some(x) }
    }

    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize> {
        let x = self.tss.partition_point(|&x| x < ts);
        if x >= self.tss.len() { None } else { Some(x) }
    }

    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize> {
        let x = self.tss.partition_point(|&x| x < ts);
        if x == 0 { None } else { Some(x - 1) }
    }

    fn find_pp_le(&self, ts: TsNano) -> usize {
        self.tss.partition_point(|&x| x <= ts)
    }

    fn tss_for_testing(&self) -> VecDeque<TsNano> {
        self.tss.clone()
    }

    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) -> DrainIntoDstResult {
        dst.tss.extend(self.tss.drain(range.clone()));
        self.vals.drain_into(&mut dst.vals, range);
        DrainIntoDstResult::Done
    }

    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewResult<Self> {
        let mut dst = Self::new();
        MergeableTy::drain_into(self, &mut dst, range);
        DrainIntoNewResult::Done(dst)
    }

    fn is_strict_monotonic(&self) -> bool {
        let mut mono = true;
        let n = self.tss.len();
        for (&ts1, &ts2) in self.tss.iter().zip(self.tss.range(n.min(1)..n)) {
            if ts1 >= ts2 {
                mono = false;
                error!("non-monotonic event data  ts1 {}  ts2 {}", ts1, ts2);
                break;
            }
        }
        mono
    }

    fn is_consistent(&self) -> bool {
        let mut good = true;
        good &= MergeableTy::is_strict_monotonic(self);
        good
    }
}

impl<EVT> MergeableDyn for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn ts_min(&self) -> Option<TsNano> {
        MergeableTy::ts_min(self)
    }

    fn ts_max(&self) -> Option<TsNano> {
        MergeableTy::ts_max(self)
    }

    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize> {
        MergeableTy::find_lowest_index_gt(self, ts)
    }

    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize> {
        MergeableTy::find_lowest_index_ge(self, ts)
    }

    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize> {
        MergeableTy::find_highest_index_lt(self, ts)
    }

    fn find_pp_le(&self, ts: TsNano) -> usize {
        MergeableTy::find_pp_le(self, ts)
    }

    fn tss_for_testing(&self) -> VecDeque<TsNano> {
        MergeableTy::tss_for_testing(self)
    }

    fn drain_into(
        &mut self,
        dst: &mut dyn MergeableDyn,
        range: Range<usize>,
    ) -> DrainIntoDstResult {
        if let Some(dst) = dst.as_any_mut().downcast_mut::<Self>() {
            MergeableTy::drain_into(self, dst, range)
        } else {
            DrainIntoDstResult::NotCompatible
        }
    }

    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewDynResult {
        match MergeableTy::drain_into_new(self, range) {
            DrainIntoNewResult::Done(x) => DrainIntoNewDynResult::Done(Box::new(x)),
            DrainIntoNewResult::Partial(x) => DrainIntoNewDynResult::Partial(Box::new(x)),
            DrainIntoNewResult::NotCompatible => DrainIntoNewDynResult::NotCompatible,
        }
    }

    fn is_strict_monotonic(&self) -> bool {
        MergeableTy::is_strict_monotonic(self)
    }

    fn is_consistent(&self) -> bool {
        MergeableTy::is_consistent(self)
    }
}

impl<EVT> TypeName for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> String {
        std::any::type_name::<Self>().into()
    }
}

#[derive(Debug, Serialize)]
pub struct ContainerEventsCollected<EVT>
where
    EVT: EventValueType,
{
    evs: ContainerEvents<EVT>,
    range_final: bool,
    timed_out: bool,
}

impl<EVT> WithLen for ContainerEventsCollected<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        self.evs.len()
    }
}

impl<EVT> TypeName for ContainerEventsCollected<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> String {
        std::any::type_name::<Self>().into()
    }
}

impl<EVT> ToUserFacingApiType for ContainerEventsCollected<EVT>
where
    EVT: EventValueType,
{
    fn into_user_facing_api_type(self: Self) -> Box<dyn UserApiType> {
        let evs = ContainerEventsApi::<EVT> {
            tss: self.evs.tss,
            values: self.evs.vals,
            range_final: self.range_final,
            timed_out: self.timed_out,
        };
        Box::new(evs)
    }

    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType> {
        (*self).into_user_facing_api_type()
    }
}

impl<EVT> CollectedDyn for ContainerEventsCollected<EVT> where EVT: EventValueType {}

impl<EVT> ByteEstimate for ContainerEventsCollector<EVT>
where
    EVT: EventValueType,
{
    fn byte_estimate(&self) -> u32 {
        self.evs.byte_estimate()
    }
}

#[derive(Debug)]
pub struct ContainerEventsCollector<EVT>
where
    EVT: EventValueType,
{
    evs: ContainerEvents<EVT>,
    range_final: bool,
    timed_out: bool,
}

impl<EVT> ContainerEventsCollector<EVT>
where
    EVT: EventValueType,
{
    pub fn new() -> Self {
        debug!("ContainerEventsCollector::new");
        Self {
            evs: ContainerEvents::new(),
            range_final: false,
            timed_out: false,
        }
    }
}

impl<EVT> WithLen for ContainerEventsCollector<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        self.evs.len()
    }
}

impl<EVT> CollectorTy for ContainerEventsCollector<EVT>
where
    EVT: EventValueType,
{
    type Input = ContainerEvents<EVT>;
    type Output = ContainerEventsCollected<EVT>;

    fn ingest(&mut self, src: &mut Self::Input) {
        MergeableTy::drain_into(src, &mut self.evs, 0..src.len());
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self) -> Result<Self::Output, daqbuf_err::Error> {
        let ret = Self::Output {
            evs: self.evs.clone(),
            range_final: self.range_final,
            timed_out: self.timed_out,
        };
        Ok(ret)
    }
}

impl<EVT> CollectableDyn for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn new_collector(&self) -> Box<dyn CollectorDyn> {
        // crate::eventsdim0::EventsDim0;
        Box::new(ContainerEventsCollector::<EVT>::new())
    }
}

impl<EVT> ToUserFacingApiType for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn into_user_facing_api_type(self: Self) -> Box<dyn UserApiType> {
        let ret = ContainerEventsApi::<EVT> {
            tss: self.tss,
            values: self.vals,
            range_final: false,
            timed_out: false,
        };
        Box::new(ret)
    }

    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType> {
        let this = *self;
        this.into_user_facing_api_type()
    }
}

impl<EVT> BinningggContainerEventsDyn for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn binned_events_timeweight_traitobj(
        &self,
        range: BinnedRange<TsNano>,
    ) -> Box<dyn items_0::timebin::BinnedEventsTimeweightTrait> {
        BinnedEventsTimeweightDynbox::<EVT>::new(range)
    }

    fn to_anybox(&mut self) -> Box<dyn std::any::Any> {
        let ret = core::mem::replace(self, Self::new());
        Box::new(ret)
    }

    fn clone_dyn(&self) -> Box<dyn BinningggContainerEventsDyn> {
        Box::new(self.clone())
    }

    fn serde_id(&self) -> u32 {
        Self::serde_id()
    }

    fn nty_id(&self) -> u32 {
        EVT::SERDE_ID
    }

    fn eq(&self, rhs: &dyn BinningggContainerEventsDyn) -> bool {
        if let Some(rhs) = rhs.as_any_ref().downcast_ref::<Self>() {
            self.eq(rhs)
        } else {
            false
        }
    }

    fn as_mergeable_dyn_mut(&mut self) -> &mut dyn MergeableDyn {
        self
    }

    fn as_collectable_dyn_mut(&mut self) -> &mut dyn CollectableDyn {
        self
    }

    fn truncate_front(&mut self, len: usize) {
        self.truncate_front(len);
    }

    fn to_f32_for_binning_v01(&self) -> Box<dyn BinningggContainerEventsDyn> {
        let mut ret = ContainerEvents::new();
        for r in self.iter_zip() {
            // TODO can be expensive
            let v: EVT = r.1.into();
            ret.push_back(r.0, v.to_f32_for_binning_v01());
        }
        Box::new(ret)
    }

    fn dbg_to_tss(&self) -> VecDeque<TsNano> {
        self.tss.clone()
    }
}

impl ContainerEvents<f32> {
    pub fn testing_cmp(lhs: &Self, rhs: &Self) -> Result<(), String> {
        use fmt::Write;
        let mut log = String::new();
        for (j, k) in lhs.iter_zip().zip(rhs.iter_zip()) {
            if j.0 != k.0 {
                write!(&mut log, "ts mismatch {:?} {:?}", j, k).unwrap();
                log.push('\n');
            }
            if j.1 != k.1 {
                write!(&mut log, "value mismatch {:?} {:?}", j, k).unwrap();
                log.push('\n');
            }
        }
        if lhs.len() != rhs.len() {
            write!(&mut log, "len mismatch {:?} {:?}", lhs.len(), rhs.len()).unwrap();
            log.push('\n');
        }
        if log.len() != 0 { Err(log) } else { Ok(()) }
    }
}

#[cfg(test)]
mod test_frame {
    use super::*;
    use crate::channelevents::ChannelEvents;
    use crate::framable::Framable;
    use crate::frame::decode_frame;
    use crate::inmem::InMemoryFrame;
    use crate::inmem::ParseResult;
    use items_0::streamitem::RangeCompletableItem;
    use items_0::streamitem::Sitemty;
    use items_0::streamitem::StreamItem;
    use items_0::streamitem::sitem_data;

    #[test]
    fn events_serialize() {
        let mut evs = ContainerEvents::<f32>::new();
        evs.push_back(TsNano::from_ns(123), 55.);
        evs.push_back(TsNano::from_ns(124), 56.);
        let item = ChannelEvents::from(evs);
        let item: Sitemty<_> = sitem_data(item);
        let buf = item.make_frame_dyn().unwrap();
        let frame = match InMemoryFrame::parse(&buf) {
            Ok(ParseResult::Parsed(n, val)) => val,
            Ok(ParseResult::NotEnoughData(n)) => panic!(),
            Err(e) => panic!("{}", e),
        };
        let item: Sitemty<ChannelEvents> = decode_frame(&frame).unwrap();
        let item = if let Ok(x) = item { x } else { panic!() };
        let item = if let StreamItem::DataItem(x) = item {
            x
        } else {
            panic!()
        };
        let item = if let RangeCompletableItem::Data(x) = item {
            x
        } else {
            panic!()
        };
        let item = if let ChannelEvents::Events(x) = item {
            x
        } else {
            panic!()
        };
        let item = if let Some(item) = item.as_any_ref().downcast_ref::<ContainerEvents<f32>>() {
            item
        } else {
            panic!()
        };
        assert_eq!(
            MergeableTy::tss_for_testing(item),
            &[TsNano::from_ns(123), TsNano::from_ns(124)]
        );
    }
}

#[cfg(test)]
mod test_serde_opt {
    use super::*;

    #[derive(Serialize)]
    struct A {
        a: Option<String>,
        #[serde(default)]
        b: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        c: Option<String>,
    }

    #[test]
    fn test_a() {
        let s = serde_json::to_string(&A {
            a: None,
            b: None,
            c: None,
        })
        .unwrap();
        assert_eq!(s, r#"{"a":null,"b":null}"#);
    }
}

#[test]
fn float_cmp_00() {
    use std::cmp::Ordering;
    assert_eq!(f32::INFINITY.partial_cmp(&2.0_f32), Some(Ordering::Greater));
    assert_eq!(
        f32::INFINITY.partial_cmp(&f32::NEG_INFINITY),
        Some(Ordering::Greater)
    );
    assert_eq!(
        f32::INFINITY.partial_cmp(&f32::INFINITY),
        Some(Ordering::Equal)
    );
    assert_eq!(f32::NAN.partial_cmp(&f32::INFINITY), None);
}
