use super::aggregator::AggTimeWeightOutputAvg;
use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::aggregator::AggregatorVecNumeric;
use super::timeweight::timeweight_events_dyn::BinnedEventsTimeweightDynbox;
use crate::log::*;
use core::fmt;
use core::ops::Range;
use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use items_0::apitypes::ContainerEventsApi;
use items_0::apitypes::ToUserFacingApiType;
use items_0::apitypes::UserApiType;
use items_0::collect_s::ToCborValue;
use items_0::collect_s::ToJsonValue;
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
use items_0::Appendable;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::WithLen;
use netpod::BinnedRange;
use netpod::EnumVariant;
use netpod::TsMs;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::collections::VecDeque;

macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug, ThisError)]
#[cstm(name = "ValueContainerError")]
pub enum ValueContainerError {}

pub trait Container<EVT>:
    fmt::Debug + Send + Unpin + Clone + PreviewRange + Serialize + for<'a> Deserialize<'a>
where
    EVT: EventValueType,
{
    fn new() -> Self;
    fn push_back(&mut self, val: EVT);
    fn pop_front(&mut self) -> Option<EVT>;
    fn get_iter_ty_1(&self, pos: usize) -> Option<EVT::IterTy1<'_>>;
    fn iter_ty_1(&self) -> impl Iterator<Item = EVT::IterTy1<'_>>;
    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>);
}

pub trait PartialOrdEvtA<EVT> {
    fn cmp_a(&self, other: &EVT) -> Option<std::cmp::Ordering>;
}

pub trait EventValueType:
    fmt::Debug + Clone + PartialOrd + Send + Unpin + 'static + Serialize + for<'a> Deserialize<'a>
{
    type Container: Container<Self>;
    type AggregatorTimeWeight: AggregatorTimeWeight<Self>;
    type AggTimeWeightOutputAvg: AggTimeWeightOutputAvg;
    type IterTy1<'a>: fmt::Debug + Clone + PartialOrdEvtA<Self> + Into<Self>;
    const SERDE_ID: u32;
}

impl<EVT> Container<EVT> for VecDeque<EVT>
where
    EVT: for<'a> EventValueType<IterTy1<'a> = EVT> + Serialize + for<'a> Deserialize<'a>,
{
    fn new() -> Self {
        trace_init!("{} as trait Container ::new", std::any::type_name::<Self>());
        VecDeque::new()
    }

    fn push_back(&mut self, val: EVT) {
        self.push_back(val);
    }

    fn pop_front(&mut self) -> Option<EVT> {
        self.pop_front()
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
}

impl Container<String> for VecDeque<String> {
    fn new() -> Self {
        VecDeque::new()
    }

    fn push_back(&mut self, val: String) {
        self.push_back(val);
    }

    fn pop_front(&mut self) -> Option<String> {
        self.pop_front()
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
}

macro_rules! impl_event_value_type {
    ($evt:ty) => {
        impl EventValueType for $evt {
            type Container = VecDeque<Self>;
            type AggregatorTimeWeight = AggregatorNumeric;
            type AggTimeWeightOutputAvg = f64;
            type IterTy1<'a> = $evt;
            const SERDE_ID: u32 = <$evt as SubFrId>::SUB;
        }

        impl PartialOrdEvtA<$evt> for $evt {
            fn cmp_a(&self, other: &$evt) -> Option<std::cmp::Ordering> {
                self.partial_cmp(other)
            }
        }
    };
}

impl_event_value_type!(u8);
impl_event_value_type!(u16);
impl_event_value_type!(u32);
impl_event_value_type!(u64);
impl_event_value_type!(i8);
impl_event_value_type!(i16);
impl_event_value_type!(i32);
impl_event_value_type!(i64);
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
    const SERDE_ID: u32 = <f32 as SubFrId>::SUB;
}

impl EventValueType for f64 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = f64;
    const SERDE_ID: u32 = <f64 as SubFrId>::SUB;
}

impl EventValueType for bool {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = bool;
    const SERDE_ID: u32 = <bool as SubFrId>::SUB;
}

impl EventValueType for String {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = &'a str;
    const SERDE_ID: u32 = <String as SubFrId>::SUB;
}

macro_rules! impl_event_value_type_vec {
    ($evt:ty) => {
        impl EventValueType for Vec<$evt> {
            type Container = VecDeque<Self>;
            type AggregatorTimeWeight = AggregatorVecNumeric;
            type AggTimeWeightOutputAvg = f32;
            type IterTy1<'a> = Vec<$evt>;
            const SERDE_ID: u32 = <Vec<$evt> as SubFrId>::SUB;
        }

        impl PartialOrdEvtA<Vec<$evt>> for Vec<$evt> {
            fn cmp_a(&self, other: &Vec<$evt>) -> Option<core::cmp::Ordering> {
                self.partial_cmp(other)
            }
        }
    };
}

impl_event_value_type_vec!(u8);
impl_event_value_type_vec!(u16);
impl_event_value_type_vec!(u32);
impl_event_value_type_vec!(u64);
impl_event_value_type_vec!(i8);
impl_event_value_type_vec!(i16);
impl_event_value_type_vec!(i32);
impl_event_value_type_vec!(i64);
impl_event_value_type_vec!(f32);
impl_event_value_type_vec!(f64);
impl_event_value_type_vec!(bool);
impl_event_value_type_vec!(String);
impl_event_value_type_vec!(EnumVariant);

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

#[derive(Debug, ThisError)]
#[cstm(name = "EventsContainerError")]
pub enum EventsContainerError {
    Unordered,
}

#[derive(Clone)]
pub struct ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    tss: VecDeque<TsNano>,
    vals: <EVT as EventValueType>::Container,
    byte_estimate: u64,
}

mod container_events_serde {
    use super::ContainerEvents;
    use super::EventValueType;
    use serde::de::MapAccess;
    use serde::de::SeqAccess;
    use serde::de::Visitor;
    use serde::ser::SerializeStruct;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
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
                // TODO make container recompute byte_estimate
                byte_estimate: 0,
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
            while let Some(key) = map.next_key::<&str>()? {
                match key {
                    "tss" => {
                        tss = Some(map.next_value()?);
                    }
                    "vals" => {
                        vals = Some(map.next_value()?);
                    }
                    _ => {
                        use serde::de::Error;
                        return Err(Error::unknown_field(key, &["tss", "vals"]));
                    }
                }
            }
            let ret = Self::Value {
                tss: tss.unwrap(),
                vals: vals.unwrap(),
                byte_estimate: 0,
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
            byte_estimate: 0,
        }
    }

    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            tss: VecDeque::new(),
            vals: Container::new(),
            byte_estimate: 0,
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
    }

    pub fn iter_zip<'a>(&'a self) -> impl Iterator<Item = (&TsNano, EVT::IterTy1<'a>)> {
        self.tss.iter().zip(self.vals.iter_ty_1())
    }

    pub fn serde_id() -> u32 {
        items_0::streamitem::CONTAINER_EVENTS_TYPE_ID
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

impl<EVT> ByteEstimate for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn byte_estimate(&self) -> u64 {
        self.byte_estimate
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

pub struct ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    evs: &'a ContainerEvents<EVT>,
    beg: usize,
    end: usize,
    pos: usize,
}

impl<'a, EVT> ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn new(evs: &'a ContainerEvents<EVT>) -> Self {
        Self {
            evs,
            beg: 0,
            end: evs.len(),
            pos: 0,
        }
    }

    pub fn constrain_up_to_ts(&mut self, end: TsNano) {
        let tss = &self.evs.tss;
        let pp = tss.partition_point(|&x| x < end);
        let pp = pp.max(self.pos);
        assert!(pp <= tss.len(), "len_before  pp {}  len {}", pp, tss.len());
        assert!(pp >= self.pos);
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
        self.evs.tss.get(self.pos).cloned()
    }

    pub fn next(&mut self) -> Option<EventSingleRef<EVT>> {
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
        if x >= self.tss.len() {
            None
        } else {
            Some(x)
        }
    }

    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize> {
        let x = self.tss.partition_point(|&x| x < ts);
        if x >= self.tss.len() {
            None
        } else {
            Some(x)
        }
    }

    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize> {
        let x = self.tss.partition_point(|&x| x < ts);
        if x == 0 || x >= self.tss.len() {
            None
        } else {
            Some(x - 1)
        }
    }

    fn tss_for_testing(&self) -> Vec<TsMs> {
        self.tss.iter().map(|&x| x.to_ts_ms()).collect()
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

    fn is_consistent(&self) -> bool {
        let mut good = true;
        let n = self.tss.len();
        for (&ts1, &ts2) in self.tss.iter().zip(self.tss.range(n.min(1)..n)) {
            if ts1 > ts2 {
                good = false;
                error!("unordered event data  ts1 {}  ts2 {}", ts1, ts2);
                break;
            }
        }
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

    fn tss_for_testing(&self) -> Vec<netpod::TsMs> {
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

    fn is_consistent(&self) -> bool {
        MergeableTy::is_consistent(self)
    }
}

impl<EVT> ToCborValue for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn to_cbor_value(&self) -> Result<ciborium::Value, ciborium::value::Error> {
        ciborium::value::Value::serialized(self)
    }
}

impl<EVT> ToJsonValue for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

impl<EVT> ToUserFacingApiType for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn to_user_facing_api_type(self: Self) -> Box<dyn UserApiType> {
        let this = self;
        let tss: VecDeque<_> = this.tss.into_iter().map(|x| x.ms()).collect();
        let ret = ContainerEventsApi {
            tss: tss.clone(),
            values: tss.clone(),
        };
        Box::new(ret)
    }

    fn to_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType> {
        let this = *self;
        this.to_user_facing_api_type()
    }
}

impl<EVT> ToUserFacingApiType for Box<ContainerEvents<EVT>>
where
    EVT: EventValueType,
{
    fn to_user_facing_api_type(self: Self) -> Box<dyn UserApiType> {
        let this = *self;
        let tss: VecDeque<_> = this.tss.into_iter().map(|x| x.ms()).collect();
        let ret = ContainerEventsApi {
            tss: tss.clone(),
            values: tss.clone(),
        };
        Box::new(ret)
    }

    fn to_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType> {
        let this = *self;
        this.to_user_facing_api_type()
    }
}

impl<EVT> BinningggContainerEventsDyn for ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

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
}

#[cfg(test)]
mod test_frame {
    use super::*;
    use crate::channelevents::ChannelEvents;
    use crate::framable::Framable;
    use crate::framable::INMEM_FRAME_ENCID;
    use crate::frame::decode_frame;
    use crate::inmem::InMemoryFrame;
    use items_0::streamitem::RangeCompletableItem;
    use items_0::streamitem::Sitemty;
    use items_0::streamitem::StreamItem;
    use netpod::TsMs;

    #[test]
    fn events_serialize() {
        let mut evs = ContainerEvents::new();
        evs.push_back(TsNano::from_ns(123), 55f32);
        let item = ChannelEvents::from(evs);
        let item: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
        let mut buf = item.make_frame_dyn().unwrap();
        let s = String::from_utf8_lossy(&buf[20..buf.len() - 4]);
        eprintln!("[[{s}]]");
        let buflen = buf.len();
        let frame = InMemoryFrame {
            encid: INMEM_FRAME_ENCID,
            tyid: 0x2500,
            len: (buflen - 24) as _,
            buf: buf.split_off(20).split_to(buflen - 20 - 4).freeze(),
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
            &[TsMs::from_ms_u64(123)]
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
