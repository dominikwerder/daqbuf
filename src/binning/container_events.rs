use super::aggregator::AggTimeWeightOutputAvg;
use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::timeweight::timeweight_events_dyn::BinnedEventsTimeweightDynbox;
use core::fmt;
use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use items_0::timebin::BinningggContainerEventsDyn;
use items_0::vecpreview::PreviewRange;
use items_0::vecpreview::VecPreview;
use items_0::AsAnyRef;
use netpod::BinnedRange;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::collections::VecDeque;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug, ThisError)]
#[cstm(name = "ValueContainerError")]
pub enum ValueContainerError {}

pub trait Container<EVT>:
    fmt::Debug + Send + Clone + PreviewRange + Serialize + for<'a> Deserialize<'a>
where
    EVT: EventValueType,
{
    fn new() -> Self;
    fn push_back(&mut self, val: EVT);
    fn pop_front(&mut self) -> Option<EVT>;
    fn get_iter_ty_1(&self, pos: usize) -> Option<EVT::IterTy1<'_>>;
}

pub trait PartialOrdEvtA<EVT> {
    fn cmp_a(&self, other: &EVT) -> Option<std::cmp::Ordering>;
}

pub trait EventValueType: fmt::Debug + Clone + PartialOrd + Send + 'static + Serialize {
    type Container: Container<Self>;
    type AggregatorTimeWeight: AggregatorTimeWeight<Self>;
    type AggTimeWeightOutputAvg: AggTimeWeightOutputAvg;
    type IterTy1<'a>: fmt::Debug + Clone + PartialOrdEvtA<Self> + Into<Self>;
}

impl<EVT> Container<EVT> for VecDeque<EVT>
where
    EVT: for<'a> EventValueType<IterTy1<'a> = EVT> + Serialize + for<'a> Deserialize<'a>,
{
    fn new() -> Self {
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
}

macro_rules! impl_event_value_type {
    ($evt:ty) => {
        impl EventValueType for $evt {
            type Container = VecDeque<Self>;
            type AggregatorTimeWeight = AggregatorNumeric;
            type AggTimeWeightOutputAvg = f64;
            type IterTy1<'a> = $evt;
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
}

impl EventValueType for f64 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = f64;
}

impl EventValueType for bool {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = bool;
}

impl EventValueType for String {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
    type IterTy1<'a> = &'a str;
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

#[derive(Debug, ThisError)]
#[cstm(name = "EventsContainerError")]
pub enum EventsContainerError {
    Unordered,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    tss: VecDeque<TsNano>,
    vals: <EVT as EventValueType>::Container,
}

impl<EVT> ContainerEvents<EVT>
where
    EVT: EventValueType,
{
    pub fn from_constituents(
        tss: VecDeque<TsNano>,
        vals: <EVT as EventValueType>::Container,
    ) -> Self {
        Self { tss, vals }
    }

    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            tss: VecDeque::new(),
            vals: Container::new(),
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
}
