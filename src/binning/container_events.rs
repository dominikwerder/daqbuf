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

pub trait Container<EVT>: fmt::Debug + Send + Clone + PreviewRange + Serialize + for<'a> Deserialize<'a> {
    fn new() -> Self;
    // fn verify(&self) -> Result<(), ValueContainerError>;
    fn push_back(&mut self, val: EVT);
    fn pop_front(&mut self) -> Option<EVT>;
}

pub trait EventValueType: fmt::Debug + Clone + PartialOrd + Send + 'static + Serialize {
    type Container: Container<Self>;
    type AggregatorTimeWeight: AggregatorTimeWeight<Self>;
    type AggTimeWeightOutputAvg: AggTimeWeightOutputAvg;

    // fn identity_sum() -> Self;
    // fn add_weighted(&self, add: &Self, f: f32) -> Self;
}

impl<EVT> Container<EVT> for VecDeque<EVT>
where
    EVT: EventValueType + Serialize + for<'a> Deserialize<'a>,
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
}

macro_rules! impl_event_value_type {
    ($evt:ty) => {
        impl EventValueType for $evt {
            type Container = VecDeque<Self>;
            type AggregatorTimeWeight = AggregatorNumeric;
            type AggTimeWeightOutputAvg = f64;
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

impl EventValueType for f32 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f32;
}

impl EventValueType for f64 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
}

impl EventValueType for bool {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
}

impl EventValueType for String {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = AggregatorNumeric;
    type AggTimeWeightOutputAvg = f64;
}

#[derive(Debug, Clone)]
pub struct EventSingle<EVT> {
    pub ts: TsNano,
    pub val: EVT,
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
    pub fn from_constituents(tss: VecDeque<TsNano>, vals: <EVT as EventValueType>::Container) -> Self {
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
        if self.tss.iter().zip(self.tss.iter().skip(1)).any(|(&a, &b)| a > b) {
            return Err(EventsContainerError::Unordered);
        }
        Ok(())
    }

    pub fn ts_first(&self) -> Option<TsNano> {
        self.tss.front().map(|&x| x)
    }

    pub fn ts_last(&self) -> Option<TsNano> {
        self.tss.back().map(|&x| x)
    }

    pub fn len_before(&self, end: TsNano) -> usize {
        let pp = self.tss.partition_point(|&x| x < end);
        assert!(pp <= self.len(), "len_before  pp {}  len {}", pp, self.len());
        pp
    }

    pub fn pop_front(&mut self) -> Option<EventSingle<EVT>> {
        if let (Some(ts), Some(val)) = (self.tss.pop_front(), self.vals.pop_front()) {
            Some(EventSingle { ts, val })
        } else {
            None
        }
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
    evs: &'a mut ContainerEvents<EVT>,
    len: usize,
}

impl<'a, EVT> ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn new(evs: &'a mut ContainerEvents<EVT>, len: usize) -> Self {
        let len = len.min(evs.len());
        Self { evs, len }
    }
}

impl<'a, EVT> ContainerEventsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn ts_first(&self) -> Option<TsNano> {
        self.evs.ts_first()
    }

    pub fn ts_last(&self) -> Option<TsNano> {
        self.evs.ts_last()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn pop_front(&mut self) -> Option<EventSingle<EVT>> {
        if self.len != 0 {
            if let Some(ev) = self.evs.pop_front() {
                self.len -= 1;
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
