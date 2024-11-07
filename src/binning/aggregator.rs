use super::container_events::EventValueType;
use core::fmt;
use netpod::log::*;
use netpod::DtNano;
use netpod::EnumVariant;
use serde::Deserialize;
use serde::Serialize;

#[allow(unused)]
macro_rules! trace_event { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_result { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

pub trait AggTimeWeightOutputAvg: fmt::Debug + Clone + Send + Serialize + for<'a> Deserialize<'a> {}

impl AggTimeWeightOutputAvg for u8 {}
impl AggTimeWeightOutputAvg for u16 {}
impl AggTimeWeightOutputAvg for u32 {}
impl AggTimeWeightOutputAvg for u64 {}
impl AggTimeWeightOutputAvg for i8 {}
impl AggTimeWeightOutputAvg for i16 {}
impl AggTimeWeightOutputAvg for i32 {}
impl AggTimeWeightOutputAvg for i64 {}
impl AggTimeWeightOutputAvg for f32 {}
impl AggTimeWeightOutputAvg for f64 {}
impl AggTimeWeightOutputAvg for EnumVariant {}
impl AggTimeWeightOutputAvg for String {}
impl AggTimeWeightOutputAvg for bool {}

pub trait AggregatorTimeWeight<EVT>: fmt::Debug + Send
where
    EVT: EventValueType,
{
    fn new() -> Self;
    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT);
    fn reset_for_new_bin(&mut self);
    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> EVT::AggTimeWeightOutputAvg;
}

#[derive(Debug)]
pub struct AggregatorNumeric {
    sum: f64,
}

trait AggWithF64: EventValueType<AggTimeWeightOutputAvg = f64> {
    fn as_f64(&self) -> f64;
}

impl AggWithF64 for f64 {
    fn as_f64(&self) -> f64 {
        *self
    }
}

impl<EVT> AggregatorTimeWeight<EVT> for AggregatorNumeric
where
    EVT: AggWithF64,
{
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace_event!("INGEST  {}  {:?}", f, val);
        self.sum += f * val.as_f64();
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> EVT::AggTimeWeightOutputAvg {
        let sum = self.sum.clone();
        trace_result!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction as f64
    }
}

impl AggregatorTimeWeight<f32> for AggregatorNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: f32) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace_event!("INGEST  {}  {}", f, val);
        self.sum += f * val as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f32 {
        let sum = self.sum.clone() as f32;
        trace_result!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction
    }
}

macro_rules! impl_agg_tw_for_agg_num {
    ($evt:ty) => {
        impl AggregatorTimeWeight<$evt> for AggregatorNumeric {
            fn new() -> Self {
                Self { sum: 0. }
            }

            fn ingest(&mut self, dt: DtNano, bl: DtNano, val: $evt) {
                let f = dt.ns() as f64 / bl.ns() as f64;
                trace_event!("INGEST  {}  {}", f, val);
                self.sum += f * val as f64;
            }

            fn reset_for_new_bin(&mut self) {
                self.sum = 0.;
            }

            fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f64 {
                let sum = self.sum.clone();
                trace_result!(
                    "result_and_reset_for_new_bin  sum {}  {}",
                    sum,
                    filled_width_fraction
                );
                self.sum = 0.;
                sum / filled_width_fraction as f64
            }
        }
    };
}

impl_agg_tw_for_agg_num!(u8);
impl_agg_tw_for_agg_num!(u16);
impl_agg_tw_for_agg_num!(u32);
impl_agg_tw_for_agg_num!(i8);
impl_agg_tw_for_agg_num!(i16);
impl_agg_tw_for_agg_num!(i32);
impl_agg_tw_for_agg_num!(i64);

impl AggregatorTimeWeight<u64> for AggregatorNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: u64) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace_event!("INGEST  {}  {}", f, val);
        self.sum += f * val as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f64 {
        let sum = self.sum.clone();
        trace_result!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction as f64
    }
}

impl AggregatorTimeWeight<bool> for AggregatorNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: bool) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace_event!("INGEST  {}  {}", f, val);
        self.sum += f * val as u8 as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f64 {
        let sum = self.sum.clone();
        trace_result!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction as f64
    }
}

impl AggregatorTimeWeight<String> for AggregatorNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: String) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace_event!("INGEST  {}  {}", f, val);
        self.sum += f * val.len() as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f64 {
        let sum = self.sum.clone();
        trace_result!("result_and_reset_for_new_bin  sum {}  {}", sum, filled_width_fraction);
        self.sum = 0.;
        sum / filled_width_fraction as f64
    }
}
