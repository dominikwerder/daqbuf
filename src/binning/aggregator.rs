pub mod agg_bins;

use super::container::bins::BinAggedType;
use super::container_events::EventValueType;
use super::container_events::PulsedVal;
use crate::log::*;
use core::fmt;
use items_0::subfr::SubFrId;
use netpod::DtNano;
use netpod::EnumVariant;
use serde::Deserialize;
use serde::Serialize;

macro_rules! trace_event { ($($arg:expr),*) => ( if false { trace!($($arg),*); }) }

macro_rules! trace_result { ($($arg:expr),*) => ( if false { trace!($($arg),*); }) }

pub trait AggTimeWeightOutputAvg: BinAggedType + Serialize + for<'a> Deserialize<'a> {}

// impl AggTimeWeightOutputAvg for u8 {}
// impl AggTimeWeightOutputAvg for u16 {}
// impl AggTimeWeightOutputAvg for u32 {}
// impl AggTimeWeightOutputAvg for u64 {}
// impl AggTimeWeightOutputAvg for i8 {}
// impl AggTimeWeightOutputAvg for i16 {}
// impl AggTimeWeightOutputAvg for i32 {}
// impl AggTimeWeightOutputAvg for i64 {}
impl AggTimeWeightOutputAvg for f32 {}
impl AggTimeWeightOutputAvg for f64 {}
// impl AggTimeWeightOutputAvg for EnumVariant {}
// impl AggTimeWeightOutputAvg for String {}
// impl AggTimeWeightOutputAvg for bool {}

pub trait AggregatorTimeWeight<EVT>: fmt::Debug + Send
where
    EVT: EventValueType,
{
    fn new() -> Self;
    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EVT);
    fn reset_for_new_bin(&mut self);
    fn result_and_reset_for_new_bin(
        &mut self,
        filled_width_fraction: f32,
    ) -> EVT::AggTimeWeightOutputAvg;
}

#[derive(Debug)]
pub struct AggregatorNumeric {
    sum: f64,
}

pub trait AggWithF64: EventValueType<AggTimeWeightOutputAvg = f64> {
    fn as_f64(&self) -> f64;
}

impl AggWithF64 for f64 {
    fn as_f64(&self) -> f64 {
        *self
    }
}

impl<EVT> AggregatorTimeWeight<EVT> for AggregatorNumeric
where
    EVT: EventValueType + AggWithF64,
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

    fn result_and_reset_for_new_bin(
        &mut self,
        filled_width_fraction: f32,
    ) -> EVT::AggTimeWeightOutputAvg {
        let sum = self.sum.clone();
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
        <Self as AggregatorTimeWeight<EVT>>::reset_for_new_bin(self);
        sum / filled_width_fraction as f64
    }
}

impl AggregatorTimeWeight<f32> for AggregatorNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: f32) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        trace_event!("INGEST  {:5}  {:7.3}  {:7.3}", dt.ms_u64(), f, val);
        self.sum += f * val as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f32 {
        let sum = self.sum.clone() as f32;
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
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
                if true {
                    panic!();
                }
                let val = 42;
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

impl_agg_tw_for_agg_num!(super::container_events::PulsedVal<u8>);

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
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
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
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
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
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
        self.sum = 0.;
        sum / filled_width_fraction as f64
    }
}

#[derive(Debug)]
pub struct AggregatorVecNumeric {
    sum: f64,
}

macro_rules! impl_agg_tw_vec {
    ($evt:ty) => {
        impl AggregatorTimeWeight<Vec<$evt>> for AggregatorVecNumeric {
            fn new() -> Self {
                Self { sum: 0. }
            }

            fn ingest(&mut self, dt: DtNano, bl: DtNano, val: Vec<$evt>) {
                let f = dt.ns() as f64 / bl.ns() as f64;
                for e in val.iter() {
                    self.sum += f * (*e) as f64;
                }
            }

            fn reset_for_new_bin(&mut self) {
                self.sum = 0.;
            }

            fn result_and_reset_for_new_bin(
                &mut self,
                filled_width_fraction: f32,
            ) -> <Vec<$evt> as EventValueType>::AggTimeWeightOutputAvg {
                let sum = self.sum.clone() as f32;
                trace_result!(
                    "result_and_reset_for_new_bin  sum {}  {}",
                    sum,
                    filled_width_fraction
                );
                self.sum = 0.;
                sum / filled_width_fraction
            }
        }
    };
}

impl_agg_tw_vec!(u8);
impl_agg_tw_vec!(u16);
impl_agg_tw_vec!(u32);
impl_agg_tw_vec!(u64);
impl_agg_tw_vec!(i8);
impl_agg_tw_vec!(i16);
impl_agg_tw_vec!(i32);
impl_agg_tw_vec!(i64);
impl_agg_tw_vec!(f32);
impl_agg_tw_vec!(f64);

impl AggregatorTimeWeight<Vec<bool>> for AggregatorVecNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: Vec<bool>) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        self.sum += f * val.len() as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f32 {
        let sum = self.sum as f32;
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
        self.sum = 0.;
        sum / filled_width_fraction
    }
}

impl AggregatorTimeWeight<Vec<String>> for AggregatorVecNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: Vec<String>) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        self.sum += f * val.len() as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f32 {
        let sum = self.sum as f32;
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
        self.sum = 0.;
        sum / filled_width_fraction
    }
}

impl AggregatorTimeWeight<Vec<EnumVariant>> for AggregatorVecNumeric {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: Vec<EnumVariant>) {
        let f = dt.ns() as f64 / bl.ns() as f64;
        self.sum += f * val.len() as f64;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> f32 {
        let sum = self.sum as f32;
        trace_result!(
            "result_and_reset_for_new_bin  sum {}  {}",
            sum,
            filled_width_fraction
        );
        self.sum = 0.;
        sum / filled_width_fraction
    }
}

#[derive(Debug)]
pub struct AggregatorPulsedNumeric<EVT>
where
    EVT: EventValueType,
{
    evt_agg: EVT::AggregatorTimeWeight,
}

impl<EVT> AggregatorTimeWeight<PulsedVal<EVT>> for AggregatorPulsedNumeric<EVT>
where
    EVT: EventValueType + SubFrId,
{
    fn new() -> Self {
        Self {
            evt_agg: EVT::AggregatorTimeWeight::new(),
        }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: PulsedVal<EVT>) {
        self.evt_agg.ingest(dt, bl, val.1);
    }

    fn reset_for_new_bin(&mut self) {
        self.evt_agg.reset_for_new_bin();
    }

    fn result_and_reset_for_new_bin(
        &mut self,
        filled_width_fraction: f32,
    ) -> <PulsedVal<EVT> as EventValueType>::AggTimeWeightOutputAvg {
        self.evt_agg
            .result_and_reset_for_new_bin(filled_width_fraction)
    }
}
