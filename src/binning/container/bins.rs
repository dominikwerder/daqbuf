use crate::binning::container_events::PartialOrdEvtA;
use items_0::vecpreview::PreviewRange;
use netpod::DtNano;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt;
use std::ops::Range;

pub trait AggBinValTw<BVT>: fmt::Debug + Send
where
    BVT: BinAggedType,
{
    fn new() -> Self;
    fn ingest(&mut self, dt: DtNano, bl: DtNano, cnt: u64, val: BVT);
    fn result(&mut self, filled_width_fraction: f32) -> BVT;
    fn reset_for_new_bin(&mut self);
}

pub trait BinAggedContainer<BVT>:
    fmt::Debug + Send + Clone + Unpin + PreviewRange + Serialize + for<'a> Deserialize<'a>
where
    BVT: BinAggedType,
{
    fn new() -> Self;
    fn push_back(&mut self, val: BVT);
    fn pop_front(&mut self) -> Option<BVT>;
    fn iter_ty_1(&self) -> impl Iterator<Item = BVT::IterTy1<'_>>;
    fn get_iter_ty_1<'a>(&'a self, pos: usize) -> Option<BVT::IterTy1<'a>>;
    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>);
}

pub trait BinAggedType:
    fmt::Debug + Clone + PartialOrd + Send + 'static + Serialize + for<'a> Deserialize<'a>
{
    type Container: BinAggedContainer<Self>;
    type AggregatorTw: AggBinValTw<Self>;
    type IterTy1<'a>: fmt::Debug + Clone + PartialOrdEvtA<Self> + Into<Self>;
}

macro_rules! impl_bin_agged_cont_simple_copyable {
    ($evt:ty) => {
        impl BinAggedContainer<$evt> for VecDeque<$evt> {
            fn new() -> Self {
                Self::new()
            }

            fn push_back(&mut self, val: $evt) {
                self.push_back(val);
            }

            fn pop_front(&mut self) -> Option<$evt> {
                self.pop_front()
            }

            fn iter_ty_1(&self) -> impl Iterator<Item = <$evt as BinAggedType>::IterTy1<'_>> {
                self.iter().map(|&x| x)
            }

            fn get_iter_ty_1<'a>(
                &'a self,
                pos: usize,
            ) -> Option<<$evt as BinAggedType>::IterTy1<'a>> {
                self.get(pos).map(|&x| x)
            }

            fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) {
                dst.extend(self.drain(range));
            }
        }
    };
}

impl_bin_agged_cont_simple_copyable!(f32);
impl_bin_agged_cont_simple_copyable!(f64);

impl BinAggedType for f32 {
    type Container = VecDeque<Self>;
    type AggregatorTw = AggBinValTwF32;
    type IterTy1<'a> = Self;
}

impl BinAggedType for f64 {
    type Container = VecDeque<Self>;
    type AggregatorTw = AggBinValTwF64;
    type IterTy1<'a> = Self;
}

#[derive(Debug)]
pub struct AggBinValTwF32 {
    sum: f32,
}

impl AggBinValTw<f32> for AggBinValTwF32 {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, _cnt: u64, val: f32) {
        let f = dt.ns() as f32 / bl.ns() as f32;
        self.sum += f * val;
    }

    fn result(&mut self, filled_width_fraction: f32) -> f32 {
        let ret = self.sum.clone() / filled_width_fraction;
        <Self as AggBinValTw<f32>>::reset_for_new_bin(self);
        ret
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }
}

#[derive(Debug)]
pub struct AggBinValTwF64 {
    sum: f64,
}

impl AggBinValTw<f64> for AggBinValTwF64 {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, _cnt: u64, val: f64) {
        let f = dt.ns() as f32 / bl.ns() as f32;
        self.sum += f as f64 * val;
    }

    fn result(&mut self, filled_width_fraction: f32) -> f64 {
        let ret = self.sum.clone() / filled_width_fraction as f64;
        <Self as AggBinValTw<f64>>::reset_for_new_bin(self);
        ret
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }
}
