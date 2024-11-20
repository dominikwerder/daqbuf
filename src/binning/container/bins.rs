use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::EventValueType;
use crate::binning::container_events::PartialOrdEvtA;
use items_0::vecpreview::PreviewRange;
use netpod::DtNano;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt;

pub trait AggBinValTw<BVT>: fmt::Debug + Send
where
    BVT: BinAggedType,
{
    fn new() -> Self;
    fn ingest(&mut self, bl: DtNano, val: BVT);
    fn reset_for_new_bin(&mut self);
    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> BVT;
}

pub trait BinAggedContainer<BVT>:
    fmt::Debug + Send + Clone + PreviewRange + Serialize + for<'a> Deserialize<'a>
where
    BVT: BinAggedType,
{
    fn new() -> Self;
    fn push_back(&mut self, val: BVT);
    fn pop_front(&mut self) -> Option<BVT>;
    fn get_iter_ty_1<'a>(&'a self, pos: usize) -> Option<BVT::IterTy1<'a>>;
}

pub trait BinAggedType:
    fmt::Debug + Clone + PartialOrd + Send + 'static + Serialize + for<'a> Deserialize<'a>
{
    type Container: BinAggedContainer<Self>;
    type AggregatorTimeWeight: AggBinValTw<Self>;
    type IterTy1<'a>: fmt::Debug + Clone + PartialOrdEvtA<Self> + Into<Self>;
}

impl<EVT, BVT> PreviewRange for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn preview<'a>(&'a self) -> Box<dyn fmt::Debug + 'a> {
        todo!()
    }
}

impl<BVT> BinAggedContainer<BVT> for VecDeque<f32>
where
    BVT: BinAggedType,
{
    fn new() -> Self {
        todo!()
    }

    fn push_back(&mut self, val: BVT) {
        todo!()
    }

    fn pop_front(&mut self) -> Option<BVT> {
        todo!()
    }

    fn get_iter_ty_1<'a>(&'a self, pos: usize) -> Option<<BVT as BinAggedType>::IterTy1<'a>> {
        todo!()
    }
}

impl<BVT> BinAggedContainer<BVT> for VecDeque<f64>
where
    BVT: BinAggedType,
{
    fn new() -> Self {
        todo!()
    }

    fn push_back(&mut self, val: BVT) {
        todo!()
    }

    fn pop_front(&mut self) -> Option<BVT> {
        todo!()
    }

    fn get_iter_ty_1<'a>(&'a self, pos: usize) -> Option<<BVT as BinAggedType>::IterTy1<'a>> {
        todo!()
    }
}

impl BinAggedType for f32 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = ();
    type IterTy1<'a> = Self;
}

impl BinAggedType for f64 {
    type Container = VecDeque<Self>;
    type AggregatorTimeWeight = ();
    type IterTy1<'a> = Self;
}

impl<T> AggBinValTw<T> for ()
where
    T: BinAggedType,
{
    fn new() -> Self {
        todo!()
    }

    fn ingest(&mut self, bl: DtNano, val: T) {
        todo!()
    }

    fn reset_for_new_bin(&mut self) {
        todo!()
    }

    fn result_and_reset_for_new_bin(&mut self, filled_width_fraction: f32) -> T {
        todo!()
    }
}

pub struct DummyPayload {}
