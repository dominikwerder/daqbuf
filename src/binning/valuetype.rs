use super::aggregator::AggregatorTimeWeight;
use super::container_events::Container;
use super::container_events::EventValueType;
use core::fmt;
use items_0::vecpreview::PreviewRange;
use netpod::DtNano;
use netpod::EnumVariant;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumVariantContainer {
    ixs: VecDeque<u16>,
    names: VecDeque<String>,
}

impl PreviewRange for EnumVariantContainer {
    fn preview<'a>(&'a self) -> Box<dyn fmt::Debug + 'a> {
        let ret = items_0::vecpreview::PreviewCell {
            a: self.ixs.front(),
            b: self.ixs.back(),
        };
        Box::new(ret)
    }
}

impl Container<EnumVariant> for EnumVariantContainer {
    fn new() -> Self {
        Self {
            ixs: VecDeque::new(),
            names: VecDeque::new(),
        }
    }

    fn push_back(&mut self, val: EnumVariant) {
        let (ix, name) = val.into_parts();
        self.ixs.push_back(ix);
        self.names.push_back(name);
    }

    fn pop_front(&mut self) -> Option<EnumVariant> {
        if let (Some(a), Some(b)) = (self.ixs.pop_front(), self.names.pop_front()) {
            Some(EnumVariant::new(a, b))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct EnumVariantAggregatorTimeWeight {
    sum: f32,
}

impl AggregatorTimeWeight<EnumVariant> for EnumVariantAggregatorTimeWeight {
    fn new() -> Self {
        Self { sum: 0. }
    }

    fn ingest(&mut self, dt: DtNano, bl: DtNano, val: EnumVariant) {
        let f = dt.ns() as f32 / bl.ns() as f32;
        eprintln!("INGEST ENUM  {}  {:?}", f, val);
        self.sum += f * val.ix() as f32;
    }

    fn reset_for_new_bin(&mut self) {
        self.sum = 0.;
    }

    fn result_and_reset_for_new_bin(
        &mut self,
        filled_width_fraction: f32,
    ) -> <EnumVariant as EventValueType>::AggTimeWeightOutputAvg {
        let ret = self.sum.clone();
        self.sum = 0.;
        ret / filled_width_fraction
    }
}

impl EventValueType for EnumVariant {
    type Container = EnumVariantContainer;
    type AggregatorTimeWeight = EnumVariantAggregatorTimeWeight;
    type AggTimeWeightOutputAvg = f32;
}
