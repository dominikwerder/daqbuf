use super::aggregator::AggregatorTimeWeight;
use super::container_events::Container;
use super::container_events::EventValueType;
use super::container_events::PartialOrdEvtA;
use crate::log;
use core::fmt;
use items_0::subfr::SubFrId;
use items_0::vecpreview::PreviewRange;
use netpod::DtNano;
use netpod::EnumVariant;
use netpod::EnumVariantRef;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;

macro_rules! trace_ingest_event { ($($arg:tt)*) => ( if false { log::trace!($($arg)*) } ); }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumVariantContainer {
    ixs: VecDeque<i16>,
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

impl FromIterator<EnumVariant> for EnumVariantContainer {
    fn from_iter<T: IntoIterator<Item = EnumVariant>>(iter: T) -> Self {
        let mut ixs = VecDeque::new();
        let mut names = VecDeque::new();
        iter.into_iter().for_each(|x| {
            let (ix, name) = x.into_parts();
            ixs.push_back(ix);
            names.push_back(name);
        });
        Self { ixs, names }
    }
}

impl Container<EnumVariant> for EnumVariantContainer {
    fn new() -> Self {
        Self {
            ixs: VecDeque::new(),
            names: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.ixs.len()
    }

    fn push_back(&mut self, val: EnumVariant) {
        let (ix, name) = val.into_parts();
        self.ixs.push_back(ix);
        self.names.push_back(name);
    }

    fn clear(&mut self) {
        self.ixs.clear();
        self.names.clear();
    }

    fn get_iter_ty_1(&self, pos: usize) -> Option<<EnumVariant as EventValueType>::IterTy1<'_>> {
        if let (Some(&ix), Some(name)) = (self.ixs.get(pos), self.names.get(pos)) {
            let ret = EnumVariantRef {
                ix,
                name: name.as_str(),
            };
            Some(ret)
        } else {
            None
        }
    }

    fn iter_ty_1(&self) -> impl Iterator<Item = <EnumVariant as EventValueType>::IterTy1<'_>> {
        self.ixs
            .iter()
            .zip(self.names.iter())
            .map(|x| EnumVariantRef {
                ix: *x.0,
                name: x.1.as_str(),
            })
    }

    fn into_iter_ty_2(self) -> impl Iterator<Item = EnumVariant> {
        self.ixs
            .into_iter()
            .zip(self.names.into_iter())
            .map(|(ix, name)| EnumVariant::new(ix, name))
    }

    fn drain_into(&mut self, dst: &mut Self, range: std::ops::Range<usize>) {
        dst.ixs.extend(self.ixs.drain(range.clone()));
        dst.names.extend(self.names.drain(range));
    }

    fn truncate_front(&mut self, len: usize) {
        if self.len() > len {
            let n = self.len() - len;
            self.ixs.drain(0..n);
            self.names.drain(0..n);
        }
    }

    fn into_user_facing_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        vec![
            ("values".into(), Box::new(self.ixs)),
            ("valuestrings".into(), Box::new(self.names)),
        ]
    }

    fn into_user_facing_fields_json(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        vec![
            ("values".into(), Box::new(self.ixs)),
            ("valuestrings".into(), Box::new(self.names)),
        ]
    }

    fn byte_estimate(&self) -> u32 {
        self.len() as u32 * 24
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
        trace_ingest_event!("ingest enum  {:.3e}  {:?}", f, val);
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

impl<'a> PartialOrdEvtA<EnumVariant> for EnumVariantRef<'a> {
    fn cmp_a(&self, other: &EnumVariant) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering::*;
        let x = self.ix.partial_cmp(&other.ix());
        if let Some(Equal) = x {
            let x = self.name.partial_cmp(other.name());
            if let Some(Equal) = x { Some(Equal) } else { x }
        } else {
            x
        }
    }
}

impl EventValueType for EnumVariant {
    type Container = EnumVariantContainer;
    type AggregatorTimeWeight = EnumVariantAggregatorTimeWeight;
    type AggTimeWeightOutputAvg = f32;
    type IterTy1<'a> = EnumVariantRef<'a>;
    const SERDE_ID: u32 = <Self as SubFrId>::SUB as u32;
    fn to_f32_for_binning_v01(&self) -> f32 {
        self.ix() as _
    }
    fn scalar_type_name_string() -> String {
        "enum".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        60
    }
}

impl PartialOrdEvtA<netpod::UnsupEvt> for netpod::UnsupEvt {
    fn cmp_a(&self, _other: &netpod::UnsupEvt) -> Option<std::cmp::Ordering> {
        todo!()
    }
}

impl PartialOrdEvtA<Vec<netpod::UnsupEvt>> for Vec<netpod::UnsupEvt> {
    fn cmp_a(&self, _other: &Vec<netpod::UnsupEvt>) -> Option<std::cmp::Ordering> {
        todo!()
    }
}

#[derive(Debug)]
pub struct UnsupEvtAgg;

impl AggregatorTimeWeight<netpod::UnsupEvt> for UnsupEvtAgg {
    fn new() -> Self {
        todo!()
    }

    fn ingest(&mut self, _dt: DtNano, _bl: DtNano, _val: netpod::UnsupEvt) {
        todo!()
    }

    fn reset_for_new_bin(&mut self) {
        todo!()
    }

    fn result_and_reset_for_new_bin(
        &mut self,
        _filled_width_fraction: f32,
    ) -> <netpod::UnsupEvt as EventValueType>::AggTimeWeightOutputAvg {
        todo!()
    }
}

impl AggregatorTimeWeight<Vec<netpod::UnsupEvt>> for UnsupEvtAgg {
    fn new() -> Self {
        todo!()
    }

    fn ingest(&mut self, _dt: DtNano, _bl: DtNano, _val: Vec<netpod::UnsupEvt>) {
        todo!()
    }

    fn reset_for_new_bin(&mut self) {
        todo!()
    }

    fn result_and_reset_for_new_bin(
        &mut self,
        _filled_width_fraction: f32,
    ) -> <Vec<netpod::UnsupEvt> as EventValueType>::AggTimeWeightOutputAvg {
        todo!()
    }
}

impl EventValueType for netpod::UnsupEvt {
    type Container = std::collections::VecDeque<netpod::UnsupEvt>;
    type AggregatorTimeWeight = UnsupEvtAgg;
    type AggTimeWeightOutputAvg = f32;
    type IterTy1<'a> = netpod::UnsupEvt;
    const SERDE_ID: u32 = <Self as SubFrId>::SUB as u32;
    fn to_f32_for_binning_v01(&self) -> f32 {
        0.
    }
    fn scalar_type_name_string() -> String {
        "unsupevt".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        345
    }
}

impl EventValueType for Vec<netpod::UnsupEvt> {
    type Container = std::collections::VecDeque<Vec<netpod::UnsupEvt>>;
    type AggregatorTimeWeight = UnsupEvtAgg;
    type AggTimeWeightOutputAvg = f32;
    type IterTy1<'a> = Vec<netpod::UnsupEvt>;
    const SERDE_ID: u32 = <Self as SubFrId>::SUB as u32;
    fn to_f32_for_binning_v01(&self) -> f32 {
        0.
    }
    fn scalar_type_name_string() -> String {
        "unsupevt".to_string()
    }
    fn byte_estimate(&self) -> u32 {
        self.len() as u32 * 345
    }
}
