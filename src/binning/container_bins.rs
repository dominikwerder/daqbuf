use super::aggregator::AggregatorNumeric;
use super::aggregator::AggregatorTimeWeight;
use super::container_events::EventValueType;
use super::___;
use crate::ts_offs_from_abs;
use crate::ts_offs_from_abs_with_anchor;
use core::fmt;
use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::ToJsonResult;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinsBoxed;
use items_0::vecpreview::VecPreview;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::TypeName;
use items_0::WithLen;
use netpod::log::*;
use netpod::EnumVariant;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::collections::VecDeque;
use std::mem;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug, ThisError)]
#[cstm(name = "ContainerBins")]
pub enum ContainerBinsError {
    Unordered,
}

pub trait BinValueType: fmt::Debug + Clone + PartialOrd {
    // type Container: Container<Self>;
    // type AggregatorTimeWeight: AggregatorTimeWeight<Self>;
    // type AggTimeWeightOutputAvg;

    // fn identity_sum() -> Self;
    // fn add_weighted(&self, add: &Self, f: f32) -> Self;
}

#[derive(Debug, Clone)]
pub struct BinSingle<EVT> {
    pub ts1: TsNano,
    pub ts2: TsNano,
    pub cnt: u64,
    pub min: EVT,
    pub max: EVT,
    pub avg: f32,
    pub lst: EVT,
    pub fnl: bool,
}

#[derive(Debug, Clone)]
pub struct BinRef<'a, EVT>
where
    EVT: EventValueType,
{
    pub ts1: TsNano,
    pub ts2: TsNano,
    pub cnt: u64,
    pub min: &'a EVT,
    pub max: &'a EVT,
    pub avg: &'a EVT::AggTimeWeightOutputAvg,
    pub lst: &'a EVT,
    pub fnl: bool,
}

pub struct IterDebug<'a, EVT>
where
    EVT: EventValueType,
{
    bins: &'a ContainerBins<EVT>,
    ix: usize,
    len: usize,
}

impl<'a, EVT> Iterator for IterDebug<'a, EVT>
where
    EVT: EventValueType,
{
    type Item = BinRef<'a, EVT>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ix < self.bins.len() && self.ix < self.len {
            let b = &self.bins;
            let i = self.ix;
            self.ix += 1;
            let ret = BinRef {
                ts1: b.ts1s[i],
                ts2: b.ts2s[i],
                cnt: b.cnts[i],
                min: &b.mins[i],
                max: &b.maxs[i],
                avg: &b.avgs[i],
                lst: &b.lsts[i],
                fnl: b.fnls[i],
            };
            Some(ret)
        } else {
            None
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ContainerBins<EVT>
where
    EVT: EventValueType,
{
    ts1s: VecDeque<TsNano>,
    ts2s: VecDeque<TsNano>,
    cnts: VecDeque<u64>,
    mins: VecDeque<EVT>,
    maxs: VecDeque<EVT>,
    avgs: VecDeque<EVT::AggTimeWeightOutputAvg>,
    lsts: VecDeque<EVT>,
    fnls: VecDeque<bool>,
}

impl<EVT> ContainerBins<EVT>
where
    EVT: EventValueType,
{
    pub fn from_constituents(
        ts1s: VecDeque<TsNano>,
        ts2s: VecDeque<TsNano>,
        cnts: VecDeque<u64>,
        mins: VecDeque<EVT>,
        maxs: VecDeque<EVT>,
        avgs: VecDeque<EVT::AggTimeWeightOutputAvg>,
        lsts: VecDeque<EVT>,
        fnls: VecDeque<bool>,
    ) -> Self {
        Self {
            ts1s,
            ts2s,
            cnts,
            mins,
            maxs,
            avgs,
            lsts,
            fnls,
        }
    }

    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            ts1s: VecDeque::new(),
            ts2s: VecDeque::new(),
            cnts: VecDeque::new(),
            mins: VecDeque::new(),
            maxs: VecDeque::new(),
            avgs: VecDeque::new(),
            lsts: VecDeque::new(),
            fnls: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.ts1s.len()
    }

    pub fn verify(&self) -> Result<(), ContainerBinsError> {
        if self.ts1s.iter().zip(self.ts1s.iter().skip(1)).any(|(&a, &b)| a > b) {
            return Err(ContainerBinsError::Unordered);
        }
        if self.ts2s.iter().zip(self.ts2s.iter().skip(1)).any(|(&a, &b)| a > b) {
            return Err(ContainerBinsError::Unordered);
        }
        Ok(())
    }

    pub fn ts1_first(&self) -> Option<TsNano> {
        self.ts1s.front().map(|&x| x)
    }

    pub fn ts2_last(&self) -> Option<TsNano> {
        self.ts2s.back().map(|&x| x)
    }

    pub fn ts1s_iter(&self) -> std::collections::vec_deque::Iter<TsNano> {
        self.ts1s.iter()
    }

    pub fn ts2s_iter(&self) -> std::collections::vec_deque::Iter<TsNano> {
        self.ts2s.iter()
    }

    pub fn cnts_iter(&self) -> std::collections::vec_deque::Iter<u64> {
        self.cnts.iter()
    }

    pub fn mins_iter(&self) -> std::collections::vec_deque::Iter<EVT> {
        self.mins.iter()
    }

    pub fn maxs_iter(&self) -> std::collections::vec_deque::Iter<EVT> {
        self.maxs.iter()
    }

    pub fn avgs_iter(&self) -> std::collections::vec_deque::Iter<EVT::AggTimeWeightOutputAvg> {
        self.avgs.iter()
    }

    pub fn fnls_iter(&self) -> std::collections::vec_deque::Iter<bool> {
        self.fnls.iter()
    }

    pub fn zip_iter(
        &self,
    ) -> std::iter::Zip<
        std::iter::Zip<
            std::iter::Zip<
                std::iter::Zip<
                    std::iter::Zip<
                        std::iter::Zip<
                            std::collections::vec_deque::Iter<TsNano>,
                            std::collections::vec_deque::Iter<TsNano>,
                        >,
                        std::collections::vec_deque::Iter<u64>,
                    >,
                    std::collections::vec_deque::Iter<EVT>,
                >,
                std::collections::vec_deque::Iter<EVT>,
            >,
            std::collections::vec_deque::Iter<EVT::AggTimeWeightOutputAvg>,
        >,
        std::collections::vec_deque::Iter<bool>,
    > {
        self.ts1s_iter()
            .zip(self.ts2s_iter())
            .zip(self.cnts_iter())
            .zip(self.mins_iter())
            .zip(self.maxs_iter())
            .zip(self.avgs_iter())
            .zip(self.fnls_iter())
    }

    pub fn edges_iter(
        &self,
    ) -> std::iter::Zip<std::collections::vec_deque::Iter<TsNano>, std::collections::vec_deque::Iter<TsNano>> {
        self.ts1s.iter().zip(self.ts2s.iter())
    }

    pub fn len_before(&self, end: TsNano) -> usize {
        let pp = self.ts2s.partition_point(|&x| x <= end);
        assert!(pp <= self.len(), "len_before  pp {}  len {}", pp, self.len());
        pp
    }

    pub fn pop_front(&mut self) -> Option<BinSingle<EVT>> {
        todo!("pop_front");
        let ts1 = if let Some(x) = self.ts1s.pop_front() {
            x
        } else {
            return None;
        };
        let ts2 = if let Some(x) = self.ts2s.pop_front() {
            x
        } else {
            return None;
        };
        todo!()
    }

    pub fn push_back(
        &mut self,
        ts1: TsNano,
        ts2: TsNano,
        cnt: u64,
        min: EVT,
        max: EVT,
        avg: EVT::AggTimeWeightOutputAvg,
        lst: EVT,
        fnl: bool,
    ) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.cnts.push_back(cnt);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
        self.lsts.push_back(lst);
        self.fnls.push_back(fnl);
    }

    pub fn iter_debug(&self) -> IterDebug<EVT> {
        IterDebug {
            bins: self,
            ix: 0,
            len: self.len(),
        }
    }
}

impl<EVT> fmt::Debug for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  {{  len: {:?},  ts1s: {:?},  ts2s: {:?}, cnts: {:?},  avgs {:?},  fnls {:?}  }}",
            self.len(),
            VecPreview::new(&self.ts1s),
            VecPreview::new(&self.ts2s),
            VecPreview::new(&self.cnts),
            VecPreview::new(&self.avgs),
            VecPreview::new(&self.fnls),
        )
    }
}

impl<EVT> fmt::Display for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl<EVT> AsAnyMut for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
}

impl<EVT> WithLen for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl<EVT> TypeName for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> String {
        BinningggContainerBinsDyn::type_name(self).into()
    }
}

impl<EVT> AsAnyRef for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn as_any_ref(&self) -> &dyn any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ContainerBinsCollectorOutput<EVT>
where
    EVT: EventValueType,
{
    bins: ContainerBins<EVT>,
}

impl<EVT> TypeName for ContainerBinsCollectorOutput<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<EVT> AsAnyRef for ContainerBinsCollectorOutput<EVT>
where
    EVT: EventValueType,
{
    fn as_any_ref(&self) -> &dyn any::Any {
        self
    }
}

impl<EVT> AsAnyMut for ContainerBinsCollectorOutput<EVT>
where
    EVT: EventValueType,
{
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
}

impl<EVT> WithLen for ContainerBinsCollectorOutput<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        self.bins.len()
    }
}

#[derive(Debug, Serialize)]
struct ContainerBinsCollectorOutputUser<EVT>
where
    EVT: EventValueType,
{
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "ts1Ms")]
    ts1_off_ms: VecDeque<u64>,
    #[serde(rename = "ts2Ms")]
    ts2_off_ms: VecDeque<u64>,
    #[serde(rename = "ts1Ns")]
    ts1_off_ns: VecDeque<u64>,
    #[serde(rename = "ts2Ns")]
    ts2_off_ns: VecDeque<u64>,
    #[serde(rename = "counts")]
    counts: VecDeque<u64>,
    #[serde(rename = "mins")]
    mins: VecDeque<EVT>,
    #[serde(rename = "maxs")]
    maxs: VecDeque<EVT>,
    #[serde(rename = "avgs")]
    avgs: VecDeque<EVT::AggTimeWeightOutputAvg>,
    // #[serde(rename = "rangeFinal", default, skip_serializing_if = "is_false")]
    // range_final: bool,
    // #[serde(rename = "timedOut", default, skip_serializing_if = "is_false")]
    // timed_out: bool,
    // #[serde(rename = "missingBins", default, skip_serializing_if = "CmpZero::is_zero")]
    // missing_bins: u32,
    // #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    // continue_at: Option<IsoDateTime>,
    // #[serde(rename = "finishedAt", default, skip_serializing_if = "Option::is_none")]
    // finished_at: Option<IsoDateTime>,
}

impl<EVT> ToJsonResult for ContainerBinsCollectorOutput<EVT>
where
    EVT: EventValueType,
{
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        let bins = &self.bins;
        let ts1sns: Vec<_> = bins.ts1s.iter().map(|x| x.ns()).collect();
        let ts2sns: Vec<_> = bins.ts2s.iter().map(|x| x.ns()).collect();
        let (ts_anch, ts1ms, ts1ns) = ts_offs_from_abs(&ts1sns);
        let (ts2ms, ts2ns) = ts_offs_from_abs_with_anchor(ts_anch, &ts2sns);
        let counts = bins.cnts.clone();
        let mins = bins.mins.clone();
        let maxs = bins.maxs.clone();
        let avgs = bins.avgs.clone();
        let val = ContainerBinsCollectorOutputUser::<EVT> {
            ts_anchor_sec: ts_anch,
            ts1_off_ms: ts1ms,
            ts2_off_ms: ts2ms,
            ts1_off_ns: ts1ns,
            ts2_off_ns: ts2ns,
            counts,
            mins,
            maxs,
            avgs,
        };
        serde_json::to_value(&val)
    }
}

impl<EVT> CollectedDyn for ContainerBinsCollectorOutput<EVT> where EVT: EventValueType {}

#[derive(Debug)]
pub struct ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    bins: ContainerBins<EVT>,
    timed_out: bool,
    range_final: bool,
}

impl<EVT> ContainerBinsCollector<EVT> where EVT: EventValueType {}

impl<EVT> WithLen for ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    fn len(&self) -> usize {
        self.bins.len()
    }
}

impl<EVT> items_0::container::ByteEstimate for ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    fn byte_estimate(&self) -> u64 {
        // TODO need better estimate
        self.bins.len() as u64 * 200
    }
}

impl<EVT> items_0::collect_s::CollectorDyn for ContainerBinsCollector<EVT>
where
    EVT: EventValueType,
{
    fn ingest(&mut self, src: &mut dyn CollectableDyn) {
        if let Some(src) = src.as_any_mut().downcast_mut::<ContainerBins<EVT>>() {
            src.drain_into(&mut self.bins, 0..src.len());
        } else {
            let srcn = src.type_name();
            panic!("wrong src type {srcn}");
        }
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn set_continue_at_here(&mut self) {
        debug!("TODO remember the continue at");
    }

    fn result(
        &mut self,
        range: Option<netpod::range::evrange::SeriesRange>,
        binrange: Option<netpod::BinnedRangeEnum>,
    ) -> Result<Box<dyn items_0::collect_s::CollectedDyn>, err::Error> {
        // TODO do we need to set timeout, continueAt or anything?
        let bins = mem::replace(&mut self.bins, ContainerBins::new());
        let ret = ContainerBinsCollectorOutput { bins };
        Ok(Box::new(ret))
    }
}

impl<EVT> CollectableDyn for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn new_collector(&self) -> Box<dyn items_0::collect_s::CollectorDyn> {
        let ret = ContainerBinsCollector::<EVT> {
            bins: ContainerBins::new(),
            timed_out: false,
            range_final: false,
        };
        Box::new(ret)
    }
}

impl<EVT> BinningggContainerBinsDyn for ContainerBins<EVT>
where
    EVT: EventValueType,
{
    fn type_name(&self) -> &'static str {
        any::type_name::<Self>()
    }

    fn empty(&self) -> BinsBoxed {
        Box::new(Self::new())
    }

    fn clone(&self) -> BinsBoxed {
        Box::new(<Self as Clone>::clone(self))
    }

    fn edges_iter(
        &self,
    ) -> std::iter::Zip<std::collections::vec_deque::Iter<TsNano>, std::collections::vec_deque::Iter<TsNano>> {
        self.ts1s.iter().zip(self.ts2s.iter())
    }

    fn drain_into(&mut self, dst: &mut dyn BinningggContainerBinsDyn, range: std::ops::Range<usize>) {
        let obj = dst.as_any_mut();
        if let Some(dst) = obj.downcast_mut::<Self>() {
            dst.ts1s.extend(self.ts1s.drain(range.clone()));
            dst.ts2s.extend(self.ts2s.drain(range.clone()));
            dst.cnts.extend(self.cnts.drain(range.clone()));
            dst.mins.extend(self.mins.drain(range.clone()));
            dst.maxs.extend(self.maxs.drain(range.clone()));
            dst.avgs.extend(self.avgs.drain(range.clone()));
            dst.lsts.extend(self.lsts.drain(range.clone()));
            dst.fnls.extend(self.fnls.drain(range.clone()));
        } else {
            let styn = any::type_name::<EVT>();
            panic!("unexpected drain  EVT {}  dst {}", styn, Self::type_name());
        }
    }

    fn fix_numerics(&mut self) {
        for ((min, max), avg) in self.mins.iter_mut().zip(self.maxs.iter_mut()).zip(self.avgs.iter_mut()) {}
    }
}

pub struct ContainerBinsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    evs: &'a mut ContainerBins<EVT>,
    len: usize,
}

impl<'a, EVT> ContainerBinsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn new(evs: &'a mut ContainerBins<EVT>, len: usize) -> Self {
        let len = len.min(evs.len());
        Self { evs, len }
    }
}

impl<'a, EVT> ContainerBinsTakeUpTo<'a, EVT>
where
    EVT: EventValueType,
{
    pub fn ts1_first(&self) -> Option<TsNano> {
        self.evs.ts1_first()
    }

    pub fn ts2_last(&self) -> Option<TsNano> {
        self.evs.ts2_last()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn pop_front(&mut self) -> Option<BinSingle<EVT>> {
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
