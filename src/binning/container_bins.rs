use super::container::bins::BinAggedType;
use super::container_events::EventValueType;
use crate::offsets::ts_offs_from_abs;
use crate::offsets::ts_offs_from_abs_with_anchor;
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

#[derive(Debug, Clone)]
pub struct BinRef<'a, EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub ts1: TsNano,
    pub ts2: TsNano,
    pub cnt: u64,
    pub min: &'a EVT,
    pub max: &'a EVT,
    pub agg: &'a BVT,
    pub lst: &'a EVT,
    pub fnl: bool,
}

pub struct IterDebug<'a, EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    bins: &'a ContainerBins<EVT, BVT>,
    ix: usize,
    len: usize,
}

impl<'a, EVT, BVT> Iterator for IterDebug<'a, EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    type Item = BinRef<'a, EVT, BVT>;

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
                agg: &b.aggs[i],
                lst: &b.lsts[i],
                fnl: b.fnls[i],
            };
            Some(ret)
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    ts1s: VecDeque<TsNano>,
    ts2s: VecDeque<TsNano>,
    cnts: VecDeque<u64>,
    mins: VecDeque<EVT>,
    maxs: VecDeque<EVT>,
    aggs: VecDeque<BVT>,
    lsts: VecDeque<EVT>,
    fnls: VecDeque<bool>,
}

mod container_bins_serde {
    use super::ContainerBins;
    use super::EventValueType;
    use crate::binning::container::bins::BinAggedType;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    impl<EVT, BVT> Serialize for ContainerBins<EVT, BVT>
    where
        EVT: EventValueType,
        BVT: BinAggedType,
    {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            todo!()
        }
    }

    impl<'de, EVT, BVT> Deserialize<'de> for ContainerBins<EVT, BVT>
    where
        EVT: EventValueType,
        BVT: BinAggedType,
    {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            todo!()
        }
    }
}

impl<EVT, BVT> ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub fn from_constituents(
        ts1s: VecDeque<TsNano>,
        ts2s: VecDeque<TsNano>,
        cnts: VecDeque<u64>,
        mins: VecDeque<EVT>,
        maxs: VecDeque<EVT>,
        aggs: VecDeque<BVT>,
        lsts: VecDeque<EVT>,
        fnls: VecDeque<bool>,
    ) -> Self {
        Self {
            ts1s,
            ts2s,
            cnts,
            mins,
            maxs,
            aggs,
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
            aggs: VecDeque::new(),
            lsts: VecDeque::new(),
            fnls: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.ts1s.len()
    }

    pub fn verify(&self) -> Result<(), ContainerBinsError> {
        if self
            .ts1s
            .iter()
            .zip(self.ts1s.iter().skip(1))
            .any(|(&a, &b)| a > b)
        {
            return Err(ContainerBinsError::Unordered);
        }
        if self
            .ts2s
            .iter()
            .zip(self.ts2s.iter().skip(1))
            .any(|(&a, &b)| a > b)
        {
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

    pub fn aggs_iter(&self) -> std::collections::vec_deque::Iter<BVT> {
        self.aggs.iter()
    }

    pub fn lsts_iter(&self) -> std::collections::vec_deque::Iter<EVT> {
        self.lsts.iter()
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
                std::collections::vec_deque::Iter<BVT>,
            >,
            std::collections::vec_deque::Iter<EVT>,
        >,
        std::collections::vec_deque::Iter<bool>,
    > {
        self.ts1s_iter()
            .zip(self.ts2s_iter())
            .zip(self.cnts_iter())
            .zip(self.mins_iter())
            .zip(self.maxs_iter())
            .zip(self.aggs_iter())
            .zip(self.lsts_iter())
            .zip(self.fnls_iter())
    }

    pub fn edges_iter(
        &self,
    ) -> std::iter::Zip<
        std::collections::vec_deque::Iter<TsNano>,
        std::collections::vec_deque::Iter<TsNano>,
    > {
        self.ts1s.iter().zip(self.ts2s.iter())
    }

    pub fn len_before(&self, end: TsNano) -> usize {
        let pp = self.ts2s.partition_point(|&x| x <= end);
        assert!(
            pp <= self.len(),
            "len_before  pp {}  len {}",
            pp,
            self.len()
        );
        pp
    }

    pub fn push_back(
        &mut self,
        ts1: TsNano,
        ts2: TsNano,
        cnt: u64,
        min: EVT,
        max: EVT,
        agg: BVT,
        lst: EVT,
        fnl: bool,
    ) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.cnts.push_back(cnt);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.aggs.push_back(agg);
        self.lsts.push_back(lst);
        self.fnls.push_back(fnl);
    }

    pub fn iter_debug(&self) -> IterDebug<EVT, BVT> {
        IterDebug {
            bins: self,
            ix: 0,
            len: self.len(),
        }
    }
}

impl<EVT, BVT> fmt::Debug for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  {{  len: {:?},  ts1s: {:?},  ts2s: {:?}, cnts: {:?},  aggs {:?},  fnls {:?}  }}",
            self.len(),
            VecPreview::new(&self.ts1s),
            VecPreview::new(&self.ts2s),
            VecPreview::new(&self.cnts),
            VecPreview::new(&self.aggs),
            VecPreview::new(&self.fnls),
        )
    }
}

impl<EVT, BVT> fmt::Display for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl<EVT, BVT> AsAnyMut for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
}

impl<EVT, BVT> WithLen for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn len(&self) -> usize {
        Self::len(self)
    }
}

impl<EVT, BVT> TypeName for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn type_name(&self) -> String {
        Self::type_name().into()
    }
}

impl<EVT, BVT> AsAnyRef for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn as_any_ref(&self) -> &dyn any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    bins: ContainerBins<EVT, BVT>,
}

impl<EVT, BVT> TypeName for ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<EVT, BVT> AsAnyRef for ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn as_any_ref(&self) -> &dyn any::Any {
        self
    }
}

impl<EVT, BVT> AsAnyMut for ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn as_any_mut(&mut self) -> &mut dyn any::Any {
        self
    }
}

impl<EVT, BVT> WithLen for ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn len(&self) -> usize {
        self.bins.len()
    }
}

#[derive(Debug, Serialize)]
struct ContainerBinsCollectorOutputUser<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
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
    aggs: VecDeque<BVT>,
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

impl<EVT, BVT> ToJsonResult for ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
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
        let aggs = bins.aggs.clone();
        let val = ContainerBinsCollectorOutputUser::<EVT, BVT> {
            ts_anchor_sec: ts_anch,
            ts1_off_ms: ts1ms,
            ts2_off_ms: ts2ms,
            ts1_off_ns: ts1ns,
            ts2_off_ns: ts2ns,
            counts,
            mins,
            maxs,
            aggs,
        };
        serde_json::to_value(&val)
    }
}

impl<EVT, BVT> CollectedDyn for ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
}

#[derive(Debug)]
pub struct ContainerBinsCollector<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    bins: ContainerBins<EVT, BVT>,
    timed_out: bool,
    range_final: bool,
}

impl<EVT, BVT> ContainerBinsCollector<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
}

impl<EVT, BVT> WithLen for ContainerBinsCollector<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn len(&self) -> usize {
        self.bins.len()
    }
}

impl<EVT, BVT> items_0::container::ByteEstimate for ContainerBinsCollector<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn byte_estimate(&self) -> u64 {
        // TODO need better estimate
        self.bins.len() as u64 * 200
    }
}

impl<EVT, BVT> items_0::collect_s::CollectorDyn for ContainerBinsCollector<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn ingest(&mut self, src: &mut dyn CollectableDyn) {
        if let Some(src) = src.as_any_mut().downcast_mut::<ContainerBins<EVT, BVT>>() {
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
        _range: Option<netpod::range::evrange::SeriesRange>,
        _binrange: Option<netpod::BinnedRangeEnum>,
    ) -> Result<Box<dyn items_0::collect_s::CollectedDyn>, err::Error> {
        // TODO do we need to set timeout, continueAt or anything?
        let bins = mem::replace(&mut self.bins, ContainerBins::new());
        let ret = ContainerBinsCollectorOutput { bins };
        Ok(Box::new(ret))
    }
}

impl<EVT, BVT> CollectableDyn for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn new_collector(&self) -> Box<dyn items_0::collect_s::CollectorDyn> {
        let ret = ContainerBinsCollector::<EVT, BVT> {
            bins: ContainerBins::new(),
            timed_out: false,
            range_final: false,
        };
        Box::new(ret)
    }
}

impl<EVT, BVT> BinningggContainerBinsDyn for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn empty(&self) -> BinsBoxed {
        Box::new(Self::new())
    }

    fn clone(&self) -> BinsBoxed {
        Box::new(<Self as Clone>::clone(self))
    }

    fn edges_iter(
        &self,
    ) -> std::iter::Zip<
        std::collections::vec_deque::Iter<TsNano>,
        std::collections::vec_deque::Iter<TsNano>,
    > {
        self.ts1s.iter().zip(self.ts2s.iter())
    }

    fn drain_into(
        &mut self,
        dst: &mut dyn BinningggContainerBinsDyn,
        range: std::ops::Range<usize>,
    ) {
        let obj = dst.as_any_mut();
        if let Some(dst) = obj.downcast_mut::<Self>() {
            dst.ts1s.extend(self.ts1s.drain(range.clone()));
            dst.ts2s.extend(self.ts2s.drain(range.clone()));
            dst.cnts.extend(self.cnts.drain(range.clone()));
            dst.mins.extend(self.mins.drain(range.clone()));
            dst.maxs.extend(self.maxs.drain(range.clone()));
            dst.aggs.extend(self.aggs.drain(range.clone()));
            dst.lsts.extend(self.lsts.drain(range.clone()));
            dst.fnls.extend(self.fnls.drain(range.clone()));
        } else {
            let styn = any::type_name::<EVT>();
            panic!("unexpected drain  EVT {}  dst {}", styn, Self::type_name());
        }
    }

    fn binned_bins_timeweight_traitobj(
        &self,
        range: netpod::BinnedRange<TsNano>,
    ) -> Box<dyn items_0::timebin::BinnedBinsTimeweightTrait> {
        let ret = super::timeweight::timeweight_bins::BinnedBinsTimeweight::<
            EVT,
            EVT::AggTimeWeightOutputAvg,
        >::new(range);
        Box::new(ret)
    }

    fn fix_numerics(&mut self) {
        for ((_min, _max), _avg) in self
            .mins
            .iter_mut()
            .zip(self.maxs.iter_mut())
            .zip(self.aggs.iter_mut())
        {}
    }
}

pub struct ContainerBinsTakeUpTo<'a, EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    evs: &'a mut ContainerBins<EVT, BVT>,
    len: usize,
}

impl<'a, EVT, BVT> ContainerBinsTakeUpTo<'a, EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub fn new(evs: &'a mut ContainerBins<EVT, BVT>, len: usize) -> Self {
        let len = len.min(evs.len());
        Self { evs, len }
    }
}

impl<'a, EVT, BVT> ContainerBinsTakeUpTo<'a, EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
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
}
