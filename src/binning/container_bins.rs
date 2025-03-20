use super::container::bins::BinAggedType;
use super::container_events::Container;
use super::container_events::EventValueType;
use crate::apitypes::ContainerBinsApi;
use crate::binning::container::bins::BinAggedContainer;
use crate::log::*;
use core::fmt;
use daqbuf_err as err;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::TypeName;
use items_0::WithLen;
use items_0::apitypes::ToUserFacingApiType;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::container::ByteEstimate;
use items_0::merge::DrainIntoDstResult;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinsBoxed;
use items_0::vecpreview::VecPreview;
use netpod::TsNano;
use netpod::f32_close;
use std::any;
use std::collections::VecDeque;
use std::mem;

autoerr::create_error_v1!(
    name(ContainerBinsError, "ContainerBins"),
    enum variants {
        Unordered,
    },
);

#[derive(Debug, Clone)]
pub struct BinRef<'a, EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub ts1: TsNano,
    pub ts2: TsNano,
    pub cnt: u64,
    pub min: EVT::IterTy1<'a>,
    pub max: EVT::IterTy1<'a>,
    pub agg: BVT::IterTy1<'a>,
    pub lst: EVT::IterTy1<'a>,
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
        use crate::binning::container_events::Container;
        if self.ix < self.bins.len() && self.ix < self.len {
            let b = &self.bins;
            let i = self.ix;
            self.ix += 1;
            let ret = BinRef {
                ts1: b.ts1s[i],
                ts2: b.ts2s[i],
                cnt: b.cnts[i],
                min: b.mins.get_iter_ty_1(i).unwrap(),
                max: b.maxs.get_iter_ty_1(i).unwrap(),
                agg: BinAggedContainer::get_iter_ty_1(&b.aggs, i).unwrap(),
                lst: b.lsts.get_iter_ty_1(i).unwrap(),
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
    mins: <EVT as EventValueType>::Container,
    maxs: <EVT as EventValueType>::Container,
    aggs: <BVT as BinAggedType>::Container,
    lsts: <EVT as EventValueType>::Container,
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
        fn serialize<S>(&self, _ser: S) -> Result<S::Ok, S::Error>
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
        fn deserialize<D>(_de: D) -> Result<Self, D::Error>
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
    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            ts1s: VecDeque::new(),
            ts2s: VecDeque::new(),
            cnts: VecDeque::new(),
            mins: <<EVT as EventValueType>::Container as Container<EVT>>::new(),
            maxs: <<EVT as EventValueType>::Container as Container<EVT>>::new(),
            aggs: <<BVT as BinAggedType>::Container as BinAggedContainer<BVT>>::new(),
            lsts: <<EVT as EventValueType>::Container as Container<EVT>>::new(),
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

    pub fn mins_iter(&self) -> impl Iterator<Item = EVT::IterTy1<'_>> {
        self.mins.iter_ty_1()
    }

    pub fn maxs_iter(&self) -> impl Iterator<Item = EVT::IterTy1<'_>> {
        self.maxs.iter_ty_1()
    }

    pub fn aggs_iter(&self) -> impl Iterator<Item = BVT::IterTy1<'_>> {
        self.aggs.iter_ty_1()
    }

    pub fn lsts_iter(&self) -> impl Iterator<Item = EVT::IterTy1<'_>> {
        self.lsts.iter_ty_1()
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
                        impl Iterator<Item = EVT::IterTy1<'_>>,
                    >,
                    impl Iterator<Item = EVT::IterTy1<'_>>,
                >,
                impl Iterator<Item = BVT::IterTy1<'_>>,
            >,
            impl Iterator<Item = EVT::IterTy1<'_>>,
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

    pub fn zip_iter_2(
        &self,
    ) -> impl Iterator<
        Item = (
            TsNano,
            TsNano,
            u64,
            EVT::IterTy1<'_>,
            EVT::IterTy1<'_>,
            BVT::IterTy1<'_>,
            EVT::IterTy1<'_>,
            bool,
        ),
    > {
        let bins = self;
        itertools::izip!(
            bins.ts1s_iter().map(Clone::clone),
            bins.ts2s_iter().map(Clone::clone),
            bins.cnts_iter().map(Clone::clone),
            bins.mins_iter(),
            bins.maxs_iter(),
            bins.aggs_iter(),
            bins.lsts_iter(),
            bins.fnls_iter().map(Clone::clone),
        )
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

impl<EVT, BVT> ByteEstimate for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn byte_estimate(&self) -> u32 {
        // TODO
        self.len() as u32 * 800
    }
}

pub fn compare_boxed_f32(lhs: &ContainerBins<f32, f32>, rhs: &ContainerBins<f32, f32>) -> bool {
    if let Some(lhs) = lhs.as_any_ref().downcast_ref::<ContainerBins<f32, f32>>() {
        if let Some(rhs) = rhs.as_any_ref().downcast_ref::<ContainerBins<f32, f32>>() {
            if lhs.len() != rhs.len() {
                error!("length differ");
                false
            } else {
                for (a, b) in lhs.zip_iter_2().zip(rhs.zip_iter_2()) {
                    if a.0 != b.0 {
                        error!("ts1 differ");
                        return false;
                    }
                    if a.1 != b.1 {
                        error!("ts2 differ");
                        return false;
                    }
                    if a.2 != b.2 {
                        error!("cnt differ  {:?}  {:?}", a, b);
                        return false;
                    }
                    if !f32_close(a.3, b.3) {
                        error!("min differ  {:?}  {:?}", a, b);
                        return false;
                    }
                    if !f32_close(a.4, b.4) {
                        error!("max differ  {:?}  {:?}", a, b);
                        return false;
                    }
                    if !f32_close(a.5, b.5) {
                        error!("agg differ  {:?}  {:?}", a, b);
                        return false;
                    }
                    if !f32_close(a.6, b.6) {
                        error!("lst differ  {:?}  {:?}", a, b);
                        return false;
                    }
                }
                true
            }
        } else {
            panic!("lhs is not bins f32")
        }
    } else {
        panic!("lhs is not bins f32")
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

impl<EVT, BVT> ToUserFacingApiType for ContainerBinsCollectorOutput<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn into_user_facing_api_type(self) -> Box<dyn items_0::apitypes::UserApiType> {
        let ret = ContainerBinsApi::<EVT, BVT> {
            ts1s: self.bins.ts1s,
            ts2s: self.bins.ts2s,
            cnts: self.bins.cnts,
            mins: self.bins.mins,
            maxs: self.bins.maxs,
            aggs: self.bins.aggs,
            fnls: self.bins.fnls,
        };
        Box::new(ret)
    }

    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn items_0::apitypes::UserApiType> {
        (*self).into_user_facing_api_type()
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
    fn byte_estimate(&self) -> u32 {
        // TODO need better estimate
        self.bins.len() as u32 * 400
    }
}

impl<EVT, BVT> items_0::collect_s::CollectorDyn for ContainerBinsCollector<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn ingest(&mut self, src: &mut dyn CollectableDyn) {
        if let Some(src) = src.as_any_mut().downcast_mut::<ContainerBins<EVT, BVT>>() {
            MergeableTy::drain_into(src, &mut self.bins, 0..src.len());
        } else {
            // TODO let trait return Result to avoid potential panic
            let src_name = src.type_name();
            let self_name = any::type_name::<Self>();
            panic!("wrong src type  self_name {self_name}  src_name {src_name}");
        }
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn result(&mut self) -> Result<Box<dyn items_0::collect_s::CollectedDyn>, err::Error> {
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
            self.mins.drain_into(&mut dst.mins, range.clone());
            self.maxs.drain_into(&mut dst.maxs, range.clone());
            self.aggs.drain_into(&mut dst.aggs, range.clone());
            self.lsts.drain_into(&mut dst.lsts, range.clone());
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

    fn boxed_into_collectable_box(self: Box<Self>) -> Box<dyn CollectableDyn> {
        Box::new(*self)
    }

    fn fix_numerics(&mut self) {
        if let Some(bins) = self.as_any_mut().downcast_mut::<ContainerBins<f32, f32>>() {
            for ((min, max), agg) in bins
                .mins
                .iter_mut()
                .zip(bins.maxs.iter_mut())
                .zip(bins.aggs.iter_mut())
            {
                *agg = agg.min(*max).max(*min)
            }
        }
    }
}

impl<EVT, BVT> MergeableTy for ContainerBins<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn ts_min(&self) -> Option<TsNano> {
        self.ts1s.front().copied()
    }

    fn ts_max(&self) -> Option<TsNano> {
        self.ts1s.back().copied()
    }

    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize> {
        let x = self.ts1s.partition_point(|&x| x <= ts);
        if x >= self.ts1s.len() { None } else { Some(x) }
    }

    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize> {
        let x = self.ts1s.partition_point(|&x| x < ts);
        if x >= self.ts1s.len() { None } else { Some(x) }
    }

    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize> {
        let x = self.ts1s.partition_point(|&x| x < ts);
        if x == 0 { None } else { Some(x - 1) }
    }

    fn tss_for_testing(&self) -> VecDeque<TsNano> {
        self.ts1s.clone()
    }

    fn drain_into(
        &mut self,
        dst: &mut Self,
        range: std::ops::Range<usize>,
    ) -> items_0::merge::DrainIntoDstResult {
        dst.ts1s.extend(self.ts1s.drain(range.clone()));
        dst.ts2s.extend(self.ts2s.drain(range.clone()));
        dst.cnts.extend(self.cnts.drain(range.clone()));
        self.mins.drain_into(&mut dst.mins, range.clone());
        self.maxs.drain_into(&mut dst.maxs, range.clone());
        self.aggs.drain_into(&mut dst.aggs, range.clone());
        self.lsts.drain_into(&mut dst.lsts, range.clone());
        dst.fnls.extend(self.fnls.drain(range.clone()));
        DrainIntoDstResult::Done
    }

    fn drain_into_new(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> items_0::merge::DrainIntoNewResult<Self> {
        let mut dst = Self::new();
        MergeableTy::drain_into(self, &mut dst, range);
        DrainIntoNewResult::Done(dst)
    }

    fn is_strict_monotonic(&self) -> bool {
        let mut mono = true;
        let n = self.ts1s.len();
        for (&ts_a, &ts_b) in self.ts1s.iter().zip(self.ts1s.range(n.min(1)..)) {
            if ts_a >= ts_b {
                mono = false;
                error!("non-monotonic event data  ts1 {}  ts2 {}", ts_a, ts_b);
                break;
            }
        }
        mono
    }

    fn is_consistent(&self) -> bool {
        let mut good = true;
        good &= self.is_strict_monotonic();
        let n = self.ts1s.len();
        let mut same_len = true;
        same_len &= n == self.ts2s.len();
        same_len &= n == self.cnts.len();
        same_len &= n == self.mins.len();
        same_len &= n == self.ts2s.len();
        good &= same_len;
        good
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
