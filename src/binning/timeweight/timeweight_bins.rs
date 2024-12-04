use crate::binning::container::bins::AggBinValTw;
use crate::binning::container::bins::BinAggedType;
use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::EventValueType;
use crate::binning::container_events::PartialOrdEvtA;
use crate::log::*;
use items_0::timebin::BinnedBinsTimeweightTrait;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use netpod::BinnedRange;
use netpod::TsNano;
use std::any;

macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

macro_rules! trace_ingest_bin { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug, thiserror::Error)]
#[cstm(name = "BinBinsTimeweight")]
pub enum Error {}

#[derive(Debug)]
pub struct BinnedBinsTimeweight<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    range: BinnedRange<TsNano>,
    active_beg: TsNano,
    active_end: TsNano,
    cnt: u64,
    min: Option<EVT>,
    max: Option<EVT>,
    lst: Option<EVT>,
    agg: <BVT as BinAggedType>::AggregatorTw,
    non_fnl: bool,
    out: ContainerBins<EVT, BVT>,
}

impl<EVT, BVT> BinnedBinsTimeweight<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        trace_init!("BinnedBinsTimeweight::new  {}", range);
        let active_beg = range.nano_beg();
        let active_end = active_beg.add_dt_nano(range.bin_len_dt_ns());
        Self {
            range,
            active_beg,
            active_end,
            cnt: 0,
            min: None,
            max: None,
            lst: None,
            agg: BVT::AggregatorTw::new(),
            non_fnl: false,
            out: ContainerBins::new(),
        }
    }

    fn maybe_emit_active(&mut self) {
        if self.cnt != 0 {
            let ts1 = self.active_beg;
            let ts2 = self.active_end;
            let cnt = self.cnt;
            let min = self.min.as_ref().unwrap().clone();
            let max = self.max.as_ref().unwrap().clone();
            let fr = 1.;
            let agg = self.agg.result(fr);
            self.agg.reset_for_new_bin();
            let lst = self.lst.as_ref().unwrap().clone();
            let fnl = self.non_fnl == false;
            self.out.push_back(ts1, ts2, cnt, min, max, agg, lst, fnl);
        }
    }

    fn active_forward(&mut self, ts1: TsNano) {
        self.cnt = 0;
        self.min = self.lst.clone();
        self.max = self.lst.clone();
        let bl = self.range.bin_len_dt_ns();
        let tsnext = TsNano::from_ns(ts1.ns() / bl.ns() * bl.ns());
        self.active_beg = tsnext;
        self.active_end = tsnext.add_dt_nano(bl);
        self.non_fnl = false;
    }

    fn bound(a: &mut Option<EVT>, b: <EVT as EventValueType>::IterTy1<'_>, d: std::cmp::Ordering) {
        if let Some(x) = a.as_mut() {
            match b.cmp_a(x) {
                Some(x) if x == d => {
                    *a = Some(b.into());
                }
                Some(_) | None => {}
            }
        } else {
            *a = Some(b.into());
        }
    }

    fn ingest_bins(&mut self, bins: &ContainerBins<EVT, BVT>) -> Result<(), BinningggError> {
        for (((((((&ts1, &ts2), &cnt), min), max), agg), lst), &fnl) in bins.zip_iter() {
            let grid = self.range.bin_len_dt_ns();
            trace_ingest_bin!("grid {:?}  ts1 {:?}  agg {:?}", grid, ts1, agg);
            if ts1 < self.active_beg {
                self.lst = Some(lst.into());
            } else {
                if ts1 >= self.active_end {
                    self.maybe_emit_active();
                    self.active_forward(ts1);
                }
                self.cnt += cnt;
                Self::bound(&mut self.min, min, std::cmp::Ordering::Less);
                Self::bound(&mut self.max, max, std::cmp::Ordering::Greater);
                let dt = ts2.delta(ts1);
                let bl = self.range.bin_len_dt_ns();
                self.agg.ingest(dt, bl, cnt, agg.into());
                self.non_fnl |= !fnl;
                self.lst = Some(lst.into());
            }
        }
        Ok(())
    }
}

impl<EVT, BVT> BinnedBinsTimeweightTrait for BinnedBinsTimeweight<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn ingest(&mut self, bins: &BinsBoxed) -> Result<(), BinningggError> {
        if let Some(bins) = bins.as_any_ref().downcast_ref::<ContainerBins<EVT, BVT>>() {
            self.ingest_bins(bins)
        } else {
            Err(BinningggError::TypeMismatch {
                have: bins.type_name().into(),
                expect: any::type_name::<BVT>().into(),
            })
        }
    }

    fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        self.maybe_emit_active();
        self.active_forward(self.active_beg.add_dt_nano(self.range.bin_len_dt_ns()));
        Ok(())
    }

    fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        self.non_fnl = true;
        self.maybe_emit_active();
        self.active_forward(self.active_beg.add_dt_nano(self.range.bin_len_dt_ns()));
        Ok(())
    }

    fn output(&mut self) -> Result<Option<BinsBoxed>, BinningggError> {
        if self.out.len() == 0 {
            Ok(None)
        } else {
            let ret = std::mem::replace(&mut self.out, ContainerBins::new());
            Ok(Some(Box::new(ret)))
        }
    }
}
