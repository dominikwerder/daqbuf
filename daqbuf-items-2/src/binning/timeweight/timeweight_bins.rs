use crate::binning::container::bins::AggBinValTw;
use crate::binning::container::bins::BinAggedType;
use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::EventValueType;
use crate::binning::container_events::PartialOrdEvtA;
use crate::log;
use items_0::timebin::BinnedBinsTimeweightTrait;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use netpod::BinnedRange;
use netpod::TsNano;
use serde::Serialize;
use std::any;

macro_rules! trace_init { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); }) }

macro_rules! trace_ingest_bin { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); }) }

macro_rules! trace_emit { ($($arg:tt)*) => ( if false { log::trace!("BIN EMIT  {}", format_args!($($arg)*)); }) }

autoerr::create_error_v1!(
    name(Error, "BinBinsTimeweight"),
    enum variants {
        InputBinlenOverflow,
        Logic,
    },
);

#[derive(Debug, Serialize)]
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
    fraction_filled: f32,
    // TODO
    #[serde(skip)]
    agg: <BVT as BinAggedType>::AggregatorTw,
    non_fnl: bool,
    out: ContainerBins<EVT, BVT>,
    produce_cnt_zero: bool,
}

impl<EVT, BVT> BinnedBinsTimeweight<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        trace_init!("BinnedBinsTimeweight::new  {}", range);
        let binlen = range.bin_len_dt_ns();
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
            fraction_filled: 1.,
            agg: BVT::AggregatorTw::new(binlen),
            non_fnl: false,
            out: ContainerBins::new(),
            produce_cnt_zero: false,
        }
    }

    pub fn cnt_zero_enable(&mut self) {
        self.produce_cnt_zero = true;
    }

    fn reset_for_new_bin(&mut self) {
        self.cnt = 0;
        self.agg.reset_for_new_bin();
        if self.lst.is_some() {
            self.fraction_filled = 1.;
        }
    }

    fn maybe_emit_active(&mut self) {
        let selfname = "maybe_emit_active";
        if self.cnt != 0 || self.produce_cnt_zero && self.min.is_some() {
            let ts1 = self.active_beg;
            let ts2 = self.active_end;
            let cnt = self.cnt;
            let min = self.min.as_ref().unwrap().clone();
            let max = self.max.as_ref().unwrap().clone();
            let agg = self.agg.result();
            let lst = self.lst.as_ref().unwrap().clone();
            let fnl = self.non_fnl == false;
            trace_emit!(
                "{selfname}  push out  {}  {}  cnt {}  min {:?}  max {:?}  agg {:?}  lst {:?}  fnl {:?}",
                ts1,
                ts2,
                cnt,
                min,
                max,
                agg,
                lst,
                fnl
            );
            self.out.push_back(ts1, ts2, cnt, min, max, agg, lst, fnl);
        } else {
            trace_emit!(
                "{selfname}  do NOT produce bin  cnt {cnt}  cz {cz}  lst {lst:?}  min {min:?}  max {max:?}",
                cnt = self.cnt,
                cz = self.produce_cnt_zero,
                lst = self.lst,
                min = self.min,
                max = self.max
            );
        }
        self.reset_for_new_bin();
    }

    fn active_forward(&mut self, ts1: TsNano) {
        let selfname = "active_forward";
        trace_emit!(
            "{selfname}  CUR  {beg}  {end}",
            beg = self.active_beg,
            end = self.active_end
        );
        if self.produce_cnt_zero {
            // TODO
            // actually produce cnt-zero bins
        } else {
        }
        self.min = self.lst.clone();
        self.max = self.lst.clone();
        let bl = self.range.bin_len_dt_ns();
        let tsnext = TsNano::from_ns(ts1.ns() / bl.ns() * bl.ns());
        self.active_beg = tsnext;
        self.active_end = tsnext.add_dt_nano(bl);
        self.non_fnl = false;
        trace_emit!(
            "{selfname}  NEW  {beg}  {end}",
            beg = self.active_beg,
            end = self.active_end
        );
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
        trace_ingest_bin!("\n\n+++++++++++++\n\ningest_bins  active_beg {}", self.active_beg);
        for (((((((&ts1, &ts2), &cnt), min), max), agg), lst), &fnl) in bins.zip_iter() {
            let binlen = self.range.bin_len_dt_ns();
            trace_ingest_bin!(
                "ingest_bins  + + + +  binlen {:?} s  ts1 {:?}  agg {:?}",
                binlen.ms_u64() / 1000,
                ts1,
                agg
            );
            if ts1 < self.active_beg {
                trace_ingest_bin!("before active-beg: just set lst");
                self.lst = Some(lst.into());
            } else {
                if ts1 >= self.active_end {
                    trace_ingest_bin!("{}", "ingest loop finish current bin A");
                    self.maybe_emit_active();
                    self.active_forward(ts1);
                }
                if ts2 > self.active_end {
                    trace_ingest_bin!("{}", "ingest loop finish current bin B");
                    self.maybe_emit_active();
                    self.active_forward(ts2);
                }
                if ts1 == self.active_beg {
                    trace_ingest_bin!("{}", "set minmax");
                    self.min = Some(min.clone().into());
                    self.max = Some(max.clone().into());
                }
                self.cnt += cnt;
                Self::bound(&mut self.min, min, std::cmp::Ordering::Less);
                Self::bound(&mut self.max, max, std::cmp::Ordering::Greater);
                let dt = ts2.delta(ts1);
                if dt > binlen {
                    return Err(BinningggError::Dyn(Box::new(Error::InputBinlenOverflow)));
                }
                trace_ingest_bin!("dt {} s", dt.ms_u64() / 1000);
                self.agg.ingest(dt, agg.into());
                self.non_fnl |= !fnl;
                self.lst = Some(lst.into());
                if ts2 >= self.active_end {
                    trace_ingest_bin!("{}", "ingest loop finish current bin C");
                    self.maybe_emit_active();
                    self.active_forward(ts2);
                }
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
    fn cnt_zero_enable(&mut self) {
        self.cnt_zero_enable();
    }

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

#[test]
fn test_input_not_covering_first_bin() {
    use netpod::DtMs;
    use netpod::range::evrange::NanoRange;
    let range = NanoRange::from_strings("1970-01-01T00:10:00Z", "1970-01-01T00:20:00Z").unwrap();
    let binlen = DtMs::from_ms_u64(1000 * 10);
    let range = BinnedRange::from_nano_range(range, binlen);
    let mut inp = ContainerBins::new();
    let ts1 = TsNano::from_ms(1000 * 60 * 10 + 1000 * 0);
    let ts2 = TsNano::from_ms(1000 * 60 * 10 + 1000 * 9);
    inp.push_back(ts1, ts2, 1, 1.8, 2.2, 2.0, 1.9, true);
    let mut binner = BinnedBinsTimeweight::<f32, f32>::new(range);
    binner.ingest_bins(&inp).unwrap();
    assert!(binner.output().unwrap().is_none());
}

#[test]
fn test_00() {
    use netpod::DtMs;
    use netpod::range::evrange::NanoRange;
    let range = NanoRange::from_strings("1970-01-01T00:10:00Z", "1970-01-01T00:20:00Z").unwrap();
    let binlen = DtMs::from_ms_u64(1000 * 10);
    let range = BinnedRange::from_nano_range(range, binlen);
    let mut inp = ContainerBins::new();
    let ts1 = TsNano::from_ms(1000 * 60 * 10 + 1000 * 0);
    let ts2 = ts1.add_dt_nano(binlen.dt_ns());
    inp.push_back(ts1, ts2, 1, 1.8, 2.2, 2.0, 1.9, true);
    // let ts1 = TsNano::from_ms(1000 * 60 * 10 + 1000 * 10);
    // let ts2 = ts1.add_dt_nano(binlen.dt_ns());
    // inp.push_back(ts1, ts2, 1, 1.8, 2.2, 2.0, 1.9, true);
    let mut binner = BinnedBinsTimeweight::<f32, f32>::new(range);
    binner.ingest_bins(&inp).unwrap();
    let out = binner.output().unwrap().unwrap();
    if let Some(bins) = out.as_any_ref().downcast_ref::<ContainerBins<f32, f32>>() {
        for x in bins.zip_iter_2() {
            eprintln!("{x:?}");
        }
    } else {
        panic!()
    }
}
