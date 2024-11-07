use crate::ts_offs_from_abs;
use crate::ts_offs_from_abs_with_anchor;
use crate::IsoDateTime;
use daqbuf_err as err;
use err::Error;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectableType;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::CollectorTy;
use items_0::collect_s::ToJsonResult;
use items_0::container::ByteEstimate;
use items_0::overlap::HasTimestampDeque;
use items_0::scalar_ops::AsPrimF32;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBinnableTy;
use items_0::timebin::TimeBinnerTy;
use items_0::timebin::TimeBins;
use items_0::vecpreview::VecPreview;
use items_0::AppendAllFrom;
use items_0::AppendEmptyBin;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::HasNonemptyFirstBin;
use items_0::Resettable;
use items_0::TypeName;
use items_0::WithLen;
use netpod::is_false;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::SEC;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::CmpZero;
use netpod::Dim0Kind;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::ops::Range;

#[allow(unused)]
macro_rules! trace_ingest { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

// TODO make members private
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct BinsDim0<NTY> {
    pub ts1s: VecDeque<u64>,
    pub ts2s: VecDeque<u64>,
    pub cnts: VecDeque<u64>,
    pub mins: VecDeque<NTY>,
    pub maxs: VecDeque<NTY>,
    pub avgs: VecDeque<f32>,
    pub lsts: VecDeque<NTY>,
    pub dim0kind: Option<Dim0Kind>,
}

impl<STY> TypeName for BinsDim0<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<NTY> fmt::Debug for BinsDim0<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        if true {
            return fmt::Display::fmt(self, fmt);
        }
        if true {
            write!(
                fmt,
                "{self_name}  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
                self.ts1s.len(),
                self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
                self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
                self.cnts,
                self.mins,
                self.maxs,
                self.avgs,
            )
        } else {
            write!(
                fmt,
                "{self_name}  count {}  edges {:?} .. {:?}  counts {:?} .. {:?}  avgs {:?} .. {:?}",
                self.ts1s.len(),
                self.ts1s.front().map(|k| k / SEC),
                self.ts2s.back().map(|k| k / SEC),
                self.cnts.front(),
                self.cnts.back(),
                self.avgs.front(),
                self.avgs.back(),
            )
        }
    }
}

impl<NTY> fmt::Display for BinsDim0<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  {{  len: {:?},  ts1s: {:?},  ts2s {:?},  counts {:?},  mins {:?},  maxs {:?},  avgs {:?}, lsts {:?}  }}",
            self.len(),
            VecPreview::new(&self.ts1s),
            VecPreview::new(&self.ts2s),
            VecPreview::new(&self.cnts),
            VecPreview::new(&self.mins),
            VecPreview::new(&self.maxs),
            VecPreview::new(&self.avgs),
            VecPreview::new(&self.lsts),
        )
    }
}

impl<NTY: ScalarOps> BinsDim0<NTY> {
    pub fn push(&mut self, ts1: u64, ts2: u64, count: u64, min: NTY, max: NTY, avg: f32, lst: NTY) {
        if avg < min.as_prim_f32_b() || avg > max.as_prim_f32_b() {
            // TODO rounding issues?
            debug!("bad avg");
        }
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.cnts.push_back(count);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
        self.lsts.push_back(lst);
    }

    pub fn equal_slack(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        for (&a, &b) in self.ts1s.iter().zip(other.ts1s.iter()) {
            if a != b {
                return false;
            }
        }
        for (&a, &b) in self.ts2s.iter().zip(other.ts2s.iter()) {
            if a != b {
                return false;
            }
        }
        for (a, b) in self.mins.iter().zip(other.mins.iter()) {
            if !a.equal_slack(b) {
                return false;
            }
        }
        for (a, b) in self.maxs.iter().zip(other.maxs.iter()) {
            if !a.equal_slack(b) {
                return false;
            }
        }
        for (a, b) in self.avgs.iter().zip(other.avgs.iter()) {
            if !a.equal_slack(b) {
                return false;
            }
        }
        true
    }

    // TODO make this part of a new bins trait, similar like Events trait.
    // TODO check for error?
    pub fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) -> () {
        dst.ts1s.extend(self.ts1s.drain(range.clone()));
        dst.ts2s.extend(self.ts2s.drain(range.clone()));
        dst.cnts.extend(self.cnts.drain(range.clone()));
        dst.mins.extend(self.mins.drain(range.clone()));
        dst.maxs.extend(self.maxs.drain(range.clone()));
        dst.avgs.extend(self.avgs.drain(range.clone()));
        dst.lsts.extend(self.lsts.drain(range.clone()));
    }
}

impl<NTY> AsAnyRef for BinsDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for BinsDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> Empty for BinsDim0<STY> {
    fn empty() -> Self {
        Self {
            ts1s: VecDeque::new(),
            ts2s: VecDeque::new(),
            cnts: VecDeque::new(),
            mins: VecDeque::new(),
            maxs: VecDeque::new(),
            avgs: VecDeque::new(),
            lsts: VecDeque::new(),
            dim0kind: None,
        }
    }
}

impl<STY> WithLen for BinsDim0<STY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<STY: ScalarOps> ByteEstimate for BinsDim0<STY> {
    fn byte_estimate(&self) -> u64 {
        // TODO
        // Should use a better estimate for waveform and string types,
        // or keep some aggregated byte count on push.
        let n = self.len();
        if n == 0 {
            0
        } else {
            // TODO use the actual size of one/some of the elements.
            let i = n * 2 / 3;
            let w1 = self.mins[i].byte_estimate();
            let w2 = self.maxs[i].byte_estimate();
            (n as u64 * (8 + 8 + 8 + 4 + w1 + w2)) as u64
        }
    }
}

impl<STY> Resettable for BinsDim0<STY> {
    fn reset(&mut self) {
        self.ts1s.clear();
        self.ts2s.clear();
        self.cnts.clear();
        self.mins.clear();
        self.maxs.clear();
        self.avgs.clear();
        self.lsts.clear();
    }
}

impl<STY: ScalarOps> HasNonemptyFirstBin for BinsDim0<STY> {
    fn has_nonempty_first_bin(&self) -> bool {
        self.cnts.front().map_or(false, |x| *x > 0)
    }
}

impl<STY: ScalarOps> HasTimestampDeque for BinsDim0<STY> {
    fn timestamp_min(&self) -> Option<u64> {
        self.ts1s.front().map(|x| *x)
    }

    fn timestamp_max(&self) -> Option<u64> {
        self.ts2s.back().map(|x| *x)
    }

    fn pulse_min(&self) -> Option<u64> {
        todo!()
    }

    fn pulse_max(&self) -> Option<u64> {
        todo!()
    }
}

impl<NTY: ScalarOps> AppendEmptyBin for BinsDim0<NTY> {
    fn append_empty_bin(&mut self, ts1: u64, ts2: u64) {
        debug!("AppendEmptyBin::append_empty_bin  should not get used");
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.cnts.push_back(0);
        self.mins.push_back(NTY::zero_b());
        self.maxs.push_back(NTY::zero_b());
        self.avgs.push_back(0.);
        self.lsts.push_back(NTY::zero_b());
    }
}

impl<NTY: ScalarOps> AppendAllFrom for BinsDim0<NTY> {
    fn append_all_from(&mut self, src: &mut Self) {
        debug!("AppendAllFrom::append_all_from  should not get used");
        self.ts1s.extend(src.ts1s.drain(..));
        self.ts2s.extend(src.ts2s.drain(..));
        self.cnts.extend(src.cnts.drain(..));
        self.mins.extend(src.mins.drain(..));
        self.maxs.extend(src.maxs.drain(..));
        self.avgs.extend(src.avgs.drain(..));
        self.lsts.extend(src.lsts.drain(..));
    }
}

impl<NTY: ScalarOps> TimeBins for BinsDim0<NTY> {
    fn ts_min(&self) -> Option<u64> {
        self.ts1s.front().map(Clone::clone)
    }

    fn ts_max(&self) -> Option<u64> {
        self.ts2s.back().map(Clone::clone)
    }

    fn ts_min_max(&self) -> Option<(u64, u64)> {
        if let (Some(min), Some(max)) = (self.ts1s.front().map(Clone::clone), self.ts2s.back().map(Clone::clone)) {
            Some((min, max))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct BinsDim0TimeBinnerTy<STY>
where
    STY: ScalarOps,
{
    ts1now: TsNano,
    ts2now: TsNano,
    binrange: BinnedRange<TsNano>,
    do_time_weight: bool,
    emit_empty_bins: bool,
    range_complete: bool,
    out: <Self as TimeBinnerTy>::Output,
    cnt: u64,
    min: STY,
    max: STY,
    avg: f64,
    lst: STY,
    filled_up_to: TsNano,
    last_seen_avg: f32,
}

impl<STY> BinsDim0TimeBinnerTy<STY>
where
    STY: ScalarOps,
{
    pub fn type_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new(binrange: BinnedRange<TsNano>, do_time_weight: bool, emit_empty_bins: bool) -> Self {
        // let ts1now = TsNano::from_ns(binrange.bin_off * binrange.bin_len.ns());
        // let ts2 = ts1.add_dt_nano(binrange.bin_len.to_dt_nano());
        let ts1now = TsNano::from_ns(binrange.nano_beg().ns());
        let ts2now = ts1now.add_dt_nano(binrange.bin_len.to_dt_nano());
        Self {
            ts1now,
            ts2now,
            binrange,
            do_time_weight,
            emit_empty_bins,
            range_complete: false,
            out: <Self as TimeBinnerTy>::Output::empty(),
            cnt: 0,
            min: STY::zero_b(),
            max: STY::zero_b(),
            avg: 0.,
            lst: STY::zero_b(),
            filled_up_to: ts1now,
            last_seen_avg: 0.,
        }
    }

    // used internally for the aggregation
    fn reset_agg(&mut self) {
        self.cnt = 0;
        self.min = STY::zero_b();
        self.max = STY::zero_b();
        self.avg = 0.;
    }
}

impl<STY> TimeBinnerTy for BinsDim0TimeBinnerTy<STY>
where
    STY: ScalarOps,
{
    type Input = BinsDim0<STY>;
    type Output = BinsDim0<STY>;

    fn ingest(&mut self, item: &mut Self::Input) {
        trace_ingest!("<{} as TimeBinnerTy>::ingest  {:?}", Self::type_name(), item);
        let mut count_before = 0;
        for ((((((&ts1, &ts2), &cnt), min), max), &avg), lst) in item
            .ts1s
            .iter()
            .zip(&item.ts2s)
            .zip(&item.cnts)
            .zip(&item.mins)
            .zip(&item.maxs)
            .zip(&item.avgs)
            .zip(&item.lsts)
        {
            if ts1 < self.ts1now.ns() {
                if ts2 > self.ts1now.ns() {
                    error!("{}  bad input grid mismatch", Self::type_name());
                    continue;
                }
                // warn!("encountered bin from time before  {}  {}", ts1, self.ts1now.ns());
                trace_ingest!("{}  input bin before  {}", Self::type_name(), TsNano::from_ns(ts1));
                self.min = min.clone();
                self.max = max.clone();
                self.lst = lst.clone();
                count_before += 1;
                continue;
            } else {
                if ts2 > self.ts2now.ns() {
                    if ts2 - ts1 > self.ts2now.ns() - self.ts1now.ns() {
                        panic!("incoming bin len too large");
                    } else if ts1 < self.ts2now.ns() {
                        panic!("encountered unaligned input bin");
                    } else {
                        let mut i = 0;
                        while ts1 >= self.ts2now.ns() {
                            self.cycle();
                            i += 1;
                            if i > 50000 {
                                panic!("cycle forward too many iterations");
                            }
                        }
                    }
                } else {
                    // ok, we're still inside the current bin
                }
            }
            if cnt == 0 {
                // ignore input bin, it does not contain any valid information.
            } else {
                if self.cnt == 0 {
                    self.cnt = cnt;
                    self.min = min.clone();
                    self.max = max.clone();
                    if self.do_time_weight {
                        let f = (ts2 - ts1) as f64 / (self.ts2now.ns() - self.ts1now.ns()) as f64;
                        self.avg = avg as f64 * f;
                    } else {
                        panic!("TODO non-time-weighted binning to be impl");
                    }
                } else {
                    self.cnt += cnt;
                    if *min < self.min {
                        self.min = min.clone();
                    }
                    if *max > self.max {
                        self.max = max.clone();
                    }
                    if self.do_time_weight {
                        let f = (ts2 - ts1) as f64 / (self.ts2now.ns() - self.ts1now.ns()) as f64;
                        self.avg += avg as f64 * f;
                    } else {
                        panic!("TODO non-time-weighted binning to be impl");
                    }
                }
                self.filled_up_to = TsNano::from_ns(ts2);
                self.last_seen_avg = avg;
            }
        }
        if count_before != 0 {
            warn!(
                "-----   seen {} / {} input bins from time before",
                count_before,
                item.len()
            );
        }
    }

    fn set_range_complete(&mut self) {
        self.range_complete = true;
    }

    fn bins_ready_count(&self) -> usize {
        self.out.len()
    }

    fn bins_ready(&mut self) -> Option<Self::Output> {
        if self.out.len() != 0 {
            let ret = core::mem::replace(&mut self.out, BinsDim0::empty());
            Some(ret)
        } else {
            None
        }
    }

    fn push_in_progress(&mut self, push_empty: bool) {
        if self.filled_up_to != self.ts2now {
            if self.cnt != 0 {
                info!("push_in_progress  partially filled bin");
                if self.do_time_weight {
                    let f = (self.ts2now.ns() - self.filled_up_to.ns()) as f64
                        / (self.ts2now.ns() - self.ts1now.ns()) as f64;
                    self.avg += self.lst.as_prim_f32_b() as f64 * f;
                    self.filled_up_to = self.ts2now;
                } else {
                    panic!("TODO non-time-weighted binning to be impl");
                }
            } else {
                if self.filled_up_to != self.ts1now {
                    error!("partially filled bin with cnt 0");
                }
            }
        }
        if self.cnt == 0 && !push_empty {
            self.reset_agg();
        } else {
            let min = self.min.clone();
            let max = self.max.clone();
            let avg = self.avg as f32;
            if avg < min.as_prim_f32_b() || avg > max.as_prim_f32_b() {
                // TODO rounding issues?
                debug!("bad avg");
            }
            self.out.ts1s.push_back(self.ts1now.ns());
            self.out.ts2s.push_back(self.ts2now.ns());
            self.out.cnts.push_back(self.cnt);
            self.out.mins.push_back(min);
            self.out.maxs.push_back(max);
            self.out.avgs.push_back(avg);
            self.out.lsts.push_back(self.lst.clone());
            self.reset_agg();
        }
    }

    fn cycle(&mut self) {
        self.push_in_progress(true);
        self.ts1now = self.ts1now.add_dt_nano(self.binrange.bin_len.to_dt_nano());
        self.ts2now = self.ts2now.add_dt_nano(self.binrange.bin_len.to_dt_nano());
    }

    fn empty(&self) -> Option<Self::Output> {
        Some(<Self as TimeBinnerTy>::Output::empty())
    }

    fn append_empty_until_end(&mut self) {
        let mut i = 0;
        while self.ts2now.ns() < self.binrange.full_range().end() {
            self.cycle();
            i += 1;
            if i > 100000 {
                panic!("append_empty_until_end too many iterations");
            }
        }
    }
}

impl<STY: ScalarOps> TimeBinnableTy for BinsDim0<STY> {
    type TimeBinner = BinsDim0TimeBinnerTy<STY>;

    fn time_binner_new(
        &self,
        binrange: BinnedRangeEnum,
        do_time_weight: bool,
        emit_empty_bins: bool,
    ) -> Self::TimeBinner {
        match binrange {
            BinnedRangeEnum::Time(binrange) => BinsDim0TimeBinnerTy::new(binrange, do_time_weight, emit_empty_bins),
            BinnedRangeEnum::Pulse(_) => todo!("TimeBinnableTy for BinsDim0 Pulse"),
        }
    }
}

// TODO rename to BinsDim0CollectorOutput
#[derive(Debug, Serialize, Deserialize)]
pub struct BinsDim0CollectedResult<NTY> {
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
    mins: VecDeque<NTY>,
    #[serde(rename = "maxs")]
    maxs: VecDeque<NTY>,
    #[serde(rename = "avgs")]
    avgs: VecDeque<f32>,
    #[serde(rename = "rangeFinal", default, skip_serializing_if = "is_false")]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "is_false")]
    timed_out: bool,
    #[serde(rename = "missingBins", default, skip_serializing_if = "CmpZero::is_zero")]
    missing_bins: u32,
    #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
    #[serde(rename = "finishedAt", default, skip_serializing_if = "Option::is_none")]
    finished_at: Option<IsoDateTime>,
}

// TODO temporary fix for the enum output
impl<STY> BinsDim0CollectedResult<STY>
where
    STY: ScalarOps,
{
    pub fn boxed_collected_with_enum_fix(&self) -> Box<dyn CollectedDyn> {
        if let Some(bins) = self
            .as_any_ref()
            .downcast_ref::<BinsDim0CollectedResult<netpod::EnumVariant>>()
        {
            debug!("boxed_collected_with_enum_fix");
            let mins = self.mins.iter().map(|x| 6).collect();
            let maxs = self.mins.iter().map(|x| 7).collect();
            let bins = BinsDim0CollectedResult::<u16> {
                ts_anchor_sec: self.ts_anchor_sec.clone(),
                ts1_off_ms: self.ts1_off_ms.clone(),
                ts2_off_ms: self.ts2_off_ms.clone(),
                ts1_off_ns: self.ts1_off_ns.clone(),
                ts2_off_ns: self.ts2_off_ns.clone(),
                counts: self.counts.clone(),
                mins,
                maxs,
                avgs: self.avgs.clone(),
                range_final: self.range_final.clone(),
                timed_out: self.timed_out.clone(),
                missing_bins: self.missing_bins.clone(),
                continue_at: self.continue_at.clone(),
                finished_at: self.finished_at.clone(),
            };
            Box::new(bins)
        } else {
            let bins = Self {
                ts_anchor_sec: self.ts_anchor_sec.clone(),
                ts1_off_ms: self.ts1_off_ms.clone(),
                ts2_off_ms: self.ts2_off_ms.clone(),
                ts1_off_ns: self.ts1_off_ns.clone(),
                ts2_off_ns: self.ts2_off_ns.clone(),
                counts: self.counts.clone(),
                mins: self.mins.clone(),
                maxs: self.maxs.clone(),
                avgs: self.avgs.clone(),
                range_final: self.range_final.clone(),
                timed_out: self.timed_out.clone(),
                missing_bins: self.missing_bins.clone(),
                continue_at: self.continue_at.clone(),
                finished_at: self.finished_at.clone(),
            };
            Box::new(bins)
        }
    }
}

impl<NTY> AsAnyRef for BinsDim0CollectedResult<NTY>
where
    NTY: 'static,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for BinsDim0CollectedResult<NTY>
where
    NTY: 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> TypeName for BinsDim0CollectedResult<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<NTY: ScalarOps> WithLen for BinsDim0CollectedResult<NTY> {
    fn len(&self) -> usize {
        self.mins.len()
    }
}

impl<NTY: ScalarOps> CollectedDyn for BinsDim0CollectedResult<NTY> {}

impl<NTY> BinsDim0CollectedResult<NTY> {
    pub fn ts_anchor_sec(&self) -> u64 {
        self.ts_anchor_sec
    }

    pub fn ts1_off_ms(&self) -> &VecDeque<u64> {
        &self.ts1_off_ms
    }

    pub fn ts2_off_ms(&self) -> &VecDeque<u64> {
        &self.ts2_off_ms
    }

    pub fn counts(&self) -> &VecDeque<u64> {
        &self.counts
    }

    pub fn range_final(&self) -> bool {
        self.range_final
    }

    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    pub fn missing_bins(&self) -> u32 {
        self.missing_bins
    }

    pub fn continue_at(&self) -> Option<IsoDateTime> {
        self.continue_at.clone()
    }

    pub fn mins(&self) -> &VecDeque<NTY> {
        &self.mins
    }

    pub fn maxs(&self) -> &VecDeque<NTY> {
        &self.maxs
    }

    pub fn avgs(&self) -> &VecDeque<f32> {
        &self.avgs
    }
}

impl<NTY: ScalarOps> ToJsonResult for BinsDim0CollectedResult<NTY> {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

#[derive(Debug)]
pub struct BinsDim0Collector<NTY> {
    vals: Option<BinsDim0<NTY>>,
    timed_out: bool,
    range_final: bool,
}

impl<NTY> BinsDim0Collector<NTY> {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            timed_out: false,
            range_final: false,
            vals: None,
        }
    }
}

impl<NTY> WithLen for BinsDim0Collector<NTY> {
    fn len(&self) -> usize {
        self.vals.as_ref().map_or(0, WithLen::len)
    }
}

impl<STY: ScalarOps> ByteEstimate for BinsDim0Collector<STY> {
    fn byte_estimate(&self) -> u64 {
        self.vals.as_ref().map_or(0, ByteEstimate::byte_estimate)
    }
}

impl<NTY: ScalarOps> CollectorTy for BinsDim0Collector<NTY> {
    type Input = BinsDim0<NTY>;
    type Output = BinsDim0CollectedResult<NTY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        if self.vals.is_none() {
            self.vals = Some(Self::Input::empty());
        }
        let vals = self.vals.as_mut().unwrap();
        vals.ts1s.append(&mut src.ts1s);
        vals.ts2s.append(&mut src.ts2s);
        vals.cnts.append(&mut src.cnts);
        vals.mins.append(&mut src.mins);
        vals.maxs.append(&mut src.maxs);
        vals.avgs.append(&mut src.avgs);
        vals.lsts.append(&mut src.lsts);
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn set_continue_at_here(&mut self) {
        debug!("{}::set_continue_at_here", Self::self_name());
        // TODO for bins, do nothing: either we have all bins or not.
    }

    fn result(
        &mut self,
        _range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Self::Output, Error> {
        trace!("trying to make a result from {self:?}");
        let bin_count_exp = if let Some(r) = &binrange {
            r.bin_count() as u32
        } else {
            debug!("no binrange given");
            0
        };
        let mut vals = if let Some(x) = self.vals.take() {
            x
        } else {
            return Err(Error::with_msg_no_trace("BinsDim0Collector without vals"));
        };
        let bin_count = vals.ts1s.len() as u32;
        debug!(
            "result  make missing bins  bin_count_exp {}  bin_count {}",
            bin_count_exp, bin_count
        );
        let (missing_bins, continue_at, finished_at) = if bin_count < bin_count_exp {
            match vals.ts2s.back() {
                Some(&k) => {
                    let missing_bins = bin_count_exp - bin_count;
                    let continue_at = IsoDateTime::from_ns_u64(k);
                    let u = k + (k - vals.ts1s.back().unwrap()) * missing_bins as u64;
                    let finished_at = IsoDateTime::from_ns_u64(u);
                    (missing_bins, Some(continue_at), Some(finished_at))
                }
                None => {
                    warn!("can not determine continue-at parameters");
                    (0, None, None)
                }
            }
        } else {
            (0, None, None)
        };
        if vals.ts1s.as_slices().1.len() != 0 {
            warn!("ts1s non-contiguous");
        }
        if vals.ts2s.as_slices().1.len() != 0 {
            warn!("ts2s non-contiguous");
        }
        let ts1s = vals.ts1s.make_contiguous();
        let ts2s = vals.ts2s.make_contiguous();
        let (ts_anch, ts1ms, ts1ns) = ts_offs_from_abs(ts1s);
        let (ts2ms, ts2ns) = ts_offs_from_abs_with_anchor(ts_anch, ts2s);
        let counts = vals.cnts;
        let mins = vals.mins;
        let maxs = vals.maxs;
        let avgs = vals.avgs;
        let ret = BinsDim0CollectedResult::<NTY> {
            ts_anchor_sec: ts_anch,
            ts1_off_ms: ts1ms,
            ts1_off_ns: ts1ns,
            ts2_off_ms: ts2ms,
            ts2_off_ns: ts2ns,
            counts,
            mins,
            maxs,
            avgs,
            range_final: self.range_final,
            timed_out: self.timed_out,
            missing_bins,
            continue_at,
            finished_at,
        };
        *self = Self::new();
        Ok(ret)
    }
}

impl<NTY: ScalarOps> CollectableType for BinsDim0<NTY> {
    type Collector = BinsDim0Collector<NTY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

#[derive(Debug)]
pub struct BinsDim0Aggregator<NTY> {
    range: SeriesRange,
    cnt: u64,
    minmaxlst: Option<(NTY, NTY, NTY)>,
    sumc: u64,
    sum: f32,
}

impl<NTY: ScalarOps> BinsDim0Aggregator<NTY> {
    pub fn new(range: SeriesRange, _do_time_weight: bool) -> Self {
        Self {
            range,
            cnt: 0,
            minmaxlst: None,
            sumc: 0,
            sum: 0f32,
        }
    }
}
