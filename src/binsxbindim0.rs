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
use items_0::scalar_ops::AsPrimF32;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBins;
use items_0::AppendEmptyBin;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Resettable;
use items_0::TypeName;
use items_0::WithLen;
use netpod::is_false;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::SEC;
use netpod::BinnedRangeEnum;
use netpod::CmpZero;
use netpod::Dim0Kind;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::ops::Range;

#[allow(unused)]
macro_rules! trace4 {
    ($($arg:tt)*) => ();
    ($($arg:tt)*) => (eprintln!($($arg)*));
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct BinsXbinDim0<NTY> {
    ts1s: VecDeque<u64>,
    ts2s: VecDeque<u64>,
    counts: VecDeque<u64>,
    mins: VecDeque<NTY>,
    maxs: VecDeque<NTY>,
    avgs: VecDeque<f32>,
    // TODO could consider more variables:
    // ts min/max, pulse min/max, avg of mins, avg of maxs, variances, etc...
    dim0kind: Option<Dim0Kind>,
}

impl<STY> TypeName for BinsXbinDim0<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<NTY> fmt::Debug for BinsXbinDim0<NTY>
where
    NTY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let self_name = any::type_name::<Self>();
        write!(
            fmt,
            "{self_name}  count {}  ts1s {:?}  ts2s {:?}  counts {:?}  mins {:?}  maxs {:?}  avgs {:?}",
            self.ts1s.len(),
            self.ts1s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.ts2s.iter().map(|k| k / SEC).collect::<Vec<_>>(),
            self.counts,
            self.mins,
            self.maxs,
            self.avgs,
        )
    }
}

impl<NTY: ScalarOps> BinsXbinDim0<NTY> {
    pub fn from_content(
        ts1s: VecDeque<u64>,
        ts2s: VecDeque<u64>,
        counts: VecDeque<u64>,
        mins: VecDeque<NTY>,
        maxs: VecDeque<NTY>,
        avgs: VecDeque<f32>,
    ) -> Self {
        Self {
            ts1s,
            ts2s,
            counts,
            mins,
            maxs,
            avgs,
            dim0kind: None,
        }
    }

    pub fn counts(&self) -> &VecDeque<u64> {
        &self.counts
    }

    pub fn push(&mut self, ts1: u64, ts2: u64, count: u64, min: NTY, max: NTY, avg: f32) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.counts.push_back(count);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
    }

    pub fn append_zero(&mut self, beg: u64, end: u64) {
        self.ts1s.push_back(beg);
        self.ts2s.push_back(end);
        self.counts.push_back(0);
        self.mins.push_back(NTY::zero_b());
        self.maxs.push_back(NTY::zero_b());
        self.avgs.push_back(0.);
    }

    pub fn append_all_from(&mut self, src: &mut Self) {
        self.ts1s.extend(src.ts1s.drain(..));
        self.ts2s.extend(src.ts2s.drain(..));
        self.counts.extend(src.counts.drain(..));
        self.mins.extend(src.mins.drain(..));
        self.maxs.extend(src.maxs.drain(..));
        self.avgs.extend(src.avgs.drain(..));
    }

    pub fn equal_slack(&self, other: &Self) -> bool {
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
}

impl<NTY> AsAnyRef for BinsXbinDim0<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for BinsXbinDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> Empty for BinsXbinDim0<STY> {
    fn empty() -> Self {
        Self {
            ts1s: VecDeque::new(),
            ts2s: VecDeque::new(),
            counts: VecDeque::new(),
            mins: VecDeque::new(),
            maxs: VecDeque::new(),
            avgs: VecDeque::new(),
            dim0kind: None,
        }
    }
}

impl<STY> WithLen for BinsXbinDim0<STY> {
    fn len(&self) -> usize {
        self.ts1s.len()
    }
}

impl<STY: ScalarOps> ByteEstimate for BinsXbinDim0<STY> {
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

impl<STY> Resettable for BinsXbinDim0<STY> {
    fn reset(&mut self) {
        self.ts1s.clear();
        self.ts2s.clear();
        self.counts.clear();
        self.mins.clear();
        self.maxs.clear();
        self.avgs.clear();
    }
}

impl<NTY: ScalarOps> AppendEmptyBin for BinsXbinDim0<NTY> {
    fn append_empty_bin(&mut self, ts1: u64, ts2: u64) {
        self.ts1s.push_back(ts1);
        self.ts2s.push_back(ts2);
        self.counts.push_back(0);
        self.mins.push_back(NTY::zero_b());
        self.maxs.push_back(NTY::zero_b());
        self.avgs.push_back(0.);
    }
}

impl<NTY: ScalarOps> TimeBins for BinsXbinDim0<NTY> {
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

// TODO rename to BinsDim0CollectorOutput
#[derive(Debug, Serialize, Deserialize)]
pub struct BinsXbinDim0CollectedResult<NTY> {
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

impl<NTY> AsAnyRef for BinsXbinDim0CollectedResult<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for BinsXbinDim0CollectedResult<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> TypeName for BinsXbinDim0CollectedResult<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<NTY: ScalarOps> WithLen for BinsXbinDim0CollectedResult<NTY> {
    fn len(&self) -> usize {
        self.mins.len()
    }
}

impl<NTY: ScalarOps> CollectedDyn for BinsXbinDim0CollectedResult<NTY> {}

impl<NTY> BinsXbinDim0CollectedResult<NTY> {
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
}

impl<NTY: ScalarOps> ToJsonResult for BinsXbinDim0CollectedResult<NTY> {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

#[derive(Debug)]
pub struct BinsXbinDim0Collector<NTY> {
    vals: BinsXbinDim0<NTY>,
    timed_out: bool,
    range_final: bool,
}

impl<NTY> BinsXbinDim0Collector<NTY> {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            vals: BinsXbinDim0::empty(),
            timed_out: false,
            range_final: false,
        }
    }
}

impl<NTY> WithLen for BinsXbinDim0Collector<NTY> {
    fn len(&self) -> usize {
        self.vals.len()
    }
}

impl<STY: ScalarOps> ByteEstimate for BinsXbinDim0Collector<STY> {
    fn byte_estimate(&self) -> u64 {
        self.vals.byte_estimate()
    }
}

impl<NTY: ScalarOps> CollectorTy for BinsXbinDim0Collector<NTY> {
    type Input = BinsXbinDim0<NTY>;
    type Output = BinsXbinDim0CollectedResult<NTY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        trace!("\n\n-----------  BinsXbinDim0Collector ingest\n{:?}\n\n", src);
        // TODO could be optimized by non-contiguous container.
        self.vals.ts1s.append(&mut src.ts1s);
        self.vals.ts2s.append(&mut src.ts2s);
        self.vals.counts.append(&mut src.counts);
        self.vals.mins.append(&mut src.mins);
        self.vals.maxs.append(&mut src.maxs);
        self.vals.avgs.append(&mut src.avgs);
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
        _range: std::option::Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Self::Output, Error> {
        let bin_count_exp = if let Some(r) = &binrange {
            r.bin_count() as u32
        } else {
            0
        };
        let bin_count = self.vals.ts1s.len() as u32;
        let (missing_bins, continue_at, finished_at) = if bin_count < bin_count_exp {
            match self.vals.ts2s.back() {
                Some(&k) => {
                    let missing_bins = bin_count_exp - bin_count;
                    let continue_at = IsoDateTime::from_ns_u64(k);
                    let u = k + (k - self.vals.ts1s.back().unwrap()) * missing_bins as u64;
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
        if self.vals.ts1s.as_slices().1.len() != 0 {
            panic!();
        }
        if self.vals.ts2s.as_slices().1.len() != 0 {
            panic!();
        }
        let tst1 = ts_offs_from_abs(self.vals.ts1s.as_slices().0);
        let tst2 = ts_offs_from_abs_with_anchor(tst1.0, self.vals.ts2s.as_slices().0);
        let counts = mem::replace(&mut self.vals.counts, VecDeque::new());
        let mins = mem::replace(&mut self.vals.mins, VecDeque::new());
        let maxs = mem::replace(&mut self.vals.maxs, VecDeque::new());
        let avgs = mem::replace(&mut self.vals.avgs, VecDeque::new());
        let ret = BinsXbinDim0CollectedResult::<NTY> {
            ts_anchor_sec: tst1.0,
            ts1_off_ms: tst1.1,
            ts1_off_ns: tst1.2,
            ts2_off_ms: tst2.0,
            ts2_off_ns: tst2.1,
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
        Ok(ret)
    }
}

impl<NTY: ScalarOps> CollectableType for BinsXbinDim0<NTY> {
    type Collector = BinsXbinDim0Collector<NTY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

#[derive(Debug)]
pub struct BinsXbinDim0Aggregator<NTY> {
    range: SeriesRange,
    count: u64,
    min: NTY,
    max: NTY,
    // Carry over to next bin:
    avg: f32,
    sumc: u64,
    sum: f32,
}

impl<NTY: ScalarOps> BinsXbinDim0Aggregator<NTY> {
    pub fn new(range: SeriesRange, _do_time_weight: bool) -> Self {
        Self {
            range,
            count: 0,
            min: NTY::zero_b(),
            max: NTY::zero_b(),
            avg: 0.,
            sumc: 0,
            sum: 0f32,
        }
    }
}
