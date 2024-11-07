use crate::binsxbindim0::BinsXbinDim0;
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
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBinnerTy;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Events;
use items_0::EventsNonObj;
use items_0::MergeError;
use items_0::TypeName;
use items_0::WithLen;
use netpod::is_false;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::SEC;
use netpod::BinnedRangeEnum;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::mem;

#[allow(unused)]
macro_rules! trace_ingest {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*) };
}

#[allow(unused)]
macro_rules! trace2 {
    ($($arg:tt)*) => {};
    ($($arg:tt)*) => { trace!($($arg)*) };
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsXbinDim0<NTY> {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub mins: VecDeque<NTY>,
    pub maxs: VecDeque<NTY>,
    pub avgs: VecDeque<f32>,
    // TODO maybe add variance?
}

impl<NTY> EventsXbinDim0<NTY> {
    #[inline(always)]
    pub fn push(&mut self, ts: u64, pulse: u64, min: NTY, max: NTY, avg: f32) {
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.mins.push_back(min);
        self.maxs.push_back(max);
        self.avgs.push_back(avg);
    }

    #[inline(always)]
    pub fn push_front(&mut self, ts: u64, pulse: u64, min: NTY, max: NTY, avg: f32) {
        self.tss.push_front(ts);
        self.pulses.push_front(pulse);
        self.mins.push_front(min);
        self.maxs.push_front(max);
        self.avgs.push_front(avg);
    }

    pub fn serde_id() -> &'static str {
        "EventsXbinDim0"
    }
}

impl<STY> TypeName for EventsXbinDim0<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<STY> fmt::Debug for EventsXbinDim0<STY>
where
    STY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if false {
            write!(
                fmt,
                "{} {{ count {}  ts {:?}  vals {:?} }}",
                self.type_name(),
                self.tss.len(),
                self.tss.iter().map(|x| x / SEC).collect::<Vec<_>>(),
                self.avgs,
            )
        } else {
            write!(
                fmt,
                "{} {{ count {}  ts {:?} .. {:?}  vals {:?} .. {:?} }}",
                self.type_name(),
                self.tss.len(),
                self.tss.front().map(|x| x / SEC),
                self.tss.back().map(|x| x / SEC),
                self.avgs.front(),
                self.avgs.back(),
            )
        }
    }
}

impl<STY> ByteEstimate for EventsXbinDim0<STY> {
    fn byte_estimate(&self) -> u64 {
        let stylen = mem::size_of::<STY>();
        (self.len() * (8 + 8 + 2 * stylen + 4)) as u64
    }
}

impl<STY> Empty for EventsXbinDim0<STY> {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            mins: VecDeque::new(),
            maxs: VecDeque::new(),
            avgs: VecDeque::new(),
        }
    }
}

impl<STY> AsAnyRef for EventsXbinDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsXbinDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> WithLen for EventsXbinDim0<STY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<STY: ScalarOps> HasTimestampDeque for EventsXbinDim0<STY> {
    fn timestamp_min(&self) -> Option<u64> {
        self.tss.front().map(|x| *x)
    }

    fn timestamp_max(&self) -> Option<u64> {
        self.tss.back().map(|x| *x)
    }

    fn pulse_min(&self) -> Option<u64> {
        self.pulses.front().map(|x| *x)
    }

    fn pulse_max(&self) -> Option<u64> {
        self.pulses.back().map(|x| *x)
    }
}

impl<STY: ScalarOps> EventsNonObj for EventsXbinDim0<STY> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        info!(
            "EventsXbinDim0::into_tss_pulses  len {}  len {}",
            self.tss.len(),
            self.pulses.len()
        );
        (self.tss, self.pulses)
    }
}

impl<STY: ScalarOps> Events for EventsXbinDim0<STY> {
    fn verify(&self) -> bool {
        let mut good = true;
        let mut ts_max = 0;
        for ts in &self.tss {
            let ts = *ts;
            if ts < ts_max {
                good = false;
                error!("unordered event data  ts {}  ts_max {}", ts, ts_max);
            }
            ts_max = ts_max.max(ts);
        }
        good
    }

    fn output_info(&self) -> String {
        let n2 = self.tss.len().max(1) - 1;
        format!(
            "EventsXbinDim0OutputInfo {{ len {}, ts_min {}, ts_max {} }}",
            self.tss.len(),
            self.tss.get(0).map_or(-1i64, |&x| x as i64),
            self.tss.get(n2).map_or(-1i64, |&x| x as i64),
        )
    }

    fn as_collectable_mut(&mut self) -> &mut dyn CollectableDyn {
        self
    }

    fn as_collectable_with_default_ref(&self) -> &dyn CollectableDyn {
        self
    }

    fn as_collectable_with_default_mut(&mut self) -> &mut dyn CollectableDyn {
        self
    }

    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events> {
        // TODO improve the search
        let n1 = self.tss.iter().take_while(|&&x| x <= ts_end).count();
        let tss = self.tss.drain(..n1).collect();
        let pulses = self.pulses.drain(..n1).collect();
        let mins = self.mins.drain(..n1).collect();
        let maxs = self.maxs.drain(..n1).collect();
        let avgs = self.avgs.drain(..n1).collect();
        let ret = Self {
            tss,
            pulses,
            mins,
            maxs,
            avgs,
        };
        Box::new(ret)
    }

    fn new_empty_evs(&self) -> Box<dyn Events> {
        Box::new(Self::empty())
    }

    fn drain_into_evs(&mut self, dst: &mut dyn Events, range: (usize, usize)) -> Result<(), MergeError> {
        // TODO as_any and as_any_mut are declared on unrelated traits. Simplify.
        if let Some(dst) = dst.as_any_mut().downcast_mut::<Self>() {
            // TODO make it harder to forget new members when the struct may get modified in the future
            let r = range.0..range.1;
            dst.tss.extend(self.tss.drain(r.clone()));
            dst.pulses.extend(self.pulses.drain(r.clone()));
            dst.mins.extend(self.mins.drain(r.clone()));
            dst.maxs.extend(self.maxs.drain(r.clone()));
            dst.avgs.extend(self.avgs.drain(r.clone()));
            Ok(())
        } else {
            error!("downcast to {} FAILED", self.type_name());
            Err(MergeError::NotCompatible)
        }
    }

    fn find_lowest_index_gt_evs(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m > ts {
                return Some(i);
            }
        }
        None
    }

    fn find_lowest_index_ge_evs(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m >= ts {
                return Some(i);
            }
        }
        None
    }

    fn find_highest_index_lt_evs(&self, ts: u64) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate().rev() {
            if m < ts {
                return Some(i);
            }
        }
        None
    }

    fn ts_min(&self) -> Option<u64> {
        self.tss.front().map(|&x| x)
    }

    fn ts_max(&self) -> Option<u64> {
        self.tss.back().map(|&x| x)
    }

    fn partial_eq_dyn(&self, other: &dyn Events) -> bool {
        if let Some(other) = other.as_any_ref().downcast_ref::<Self>() {
            self == other
        } else {
            false
        }
    }

    fn serde_id(&self) -> &'static str {
        Self::serde_id()
    }

    fn nty_id(&self) -> u32 {
        STY::SUB
    }

    fn clone_dyn(&self) -> Box<dyn Events> {
        Box::new(self.clone())
    }

    fn tss(&self) -> &VecDeque<u64> {
        &self.tss
    }

    fn pulses(&self) -> &VecDeque<u64> {
        &self.pulses
    }

    fn frame_type_id(&self) -> u32 {
        error!("TODO frame_type_id should not be called");
        // TODO make more nice
        panic!()
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        let dst = Self {
            tss: mem::replace(&mut self.tss, Default::default()),
            pulses: mem::replace(&mut self.pulses, Default::default()),
            mins: mem::replace(&mut self.mins, Default::default()),
            maxs: mem::replace(&mut self.maxs, Default::default()),
            avgs: mem::replace(&mut self.avgs, Default::default()),
        };
        Box::new(dst)
    }

    fn to_json_string(&self) -> String {
        todo!()
    }

    fn to_json_vec_u8(&self) -> Vec<u8> {
        todo!()
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        todo!()
    }

    fn clear(&mut self) {
        self.tss.clear();
        self.pulses.clear();
        self.mins.clear();
        self.maxs.clear();
        self.avgs.clear();
    }

    fn to_dim0_f32_for_binning(&self) -> Box<dyn Events> {
        todo!("{}::to_dim0_f32_for_binning", self.type_name())
    }

    fn to_container_events(&self) -> Box<dyn ::items_0::timebin::BinningggContainerEventsDyn> {
        todo!("{}::to_container_events", self.type_name())
    }
}

#[derive(Debug)]
pub struct EventsXbinDim0Aggregator<STY>
where
    STY: ScalarOps,
{
    range: SeriesRange,
    /// Number of events which actually fall in this bin.
    count: u64,
    min: STY,
    max: STY,
    /// Number of times we accumulated to the sum of this bin.
    sumc: u64,
    sum: f32,
    int_ts: u64,
    last_ts: u64,
    last_vals: Option<(STY, STY, f32)>,
    did_min_max: bool,
    do_time_weight: bool,
    events_ignored_count: u64,
}

impl<STY> EventsXbinDim0Aggregator<STY>
where
    STY: ScalarOps,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(range: SeriesRange, do_time_weight: bool) -> Self {
        let int_ts = range.beg_u64();
        Self {
            range,
            did_min_max: false,
            count: 0,
            min: STY::zero_b(),
            max: STY::zero_b(),
            sumc: 0,
            sum: 0f32,
            int_ts,
            last_ts: 0,
            last_vals: None,
            events_ignored_count: 0,
            do_time_weight,
        }
    }

    fn apply_min_max(&mut self, min: &STY, max: &STY) {
        if self.did_min_max != (self.sumc > 0) {
            panic!("logic error apply_min_max  {}  {}", self.did_min_max, self.sumc);
        }
        if self.sumc == 0 {
            self.did_min_max = true;
            self.min = min.clone();
            self.max = max.clone();
        } else {
            if *min < self.min {
                self.min = min.clone();
            }
            if *max > self.max {
                self.max = max.clone();
            }
        }
    }

    fn apply_event_unweight(&mut self, avg: f32, min: STY, max: STY) {
        //debug!("apply_event_unweight");
        self.apply_min_max(&min, &max);
        self.sumc += 1;
        let vf = avg;
        if vf.is_nan() {
        } else {
            self.sum += vf;
        }
    }

    // Only integrate, do not count because it is used even if the event does not fall into current bin.
    fn apply_event_time_weight(&mut self, px: u64) {
        trace_ingest!(
            "apply_event_time_weight  px {}  count {}  sumc {}  events_ignored_count {}",
            px,
            self.count,
            self.sumc,
            self.events_ignored_count
        );
        if let Some((min, max, avg)) = self.last_vals.as_ref() {
            let vf = *avg;
            {
                let min = min.clone();
                let max = max.clone();
                self.apply_min_max(&min, &max);
            }
            self.sumc += 1;
            let w = (px - self.int_ts) as f32 * 1e-9;
            if vf.is_nan() {
            } else {
                self.sum += vf * w;
            }
            self.int_ts = px;
        } else {
            debug!("apply_event_time_weight NO VALUE");
        }
    }

    fn ingest_unweight(&mut self, item: &EventsXbinDim0<STY>) {
        /*for i1 in 0..item.tss.len() {
            let ts = item.tss[i1];
            let avg = item.avgs[i1];
            let min = item.mins[i1].clone();
            let max = item.maxs[i1].clone();
            if ts < self.range.beg {
            } else if ts >= self.range.end {
            } else {
                self.apply_event_unweight(avg, min, max);
            }
        }*/
        todo!()
    }

    fn ingest_time_weight(&mut self, item: &EventsXbinDim0<STY>) {
        trace!(
            "{} ingest_time_weight  range {:?}  last_ts {:?}  int_ts {:?}",
            Self::type_name(),
            self.range,
            self.last_ts,
            self.int_ts
        );
        let range_beg = self.range.beg_u64();
        let range_end = self.range.end_u64();
        for (((&ts, min), max), avg) in item
            .tss
            .iter()
            .zip(item.mins.iter())
            .zip(item.maxs.iter())
            .zip(item.avgs.iter())
        {
            if ts >= range_end {
                self.events_ignored_count += 1;
                // TODO break early when tests pass.
                //break;
            } else if ts >= range_beg {
                self.apply_event_time_weight(ts);
                self.count += 1;
                self.last_ts = ts;
                self.last_vals = Some((min.clone(), max.clone(), avg.clone()));
            } else {
                self.events_ignored_count += 1;
                self.last_ts = ts;
                self.last_vals = Some((min.clone(), max.clone(), avg.clone()));
            }
        }
    }

    fn result_reset_unweight(&mut self, range: SeriesRange) -> BinsXbinDim0<STY> {
        /*let avg = if self.sumc == 0 {
            0f32
        } else {
            self.sum / self.sumc as f32
        };
        let ret = BinsXbinDim0::from_content(
            [self.range.beg].into(),
            [self.range.end].into(),
            [self.count].into(),
            [self.min.clone()].into(),
            [self.max.clone()].into(),
            [avg].into(),
        );
        self.int_ts = range.beg;
        self.range = range;
        self.sum = 0f32;
        self.sumc = 0;
        self.did_min_max = false;
        self.min = NTY::zero_b();
        self.max = NTY::zero_b();
        ret*/
        todo!()
    }

    fn result_reset_time_weight(&mut self, range: SeriesRange) -> BinsXbinDim0<STY> {
        trace!("{} result_reset_time_weight", Self::type_name());
        // TODO check callsite for correct expand status.
        if self.range.is_time() {
            self.apply_event_time_weight(self.range.end_u64());
        } else {
            error!("TODO result_reset_time_weight");
            err::todoval()
        }
        let range_beg = self.range.beg_u64();
        let range_end = self.range.end_u64();
        let (min, max, avg) = if self.sumc > 0 {
            let avg = self.sum / (self.range.delta_u64() as f32 * 1e-9);
            (self.min.clone(), self.max.clone(), avg)
        } else {
            let (min, max, avg) = match &self.last_vals {
                Some((min, max, avg)) => {
                    warn!("\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   SHOULD ALWAYS HAVE ACCUMULATED IN THIS CASE");
                    (min.clone(), max.clone(), avg.clone())
                }
                None => (STY::zero_b(), STY::zero_b(), 0.),
            };
            (min, max, avg)
        };
        let ret = BinsXbinDim0::from_content(
            [range_beg].into(),
            [range_end].into(),
            [self.count].into(),
            [min.clone()].into(),
            [max.clone()].into(),
            [avg].into(),
        );
        self.int_ts = range.beg_u64();
        self.range = range;
        self.count = 0;
        self.sumc = 0;
        self.sum = 0.;
        self.did_min_max = false;
        self.min = STY::zero_b();
        self.max = STY::zero_b();
        ret
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsXbinDim0CollectorOutput<NTY> {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: VecDeque<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: VecDeque<u64>,
    #[serde(rename = "pulseAnchor")]
    pulse_anchor: u64,
    #[serde(rename = "pulseOff")]
    pulse_off: VecDeque<u64>,
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
    #[serde(rename = "continueAt", default, skip_serializing_if = "Option::is_none")]
    continue_at: Option<IsoDateTime>,
}

impl<NTY> AsAnyRef for EventsXbinDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<NTY> AsAnyMut for EventsXbinDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> TypeName for EventsXbinDim0CollectorOutput<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<NTY: ScalarOps> WithLen for EventsXbinDim0CollectorOutput<NTY> {
    fn len(&self) -> usize {
        self.mins.len()
    }
}

impl<NTY> ToJsonResult for EventsXbinDim0CollectorOutput<NTY>
where
    NTY: ScalarOps,
{
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

impl<NTY> CollectedDyn for EventsXbinDim0CollectorOutput<NTY> where NTY: ScalarOps {}

#[derive(Debug)]
pub struct EventsXbinDim0Collector<NTY> {
    vals: EventsXbinDim0<NTY>,
    range_final: bool,
    timed_out: bool,
    needs_continue_at: bool,
}

impl<NTY> EventsXbinDim0Collector<NTY> {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            range_final: false,
            timed_out: false,
            vals: EventsXbinDim0::empty(),
            needs_continue_at: false,
        }
    }
}

impl<NTY> WithLen for EventsXbinDim0Collector<NTY> {
    fn len(&self) -> usize {
        WithLen::len(&self.vals)
    }
}

impl<STY> ByteEstimate for EventsXbinDim0Collector<STY> {
    fn byte_estimate(&self) -> u64 {
        ByteEstimate::byte_estimate(&self.vals)
    }
}

impl<NTY> CollectorTy for EventsXbinDim0Collector<NTY>
where
    NTY: ScalarOps,
{
    type Input = EventsXbinDim0<NTY>;
    type Output = EventsXbinDim0CollectorOutput<NTY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        self.vals.tss.append(&mut src.tss);
        self.vals.pulses.append(&mut src.pulses);
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
        self.needs_continue_at = true;
    }

    fn result(
        &mut self,
        range: Option<SeriesRange>,
        _binrange: Option<BinnedRangeEnum>,
    ) -> Result<Self::Output, Error> {
        /*use std::mem::replace;
        let continue_at = if self.timed_out {
            if let Some(ts) = self.vals.tss.back() {
                Some(IsoDateTime::from_u64(*ts + netpod::timeunits::MS))
            } else {
                if let Some(range) = &range {
                    Some(IsoDateTime::from_u64(range.beg + netpod::timeunits::SEC))
                } else {
                    warn!("can not determine continue-at parameters");
                    None
                }
            }
        } else {
            None
        };
        let mins = replace(&mut self.vals.mins, VecDeque::new());
        let maxs = replace(&mut self.vals.maxs, VecDeque::new());
        let avgs = replace(&mut self.vals.avgs, VecDeque::new());
        self.vals.tss.make_contiguous();
        self.vals.pulses.make_contiguous();
        let tst = crate::ts_offs_from_abs(self.vals.tss.as_slices().0);
        let (pulse_anchor, pulse_off) = crate::pulse_offs_from_abs(&self.vals.pulses.as_slices().0);
        let ret = Self::Output {
            ts_anchor_sec: tst.0,
            ts_off_ms: tst.1,
            ts_off_ns: tst.2,
            pulse_anchor,
            pulse_off,
            mins,
            maxs,
            avgs,
            range_final: self.range_final,
            timed_out: self.timed_out,
            continue_at,
        };
        Ok(ret)*/
        todo!()
    }
}

impl<NTY> CollectableType for EventsXbinDim0<NTY>
where
    NTY: ScalarOps,
{
    type Collector = EventsXbinDim0Collector<NTY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}
