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
use items_0::Appendable;
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
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::MS;
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
macro_rules! trace2 {
    (EN$($arg:tt)*) => ();
    ($($arg:tt)*) => (trace!($($arg)*));
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsDim1NoPulse<STY> {
    pub tss: VecDeque<u64>,
    pub values: VecDeque<Vec<STY>>,
}

impl<STY> From<EventsDim1NoPulse<STY>> for EventsDim1<STY> {
    fn from(value: EventsDim1NoPulse<STY>) -> Self {
        let pulses = vec![0; value.tss.len()].into();
        Self {
            tss: value.tss,
            pulses,
            values: value.values,
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsDim1<STY> {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub values: VecDeque<Vec<STY>>,
}

impl<STY> EventsDim1<STY> {
    #[inline(always)]
    pub fn push(&mut self, ts: u64, pulse: u64, value: Vec<STY>) {
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.values.push_back(value);
    }

    #[inline(always)]
    pub fn push_front(&mut self, ts: u64, pulse: u64, value: Vec<STY>) {
        self.tss.push_front(ts);
        self.pulses.push_front(pulse);
        self.values.push_front(value);
    }

    pub fn serde_id() -> &'static str {
        "EventsDim1"
    }

    pub fn tss(&self) -> &VecDeque<u64> {
        &self.tss
    }
}

impl<STY> AsAnyRef for EventsDim1<STY>
where
    STY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsDim1<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> Empty for EventsDim1<STY> {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            values: VecDeque::new(),
        }
    }
}

impl<STY> fmt::Debug for EventsDim1<STY>
where
    STY: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if false {
            write!(
                fmt,
                "EventsDim1 {{ count {}  ts {:?}  vals {:?} }}",
                self.tss.len(),
                self.tss.iter().map(|x| x / SEC).collect::<Vec<_>>(),
                self.values,
            )
        } else {
            write!(
                fmt,
                "EventsDim1 {{ count {}  ts {:?} .. {:?}  vals {:?} .. {:?} }}",
                self.tss.len(),
                self.tss.front().map(|x| x / SEC),
                self.tss.back().map(|x| x / SEC),
                self.values.front(),
                self.values.back(),
            )
        }
    }
}

impl<STY> WithLen for EventsDim1<STY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<STY> ByteEstimate for EventsDim1<STY> {
    fn byte_estimate(&self) -> u64 {
        let stylen = mem::size_of::<STY>();
        let n = self.values.front().map_or(0, Vec::len);
        (self.len() * (8 + 8 + n * stylen)) as u64
    }
}

impl<STY: ScalarOps> HasTimestampDeque for EventsDim1<STY> {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim1ChunkOutput<STY> {
    tss: VecDeque<u64>,
    pulses: VecDeque<u64>,
    values: VecDeque<Vec<STY>>,
    scalar_type: String,
}

impl<STY: ScalarOps> EventsDim1ChunkOutput<STY> {}

#[derive(Debug)]
pub struct EventsDim1Collector<STY> {
    vals: EventsDim1<STY>,
    range_final: bool,
    timed_out: bool,
    needs_continue_at: bool,
}

impl<STY> EventsDim1Collector<STY> {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        Self {
            vals: EventsDim1::empty(),
            range_final: false,
            timed_out: false,
            needs_continue_at: false,
        }
    }
}

impl<STY> WithLen for EventsDim1Collector<STY> {
    fn len(&self) -> usize {
        WithLen::len(&self.vals)
    }
}

impl<STY> ByteEstimate for EventsDim1Collector<STY> {
    fn byte_estimate(&self) -> u64 {
        ByteEstimate::byte_estimate(&self.vals)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim1CollectorOutput<STY> {
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
    #[serde(rename = "values")]
    values: VecDeque<Vec<STY>>,
    #[serde(rename = "rangeFinal", default, skip_serializing_if = "is_false")]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "is_false")]
    timed_out: bool,
    #[serde(
        rename = "continueAt",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    continue_at: Option<IsoDateTime>,
}

impl<STY: ScalarOps> EventsDim1CollectorOutput<STY> {
    pub fn ts_anchor_sec(&self) -> u64 {
        self.ts_anchor_sec
    }

    pub fn ts_off_ms(&self) -> &VecDeque<u64> {
        &self.ts_off_ms
    }

    pub fn pulse_anchor(&self) -> u64 {
        self.pulse_anchor
    }

    pub fn pulse_off(&self) -> &VecDeque<u64> {
        &self.pulse_off
    }

    /// Note: only used for unit tests.
    pub fn values_to_f32(&self) -> VecDeque<Vec<f32>> {
        self.values
            .iter()
            .map(|x| x.iter().map(|x| x.as_prim_f32_b()).collect())
            .collect()
    }

    pub fn range_final(&self) -> bool {
        self.range_final
    }

    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    pub fn is_valid(&self) -> bool {
        if self.ts_off_ms.len() != self.ts_off_ns.len() {
            false
        } else if self.ts_off_ms.len() != self.pulse_off.len() {
            false
        } else if self.ts_off_ms.len() != self.values.len() {
            false
        } else {
            true
        }
    }

    pub fn info_str(&self) -> String {
        use fmt::Write;
        let mut out = String::new();
        write!(
            out,
            "ts_off_ms {}  ts_off_ns {}  pulse_off {}  values {}",
            self.ts_off_ms.len(),
            self.ts_off_ns.len(),
            self.pulse_off.len(),
            self.values.len(),
        )
        .unwrap();
        out
    }
}

impl<STY> AsAnyRef for EventsDim1CollectorOutput<STY>
where
    STY: 'static,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsDim1CollectorOutput<STY>
where
    STY: 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> TypeName for EventsDim1CollectorOutput<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<STY: ScalarOps> WithLen for EventsDim1CollectorOutput<STY> {
    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<STY: ScalarOps> ToJsonResult for EventsDim1CollectorOutput<STY> {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

impl<STY: ScalarOps> CollectedDyn for EventsDim1CollectorOutput<STY> {}

impl<STY: ScalarOps> CollectorTy for EventsDim1Collector<STY> {
    type Input = EventsDim1<STY>;
    type Output = EventsDim1CollectorOutput<STY>;

    fn ingest(&mut self, src: &mut Self::Input) {
        self.vals.tss.append(&mut src.tss);
        self.vals.pulses.append(&mut src.pulses);
        self.vals.values.append(&mut src.values);
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
    }

    fn set_continue_at_here(&mut self) {
        debug!("{}::set_continue_at_here", Self::self_name());
        self.needs_continue_at = true;
    }

    // TODO unify with dim0 case
    fn result(
        &mut self,
        range: Option<SeriesRange>,
        _binrange: Option<BinnedRangeEnum>,
    ) -> Result<Self::Output, Error> {
        // If we timed out, we want to hint the client from where to continue.
        // This is tricky: currently, client can not request a left-exclusive range.
        // We currently give the timestamp of the last event plus a small delta.
        // The amount of the delta must take into account what kind of timestamp precision the client
        // can parse and handle.
        let vals = &mut self.vals;
        let continue_at = if self.timed_out {
            if let Some(ts) = vals.tss.back() {
                Some(IsoDateTime::from_ns_u64(*ts + MS))
            } else {
                if let Some(range) = &range {
                    match range {
                        SeriesRange::TimeRange(x) => Some(IsoDateTime::from_ns_u64(x.beg + SEC)),
                        SeriesRange::PulseRange(_) => {
                            error!("TODO emit create continueAt for pulse range");
                            Some(IsoDateTime::from_ns_u64(0))
                        }
                    }
                } else {
                    warn!("can not determine continue-at parameters");
                    Some(IsoDateTime::from_ns_u64(0))
                }
            }
        } else {
            None
        };
        let tss_sl = vals.tss.make_contiguous();
        let pulses_sl = vals.pulses.make_contiguous();
        let (ts_anchor_sec, ts_off_ms, ts_off_ns) = crate::ts_offs_from_abs(tss_sl);
        let (pulse_anchor, pulse_off) = crate::pulse_offs_from_abs(pulses_sl);
        let values = mem::replace(&mut vals.values, VecDeque::new());
        if ts_off_ms.len() != ts_off_ns.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        if ts_off_ms.len() != pulse_off.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        if ts_off_ms.len() != values.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        let ret = Self::Output {
            ts_anchor_sec,
            ts_off_ms,
            ts_off_ns,
            pulse_anchor,
            pulse_off,
            values,
            range_final: self.range_final,
            timed_out: self.timed_out,
            continue_at,
        };
        if !ret.is_valid() {
            error!("invalid:\n{}", ret.info_str());
        }
        Ok(ret)
    }
}

impl<STY: ScalarOps> CollectableType for EventsDim1<STY> {
    type Collector = EventsDim1Collector<STY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

#[derive(Debug)]
pub struct EventsDim1Aggregator<STY> {
    _last_seen_val: Option<STY>,
    events_taken_count: u64,
    events_ignored_count: u64,
}

impl<STY> Drop for EventsDim1Aggregator<STY> {
    fn drop(&mut self) {
        // TODO collect as stats for the request context:
        trace!(
            "taken {}  ignored {}",
            self.events_taken_count,
            self.events_ignored_count
        );
    }
}

impl<STY: ScalarOps> EventsDim1Aggregator<STY> {
    pub fn new(_range: SeriesRange, _do_time_weight: bool) -> Self {
        panic!("TODO remove")
    }
}

impl<STY> items_0::TypeName for EventsDim1<STY> {
    fn type_name(&self) -> String {
        let sty = std::any::type_name::<STY>();
        format!("EventsDim1<{sty}>")
    }
}

impl<STY: ScalarOps> EventsNonObj for EventsDim1<STY> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        panic!("TODO remove")
    }
}

impl<STY: ScalarOps> Events for EventsDim1<STY> {
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
            "EventsDim1OutputInfo {{ len {}, ts_min {}, ts_max {} }}",
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
        let values = self.values.drain(..n1).collect();
        let ret = Self {
            tss,
            pulses,
            values,
        };
        Box::new(ret)
    }

    fn new_empty_evs(&self) -> Box<dyn Events> {
        Box::new(Self::empty())
    }

    fn drain_into_evs(
        &mut self,
        dst: &mut dyn Events,
        range: (usize, usize),
    ) -> Result<(), MergeError> {
        // TODO as_any and as_any_mut are declared on unrelated traits. Simplify.
        if let Some(dst) = dst.as_any_mut().downcast_mut::<Self>() {
            // TODO make it harder to forget new members when the struct may get modified in the future
            let r = range.0..range.1;
            dst.tss.extend(self.tss.drain(r.clone()));
            dst.pulses.extend(self.pulses.drain(r.clone()));
            dst.values.extend(self.values.drain(r.clone()));
            Ok(())
        } else {
            error!("downcast to EventsDim0 FAILED");
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
        // TODO make more nice
        panic!()
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        panic!("discontinued support for EventsDim1")
    }

    fn to_json_string(&self) -> String {
        let ret = EventsDim1ChunkOutput {
            // TODO use &mut to swap the content
            tss: self.tss.clone(),
            pulses: self.pulses.clone(),
            values: self.values.clone(),
            scalar_type: STY::scalar_type_name().into(),
        };
        serde_json::to_string(&ret).unwrap()
    }

    fn to_json_vec_u8(&self) -> Vec<u8> {
        self.to_json_string().into_bytes()
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        let ret = EventsDim1ChunkOutput {
            // TODO use &mut to swap the content
            tss: self.tss.clone(),
            pulses: self.pulses.clone(),
            values: self.values.clone(),
            scalar_type: STY::scalar_type_name().into(),
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&ret, &mut buf).unwrap();
        buf
    }

    fn clear(&mut self) {
        self.tss.clear();
        self.pulses.clear();
        self.values.clear();
    }

    fn to_dim0_f32_for_binning(&self) -> Box<dyn Events> {
        todo!("{}::to_dim0_f32_for_binning", self.type_name())
    }

    fn to_container_events(&self) -> Box<dyn ::items_0::timebin::BinningggContainerEventsDyn> {
        todo!("{}::to_container_events", self.type_name())
    }
}

impl<STY> Appendable<Vec<STY>> for EventsDim1<STY>
where
    STY: ScalarOps,
{
    fn push(&mut self, ts: u64, pulse: u64, value: Vec<STY>) {
        Self::push(self, ts, pulse, value)
    }
}
