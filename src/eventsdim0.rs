use crate::IsoDateTime;
use daqbuf_err as err;
use err::Error;
use items_0::apitypes::ToUserFacingApiType;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::CollectorTy;
use items_0::collect_s::ToJsonValue;
use items_0::container::ByteEstimate;
use items_0::overlap::HasTimestampDeque;
use items_0::scalar_ops::ScalarOps;
use items_0::Appendable;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Empty;
use items_0::Events;
use items_0::EventsNonObj;
use items_0::Resettable;
use items_0::TypeName;
use items_0::WithLen;
use netpod::is_false;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use netpod::BinnedRangeEnum;
use netpod::TsNano;
use serde::Deserialize;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::mem;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_item { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_ingest_event { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace2 { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! trace_binning { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[allow(unused)]
macro_rules! debug_ingest { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsDim0NoPulse<STY> {
    pub tss: VecDeque<u64>,
    pub values: VecDeque<STY>,
}

impl<STY> From<EventsDim0NoPulse<STY>> for EventsDim0<STY> {
    fn from(value: EventsDim0NoPulse<STY>) -> Self {
        let pulses = vec![0; value.tss.len()].into();
        Self {
            tss: value.tss,
            pulses,
            values: value.values,
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsDim0<STY> {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub values: VecDeque<STY>,
}

impl<STY> EventsDim0<STY> {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn push_back(&mut self, ts: u64, pulse: u64, value: STY) {
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.values.push_back(value);
    }

    pub fn push_front(&mut self, ts: u64, pulse: u64, value: STY) {
        self.tss.push_front(ts);
        self.pulses.push_front(pulse);
        self.values.push_front(value);
    }

    pub fn serde_id() -> &'static str {
        "EventsDim0"
    }

    pub fn tss(&self) -> &VecDeque<u64> {
        &self.tss
    }

    // only for testing at the moment
    pub fn private_values_ref(&self) -> &VecDeque<STY> {
        &self.values
    }
    pub fn private_values_mut(&mut self) -> &mut VecDeque<STY> {
        &mut self.values
    }
}

impl<STY> AsAnyRef for EventsDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsDim0<STY>
where
    STY: ScalarOps,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> Empty for EventsDim0<STY> {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            values: VecDeque::new(),
        }
    }
}

impl<STY> fmt::Debug for EventsDim0<STY>
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
                self.values,
            )
        } else {
            write!(
                fmt,
                "{} {{ count {}  ts {:?} .. {:?}  vals {:?} .. {:?} }}",
                self.type_name(),
                self.tss.len(),
                self.tss.front().map(|&x| TsNano::from_ns(x)),
                self.tss.back().map(|&x| TsNano::from_ns(x)),
                self.values.front(),
                self.values.back(),
            )
        }
    }
}

impl<STY> WithLen for EventsDim0<STY> {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl<STY: ScalarOps> ByteEstimate for EventsDim0<STY> {
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
            let sty_bytes = self.values[i].byte_estimate();
            (n as u64 * (8 + 8 + sty_bytes)) as u64
        }
    }
}

impl<STY> Resettable for EventsDim0<STY> {
    fn reset(&mut self) {
        self.tss.clear();
        self.pulses.clear();
        self.values.clear();
    }
}

impl<STY: ScalarOps> HasTimestampDeque for EventsDim0<STY> {
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
pub struct EventsDim0ChunkOutput<STY> {
    tss: VecDeque<u64>,
    pulses: VecDeque<u64>,
    values: VecDeque<STY>,
    scalar_type: String,
}

impl<STY: ScalarOps> EventsDim0ChunkOutput<STY> {}

#[derive(Debug)]
pub struct EventsDim0Collector<STY> {
    vals: EventsDim0<STY>,
    range_final: bool,
    timed_out: bool,
    needs_continue_at: bool,
}

impl<STY> EventsDim0Collector<STY> {
    pub fn self_name() -> &'static str {
        any::type_name::<Self>()
    }

    pub fn new() -> Self {
        debug!("EventsDim0Collector  NEW");
        Self {
            vals: EventsDim0::empty(),
            range_final: false,
            timed_out: false,
            needs_continue_at: false,
        }
    }
}

impl<STY> WithLen for EventsDim0Collector<STY> {
    fn len(&self) -> usize {
        WithLen::len(&self.vals)
    }
}

impl<STY: ScalarOps> ByteEstimate for EventsDim0Collector<STY> {
    fn byte_estimate(&self) -> u64 {
        ByteEstimate::byte_estimate(&self.vals)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim0CollectorOutput<STY> {
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
    values: VecDeque<STY>,
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

impl<STY: ScalarOps> EventsDim0CollectorOutput<STY> {
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
    pub fn values_to_f32(&self) -> VecDeque<f32> {
        self.values.iter().map(|x| x.as_prim_f32_b()).collect()
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

impl<STY> AsAnyRef for EventsDim0CollectorOutput<STY>
where
    STY: 'static,
{
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<STY> AsAnyMut for EventsDim0CollectorOutput<STY>
where
    STY: 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<STY> TypeName for EventsDim0CollectorOutput<STY> {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl<STY: ScalarOps> WithLen for EventsDim0CollectorOutput<STY> {
    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<STY: ScalarOps> ToUserFacingApiType for EventsDim0CollectorOutput<STY> {
    fn into_user_facing_api_type(self: Self) -> Box<dyn items_0::apitypes::UserApiType> {
        todo!()
    }

    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn items_0::apitypes::UserApiType> {
        todo!()
    }
}

impl<STY: ScalarOps> CollectedDyn for EventsDim0CollectorOutput<STY> {}

impl<STY: ScalarOps> CollectorTy for EventsDim0Collector<STY> {
    type Input = EventsDim0<STY>;
    type Output = EventsDim0CollectorOutput<STY>;

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
        self.needs_continue_at = true;
    }

    fn result(&mut self) -> Result<Self::Output, Error> {
        let range: Option<SeriesRange> = None;
        debug!(
            "{}  result()  needs_continue_at {}",
            Self::self_name(),
            self.needs_continue_at
        );
        // If we timed out, we want to hint the client from where to continue.
        // This is tricky: currently, client can not request a left-exclusive range.
        // We currently give the timestamp of the last event plus a small delta.
        // The amount of the delta must take into account what kind of timestamp precision the client
        // can parse and handle.
        let vals = &mut self.vals;
        let continue_at = if self.needs_continue_at {
            if let Some(ts) = vals.tss.back() {
                let x = Some(IsoDateTime::from_ns_u64(*ts / MS * MS + MS));
                x
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
                    Some(IsoDateTime::from_ns_u64(0))
                }
            }
        } else {
            None
        };
        let (ts_anchor_sec, ts_off_ms, ts_off_ns) = crate::offsets::ts_offs_from_abs(todo!());
        let (pulse_anchor, pulse_off) = crate::offsets::pulse_offs_from_abs(&self.vals.pulses);
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

impl<STY: ScalarOps> items_0::collect_s::CollectableType for EventsDim0<STY> {
    type Collector = EventsDim0Collector<STY>;

    fn new_collector() -> Self::Collector {
        Self::Collector::new()
    }
}

impl<STY> TypeName for EventsDim0<STY> {
    fn type_name(&self) -> String {
        let self_name = any::type_name::<Self>();
        format!("{self_name}")
    }
}

impl<STY: ScalarOps> EventsNonObj for EventsDim0<STY> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        trace!(
            "{}::into_tss_pulses  len {}  len {}",
            Self::type_name(),
            self.tss.len(),
            self.pulses.len()
        );
        (self.tss, self.pulses)
    }
}

macro_rules! try_to_container_events {
    ($sty:ty, $this:expr) => {
        let this = $this;
        if let Some(evs) = this.as_any_ref().downcast_ref::<EventsDim0<$sty>>() {
            use crate::binning::container_events::ContainerEvents;
            let tss = this.tss.iter().map(|&x| TsNano::from_ns(x)).collect();
            let vals = evs.values.clone();
            let ret = ContainerEvents::<$sty>::from_constituents(tss, vals);
            return Box::new(ret);
        }
    };
}

impl<STY: ScalarOps> Events for EventsDim0<STY> {
    fn verify(&self) -> bool {
        let mut good = true;
        let n = self.tss.len();
        for (&ts1, &ts2) in self.tss.iter().zip(self.tss.range(n.min(1)..n)) {
            if ts1 > ts2 {
                good = false;
                error!("unordered event data  ts1 {}  ts2 {}", ts1, ts2);
                break;
            }
        }
        good
    }

    fn output_info(&self) -> String {
        let n2 = self.tss.len().max(1) - 1;
        let min = if let Some(ts) = self.tss.get(0) {
            TsNano::from_ns(*ts).fmt().to_string()
        } else {
            String::from("None")
        };
        let max = if let Some(ts) = self.tss.get(n2) {
            TsNano::from_ns(*ts).fmt().to_string()
        } else {
            String::from("None")
        };
        format!(
            "EventsDim0OutputInfo {{ len {}, ts_min {}, ts_max {} }}",
            self.tss.len(),
            min,
            max,
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

    fn drain_into_evs(&mut self, dst: &mut dyn Events, range: (usize, usize)) -> Result<(), Error> {
        // TODO as_any and as_any_mut are declared on unrelated traits. Simplify.
        if let Some(dst) = dst.as_any_mut().downcast_mut::<Self>() {
            // TODO make it harder to forget new members when the struct may get modified in the future
            let r = range.0..range.1;
            dst.tss.extend(self.tss.drain(r.clone()));
            dst.pulses.extend(self.pulses.drain(r.clone()));
            dst.values.extend(self.values.drain(r.clone()));
            Ok(())
        } else {
            error!(
                "downcast to EventsDim0 FAILED\n\n{}\n\n{}\n\n",
                self.type_name(),
                dst.type_name()
            );
            panic!();
            // Err(MergeError::NotCompatible)
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
        STY::SUB as u32
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
            values: mem::replace(&mut self.values, Default::default()),
        };
        Box::new(dst)
    }

    fn to_json_string(&self) -> String {
        // TODO redesign with mut access, rename to `into_` and take the values out.
        let mut values = self.values.clone();
        let (ts_anchor_sec, ts_off_ms, ts_off_ns) = crate::offsets::ts_offs_from_abs(todo!());
        let (pulse_anchor, pulse_off) = crate::offsets::pulse_offs_from_abs(&self.pulses);
        // let values = mem::replace(&mut values, VecDeque::new());
        let ret = EventsDim0CollectorOutput {
            ts_anchor_sec,
            ts_off_ms,
            ts_off_ns,
            pulse_anchor,
            pulse_off,
            values,
            range_final: false,
            timed_out: false,
            continue_at: None,
        };
        serde_json::to_string(&ret).unwrap()
    }

    fn to_json_vec_u8(&self) -> Vec<u8> {
        self.to_json_string().into_bytes()
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        // TODO redesign with mut access, rename to `into_` and take the values out.
        let ret = EventsDim0ChunkOutput {
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
        let mut ret = EventsDim0::empty();
        for (&ts, val) in self.tss.iter().zip(self.values.iter()) {
            ret.push(TsNano::from_ns(ts), val.as_prim_f32_b());
        }
        Box::new(ret)
    }

    fn to_container_events(&self) -> Box<dyn ::items_0::timebin::BinningggContainerEventsDyn> {
        try_to_container_events!(u8, self);
        try_to_container_events!(u16, self);
        try_to_container_events!(u32, self);
        try_to_container_events!(u64, self);
        try_to_container_events!(i8, self);
        try_to_container_events!(i16, self);
        try_to_container_events!(i32, self);
        try_to_container_events!(i64, self);
        try_to_container_events!(f32, self);
        try_to_container_events!(f64, self);
        try_to_container_events!(bool, self);
        try_to_container_events!(String, self);
        let this = self;
        if let Some(evs) = self
            .as_any_ref()
            .downcast_ref::<EventsDim0<netpod::EnumVariant>>()
        {
            use crate::binning::container_events::ContainerEvents;
            let tss = this.tss.iter().map(|&x| TsNano::from_ns(x)).collect();
            use crate::binning::container_events::Container;
            let mut vals = crate::binning::valuetype::EnumVariantContainer::new();
            for x in evs.values.iter() {
                vals.push_back(x.clone());
            }
            let ret = ContainerEvents::<netpod::EnumVariant>::from_constituents(tss, vals);
            return Box::new(ret);
        }
        let styn = any::type_name::<STY>();
        todo!("TODO to_container_events for {styn}")
    }
}

impl<STY> Appendable<STY> for EventsDim0<STY>
where
    STY: ScalarOps,
{
    fn push(&mut self, ts: TsNano, value: STY) {
        self.tss.push_back(ts.ns());
        self.pulses.push_back(0);
        self.values.push_back(value);
    }
}
