use daqbuf_err as err;
use err::Error;
use items_0::apitypes::ToUserFacingApiType;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::CollectorDyn;
use items_0::collect_s::CollectorTy;
use items_0::collect_s::ToJsonValue;
use items_0::container::ByteEstimate;
use items_0::isodate::IsoDateTime;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::TimeBinnableTy;
use items_0::timebin::TimeBinnerTy;
use items_0::AsAnyMut;
use items_0::AsAnyRef;
use items_0::Events;
use items_0::EventsNonObj;
use items_0::TypeName;
use items_0::WithLen;
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
use std::mem;

#[allow(unused)]
macro_rules! trace_collect_result {
    ($($arg:tt)*) => {
        if false {
            trace!($($arg)*);
        }
    };
}

#[derive(Debug)]
pub struct EventsDim0EnumCollector {
    vals: EventsDim0Enum,
    range_final: bool,
    timed_out: bool,
    needs_continue_at: bool,
}

impl EventsDim0EnumCollector {
    pub fn new() -> Self {
        Self {
            vals: EventsDim0Enum::new(),
            range_final: false,
            timed_out: false,
            needs_continue_at: false,
        }
    }
}

impl TypeName for EventsDim0EnumCollector {
    fn type_name(&self) -> String {
        "EventsDim0EnumCollector".into()
    }
}

impl WithLen for EventsDim0EnumCollector {
    fn len(&self) -> usize {
        self.vals.tss.len()
    }
}

impl ByteEstimate for EventsDim0EnumCollector {
    fn byte_estimate(&self) -> u64 {
        // TODO does it need to be more accurate?
        30 * self.len() as u64
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim0EnumCollectorOutput {
    #[serde(rename = "tsAnchor")]
    ts_anchor_sec: u64,
    #[serde(rename = "tsMs")]
    ts_off_ms: VecDeque<u64>,
    #[serde(rename = "tsNs")]
    ts_off_ns: VecDeque<u64>,
    #[serde(rename = "values")]
    vals: VecDeque<u16>,
    #[serde(rename = "valuestrings")]
    valstrs: VecDeque<String>,
    #[serde(
        rename = "rangeFinal",
        default,
        skip_serializing_if = "netpod::is_false"
    )]
    range_final: bool,
    #[serde(rename = "timedOut", default, skip_serializing_if = "netpod::is_false")]
    timed_out: bool,
    #[serde(
        rename = "continueAt",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    continue_at: Option<IsoDateTime>,
}

impl WithLen for EventsDim0EnumCollectorOutput {
    fn len(&self) -> usize {
        todo!()
    }
}

impl AsAnyRef for EventsDim0EnumCollectorOutput {
    fn as_any_ref(&self) -> &dyn Any {
        todo!()
    }
}

impl AsAnyMut for EventsDim0EnumCollectorOutput {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        todo!()
    }
}

impl TypeName for EventsDim0EnumCollectorOutput {
    fn type_name(&self) -> String {
        any::type_name::<Self>().into()
    }
}

impl ToUserFacingApiType for EventsDim0EnumCollectorOutput {
    fn into_user_facing_api_type(self: Self) -> Box<dyn items_0::apitypes::UserApiType> {
        todo!()
    }

    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn items_0::apitypes::UserApiType> {
        todo!()
    }
}

impl CollectedDyn for EventsDim0EnumCollectorOutput {}

impl CollectorTy for EventsDim0EnumCollector {
    type Input = EventsDim0Enum;
    type Output = EventsDim0EnumCollectorOutput;

    fn ingest(&mut self, src: &mut EventsDim0Enum) {
        self.vals.tss.append(&mut src.tss);
        self.vals.values.append(&mut src.values);
        self.vals.valuestrs.append(&mut src.valuestrs);
    }

    fn set_range_complete(&mut self) {
        self.range_final = true;
    }

    fn set_timed_out(&mut self) {
        self.timed_out = true;
        self.needs_continue_at = true;
    }

    fn result(&mut self) -> Result<EventsDim0EnumCollectorOutput, Error> {
        trace_collect_result!(
            "{}  result()  needs_continue_at {}",
            self.type_name(),
            self.needs_continue_at
        );
        let range: Option<SeriesRange> = None;
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
        let valixs = mem::replace(&mut vals.values, VecDeque::new());
        let valstrs = mem::replace(&mut vals.valuestrs, VecDeque::new());
        let vals = valixs;
        if ts_off_ms.len() != ts_off_ns.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        if ts_off_ms.len() != vals.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        if ts_off_ms.len() != valstrs.len() {
            return Err(Error::with_msg_no_trace("collected len mismatch"));
        }
        let ret = Self::Output {
            ts_anchor_sec,
            ts_off_ms,
            ts_off_ns,
            vals,
            valstrs,
            range_final: self.range_final,
            timed_out: self.timed_out,
            continue_at,
        };
        Ok(ret)
    }
}

// Experiment with having this special case for enums
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventsDim0Enum {
    pub tss: VecDeque<u64>,
    pub values: VecDeque<u16>,
    pub valuestrs: VecDeque<String>,
}

impl EventsDim0Enum {
    pub fn new() -> Self {
        Self {
            tss: VecDeque::new(),
            values: VecDeque::new(),
            valuestrs: VecDeque::new(),
        }
    }

    pub fn push_back(&mut self, ts: u64, value: u16, valuestr: String) {
        self.tss.push_back(ts);
        self.values.push_back(value);
        self.valuestrs.push_back(valuestr);
    }
}

impl TypeName for EventsDim0Enum {
    fn type_name(&self) -> String {
        "EventsDim0Enum".into()
    }
}

impl AsAnyRef for EventsDim0Enum {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl AsAnyMut for EventsDim0Enum {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl WithLen for EventsDim0Enum {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl CollectableDyn for EventsDim0Enum {
    fn new_collector(&self) -> Box<dyn CollectorDyn> {
        Box::new(EventsDim0EnumCollector::new())
    }
}

// impl Events

impl ByteEstimate for EventsDim0Enum {
    fn byte_estimate(&self) -> u64 {
        todo!()
    }
}

impl EventsNonObj for EventsDim0Enum {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        todo!()
    }
}

// NOTE just a dummy because currently we don't use this for time binning
#[derive(Debug)]
pub struct EventsDim0EnumTimeBinner;

impl TimeBinnerTy for EventsDim0EnumTimeBinner {
    type Input = EventsDim0Enum;
    type Output = ();

    fn ingest(&mut self, _item: &mut Self::Input) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn bins_ready_count(&self) -> usize {
        todo!()
    }

    fn bins_ready(&mut self) -> Option<Self::Output> {
        todo!()
    }

    fn push_in_progress(&mut self, _push_empty: bool) {
        todo!()
    }

    fn cycle(&mut self) {
        todo!()
    }

    fn empty(&self) -> Option<Self::Output> {
        todo!()
    }

    fn append_empty_until_end(&mut self) {
        todo!()
    }
}

// NOTE just a dummy because currently we don't use this for time binning
impl TimeBinnableTy for EventsDim0Enum {
    type TimeBinner = EventsDim0EnumTimeBinner;

    fn time_binner_new(
        &self,
        _binrange: BinnedRangeEnum,
        _do_time_weight: bool,
        _emit_empty_bins: bool,
    ) -> Self::TimeBinner {
        todo!()
    }
}

// NOTE just a dummy because currently we don't use this for time binning

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsDim0EnumChunkOutput {
    tss: VecDeque<u64>,
    values: VecDeque<u16>,
    valuestrings: VecDeque<String>,
    scalar_type: String,
}

impl Events for EventsDim0Enum {
    fn verify(&self) -> bool {
        todo!()
    }

    fn output_info(&self) -> String {
        todo!()
    }

    fn as_collectable_mut(&mut self) -> &mut dyn CollectableDyn {
        todo!()
    }

    fn as_collectable_with_default_ref(&self) -> &dyn CollectableDyn {
        todo!()
    }

    fn as_collectable_with_default_mut(&mut self) -> &mut dyn CollectableDyn {
        todo!()
    }

    fn ts_min(&self) -> Option<u64> {
        todo!()
    }

    fn ts_max(&self) -> Option<u64> {
        todo!()
    }

    fn take_new_events_until_ts(&mut self, _ts_end: u64) -> Box<dyn Events> {
        todo!()
    }

    fn new_empty_evs(&self) -> Box<dyn Events> {
        todo!()
    }

    fn drain_into_evs(
        &mut self,
        _dst: &mut dyn Events,
        _range: (usize, usize),
    ) -> Result<(), err::Error> {
        todo!()
    }

    fn find_lowest_index_gt_evs(&self, _ts: u64) -> Option<usize> {
        todo!()
    }

    fn find_lowest_index_ge_evs(&self, _ts: u64) -> Option<usize> {
        todo!()
    }

    fn find_highest_index_lt_evs(&self, _ts: u64) -> Option<usize> {
        todo!()
    }

    fn clone_dyn(&self) -> Box<dyn Events> {
        todo!()
    }

    fn partial_eq_dyn(&self, _other: &dyn Events) -> bool {
        todo!()
    }

    fn serde_id(&self) -> &'static str {
        todo!()
    }

    fn nty_id(&self) -> u32 {
        todo!()
    }

    fn tss(&self) -> &VecDeque<u64> {
        todo!()
    }

    fn pulses(&self) -> &VecDeque<u64> {
        todo!()
    }

    fn frame_type_id(&self) -> u32 {
        todo!()
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        todo!()
    }

    fn to_json_string(&self) -> String {
        todo!()
    }

    fn to_json_vec_u8(&self) -> Vec<u8> {
        self.to_json_string().into_bytes()
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        // TODO redesign with mut access, rename to `into_` and take the values out.
        let ret = EventsDim0EnumChunkOutput {
            // TODO use &mut to swap the content
            tss: self.tss.clone(),
            values: self.values.clone(),
            valuestrings: self.valuestrs.clone(),
            scalar_type: netpod::EnumVariant::scalar_type_name().into(),
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&ret, &mut buf).unwrap();
        buf
    }

    fn clear(&mut self) {
        todo!()
    }

    fn to_dim0_f32_for_binning(&self) -> Box<dyn Events> {
        todo!("{}::to_dim0_f32_for_binning", self.type_name())
    }

    fn to_container_events(&self) -> Box<dyn ::items_0::timebin::BinningggContainerEventsDyn> {
        todo!("{}::to_container_events", self.type_name())
    }
}
