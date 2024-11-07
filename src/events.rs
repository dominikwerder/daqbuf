pub use futures_util;

use crate::collect_s::CollectableDyn;
use crate::container::ByteEstimate;
use crate::timebin::BinningggContainerEventsDyn;
use daqbuf_err as err;
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;

pub trait WithLen {
    fn len(&self) -> usize;
}

impl WithLen for bytes::Bytes {
    fn len(&self) -> usize {
        self.len()
    }
}

pub trait Empty {
    fn empty() -> Self;
}

pub trait Resettable {
    fn reset(&mut self);
}

pub trait Appendable<STY>: Empty + WithLen {
    fn push(&mut self, ts: u64, pulse: u64, value: STY);
}

pub trait Extendable: Empty + WithLen {
    fn extend_from(&mut self, src: &mut Self);
}

pub trait TypeName {
    fn type_name(&self) -> String;
}

pub trait AppendEmptyBin {
    fn append_empty_bin(&mut self, ts1: u64, ts2: u64);
}

// TODO rename to make it clear that this moves. Better use drain-into or something similar.
pub trait AppendAllFrom {
    fn append_all_from(&mut self, src: &mut Self);
}

// TODO check usage, probably only for legacy
pub trait HasNonemptyFirstBin {
    fn has_nonempty_first_bin(&self) -> bool;
}

pub trait AsAnyRef {
    fn as_any_ref(&self) -> &dyn Any;
}

pub trait AsAnyMut {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T> AsAnyRef for Box<T>
where
    T: AsAnyRef + ?Sized,
{
    fn as_any_ref(&self) -> &dyn Any {
        self.as_ref().as_any_ref()
    }
}

impl<T> AsAnyMut for Box<T>
where
    T: AsAnyMut + ?Sized,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self.as_mut().as_any_mut()
    }
}

#[derive(Debug)]
pub enum MergeError {
    NotCompatible,
    Full,
}

impl From<MergeError> for err::Error {
    fn from(e: MergeError) -> Self {
        e.to_string().into()
    }
}

impl fmt::Display for MergeError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for MergeError {}

// TODO can I remove the Any bound?

/// Container of some form of events, for use as trait object.
pub trait Events:
    fmt::Debug
    + TypeName
    + Any
    + CollectableDyn
    + WithLen
    + ByteEstimate
    + Send
    + erased_serde::Serialize
    + EventsNonObj
{
    fn verify(&self) -> bool;
    fn output_info(&self) -> String;
    fn as_collectable_mut(&mut self) -> &mut dyn CollectableDyn;
    fn as_collectable_with_default_ref(&self) -> &dyn CollectableDyn;
    fn as_collectable_with_default_mut(&mut self) -> &mut dyn CollectableDyn;
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    // TODO is this used?
    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events>;
    fn new_empty_evs(&self) -> Box<dyn Events>;
    fn drain_into_evs(
        &mut self,
        dst: &mut dyn Events,
        range: (usize, usize),
    ) -> Result<(), MergeError>;
    fn find_lowest_index_gt_evs(&self, ts: u64) -> Option<usize>;
    fn find_lowest_index_ge_evs(&self, ts: u64) -> Option<usize>;
    fn find_highest_index_lt_evs(&self, ts: u64) -> Option<usize>;
    fn clone_dyn(&self) -> Box<dyn Events>;
    fn partial_eq_dyn(&self, other: &dyn Events) -> bool;
    fn serde_id(&self) -> &'static str;
    fn nty_id(&self) -> u32;
    fn tss(&self) -> &VecDeque<u64>;
    fn pulses(&self) -> &VecDeque<u64>;
    fn frame_type_id(&self) -> u32;
    fn to_min_max_avg(&mut self) -> Box<dyn Events>;
    fn to_json_string(&self) -> String;
    fn to_json_vec_u8(&self) -> Vec<u8>;
    fn to_cbor_vec_u8(&self) -> Vec<u8>;
    fn clear(&mut self);
    // TODO: can not name EventsDim0 from here, so use trait object for now. Anyway is a workaround.
    fn to_dim0_f32_for_binning(&self) -> Box<dyn Events>;
    fn to_container_events(&self) -> Box<dyn BinningggContainerEventsDyn>;
}

impl WithLen for Box<dyn Events> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

pub trait EventsNonObj {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>);
}

erased_serde::serialize_trait_object!(Events);

impl PartialEq for Box<dyn Events> {
    fn eq(&self, other: &Self) -> bool {
        Events::partial_eq_dyn(self.as_ref(), other.as_ref())
    }
}

impl EventsNonObj for Box<dyn Events> {
    fn into_tss_pulses(self: Box<Self>) -> (VecDeque<u64>, VecDeque<u64>) {
        todo!()
    }
}

impl Events for Box<dyn Events> {
    fn verify(&self) -> bool {
        Events::verify(self.as_ref())
    }

    fn output_info(&self) -> String {
        Events::output_info(self.as_ref())
    }

    fn as_collectable_mut(&mut self) -> &mut dyn CollectableDyn {
        Events::as_collectable_mut(self.as_mut())
    }

    fn as_collectable_with_default_ref(&self) -> &dyn CollectableDyn {
        Events::as_collectable_with_default_ref(self.as_ref())
    }

    fn as_collectable_with_default_mut(&mut self) -> &mut dyn CollectableDyn {
        Events::as_collectable_with_default_mut(self.as_mut())
    }

    fn ts_min(&self) -> Option<u64> {
        Events::ts_min(self.as_ref())
    }

    fn ts_max(&self) -> Option<u64> {
        Events::ts_max(self.as_ref())
    }

    fn take_new_events_until_ts(&mut self, ts_end: u64) -> Box<dyn Events> {
        Events::take_new_events_until_ts(self.as_mut(), ts_end)
    }

    fn new_empty_evs(&self) -> Box<dyn Events> {
        Events::new_empty_evs(self.as_ref())
    }

    fn drain_into_evs(
        &mut self,
        dst: &mut dyn Events,
        range: (usize, usize),
    ) -> Result<(), MergeError> {
        Events::drain_into_evs(self.as_mut(), dst, range)
    }

    fn find_lowest_index_gt_evs(&self, ts: u64) -> Option<usize> {
        Events::find_lowest_index_gt_evs(self.as_ref(), ts)
    }

    fn find_lowest_index_ge_evs(&self, ts: u64) -> Option<usize> {
        Events::find_lowest_index_ge_evs(self.as_ref(), ts)
    }

    fn find_highest_index_lt_evs(&self, ts: u64) -> Option<usize> {
        Events::find_highest_index_lt_evs(self.as_ref(), ts)
    }

    fn clone_dyn(&self) -> Box<dyn Events> {
        Events::clone_dyn(self.as_ref())
    }

    fn partial_eq_dyn(&self, other: &dyn Events) -> bool {
        Events::partial_eq_dyn(self.as_ref(), other)
    }

    fn serde_id(&self) -> &'static str {
        Events::serde_id(self.as_ref())
    }

    fn nty_id(&self) -> u32 {
        Events::nty_id(self.as_ref())
    }

    fn tss(&self) -> &VecDeque<u64> {
        Events::tss(self.as_ref())
    }

    fn pulses(&self) -> &VecDeque<u64> {
        Events::pulses(self.as_ref())
    }

    fn frame_type_id(&self) -> u32 {
        Events::frame_type_id(self.as_ref())
    }

    fn to_min_max_avg(&mut self) -> Box<dyn Events> {
        Events::to_min_max_avg(self.as_mut())
    }

    fn to_json_string(&self) -> String {
        Events::to_json_string(self.as_ref())
    }

    fn to_json_vec_u8(&self) -> Vec<u8> {
        Events::to_json_vec_u8(self.as_ref())
    }

    fn to_cbor_vec_u8(&self) -> Vec<u8> {
        Events::to_cbor_vec_u8(self.as_ref())
    }

    fn clear(&mut self) {
        Events::clear(self.as_mut())
    }

    fn to_dim0_f32_for_binning(&self) -> Box<dyn Events> {
        Events::to_dim0_f32_for_binning(self.as_ref())
    }

    fn to_container_events(&self) -> Box<dyn BinningggContainerEventsDyn> {
        Events::to_container_events(self.as_ref())
    }
}
