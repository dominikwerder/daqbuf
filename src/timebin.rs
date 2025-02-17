use crate::apitypes::ToUserFacingApiType;
use crate::collect_s::CollectableDyn;
use crate::container::ByteEstimate;
use crate::merge::MergeableDyn;
use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::TypeName;
use crate::WithLen;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::TsNano;
use std::fmt;
use std::ops::Range;

// TODO remove
pub trait TimeBinnerTy: fmt::Debug + Send + Unpin {
    type Input: fmt::Debug;
    type Output: fmt::Debug;
    fn ingest(&mut self, item: &mut Self::Input);
    fn set_range_complete(&mut self);
    fn bins_ready_count(&self) -> usize;
    fn bins_ready(&mut self) -> Option<Self::Output>;
    /// If there is a bin in progress with non-zero count, push it to the result set.
    /// With push_empty == true, a bin in progress is pushed even if it contains no counts.
    fn push_in_progress(&mut self, push_empty: bool);
    /// Implies `Self::push_in_progress` but in addition, pushes a zero-count bin if the call
    /// to `push_in_progress` did not change the result count, as long as edges are left.
    /// The next call to `Self::bins_ready_count` must return one higher count than before.
    fn cycle(&mut self);
    fn empty(&self) -> Option<Self::Output>;
    fn append_empty_until_end(&mut self);
}

pub trait TimeBinnableTy: fmt::Debug + WithLen + Send + Sized {
    type TimeBinner: TimeBinnerTy<Input = Self>;

    fn time_binner_new(
        &self,
        binrange: BinnedRangeEnum,
        do_time_weight: bool,
        emit_empty_bins: bool,
    ) -> Self::TimeBinner;
}

#[derive(Debug)]
pub enum BinningggError {
    Dyn(Box<dyn std::error::Error + Send>),
    TypeMismatch { have: String, expect: String },
}

impl fmt::Display for BinningggError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BinningggError::Dyn(e) => write!(fmt, "{e}"),
            BinningggError::TypeMismatch { have, expect } => {
                write!(fmt, "TypeMismatch(have: {have}, expect: {expect})")
            }
        }
    }
}

impl<E> From<E> for BinningggError
where
    E: std::error::Error + Send + 'static,
{
    fn from(value: E) -> Self {
        Self::Dyn(Box::new(value))
    }
}

pub trait BinningggContainerEventsDyn:
    fmt::Debug
    + TypeName
    + Send
    + AsAnyRef
    + WithLen
    + ByteEstimate
    + MergeableDyn
    + ToUserFacingApiType
    + CollectableDyn
{
    fn binned_events_timeweight_traitobj(
        &self,
        range: BinnedRange<TsNano>,
    ) -> Box<dyn BinnedEventsTimeweightTrait>;
    fn to_anybox(&mut self) -> Box<dyn std::any::Any>;
    fn clone_dyn(&self) -> Box<dyn BinningggContainerEventsDyn>;
    fn serde_id(&self) -> u32;
    fn nty_id(&self) -> u32;
    fn eq(&self, rhs: &dyn BinningggContainerEventsDyn) -> bool;
    fn as_mergeable_dyn_mut(&mut self) -> &mut dyn MergeableDyn;
    fn as_collectable_dyn_mut(&mut self) -> &mut dyn CollectableDyn;
    fn to_f32_for_binning_v01(&self) -> Box<dyn BinningggContainerEventsDyn>;
}

pub trait BinningggContainerBinsDyn:
    fmt::Debug + Send + fmt::Display + TypeName + WithLen + AsAnyMut + CollectableDyn
{
    fn empty(&self) -> BinsBoxed;
    fn clone(&self) -> BinsBoxed;
    fn edges_iter(
        &self,
    ) -> std::iter::Zip<
        std::collections::vec_deque::Iter<TsNano>,
        std::collections::vec_deque::Iter<TsNano>,
    >;
    fn drain_into(&mut self, dst: &mut dyn BinningggContainerBinsDyn, range: Range<usize>);
    fn binned_bins_timeweight_traitobj(
        &self,
        range: BinnedRange<TsNano>,
    ) -> Box<dyn BinnedBinsTimeweightTrait>;
    fn boxed_into_collectable_box(self: Box<Self>) -> Box<dyn CollectableDyn>;
    fn fix_numerics(&mut self);
}

pub type BinsBoxed = Box<dyn BinningggContainerBinsDyn>;

pub type EventsBoxed = Box<dyn BinningggContainerEventsDyn>;

pub trait BinningggBinnerTy: fmt::Debug + Send {
    type Input: fmt::Debug;
    type Output: fmt::Debug;
    fn ingest(&mut self, item: &mut Self::Input);
    fn range_final(&mut self);
    fn bins_ready_count(&self) -> usize;
    fn bins_ready(&mut self) -> Option<Self::Output>;
}

pub trait BinningggBinnableTy: fmt::Debug + WithLen + Send {
    type Binner: BinningggBinnerTy<Input = Self>;
    fn binner_new(range: BinnedRange<TsNano>) -> Self::Binner;
}

pub trait BinningggBinnerDyn: fmt::Debug + Send {
    fn input_done_range_final(&mut self) -> Result<(), BinningggError>;
    fn input_done_range_open(&mut self) -> Result<(), BinningggError>;
}

pub trait BinnedEventsTimeweightTrait: fmt::Debug + Send {
    fn cnt_zero_enable(&mut self);
    fn ingest(&mut self, evs: &EventsBoxed) -> Result<(), BinningggError>;
    fn input_done_range_final(&mut self) -> Result<(), BinningggError>;
    fn input_done_range_open(&mut self) -> Result<(), BinningggError>;
    fn output(&mut self) -> Result<Option<BinsBoxed>, BinningggError>;
}

pub trait BinnedBinsTimeweightTrait: fmt::Debug + Send {
    fn ingest(&mut self, bins: &BinsBoxed) -> Result<(), BinningggError>;
    fn input_done_range_final(&mut self) -> Result<(), BinningggError>;
    fn input_done_range_open(&mut self) -> Result<(), BinningggError>;
    fn output(&mut self) -> Result<Option<BinsBoxed>, BinningggError>;
}
