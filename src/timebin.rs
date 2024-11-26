use crate::apitypes::ToUserFacingApiType;
use crate::collect_s::CollectableDyn;
use crate::collect_s::ToCborValue;
use crate::collect_s::ToJsonValue;
use crate::container::ByteEstimate;
use crate::merge::DrainIntoDstResult;
use crate::merge::DrainIntoNewDynResult;
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

// TODO can probably be removed.
pub trait TimeBins {
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    fn ts_min_max(&self) -> Option<(u64, u64)>;
}

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

// #[derive(Debug, ThisError)]
// #[cstm(name = "Binninggg")]
pub enum BinningggError {
    Dyn(Box<dyn std::error::Error>),
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
    E: std::error::Error + 'static,
{
    fn from(value: E) -> Self {
        Self::Dyn(Box::new(value))
    }
}

pub trait BinningggContainerEventsDyn:
    fmt::Debug
    + Send
    + AsAnyRef
    + WithLen
    + ByteEstimate
    + MergeableDyn
    + ToJsonValue
    + ToCborValue
    + ToUserFacingApiType
{
    fn type_name(&self) -> &'static str;
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
}

impl ToUserFacingApiType for Box<dyn BinningggContainerEventsDyn> {
    fn to_user_facing_api_type(self) -> Box<dyn crate::apitypes::UserApiType> {
        let inner = *self;
    }
}

impl<T> MergeableDyn for Box<T>
where
    T: MergeableDyn,
{
    fn ts_min(&self) -> Option<TsNano> {
        self.as_ref().ts_min()
    }

    fn ts_max(&self) -> Option<TsNano> {
        self.as_ref().ts_max()
    }

    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize> {
        self.as_ref().find_lowest_index_gt(ts)
    }

    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize> {
        self.as_ref().find_lowest_index_ge(ts)
    }

    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize> {
        self.as_ref().find_highest_index_lt(ts)
    }

    fn tss_for_testing(&self) -> Vec<netpod::TsMs> {
        self.as_ref().tss_for_testing()
    }

    fn drain_into(
        &mut self,
        dst: &mut dyn MergeableDyn,
        range: Range<usize>,
    ) -> DrainIntoDstResult {
        self.as_mut().drain_into(dst, range)
    }

    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewDynResult {
        self.as_mut().drain_into_new(range)
    }

    fn is_consistent(&self) -> bool {
        self.as_ref().is_consistent()
    }
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
