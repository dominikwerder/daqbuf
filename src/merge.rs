use crate::AsAnyMut;
use crate::WithLen;
use crate::container::ByteEstimate;
use crate::timebin::BinningggContainerEventsDyn;
use netpod::TsNano;
use std::collections::VecDeque;
use std::fmt;
use std::ops::Range;

autoerr::create_error_v1!(
    name(Error, "MergeError"),
    enum variants {
        Logic,
    },
);

impl From<Error> for daqbuf_err::Error {
    fn from(e: Error) -> Self {
        daqbuf_err::Error::from_string(e)
    }
}

#[derive(Debug)]
pub enum DrainIntoDstResult {
    Done,
    Partial,
    NotCompatible,
}

#[derive(Debug)]
pub enum DrainIntoNewResult<T> {
    Done(T),
    Partial(T),
    NotCompatible,
}

#[derive(Debug)]
pub enum DrainIntoNewDynResult {
    Done(Box<dyn BinningggContainerEventsDyn>),
    Partial(Box<dyn BinningggContainerEventsDyn>),
    NotCompatible,
}

pub trait MergeableTy: fmt::Debug + WithLen + ByteEstimate + Unpin + Sized {
    fn ts_min(&self) -> Option<TsNano>;
    fn ts_max(&self) -> Option<TsNano>;
    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize>;
    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize>;
    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize>;
    fn tss_for_testing(&self) -> VecDeque<TsNano>;
    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) -> DrainIntoDstResult;
    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewResult<Self>;
    fn is_strict_monotonic(&self) -> bool;
    fn is_consistent(&self) -> bool;
}

pub trait MergeableDyn: fmt::Debug + WithLen + ByteEstimate + Unpin + AsAnyMut {
    fn ts_min(&self) -> Option<TsNano>;
    fn ts_max(&self) -> Option<TsNano>;
    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize>;
    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize>;
    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize>;
    fn tss_for_testing(&self) -> VecDeque<TsNano>;
    fn drain_into(&mut self, dst: &mut dyn MergeableDyn, range: Range<usize>)
    -> DrainIntoDstResult;
    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewDynResult;
    fn is_strict_monotonic(&self) -> bool;
    fn is_consistent(&self) -> bool;
}
