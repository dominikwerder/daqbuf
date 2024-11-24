use crate::container::ByteEstimate;
use crate::AsAnyMut;
use crate::WithLen;
use core::ops::Range;
use netpod::TsMs;
use netpod::TsNano;
use std::fmt;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "MergeError")]
pub enum Error {}

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

pub trait MergeableTy: fmt::Debug + WithLen + ByteEstimate + Unpin + Sized {
    fn ts_min(&self) -> Option<TsNano>;
    fn ts_max(&self) -> Option<TsNano>;
    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize>;
    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize>;
    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize>;
    fn tss_for_testing(&self) -> Vec<TsMs>;
    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) -> DrainIntoDstResult;
    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewResult<Self>;
}

pub trait MergeableDyn: fmt::Debug + WithLen + ByteEstimate + Unpin + AsAnyMut {
    fn ts_min(&self) -> Option<TsNano>;
    fn ts_max(&self) -> Option<TsNano>;
    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize>;
    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize>;
    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize>;
    fn tss_for_testing(&self) -> Vec<TsMs>;
    fn drain_into(&mut self, dst: &mut dyn MergeableDyn, range: Range<usize>)
        -> DrainIntoDstResult;
}
