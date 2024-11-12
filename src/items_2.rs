pub mod accounting;
pub mod binning;
pub mod channelevents;
pub mod empty;
pub mod eventfull;
pub mod eventsdim0;
pub mod eventsdim0enum;
pub mod eventsdim1;
pub mod framable;
pub mod frame;
pub mod inmem;
pub mod merger;
pub mod streams;
#[cfg(feature = "heavy")]
#[cfg(test)]
pub mod test;
pub mod testgen;
pub mod transform;

use channelevents::ChannelEvents;
use daqbuf_err as err;
use futures_util::Stream;
use items_0::isodate::IsoDateTime;
use items_0::streamitem::Sitemty;
use items_0::transform::EventTransform;
use items_0::Events;
use items_0::MergeError;
use merger::Mergeable;
use netpod::timeunits::*;
use std::collections::VecDeque;
use std::fmt;

pub fn ts_offs_from_abs(tss: &[u64]) -> (u64, VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_sec = tss.first().map_or(0, |&k| k) / SEC;
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: VecDeque<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_anchor_sec, ts_off_ms, ts_off_ns)
}

pub fn ts_offs_from_abs_with_anchor(
    ts_anchor_sec: u64,
    tss: &[u64],
) -> (VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: VecDeque<_> = tss.iter().map(|&k| (k - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| (j - ts_anchor_ns - k))
        .collect();
    (ts_off_ms, ts_off_ns)
}

pub fn pulse_offs_from_abs(pulse: &[u64]) -> (u64, VecDeque<u64>) {
    let pulse_anchor = pulse.first().map_or(0, |&k| k) / 10000 * 10000;
    let pulse_off = pulse.iter().map(|&k| k - pulse_anchor).collect();
    (pulse_anchor, pulse_off)
}

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    General,
    #[allow(unused)]
    MismatchedType,
}

// TODO stack error better
#[derive(Debug, PartialEq)]
pub struct Error {
    #[allow(unused)]
    kind: ErrorKind,
    msg: Option<String>,
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind, msg: None }
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Self {
            msg: Some(msg),
            kind: ErrorKind::General,
        }
    }
}

// TODO this discards structure
impl From<err::Error> for Error {
    fn from(e: err::Error) -> Self {
        Self {
            msg: Some(format!("{e}")),
            kind: ErrorKind::General,
        }
    }
}

// TODO this discards structure
impl From<Error> for err::Error {
    fn from(e: Error) -> Self {
        err::Error::with_msg_no_trace(format!("{e}"))
    }
}

impl std::error::Error for Error {}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        format!("{msg}").into()
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter().map(|&k| IsoDateTime::from_ns_u64(k)).collect()
}

impl Mergeable for Box<dyn Events> {
    fn ts_min(&self) -> Option<u64> {
        self.as_ref().ts_min()
    }

    fn ts_max(&self) -> Option<u64> {
        self.as_ref().ts_max()
    }

    fn new_empty(&self) -> Self {
        self.as_ref().new_empty_evs()
    }

    fn clear(&mut self) {
        Events::clear(self.as_mut())
    }

    fn drain_into(&mut self, dst: &mut Self, range: (usize, usize)) -> Result<(), MergeError> {
        self.as_mut().drain_into_evs(dst, range)
    }

    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_lowest_index_gt_evs(ts)
    }

    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_lowest_index_ge_evs(ts)
    }

    fn find_highest_index_lt(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_highest_index_lt_evs(ts)
    }

    fn tss(&self) -> Vec<netpod::TsMs> {
        Events::tss(self)
            .iter()
            .map(|x| netpod::TsMs::from_ns_u64(*x))
            .collect()
    }
}

pub trait ChannelEventsInput:
    Stream<Item = Sitemty<ChannelEvents>> + EventTransform + Send
{
}

impl<T> ChannelEventsInput for T where
    T: Stream<Item = Sitemty<ChannelEvents>> + EventTransform + Send
{
}
