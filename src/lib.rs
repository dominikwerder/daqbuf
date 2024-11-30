pub mod accounting;
pub mod apitypes;
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
pub mod jsonbytes;
pub mod merger;
pub mod offsets;
pub mod streams;
#[cfg(feature = "heavy")]
#[cfg(test)]
pub mod test;
pub mod testgen;

use daqbuf_err as err;
use items_0::isodate::IsoDateTime;
use items_0::Events;
use std::fmt;

mod log {
    #[cfg(not(test))]
    pub use netpod::log::*;
    #[cfg(test)]
    pub use netpod::log_direct::*;
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
