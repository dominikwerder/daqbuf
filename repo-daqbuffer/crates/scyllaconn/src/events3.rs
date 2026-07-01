#![allow(unused_macros)]
pub mod jobtrace;
pub mod ks;
pub mod lspfwd;
pub mod lsplst;
pub mod mspbck;
pub mod mspfwd;
pub mod msplsp;
mod test_data;

#[cfg(test)]
mod test;

use daqbuf_series::SeriesId;
use netpod::ChConf;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "EventsMspMerge"),
    enum variants {
        Logic,
    },
);

pub const SERIES_ID_A: SeriesId = SeriesId::new(291);
// 1773841020
// 2026, 3, 18, 13, 37, UTC
pub const MSP_A_00: TsMs = TsMs::from_ms_u64(1000 * 1773841020);

#[derive(Debug, Clone)]
pub struct SeriesInfo {
    series: SeriesId,
    scalar_type: ScalarType,
    shape: Shape,
}

impl SeriesInfo {
    pub fn id(&self) -> SeriesId {
        self.series.clone()
    }

    pub fn scalar_type(&self) -> ScalarType {
        self.scalar_type.clone()
    }

    pub fn shape(&self) -> Shape {
        self.shape.clone()
    }

    pub fn new(series: SeriesId, scalar_type: ScalarType, shape: Shape) -> Self {
        Self {
            series,
            scalar_type,
            shape,
        }
    }
}

impl From<&ChConf> for SeriesInfo {
    fn from(chconf: &ChConf) -> Self {
        SeriesInfo {
            series: SeriesId::new(chconf.series()),
            scalar_type: chconf.scalar_type().clone(),
            shape: chconf.shape().clone(),
        }
    }
}
