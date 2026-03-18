mod events_msp_fwd_old;
pub mod jobtrace;
pub mod ks;
pub mod lsplst;
pub mod mspbck;
pub mod mspfwd;
pub mod msplsp;

use crate::events2::msp::MspStreamRt;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::MspU32;
use futures_util::Stream;
use items_0::streamitem::Sitemty2;
use items_2::channelevents::ChannelEvents;
use netpod::ChConf;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::ttl::RetentionTime;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

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

// Given series-id, retention-time, scylla-queue, read ordered stream of events.
// Does not eliminate duplicate timestamps.
// Allows for overlapping msp buckets.
#[derive(Debug)]
pub struct EventsMspMerge {
    series: SeriesId,
    rt: RetentionTime,
    msp_outlook: Option<MspU32>,
    msp_inp: MspStreamRt,
}

impl Stream for EventsMspMerge {
    type Item = Sitemty2<ChannelEvents, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
