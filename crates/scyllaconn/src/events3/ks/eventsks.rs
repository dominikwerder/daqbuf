use crate::events3::SeriesInfo;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use futures_util::Stream;
use items_0::streamitem::Sitemty2;
use items_2::channelevents::ChannelEvents;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "EventsKs"),
    enum variants {
        Logic,
    },
);

// TODO impl optional with-or-without values, needs more prepared statements?

// TODO impl optional one-before: if with one-before, can reuse msp?

// TODO impl transform, by DynTrans obj?

#[derive(Debug, Clone)]
pub struct Opts {
    with_values: bool,
    one_before: bool,
    qucap: u32,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
}

impl Opts {
    pub fn new() -> Self {
        Self {
            with_values: false,
            one_before: false,
            qucap: 6,
            scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery::new(),
        }
    }

    pub fn set_values(mut self, x: bool) -> Self {
        self.with_values = x;
        self
    }

    pub fn set_one_before(mut self, x: bool) -> Self {
        self.one_before = x;
        self
    }

    pub fn is_values(&self) -> bool {
        self.with_values
    }

    pub fn is_one_before(&self) -> bool {
        self.one_before
    }
}

#[derive(Debug)]
enum State {
    BckMsps,
    BckEventsLst,
    BckEventsCstr,
    FwdEvents,
    Done,
}

/// All, optionally including one-before, optionally without values, or transformed.
#[derive(Debug)]
pub struct EventsKs {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    scyqueue: ScyllaQueueCluster,
    range: ScyllaSeriesRange,
    opts: Opts,
}

impl Stream for EventsKs {
    type Item = Sitemty2<ChannelEvents, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        todo!()
    }
}
