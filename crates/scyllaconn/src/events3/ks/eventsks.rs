mod bck_events_lst;

use crate::events3::SeriesInfo;
use crate::events3::lsplst;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use bck_events_lst::BckEventsLst;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::TsMs;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use netpod::hpp::HaveProgressPending;
use serde::Serialize;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio::io::Ready;

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
struct BckMsps {
    fut: FutDbg<Result<VecDeque<TsMs>, crate::events3::mspbck::Error>>,
}

#[derive(Debug)]
enum State {
    BckMsps(BckMsps),
    BckEventsLst(BckEventsLst),
    BckEventsCstr,
    FwdEvents,
    Done,
}

/// All, optionally including one-before, optionally without values, or transformed.
#[derive(Debug)]
pub struct EventsKs {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    scyqu: ScyllaQueueCluster,
    range: ScyllaSeriesRange,
    opts: Opts,
    state: State,
}

impl EventsKs {
    pub fn new(
        series_info: SeriesInfo,
        ks: KeyspaceId,
        scyqu: ScyllaQueueCluster,
        range: ScyllaSeriesRange,
        opts: Opts,
    ) -> Self {
        let st1 = BckMsps {
            fut: {
                let ks = ks.clone();
                let id = series_info.id();
                let range = range.clone();
                let scyqu = scyqu.clone();
                async move { scyqu.read_msp_03_bck(ks, id, range).await }.box2()
            },
        };
        let state = State::BckMsps(st1);
        Self {
            series_info,
            ks,
            scyqu,
            range,
            opts,
            state,
        }
    }
}

#[derive(Debug, Serialize)]
pub enum ItemType {
    ChannelEvents(ChannelEvents),
    BckMsps(VecDeque<TsMs>),
    BckEventsLst(bck_events_lst::Res1),
}

impl Stream for EventsKs {
    type Item = Sitemty2<ItemType, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match &mut self.state {
                State::BckMsps(st1) => match st1.fut.poll_unpin(cx) {
                    Ready(x) => {
                        hpp.mark_progress();
                        match x {
                            Ok(msps) => {
                                let msps2 = msps.iter().map(|x| x.clone().into()).collect();
                                let stn = bck_events_lst::BckEventsLst::new(
                                    self.series_info.clone(),
                                    self.ks.clone(),
                                    self.range.clone(),
                                    msps2,
                                    self.scyqu.clone(),
                                );
                                self.state = State::BckEventsLst(stn);
                                break Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                                    ItemType::BckMsps(msps),
                                )))));
                            }
                            Err(e) => {
                                self.state = State::Done;
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::BckEventsLst(st1) => match st1.poll_unpin(cx) {
                    Ready(x) => {
                        hpp.mark_progress();
                        match x {
                            Ok(res1) => {
                                self.state = State::Done;
                                break Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                                    ItemType::BckEventsLst(res1),
                                )))));
                            }
                            Err(e) => {
                                self.state = State::Done;
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::BckEventsCstr => todo!(),
                State::FwdEvents => todo!(),
                State::Done => break Ready(None),
            }
        }
    }
}
