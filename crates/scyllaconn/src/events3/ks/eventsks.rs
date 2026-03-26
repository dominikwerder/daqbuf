mod bck_events_lst;

use crate::events3::SeriesInfo;
use crate::events3::lsplst;
use crate::events3::mspfwd::ReadMsp03Fwd;
use crate::events3::mspfwd::ReadMspFwdStream;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use bck_events_lst::BckEventsLst;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::DtNano;
use netpod::RangeExcl;
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
        MspFwd(#[from] crate::events3::mspfwd::Error),
        BckLspLst(#[from] bck_events_lst::Error),
        MspBckTooMany,
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
    fut: FutDbg<Result<VecDeque<TsMs>, Error>>,
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
                // Collect all msp from the backward window.
                // Error if we find too many.
                let ks = ks.clone();
                let series = series_info.id();
                let win = DtNano::from_sec(ks.rt().msp_rollover_ivl_on_read().as_secs());
                debug!("backward window {win} h", win = win.sec_u64() / 60 / 60);
                let range = { ScyllaSeriesRange::new(range.beg().sub(win), range.beg()) };
                let scyqu = scyqu.clone();
                // TODO change the limit to larger for non-test-data
                let mut stream = ReadMspFwdStream::new(ks, series, range, RangeExcl::None, 1, scyqu);
                async move {
                    let mut msps = VecDeque::new();
                    while let Some(x) = stream.next().await {
                        msps.extend(x?);
                        if msps.len() > 80 {
                            error!("too many msp in backward window");
                            return Err(Error::MspBckTooMany);
                        }
                    }
                    for e in &msps {
                        debug!("got backward msp {e}");
                    }
                    Ok(msps)
                }
                .box2()
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
                                break Ready(Some(Err(e)));
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
                                break Ready(Some(Err(e.into())));
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
