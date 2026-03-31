use crate::events3::SeriesInfo;
use crate::events3::ks::lsp_fwd_msp_single::LspFwdMspSingleStream;
use crate::events3::lsplst;
use crate::events3::mspfwd::ReadMsp03Fwd;
use crate::events3::mspfwd::ReadMspFwdStream;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::channelevents::ChannelEvents;
use netpod::DtNano;
use netpod::RangeExcl;
use netpod::TsMs;
use netpod::TsNano;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use netpod::hpp::HaveProgressPending;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio::io::Ready;
use taskrun::tracing_subscriber::field::debug;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "LspFwdMspMulti"),
    enum variants {
        MspFwd(#[from] crate::events3::mspfwd::Error),
        LspFwd(#[from] crate::events3::lspfwd::Error),
        FindNextTsWithoutBuf,
        FindNextTsOnEmptyBuf,
    },
);

#[derive(Debug, Clone)]
pub struct Opts {
    with_values: bool,
    qucap: u32,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
}

impl Opts {
    pub fn new() -> Self {
        Self {
            with_values: false,
            qucap: 6,
            scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery::new(),
        }
    }

    pub fn set_values(mut self, x: bool) -> Self {
        self.with_values = x;
        self
    }

    pub fn is_values(&self) -> bool {
        self.with_values
    }
}

// Then merge those here.
// The user has already found the ts of the latest-one-before.

// TODO in constructor, take also a list of msp for which the user of this type already know that it makes sense to read from.
// TODO open a stream of msp which starts reading msp after the highest msp that the user gave us in the list.
// TODO for each msp, open a LspFwdMspSingleStream.
// TODO merge all those streams into a single output stream.
// TODO to implement the merging, we must utilize the trait MergeableDyn.
// TODO As long as we have some event from any of the opnened streams that is smaller than the next msp from the msp stream,
// we do not yet need to open a event stream for that msp, because it can not produce events before.
// TODO We want to keep the number of open streams low.

/*
PLAN
====

Goal: A Stream<Item = EventsBoxed> that merges events from multiple MSP buckets into a single
time-ordered output. Each MSP bucket is read by a LspFwdMspSingleStream. Because an event stored
in MSP bucket M always has actual_ts >= M.start, we know that once all MSP buckets with
start <= T have been opened, all events with actual_ts < T are already in the open streams.
This gives us a safe delivery frontier.


── Additional fields for FwdMspMerged ───────────────────────────────────────────────────────

  known_msps: VecDeque<MspEv>
      MSPs provided by the caller (sorted ascending), not yet opened as event streams.

  msp_stream: ReadMspFwdStream
      Streams new MSP timestamps from the DB, starting after the highest MSP in known_msps.
      Used to discover MSP buckets that the caller did not know about in advance.

  open: Vec<(LspFwdMspSingleStream, Option<EventsBoxed>)>
      Each entry is one active event stream plus the most-recently-fetched batch (its buffer).
      The buffer is None when the stream has not yet been polled or when the previous batch
      was fully consumed.

  msp_stream_done: bool
      Set when ReadMspFwdStream returns Ready(None).



── poll_next state machine ──────────────────────────────────────────────────────────────────

  Each call to poll_next runs a loop with the following steps:

  Step 1 — poll msp_stream for new MSPs (if not done):
    Poll msp_stream.poll_next(cx):
      Ready(Some(Ok(batch))): for each TsMs in the batch, convert to MspEv, open a
        LspFwdMspSingleStream (limit = 40), push to open with buffer = None.
      Ready(Some(Err(_))): set msp_stream_done = true (treat as done on error).
      Ready(None): set msp_stream_done = true.
      Pending: continue to step 2.

  Step 2 — fill empty buffers (poll open streams that have buffer = None):
    For each entry in open:
      If buffer is None, poll the stream:
        Ready(Some(Ok(v))): set buffer = Some(v).
        Ready(Some(Err(e))): return Ready(Some(Err(e.into()))).
        Ready(None): remove the entry (stream exhausted).
        Pending: note "has_pending = true".

  Step 3 — compute safe delivery frontier:
    safe_ts = if msp_stream_done && open.iter().all(|e| e.buffer.is_some()):
        // All future events are accounted for; drain everything.
        range.end()
      else:
        // Frontier is the earliest MSP not yet opened (its events can't arrive before it).
        // We use ts_min of all current buffers as an additional lower bound.
        // Since msp_stream yields TsMs, the next MSP's start is the start of the
        // stream's internal cursor.  Approximate as:
        //   min ts_min over all non-None buffers
        // This is conservative: we only yield events that every open stream agrees are "past".
        let frontier = open.iter()
            .filter_map(|(_, buf)| buf.as_ref()?.as_mergeable_dyn_mut_ref().ts_min())
            .min();
        match frontier { Some(t) => t, None => return if has_pending { Pending } else { Ready(None) } }

  Step 4 — drain and merge events with actual_ts < safe_ts from all buffers:
    let mut merged: Option<Box<dyn BinningggContainerEventsDyn>> = None;
    For each entry whose buffer is Some(v):
      Let idx = v.as_mergeable_dyn_mut().find_highest_index_lt(safe_ts):
        Some(i): drain_into_new(0..i+1) → DrainIntoNewDynResult::Done(chunk) or Partial(chunk):
          merge chunk into `merged` via drain_into (or just set merged = Some(chunk) for first).
          If Partial: keep remainder in buffer.
          If Done: set buffer = None (stream needs more data next iteration).
        None: nothing to drain from this buffer.

── Notes ────────────────────────────────────────────────────────────────────────────────────

  - EventsBoxed = Box<dyn BinningggContainerEventsDyn>  (from items_0::timebin).
  - Merging two EventsBoxed requires they have the same concrete type (same scalar_type and
    shape). Since series_info is fixed across all streams this holds.
  - The Opts struct and State enum already present can be retained or simplified as needed.
  - The Error type already has a Logic variant; add LspFwdError(#[from] lspfwd::Error) and
    MspFwdError(#[from] mspfwd::Error) variants.
*/

pub type Item = Box<dyn BinningggContainerEventsDyn>;

#[derive(Debug)]
struct Inp {
    evs: Option<LspFwdMspSingleStream>,
    buf: Option<Item>,
}

#[derive(Debug)]
struct InputIx(usize);

#[derive(Debug)]
enum NextTs {
    None,
    One(TsNano, InputIx),
    Two(TsNano, InputIx, TsNano, InputIx),
}

struct BaseRefs<'a> {
    series_info: &'a SeriesInfo,
    ks: &'a KeyspaceId,
    range: &'a ScyllaSeriesRange,
    scyqu: &'a mut ScyllaQueueCluster,
}

#[derive(Debug)]
enum CheckInputItem {
    OpenNextMsp,
    Item(Item),
}

#[derive(Debug)]
struct Merging {
    msps: Option<ReadMspFwdStream>,
    mspbuf: VecDeque<TsMs>,
    inps: VecDeque<Inp>,
}

impl Merging {
    fn find_next_ts(self: Pin<&mut Self>) -> Result<NextTs, Error> {
        let self2 = self.get_mut();
        let mut best: Option<(TsNano, usize)> = None;
        let mut second: Option<(TsNano, usize)> = None;
        for (ix, inp) in self2.inps.iter_mut().enumerate() {
            if let Some(buf) = inp.buf.as_mut() {
                if let Some(ts) = buf.as_mergeable_dyn_mut().ts_min() {
                    match best {
                        Some((ts_1st, _)) => {
                            if ts < ts_1st {
                                second = best;
                                best = Some((ts, ix));
                            } else {
                                match second {
                                    None => {
                                        second = Some((ts, ix));
                                    }
                                    Some((ts_2nd, _)) => {
                                        if ts < ts_2nd {
                                            second = Some((ts, ix));
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            best = Some((ts, ix));
                        }
                    }
                } else {
                    return Err(Error::FindNextTsOnEmptyBuf);
                }
            } else {
                return Err(Error::FindNextTsWithoutBuf);
            }
        }
        let ret = match (best, second) {
            (None, _) => NextTs::None,
            (Some((ts1, ix1)), None) => NextTs::One(ts1, InputIx(ix1)),
            (Some((ts1, ix1)), Some((ts2, ix2))) => NextTs::Two(ts1, InputIx(ix1), ts2, InputIx(ix2)),
        };
        Ok(ret)
    }

    fn consider_input(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        brefs: BaseRefs,
        msp_next: Option<TsMs>,
    ) -> Poll<Option<Sitemty2<Item, Error>>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }

    fn poll_all_inp(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        brefs: BaseRefs,
    ) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        // TODO when does this loop actually terminate?
        'outer: loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            for inp in &mut self2.inps {
                if let Some(buf) = inp.buf.as_mut() {
                    if buf.len() == 0 {
                        hpp.mark_progress();
                        inp.buf = None;
                    }
                } else {
                    if let Some(inps) = inp.evs.as_mut() {
                        match inps.poll_next_unpin(cx) {
                            Ready(Some(x)) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(x) => {
                                        if x.len() == 0 {
                                            // TODO count for metrics
                                        } else {
                                            inp.buf = Some(x);
                                        }
                                    }
                                    Err(e) => break 'outer Ready(Some(Err(e.into()))),
                                }
                            }
                            Ready(None) => {
                                hpp.mark_progress();
                                inp.evs = None;
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        // will get removed
                    }
                }
            }
            {
                let n1 = self2.inps.len();
                self2.inps.retain(|x| x.buf.is_some() || x.evs.is_some());
                let n2 = self2.inps.len();
                if n1 != n2 {
                    hpp.mark_progress();
                }
            }
            let n_it = self2.inps.iter().filter(|x| x.buf.is_none());
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else if n_it.count() == 0 {
                Ready(Some(Ok(())))
            } else {
                Ready(None)
            };
        }
    }

    fn check_inputs(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        brefs: BaseRefs,
        msp_next: Option<TsMs>,
    ) -> Poll<Option<Result<CheckInputItem, Error>>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let brefs2 = BaseRefs {
                series_info: brefs.series_info,
                ks: brefs.ks,
                range: brefs.range,
                scyqu: brefs.scyqu,
            };
            match self.as_mut().poll_all_inp(cx, brefs2) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match x {
                        Ok(()) => match self.as_mut().find_next_ts()? {
                            NextTs::None => {}
                            NextTs::One(ts1, ix1) => {
                                // TODO check if msp lower that final entry in ix1 buffer.
                                // if yes, add a new stream for that msp and start over.
                            }
                            NextTs::Two(ts1, ix1, ts2, ix2) => {
                                if msp_next.map_or(false, |x| x.ns() <= ts2) {
                                    // TODO add a new stream from that msp.
                                    // TODO but also have to drain that msp so that it can get replenished from stream.
                                } else {
                                    // TODO drain all events with ts <= ts2.
                                }
                            }
                        },
                        Err(e) => break Ready(Some(Err(e.into()))),
                    }
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            let self2 = self.as_mut().get_mut();
            // TODO check whether we have to open next msp before we can make a decision.
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }

    fn gen_limit(&mut self) -> u32 {
        3
    }

    fn poll_state(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        brefs: BaseRefs,
    ) -> Poll<Option<Sitemty2<Item, Error>>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            let brefs2 = BaseRefs {
                series_info: brefs.series_info,
                ks: brefs.ks,
                range: brefs.range,
                scyqu: brefs.scyqu,
            };
            if let Some(&msp_next) = self2.mspbuf.front() {
                match Pin::new(&mut *self2).check_inputs(cx, brefs2, Some(msp_next)) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => match x {
                                CheckInputItem::OpenNextMsp => {
                                    // In this scope we know that there exists one:
                                    let msp_next = self2.mspbuf.pop_front().unwrap();
                                    let msp = MspEv::from(msp_next);
                                    let stream = LspFwdMspSingleStream::new(
                                        brefs.ks.clone(),
                                        brefs.series_info.clone(),
                                        msp,
                                        brefs.range.clone(),
                                        self2.gen_limit(),
                                        brefs.scyqu.clone(),
                                    );
                                    self2.inps.push_back(Inp {
                                        evs: Some(stream),
                                        buf: None,
                                    });
                                }
                                CheckInputItem::Item(x) => {
                                    break Ready(Some(items_0::streamitem::sitem2_data(x)));
                                }
                            },
                            Err(e) => break Ready(Some(Err(e.into()))),
                        }
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else if let Some(inp) = self2.msps.as_mut() {
                match inp.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => {
                                self2.mspbuf.extend(x);
                            }
                            Err(e) => break Ready(Some(Err(e.into()))),
                        }
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self2.msps = None;
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else {
                // TODO can we unify better with the case above?
                match Pin::new(&mut *self2).check_inputs(cx, brefs2, None) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => match x {
                                CheckInputItem::OpenNextMsp => {
                                    // In this scope we know that there exists one:
                                    let msp_next = self2.mspbuf.pop_front().unwrap();
                                    let msp = MspEv::from(msp_next);
                                    let stream = LspFwdMspSingleStream::new(
                                        brefs.ks.clone(),
                                        brefs.series_info.clone(),
                                        msp,
                                        brefs.range.clone(),
                                        self2.gen_limit(),
                                        brefs.scyqu.clone(),
                                    );
                                    self2.inps.push_back(Inp {
                                        evs: Some(stream),
                                        buf: None,
                                    });
                                }
                                CheckInputItem::Item(x) => {
                                    break Ready(Some(items_0::streamitem::sitem2_data(x)));
                                }
                            },
                            Err(e) => break Ready(Some(Err(e.into()))),
                        }
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}

#[derive(Debug)]
enum State {
    Merging(Merging),
    Done,
}

#[derive(Debug)]
pub struct LspFwdMspMulti {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    range: ScyllaSeriesRange,
    opts: Opts,
    state: State,
    scyqu: ScyllaQueueCluster,
}

impl LspFwdMspMulti {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        range: ScyllaSeriesRange,
        opts: Opts,
        scyqu: ScyllaQueueCluster,
        msps: VecDeque<MspEv>,
    ) -> Self {
        todo!("use the passed msps, because the caller already has this reduced set at hand.");
        // TODO start the msp stream only from the largest passed msp onwards.
        // TODO what if the passed set is empty? Then start msp stream only from range.beg()
        let state = State::Merging(Merging {
            msps: None,
            mspbuf: VecDeque::new(),
            inps: VecDeque::new(),
        });
        Self {
            ks,
            series_info,
            range,
            opts,
            state,
            scyqu,
        }
    }
}

impl Stream for LspFwdMspMulti {
    type Item = Sitemty2<Item, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Merging(st1) => {
                    let brefs = BaseRefs {
                        series_info: &self2.series_info,
                        ks: &self2.ks,
                        range: &self2.range,
                        scyqu: &mut self2.scyqu,
                    };
                    match Pin::new(st1).poll_state(cx, brefs) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            break Ready(Some(x));
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            self2.state = State::Done;
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Done => {}
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}
