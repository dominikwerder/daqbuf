use super::msp::MspStreamRt;
use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use daqbuf_err as err;
use daqbuf_series::SeriesId;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::Appendable;
use items_0::Empty;
use items_0::WithLen;
use items_0::container::ByteEstimate;
use items_0::merge::DrainIntoNewDynResult;
use items_0::merge::MergeableDyn;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::ChConf;
use netpod::DtNano;
use netpod::EnumVariant;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsMsVecFmt;
use netpod::TsNano;
use netpod::log;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tracing;

macro_rules! error { ($($arg:expr),*) => ( if true { log::error!($($arg),*); } ) }

macro_rules! warn { ($($arg:expr),*) => ( if true { log::warn!($($arg),*); } ) }

macro_rules! trace_init { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace_fetch { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace_msp_fetch { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace_redo_fwd_read { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace_emit { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace_every_event { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ) }

macro_rules! warn_item { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }

macro_rules! log_fetch_result {
    ($($arg:tt)*) => { if false { log::trace!("fetch  {}", format_args!($($arg)*)); } };
}

#[derive(Debug, Clone)]
pub struct EventReadOpts {
    with_values: bool,
    one_before: bool,
    qucap: u32,
}

impl EventReadOpts {
    pub fn new(one_before: bool, with_values: bool, qucap: Option<u32>) -> Self {
        Self {
            one_before,
            with_values,
            qucap: qucap.unwrap_or(6),
        }
    }

    pub fn with_values(&self) -> bool {
        self.with_values
    }
}

autoerr::create_error_v1!(
    name(Error, "ScyllaEvents"),
    enum variants {
        Worker(Box<crate::worker::Error>),
        Msp(#[from] crate::events2::msp::Error),
        Unordered,
        OutOfRange,
        BadBatch,
        ReadQueueEmptyBck,
        ReadQueueEmptyFwd,
        Logic,
        TruncateLogic,
        AlreadyTaken,
        DrainFailure,
        RangeEndOverflow,
        NotTokenAware,
        Prepare(#[from] crate::events2::prepare::Error),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaWorker(Box<crate::worker::Error>),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
    },
);

impl From<crate::worker::Error> for Error {
    fn from(e: crate::worker::Error) -> Self {
        Error::Worker(Box::new(e))
    }
}

struct FetchMsp {
    fut: Pin<Box<dyn Future<Output = Result<Vec<TsMs>, crate::events2::msp::Error>> + Send>>,
}

type ReadEventsFutOut = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), crate::events2::events::Error>;

type FetchEventsFut = Pin<Box<dyn Future<Output = ReadEventsFutOut> + Send>>;

enum Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
    Ongoing(F),
    Ready(<F as Future>::Output),
}

impl<F> Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
}

impl<F> Future for Fst<F>
where
    F: Future + Unpin,
    <F as Future>::Output: Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        match self.as_mut().get_mut() {
            Fst::Ongoing(fut) => match fut.poll_unpin(cx) {
                Ready(x) => {
                    *self = Fst::Ready(x);
                    Ready(())
                }
                Pending => Pending,
            },
            Fst::Ready(_) => Ready(()),
        }
    }
}

struct ReadQueue {
    cap: usize,
    futs: VecDeque<Fst<FetchEventsFut>>,
}

impl ReadQueue {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            futs: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.futs.len()
    }

    fn has_space(&self) -> bool {
        self.len() < self.cap
    }

    fn push(&mut self, fut: FetchEventsFut) {
        self.futs.push_back(Fst::Ongoing(fut));
    }
}

impl Stream for ReadQueue {
    type Item = <FetchEventsFut as Future>::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.futs.len() == 0 {
            Ready(None)
        } else {
            for fut in self.futs.iter_mut() {
                let _ = fut.poll_unpin(cx);
            }
            if let Some(Fst::Ready(_)) = self.futs.front() {
                if let Some(Fst::Ready(k)) = self.futs.pop_front() {
                    Ready(Some(k))
                } else {
                    panic!()
                }
            } else {
                Pending
            }
        }
    }
}

struct FetchEvents {
    fut: FetchEventsFut,
}

impl FetchEvents {
    fn from_fut(
        fut: Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>>,
    ) -> Self {
        Self { fut }
    }
}

enum ReadingState {
    FetchMsp(FetchMsp),
    FetchEvents(FetchEvents),
}

struct ReadingBck {
    reading_state: ReadingState,
}

struct ReadingFwd {
    msp_done: bool,
    msp_fut: Option<FetchMsp>,
    qu: ReadQueue,
    make_fut_info: MakeFutInfo,
}

impl ReadingFwd {
    fn new(k: &EventsStreamRt) -> Self {
        Self {
            msp_done: false,
            msp_fut: None,
            qu: ReadQueue::new(k.qucap),
            make_fut_info: MakeFutInfo::new(k),
        }
    }
}

#[derive(Debug)]
pub enum ReadEventKind {
    Create,
    FutgenCallingReadNextValues,
    FutgenFutureCreated,
    CallExecuteIter,
    ScyllaReadRow(u32),
    ScyllaReadRowDone(u32),
    ReadNextValuesFutureDone,
    EventsStreamRtSees(u32),
}

#[derive(Debug)]
pub struct ReadJobTrace {
    jobid: u64,
    ts0: Instant,
    events: Vec<(Instant, ReadEventKind)>,
}

impl ReadJobTrace {
    pub fn new() -> Self {
        static JOBID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self {
            jobid: JOBID.fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            ts0: Instant::now(),
            events: Vec::with_capacity(128),
        }
    }

    pub fn add_event_now(&mut self, kind: ReadEventKind) {
        self.events.push((Instant::now(), kind))
    }
}

impl fmt::Display for ReadJobTrace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "ReadJobTrace  jobid {jid}", jid = self.jobid)?;
        for (ts, kind) in &self.events {
            let dt = 1e3 * ts.saturating_duration_since(self.ts0).as_secs_f32();
            write!(fmt, "\njobid {jid:4}  {dt:7.2}  {kind:?}", jid = self.jobid)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct ReadNextValuesOpts {
    rt: RetentionTime,
    series: u64,
    ts_msp: TsMs,
    range: ScyllaSeriesRange,
    fwd: bool,
    readopts: EventReadOpts,
    scyqueue: ScyllaQueue,
}

impl ReadNextValuesOpts {
    pub(super) fn new(
        rt: RetentionTime,
        series: SeriesId,
        ts_msp: TsMs,
        range: ScyllaSeriesRange,
        fwd: bool,
        readopts: EventReadOpts,
        scyqueue: ScyllaQueue,
    ) -> Self {
        Self {
            rt,
            series: series.id(),
            ts_msp,
            range,
            fwd,
            readopts,
            scyqueue,
        }
    }
}

struct ReadNextValuesParams {
    opts: ReadNextValuesOpts,
    jobtrace: ReadJobTrace,
}

#[derive(Clone)]
struct MakeFutInfo {
    scyqueue: ScyllaQueue,
    rt: RetentionTime,
    series: SeriesId,
    range: ScyllaSeriesRange,
    readopts: EventReadOpts,
    ch_conf: ChConf,
}

impl MakeFutInfo {
    fn new(k: &EventsStreamRt) -> Self {
        Self {
            scyqueue: k.scyqueue.clone(),
            rt: k.rt.clone(),
            series: k.series.clone(),
            range: k.range.clone(),
            readopts: k.readopts.clone(),
            ch_conf: k.ch_conf.clone(),
        }
    }
}

enum State {
    Begin,
    ReadingBck(ReadingBck),
    ReadingFwd(ReadingFwd),
    InputDone,
    Done,
}

pub struct EventsStreamRt {
    rt: RetentionTime,
    ch_conf: ChConf,
    series: SeriesId,
    range: ScyllaSeriesRange,
    readopts: EventReadOpts,
    state: State,
    scyqueue: ScyllaQueue,
    msp_inp: MspStreamRt,
    msp_buf: VecDeque<TsMs>,
    msp_buf_bck: VecDeque<TsMs>,
    out: VecDeque<Box<dyn BinningggContainerEventsDyn>>,
    out_cnt: u64,
    ts_seen_max: TsNano,
    qucap: usize,
}

impl EventsStreamRt {
    pub fn new(
        rt: RetentionTime,
        ch_conf: ChConf,
        range: ScyllaSeriesRange,
        readopts: EventReadOpts,
        scyqueue: ScyllaQueue,
    ) -> Self {
        trace_init!("EventsStreamRt::new  {ch_conf:?}  {range:?}  {rt:?}  {readopts:?}");
        let series = SeriesId::new(ch_conf.series());
        let msp_inp = crate::events2::msp::MspStreamRt::new(rt.clone(), series, range.clone(), scyqueue.clone());
        Self {
            qucap: readopts.qucap as usize,
            rt,
            ch_conf,
            series,
            range,
            readopts,
            state: State::Begin,
            scyqueue,
            msp_inp,
            msp_buf: VecDeque::new(),
            msp_buf_bck: VecDeque::new(),
            out: VecDeque::new(),
            out_cnt: 0,
            ts_seen_max: TsNano::from_ns(0),
        }
    }

    fn make_msp_read_fut(
        msp_inp: &mut MspStreamRt,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TsMs>, crate::events2::msp::Error>> + Send>> {
        trace_fetch!("make_msp_read_fut");
        let msp_inp = unsafe {
            let ptr = msp_inp as *mut MspStreamRt;
            &mut *ptr
        };
        let fut = async {
            let cap = 128;
            let mut a = Vec::with_capacity(cap);
            while let Some(x) = msp_inp.next().await {
                match x {
                    Ok(x) => {
                        a.push(x);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
                if a.len() >= cap {
                    break;
                }
            }
            Ok(a)
        };
        let fut = Box::pin(fut);
        fut
    }

    fn make_read_events_fut(
        ts_msp: TsMs,
        bck: bool,
        mfi: MakeFutInfo,
        jobtrace: ReadJobTrace,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>> {
        let opts = ReadNextValuesOpts::new(mfi.rt, mfi.series, ts_msp, mfi.range, !bck, mfi.readopts, mfi.scyqueue);
        let scalar_type = mfi.ch_conf.scalar_type().clone();
        let shape = mfi.ch_conf.shape().clone();
        trace_fetch!(
            "make_read_events_fut  bck {}  msp {:?} {}  {:?}  {:?}",
            bck,
            ts_msp,
            ts_msp.fmt(),
            shape,
            scalar_type
        );
        let fut = async move {
            if false {
                taskrun::tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let params = ReadNextValuesParams { opts, jobtrace };
            let ret = match &shape {
                Shape::Scalar => match &scalar_type {
                    ScalarType::U8 => read_next_values::<u8>(params).await,
                    ScalarType::U16 => read_next_values::<u16>(params).await,
                    ScalarType::U32 => read_next_values::<u32>(params).await,
                    ScalarType::U64 => read_next_values::<u64>(params).await,
                    ScalarType::I8 => read_next_values::<i8>(params).await,
                    ScalarType::I16 => read_next_values::<i16>(params).await,
                    ScalarType::I32 => read_next_values::<i32>(params).await,
                    ScalarType::I64 => read_next_values::<i64>(params).await,
                    ScalarType::F32 => read_next_values::<f32>(params).await,
                    ScalarType::F64 => read_next_values::<f64>(params).await,
                    ScalarType::BOOL => read_next_values::<bool>(params).await,
                    ScalarType::STRING => read_next_values::<String>(params).await,
                    ScalarType::Enum => read_next_values::<EnumVariant>(params).await,
                },
                Shape::Wave(_) => match &scalar_type {
                    ScalarType::U8 => read_next_values::<Vec<u8>>(params).await,
                    ScalarType::U16 => read_next_values::<Vec<u16>>(params).await,
                    ScalarType::U32 => read_next_values::<Vec<u32>>(params).await,
                    ScalarType::U64 => read_next_values::<Vec<u64>>(params).await,
                    ScalarType::I8 => read_next_values::<Vec<i8>>(params).await,
                    ScalarType::I16 => read_next_values::<Vec<i16>>(params).await,
                    ScalarType::I32 => read_next_values::<Vec<i32>>(params).await,
                    ScalarType::I64 => read_next_values::<Vec<i64>>(params).await,
                    ScalarType::F32 => read_next_values::<Vec<f32>>(params).await,
                    ScalarType::F64 => read_next_values::<Vec<f64>>(params).await,
                    ScalarType::BOOL => read_next_values::<Vec<bool>>(params).await,
                    ScalarType::STRING => {
                        warn!("read not yet supported  {:?}  {:?}", shape, scalar_type);
                        err::todoval()
                    }
                    ScalarType::Enum => {
                        warn!("read not yet supported  {:?}  {:?}", shape, scalar_type);
                        err::todoval()
                    }
                },
                _ => {
                    error!("TODO ReadValues add more types");
                    err::todoval()
                }
            };
            ret.map_err(Error::from)
        };
        Box::pin(fut)
    }

    fn transition_to_bck_read(&mut self) {
        trace_fetch!("transition_to_bck_read  A  {}", TsMsVecFmt(&self.msp_buf));
        for ts in self.msp_buf.iter() {
            if ts.ns() < self.range.beg() {
                self.msp_buf_bck.push_back(ts.clone());
            }
        }
        let c = self.msp_buf.iter().take_while(|x| x.ns() < self.range.beg()).count();
        let g = c.max(1) - 1;
        for _ in 0..g {
            self.msp_buf.pop_front();
        }
        trace_fetch!(
            "transition_to_bck_read  B  {}  {}",
            TsMsVecFmt(&self.msp_buf_bck),
            TsMsVecFmt(&self.msp_buf)
        );
        self.setup_bck_read();
    }

    fn setup_bck_read(&mut self) {
        if let Some(ts) = self.msp_buf_bck.pop_back() {
            trace_fetch!("setup_bck_read  {}", ts.fmt());
            let jobtrace = ReadJobTrace::new();
            let mfi = MakeFutInfo::new(self);
            let fut = Self::make_read_events_fut(ts, true, mfi, jobtrace);
            self.state = State::ReadingBck(ReadingBck {
                reading_state: ReadingState::FetchEvents(FetchEvents::from_fut(fut)),
            });
        } else {
            trace_fetch!("setup_bck_read  no msp");
            self.transition_to_fwd_read();
        }
    }

    fn transition_to_fwd_read(&mut self) {
        trace_fetch!("transition_to_fwd_read");
        self.msp_buf_bck = VecDeque::new();
        trace_fetch!("transition_to_fwd_read  {}", TsMsVecFmt(&self.msp_buf));
        self.setup_fwd_read();
    }

    fn setup_fwd_read(&mut self) {
        let selfname = "setup_fwd_read";
        trace_fetch!("{selfname}");
        self.state = State::ReadingFwd(ReadingFwd::new(self));
    }

    fn redo_fwd_read(st: &mut ReadingFwd, msp_buf: &mut VecDeque<TsMs>) {
        let selfname = "redo_fwd_read";
        let qu = &mut st.qu;
        trace_redo_fwd_read!("{selfname}  {}  {}  BEFORE", msp_buf.len(), qu.len());
        while qu.has_space() {
            if let Some(ts) = msp_buf.pop_front() {
                trace_fetch!("{selfname}  {}  FILL A SLOT", ts.fmt());
                let jobtrace = ReadJobTrace::new();
                let mfi = st.make_fut_info.clone();
                let fut = Self::make_read_events_fut(ts, false, mfi, jobtrace);
                qu.push(fut);
            } else {
                break;
            }
        }
        trace_redo_fwd_read!("{selfname}  {}  {}  AFTER", msp_buf.len(), qu.len());
    }
}

impl Stream for EventsStreamRt {
    type Item = Result<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            if let Some(mut item) = self.out.pop_front() {
                if item.is_consistent() == false {
                    warn_item!("{}bad item {:?}", "\n\n--------------------------\n", item);
                    self.state = State::Done;
                    break Ready(Some(Err(Error::BadBatch)));
                }
                if let Some(item_min) = item.ts_min() {
                    if !self.readopts.one_before && item_min < self.range.beg() {
                        warn_item!(
                            "{}out of range error A  {}  {:?}",
                            "\n\n--------------------------\n",
                            item_min,
                            self.range
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::OutOfRange)));
                    }
                    if item_min < self.ts_seen_max {
                        warn_item!(
                            "{}ordering error A  {}  {}",
                            "\n\n--------------------------\n",
                            item_min,
                            self.ts_seen_max
                        );
                        match MergeableDyn::find_highest_index_lt(item.as_ref(), self.ts_seen_max) {
                            Some(ix) => match MergeableDyn::drain_into_new(item.as_mut(), 0..1 + ix) {
                                DrainIntoNewDynResult::Done(_) => {
                                    // TODO count drained elements for metrics
                                }
                                DrainIntoNewDynResult::Partial(_) => {
                                    self.state = State::Done;
                                    break Ready(Some(Err(Error::DrainFailure)));
                                }
                                DrainIntoNewDynResult::NotCompatible => {
                                    self.state = State::Done;
                                    break Ready(Some(Err(Error::DrainFailure)));
                                }
                            },
                            None => {
                                self.state = State::Done;
                                break Ready(Some(Err(Error::TruncateLogic)));
                            }
                        }
                    }
                }
                if let Some(item_max) = item.ts_max() {
                    if item_max >= self.range.end() {
                        warn_item!(
                            "{}out of range error B  {}  {:?}",
                            "\n\n--------------------------\n",
                            item_max,
                            self.range
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::OutOfRange)));
                    }
                    if item_max < self.ts_seen_max {
                        warn_item!(
                            "{}ordering error B  {}  {}",
                            "\n\n--------------------------\n",
                            item_max,
                            self.ts_seen_max
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::Unordered)));
                    } else {
                        self.ts_seen_max = item_max;
                    }
                }
                trace_emit!("deliver item  {:?}", item);
                self.out_cnt += item.len() as u64;
                break Ready(Some(Ok(ChannelEvents::Events(item))));
            }
            let self2 = self.as_mut().get_mut();
            let (state, msp_buf) = (&mut self2.state, &mut self2.msp_buf);
            break match state {
                State::Begin => {
                    if self.readopts.one_before {
                        trace_fetch!("State::Begin  Bck");
                        let fut = Self::make_msp_read_fut(&mut self.msp_inp);
                        self.state = State::ReadingBck(ReadingBck {
                            reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                        });
                    } else {
                        trace_fetch!("State::Begin  Fwd");
                        self.setup_fwd_read();
                    }
                    continue;
                }
                State::ReadingBck(st) => match &mut st.reading_state {
                    ReadingState::FetchMsp(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(Ok(a)) => {
                            if a.len() == 0 {
                                self.transition_to_bck_read();
                                continue;
                            } else {
                                for x in a {
                                    self.msp_buf.push_back(x);
                                }
                                if let Some(ts) = self.msp_buf.back() {
                                    if ts.ns() >= self.range.beg() {
                                        self.transition_to_bck_read();
                                    } else {
                                        let fut = Self::make_msp_read_fut(&mut self.msp_inp);
                                        self.state = State::ReadingBck(ReadingBck {
                                            reading_state: ReadingState::FetchMsp(FetchMsp { fut }),
                                        });
                                    }
                                } else {
                                    panic!("absolutely nothing to read");
                                }
                                continue;
                            }
                        }
                        Ready(Err(e)) => Ready(Some(Err(e.into()))),
                        Pending => Pending,
                    },
                    ReadingState::FetchEvents(st2) => match st2.fut.poll_unpin(cx) {
                        Ready(x) => match x {
                            Ok((mut evs, jobtrace)) => {
                                trace_fetch!("ReadingBck  {jobtrace}");
                                trace_fetch!("ReadingBck  FetchEvents  got len {}", evs.len());
                                for ts in MergeableDyn::tss_for_testing(evs.as_ref()) {
                                    trace_every_event!("ReadingBck  FetchEvents     ts {}", ts.fmt());
                                }
                                if let Some(ix) = MergeableDyn::find_highest_index_lt(evs.as_ref(), self.range.beg()) {
                                    trace_fetch!("ReadingBck  FetchEvents  find_highest_index_lt {:?}", ix);
                                    match MergeableDyn::drain_into_new(evs.as_mut(), ix..1 + ix) {
                                        DrainIntoNewDynResult::Done(y) => {
                                            trace_fetch!("ReadingBck  FetchEvents  drained y len {:?}", y.len());
                                            self.out.push_back(y);
                                            self.transition_to_fwd_read();
                                            continue;
                                        }
                                        DrainIntoNewDynResult::Partial(_) => {
                                            self.state = State::Done;
                                            Ready(Some(Err(Error::DrainFailure)))
                                        }
                                        DrainIntoNewDynResult::NotCompatible => {
                                            self.state = State::Done;
                                            Ready(Some(Err(Error::DrainFailure)))
                                        }
                                    }
                                } else {
                                    trace_fetch!("ReadingBck  FetchEvents  find_highest_index_lt None");
                                    self.setup_bck_read();
                                    continue;
                                }
                            }
                            Err(e) => {
                                self.state = State::Done;
                                Ready(Some(Err(e)))
                            }
                        },
                        Pending => Pending,
                    },
                },
                State::ReadingFwd(st) => {
                    let mut have_pending = false;
                    let mut dbg_have_new_msp_fut = false;
                    if let Some(fut) = st.msp_fut.as_mut() {
                        match fut.fut.poll_unpin(cx) {
                            Ready(a) => {
                                st.msp_fut = None;
                                match a {
                                    Ok(a) => {
                                        if a.len() == 0 {
                                            trace_msp_fetch!("msp input done");
                                            st.msp_done = true;
                                        }
                                        for x in a {
                                            msp_buf.push_back(x);
                                        }
                                    }
                                    Err(e) => {
                                        self.state = State::Done;
                                        return Ready(Some(Err(e.into())));
                                    }
                                }
                            }
                            Pending => {
                                have_pending = true;
                            }
                        }
                    } else if st.msp_done == false && msp_buf.len() < 100 {
                        trace_msp_fetch!("create msp read fut");
                        let fut = Self::make_msp_read_fut(&mut self2.msp_inp);
                        st.msp_fut = Some(FetchMsp { fut });
                        dbg_have_new_msp_fut = true;
                    }
                    if st.qu.has_space() {
                        Self::redo_fwd_read(st, msp_buf);
                    }
                    match st.qu.poll_next_unpin(cx) {
                        Ready(Some(x)) => match x {
                            Ok((evs, mut jobtrace)) => {
                                jobtrace.add_event_now(ReadEventKind::EventsStreamRtSees(evs.len() as u32));
                                trace_fetch!("ReadingFwd  {jobtrace}");
                                for ts in MergeableDyn::tss_for_testing(evs.as_ref()) {
                                    trace_every_event!("ReadingFwd  FetchEvents     ts {}", ts.fmt());
                                }
                                self.out.push_back(evs);
                                continue;
                            }
                            Err(e) => {
                                self.state = State::Done;
                                return Ready(Some(Err(e.into())));
                            }
                        },
                        Ready(None) => {}
                        Pending => {
                            have_pending = true;
                        }
                    }
                    if have_pending {
                        Pending
                    } else {
                        if msp_buf.len() == 0 && st.msp_done && st.qu.len() == 0 {
                            self.state = State::InputDone;
                            continue;
                        } else if self.out.len() != 0 {
                            continue;
                        } else if dbg_have_new_msp_fut {
                            continue;
                        } else {
                            panic!("not pending, nothing to output")
                        }
                    }
                }
                State::InputDone => {
                    if self.out.len() == 0 {
                        self.state = State::Done;
                        if self.out_cnt == 0 {
                            let d =
                                items_2::empty::empty_events_dyn_ev(self.ch_conf.scalar_type(), self.ch_conf.shape());
                            match d {
                                Ok(empty) => {
                                    let item = items_2::channelevents::ChannelEvents::Events(empty);
                                    Ready(Some(Ok(item)))
                                }
                                Err(_) => {
                                    self.state = State::Done;
                                    Ready(Some(Err(Error::Logic)))
                                }
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                State::Done => Ready(None),
            };
        }
    }
}

fn trait_assert<T>(_: T)
where
    T: Stream + Unpin + Send,
{
}

#[allow(unused)]
fn trait_assert_try() {
    let x: EventsStreamRt = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}

async fn read_next_values_2<ST>(
    opts: ReadNextValuesOpts,
    mut jobtrace: ReadJobTrace,
    scy: Arc<Session>,
    stmts: Arc<StmtsEvents>,
) -> Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>
where
    ST: ValTy,
{
    trace_fetch!("read_next_values_2  {:?}  st_name {}", opts, ST::st_name());
    let series = opts.series;
    let ts_msp = opts.ts_msp;
    let range = opts.range;
    let table_name = ST::table_name();
    let with_values = opts.readopts.with_values();
    if range.end() > TsNano::from_ns(i64::MAX as u64) {
        return Err(Error::RangeEndOverflow);
    }
    let ret = if opts.fwd {
        let ts_lsp_min = if range.beg() > ts_msp.ns() {
            range.beg().delta(ts_msp.ns())
        } else {
            DtNano::from_ns(0)
        };
        let ts_lsp_max = if range.end() > ts_msp.ns() {
            range.end().delta(ts_msp.ns())
        } else {
            DtNano::from_ns(0)
        };
        trace_fetch!(
            "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  {}",
            ts_msp.fmt(),
            ts_lsp_min,
            ts_lsp_max,
            table_name
        );
        let qu = stmts
            .rt(&opts.rt)
            .lsp(!opts.fwd, with_values)
            .shape(ST::is_valueblob())
            .st(ST::st_name())?;
        let qu = {
            let mut qu = qu.clone();
            if qu.is_token_aware() == false {
                return Err(Error::NotTokenAware);
            }
            qu.set_page_size(10000);
            // qu.disable_paging();
            qu
        };
        let params = (
            series as i64,
            ts_msp.ms() as i64,
            ts_lsp_min.ns() as i64,
            ts_lsp_max.ns() as i64,
        );
        trace_fetch!("FWD event search  params {:?}", params);
        jobtrace.add_event_now(ReadEventKind::CallExecuteIter);
        log_fetch_result!("read_next_values_2  {params:?}");
        let res = scy.execute_iter(qu.clone(), params).await?;
        {
            let mut ret = <ST as ValTy>::Container::empty();
            // TODO must branch already here depending on what input columns we expect
            if with_values {
                if <ST as ValTy>::is_valueblob() {
                    let mut it = res.rows_stream::<(i64, Vec<u8>)>()?;
                    while let Some(row) = it.try_next().await? {
                        let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                        let value = <ST as ValTy>::from_valueblob(row.1);
                        ret.push(ts, value);
                    }
                    ret
                } else {
                    let mut i = 0;
                    let mut it = res.rows_stream::<<ST as ValTy>::ScyRowTy>()?;
                    while let Some(row) = it.try_next().await? {
                        let (ts, value) = <ST as ValTy>::scy_row_to_ts_val(ts_msp, row);
                        log_fetch_result!("read_next_values_2  {params:?}  {ts}");
                        // let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                        // let value = <ST as ValTy>::from_scyty(row.1);
                        ret.push(ts, value);
                        i += 1;
                        if i % 2000 == 0 {
                            jobtrace.add_event_now(ReadEventKind::ScyllaReadRow(i));
                        }
                    }
                    {
                        jobtrace.add_event_now(ReadEventKind::ScyllaReadRowDone(i));
                    }
                    ret
                }
            } else {
                let mut it = res.rows_stream::<(i64,)>()?;
                while let Some(row) = it.try_next().await? {
                    let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                    let value = <ST as ValTy>::default();
                    ret.push(ts, value);
                }
                ret
            }
        }
    } else {
        let ts_lsp_max = if ts_msp.ns() < range.beg() {
            range.beg().delta(ts_msp.ns())
        } else {
            DtNano::from_ns(0)
        };
        trace_fetch!(
            "BCK  ts_msp {}  ts_lsp_max {}  {}",
            ts_msp.fmt(),
            ts_lsp_max,
            table_name
        );
        let qu = stmts
            .rt(&opts.rt)
            .lsp(!opts.fwd, with_values)
            .shape(ST::is_valueblob())
            .st(ST::st_name())?;
        let params = (series as i64, ts_msp.ms() as i64, ts_lsp_max.ns() as i64);
        trace_fetch!("BCK event search  params {:?}", params);
        let res = scy.execute_iter(qu.clone(), params).await?;
        {
            let mut ret = <ST as ValTy>::Container::empty();
            // TODO must branch already here depending on what input columns we expect
            if with_values {
                if <ST as ValTy>::is_valueblob() {
                    let mut it = res.rows_stream::<(i64, Vec<u8>)>()?;
                    while let Some(row) = it.try_next().await? {
                        let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                        let value = <ST as ValTy>::from_valueblob(row.1);
                        ret.push(ts, value);
                    }
                    ret
                } else {
                    let mut i = 0;
                    let mut it = res.rows_stream::<<ST as ValTy>::ScyRowTy>()?;
                    while let Some(row) = it.try_next().await? {
                        let (ts, value) = <ST as ValTy>::scy_row_to_ts_val(ts_msp, row);
                        // let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                        // let value = <ST as ValTy>::from_scyty(row.1);
                        ret.push(ts, value);
                        i += 1;
                        if i % 2000 == 0 {
                            jobtrace.add_event_now(ReadEventKind::ScyllaReadRow(i));
                        }
                    }
                    {
                        jobtrace.add_event_now(ReadEventKind::ScyllaReadRowDone(i));
                    }
                    ret
                }
            } else {
                let mut it = res.rows_stream::<(i64,)>()?;
                while let Some(row) = it.try_next().await? {
                    let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                    let value = <ST as ValTy>::default();
                    ret.push(ts, value);
                }
                ret
            }
        }
    };
    let byte_est = ret.byte_estimate();
    trace_fetch!(
        "read  ts_msp {}  len {}  byte_est {}",
        ts_msp.fmt(),
        ret.len(),
        byte_est
    );
    let ret = Box::new(ret);
    Ok((ret, jobtrace))
}

async fn read_next_values<ST>(
    params: ReadNextValuesParams,
) -> Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>
where
    ST: ValTy,
{
    let opts = params.opts;
    let jobtrace = params.jobtrace;
    // TODO could take scyqeue out of opts struct.
    let scyqueue = opts.scyqueue.clone();
    let level = taskrun::query_log_level();
    let futgen = move |scy: Arc<Session>, stmts: Arc<StmtsEvents>, mut jobtrace: ReadJobTrace| {
        // TODO avoid this
        // opts.jobtrace = jobtrace;
        let fut = async move {
            // let jobtrace = &mut opts.jobtrace;
            let logspan = if level == log::Level::DEBUG {
                tracing::span!(log::Level::INFO, "log_span_debug")
            } else if level == log::Level::TRACE {
                tracing::span!(log::Level::INFO, "log_span_trace")
            } else {
                tracing::Span::none()
            };
            jobtrace.add_event_now(ReadEventKind::FutgenCallingReadNextValues);
            let fut = ST::read_next_values_trait(opts, jobtrace, scy, stmts);
            let fut = tracing::Instrument::instrument(fut, logspan);
            match fut.await.map_err(crate::worker::Error::from) {
                Ok((ret, mut jobtrace)) => {
                    jobtrace.add_event_now(ReadEventKind::ReadNextValuesFutureDone);
                    Ok((ret, jobtrace))
                }
                Err(e) => Err(e),
            }
        };
        Box::pin(fut)
            as Pin<
                Box<
                    dyn Future<
                            Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), crate::worker::Error>,
                        > + Send,
                >,
            >
    };
    let (res, jobtrace) = scyqueue.read_next_values(futgen, jobtrace).await?;
    Ok((res, jobtrace))
}

trait ValTy: Sized + 'static {
    type ScaTy: ScalarOps + std::default::Default;
    type ScyTy: for<'a, 'b> scylla::deserialize::value::DeserializeValue<'a, 'b>;
    type ScyRowTy: for<'a, 'b> scylla::deserialize::row::DeserializeRow<'a, 'b>;
    type Container: BinningggContainerEventsDyn + Empty + Appendable<Self>;
    fn from_valueblob(inp: Vec<u8>) -> Self;
    fn table_name() -> &'static str;
    fn default() -> Self;
    fn is_valueblob() -> bool;
    fn st_name() -> &'static str;
    fn read_next_values_trait(
        opts: ReadNextValuesOpts,
        jobtrace: ReadJobTrace,
        scy: Arc<Session>,
        stmts: Arc<StmtsEvents>,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>>;
    fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self);
}

macro_rules! impl_scaty_scalar {
    ($st:ty, $st_scy:ty, $st_name:expr, $table_name:expr) => {
        impl ValTy for $st {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type ScyRowTy = (i64, $st_scy);
            type Container = ContainerEvents<Self::ScaTy>;

            fn from_valueblob(_inp: Vec<u8>) -> Self {
                panic!("unused")
            }

            fn table_name() -> &'static str {
                concat!("scalar_", $table_name)
            }

            fn default() -> Self {
                <Self as std::default::Default>::default()
            }

            fn is_valueblob() -> bool {
                false
            }

            fn st_name() -> &'static str {
                $st_name
            }

            fn read_next_values_trait(
                opts: ReadNextValuesOpts,
                jobtrace: ReadJobTrace,
                scy: Arc<Session>,
                stmts: Arc<StmtsEvents>,
            ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>> {
                Box::pin(read_next_values_2::<Self>(opts, jobtrace, scy, stmts))
            }

            fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
                let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
                (ts, inp.1 as Self::ScaTy)
            }
        }
    };
}

macro_rules! impl_scaty_array {
    ($vt:ty, $st:ty, $st_scy:ty, $st_name:expr, $table_name:expr) => {
        impl ValTy for $vt {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type ScyRowTy = (i64, $st_scy);
            type Container = ContainerEvents<Vec<Self::ScaTy>>;

            fn from_valueblob(inp: Vec<u8>) -> Self {
                if inp.len() < 32 {
                    <Self as ValTy>::default()
                } else {
                    let en = std::mem::size_of::<Self::ScaTy>();
                    let n = (inp.len().max(32) - 32) / en;
                    let mut c = Vec::with_capacity(n);
                    for i in 0..n {
                        let r1 = &inp[32 + en * (0 + i)..32 + en * (1 + i)];
                        let p1 = r1 as *const _ as *const $st;
                        let v1 = unsafe { p1.read_unaligned() };
                        c.push(v1);
                    }
                    c
                }
            }

            fn table_name() -> &'static str {
                concat!("array_", $table_name)
            }

            fn default() -> Self {
                Vec::new()
            }

            fn is_valueblob() -> bool {
                true
            }

            fn st_name() -> &'static str {
                $st_name
            }

            fn read_next_values_trait(
                opts: ReadNextValuesOpts,
                jobtrace: ReadJobTrace,
                scy: Arc<Session>,
                stmts: Arc<StmtsEvents>,
            ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>> {
                Box::pin(read_next_values_2::<Self>(opts, jobtrace, scy, stmts))
            }

            fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
                let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
                (ts, inp.1 .into_iter().map(|x| x as _).collect())
            }
        }
    };
}

impl ValTy for EnumVariant {
    type ScaTy = EnumVariant;
    type ScyTy = i16;
    type ScyRowTy = (i64, i16, String);
    type Container = ContainerEvents<EnumVariant>;

    fn from_valueblob(_inp: Vec<u8>) -> Self {
        panic!("unused")
    }

    fn table_name() -> &'static str {
        "array_string"
    }

    fn default() -> Self {
        <Self as Default>::default()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "enum"
    }

    fn read_next_values_trait(
        opts: ReadNextValuesOpts,
        jobtrace: ReadJobTrace,
        scy: Arc<Session>,
        stmts: Arc<StmtsEvents>,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>> {
        Box::pin(read_next_values_2::<Self>(opts, jobtrace, scy, stmts))
    }

    fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
        let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
        (ts, EnumVariant::new(inp.1 as u16, inp.2))
    }
}

impl ValTy for Vec<String> {
    type ScaTy = String;
    type ScyTy = Vec<String>;
    type ScyRowTy = (i64, Vec<String>);
    type Container = ContainerEvents<Vec<String>>;

    fn from_valueblob(_inp: Vec<u8>) -> Self {
        panic!("unused")
    }

    fn table_name() -> &'static str {
        "array_string"
    }

    fn default() -> Self {
        Vec::new()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "string"
    }

    fn read_next_values_trait(
        opts: ReadNextValuesOpts,
        jobtrace: ReadJobTrace,
        scy: Arc<Session>,
        stmts: Arc<StmtsEvents>,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>> {
        let fut = read_next_values_2::<Self>(opts, jobtrace, scy, stmts);
        Box::pin(fut)
    }

    fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
        let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
        (ts, inp.1)
    }
}

impl_scaty_scalar!(u8, i8, "u8", "u8");
impl_scaty_scalar!(u16, i16, "u16", "u16");
impl_scaty_scalar!(u32, i32, "u32", "u32");
impl_scaty_scalar!(u64, i64, "u64", "u64");
impl_scaty_scalar!(i8, i8, "i8", "i8");
impl_scaty_scalar!(i16, i16, "i16", "i16");
impl_scaty_scalar!(i32, i32, "i32", "i32");
impl_scaty_scalar!(i64, i64, "i64", "i64");
impl_scaty_scalar!(f32, f32, "f32", "f32");
impl_scaty_scalar!(f64, f64, "f64", "f64");
impl_scaty_scalar!(bool, bool, "bool", "bool");
impl_scaty_scalar!(String, String, "string", "string");

impl_scaty_array!(Vec<u8>, u8, Vec<i8>, "u8", "u8");
impl_scaty_array!(Vec<u16>, u16, Vec<i16>, "u16", "u16");
impl_scaty_array!(Vec<u32>, u32, Vec<i32>, "u32", "u32");
impl_scaty_array!(Vec<u64>, u64, Vec<i64>, "u64", "u64");
impl_scaty_array!(Vec<i8>, i8, Vec<i8>, "i8", "i8");
impl_scaty_array!(Vec<i16>, i16, Vec<i16>, "i16", "i16");
impl_scaty_array!(Vec<i32>, i32, Vec<i32>, "i32", "i32");
impl_scaty_array!(Vec<i64>, i64, Vec<i64>, "i64", "i64");
impl_scaty_array!(Vec<f32>, f32, Vec<f32>, "f32", "f32");
impl_scaty_array!(Vec<f64>, f64, Vec<f64>, "f64", "f64");
impl_scaty_array!(Vec<bool>, bool, Vec<bool>, "bool", "bool");
