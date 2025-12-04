use super::events::EventReadOpts;
use super::events::EventsStreamRt;
use super::onebeforeandbulk::OneBeforeAndBulk;
use crate::events2::onebeforeandbulk;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::WithLen;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::ChConf;
use netpod::TsNano;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::stream_impl_tracer::StreamImplTracer;
use netpod::ttl::RetentionTime;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

macro_rules! trace_fetch { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

macro_rules! trace_emit { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

macro_rules! trace_switch { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

macro_rules! tracer_poll_enter {
    ($self:expr) => {
        if false && $self.tracer.poll_enter() {
            return Ready(Some(Err(Error::LimitPoll)));
        }
    };
}

macro_rules! tracer_loop_enter {
    ($self:expr) => {
        if false && $self.tracer.loop_enter() {
            return Ready(Some(Err(Error::LimitLoop)));
        }
    };
}

autoerr::create_error_v1!(
    name(Error, "EventsMergeRt"),
    enum variants {
        Input(#[from] crate::events2::onebeforeandbulk::Error),
        Events(#[from] crate::events2::events::Error),
        Logic,
        OrderMin,
        OrderMax,
        LimitPoll,
        LimitLoop,
    },
);

#[allow(unused)]
enum Resolvable<F>
where
    F: Future,
{
    Future(F),
    Output(<F as Future>::Output),
    Taken,
}

#[allow(unused)]
impl<F> Resolvable<F>
where
    F: Future,
{
    fn unresolved(&self) -> bool {
        match self {
            Resolvable::Future(_) => true,
            Resolvable::Output(_) => false,
            Resolvable::Taken => false,
        }
    }

    fn take(&mut self) -> Option<<F as Future>::Output> {
        let x = std::mem::replace(self, Resolvable::Taken);
        match x {
            Resolvable::Future(_) => None,
            Resolvable::Output(x) => Some(x),
            Resolvable::Taken => None,
        }
    }
}

impl<F> Future for Resolvable<F>
where
    F: Future + Unpin,
{
    type Output = <F as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<<F as Future>::Output> {
        match unsafe { self.get_unchecked_mut() } {
            Resolvable::Future(fut) => fut.poll_unpin(cx),
            Resolvable::Output(_) => panic!(),
            Resolvable::Taken => panic!(),
        }
    }
}

type TI = OneBeforeAndBulk<EventsStreamRt, ChannelEvents>;
type INPI = Sitemty2<crate::events2::onebeforeandbulk::Output<ChannelEvents>, crate::events2::onebeforeandbulk::Error>;

struct ReadEvents {
    fut: Pin<Box<dyn Future<Output = Option<INPI>> + Send>>,
}

enum State {
    Begin,
    FetchFirstSt(ReadEvents),
    FetchFirstMt(ReadEvents),
    FetchFirstLt(ReadEvents),
    ReadingLt(Option<ReadEvents>, VecDeque<ChannelEvents>, Option<Box<TI>>),
    ReadingMt(Option<ReadEvents>, VecDeque<ChannelEvents>, Option<Box<TI>>),
    ReadingSt(Option<ReadEvents>, VecDeque<ChannelEvents>, Option<Box<TI>>),
    Done,
}

pub struct MergeRtsChained {
    ch_conf: ChConf,
    range: ScyllaSeriesRange,
    range_st: ScyllaSeriesRange,
    range_mt: ScyllaSeriesRange,
    range_lt: ScyllaSeriesRange,
    readopts: EventReadOpts,
    scyqueue: ScyllaQueue,
    inp_st: Option<Box<TI>>,
    inp_mt: Option<Box<TI>>,
    inp_lt: Option<Box<TI>>,
    state: State,
    buf_st: VecDeque<ChannelEvents>,
    buf_mt: VecDeque<ChannelEvents>,
    buf_lt: VecDeque<ChannelEvents>,
    out: VecDeque<ChannelEvents>,
    buf_before: Option<ChannelEvents>,
    ts_seen_max: TsNano,
    tracer: StreamImplTracer,
}

impl MergeRtsChained {
    pub fn new(ch_conf: ChConf, range: ScyllaSeriesRange, readopts: EventReadOpts, scyqueue: ScyllaQueue) -> Self {
        trace_init!("MergeRtsChained  readopts {readopts:?}");
        Self {
            ch_conf,
            range_st: range.clone(),
            range_mt: range.clone(),
            range_lt: range.clone(),
            range,
            readopts,
            scyqueue,
            inp_st: None,
            inp_mt: None,
            inp_lt: None,
            state: State::Begin,
            buf_st: VecDeque::new(),
            buf_mt: VecDeque::new(),
            buf_lt: VecDeque::new(),
            out: VecDeque::new(),
            buf_before: None,
            ts_seen_max: TsNano::from_ns(0),
            tracer: StreamImplTracer::new("MergeRtsChained".into(), 2000, 2000),
        }
    }

    fn setup_first_st(&mut self) {
        let rt = RetentionTime::Short;
        let limbuf = &VecDeque::new();
        let inpdst = &mut self.inp_st;
        let range = Self::constrained_range(&self.range_st, limbuf);
        self.range_st = range.clone();
        self.range_mt = range.clone();
        self.range_lt = range.clone();
        trace_fetch!("setup_first_st  constrained beg  {}", range.beg().ns());
        let tsbeg = range.beg();
        let inp = EventsStreamRt::new(
            rt,
            self.ch_conf.clone(),
            range,
            self.readopts.clone(),
            self.scyqueue.clone(),
        );
        let inp = TI::new(inp, tsbeg, "ST".into());
        *inpdst = Some(Box::new(inp));
    }

    fn setup_first_mt(&mut self) {
        let rt = RetentionTime::Medium;
        let limbuf = &self.buf_st;
        let inpdst = &mut self.inp_mt;
        let range = Self::constrained_range(&self.range_mt, limbuf);
        self.range_mt = range.clone();
        self.range_lt = range.clone();
        trace_fetch!("setup_first_mt  constrained beg  {}", range.beg().ns());
        let tsbeg = range.beg();
        let inp = EventsStreamRt::new(
            rt,
            self.ch_conf.clone(),
            range,
            self.readopts.clone(),
            self.scyqueue.clone(),
        );
        let inp = TI::new(inp, tsbeg, "MT".into());
        *inpdst = Some(Box::new(inp));
    }

    fn setup_first_lt(&mut self) {
        let rt = RetentionTime::Long;
        let limbuf = &self.buf_mt;
        let inpdst = &mut self.inp_lt;
        let range = Self::constrained_range(&self.range_lt, limbuf);
        self.range_lt = range.clone();
        trace_fetch!("setup_first_lt  constrained beg  {}", range.beg().ns());
        let tsbeg = range.beg();
        let inp = EventsStreamRt::new(
            rt,
            self.ch_conf.clone(),
            range,
            self.readopts.clone(),
            self.scyqueue.clone(),
        );
        let inp = TI::new(inp, tsbeg, "LT".into());
        *inpdst = Some(Box::new(inp));
    }

    fn setup_read_st(&mut self) -> ReadEvents {
        trace_fetch!("setup_read_st");
        Self::setup_read_any(&mut self.inp_st)
    }

    fn setup_read_mt(&mut self) -> ReadEvents {
        trace_fetch!("setup_read_mt");
        Self::setup_read_any(&mut self.inp_mt)
    }

    fn setup_read_lt(&mut self) -> ReadEvents {
        trace_fetch!("setup_read_lt");
        Self::setup_read_any(&mut self.inp_lt)
    }

    fn setup_read_any(inp: &mut Option<Box<TI>>) -> ReadEvents {
        trace_fetch!("setup_read_any");
        let stream = unsafe { &mut *(inp.as_mut().unwrap().as_mut() as *mut TI) };
        let fut = Box::pin(stream.next());
        ReadEvents { fut }
    }

    fn constrained_range(full: &ScyllaSeriesRange, buf: &VecDeque<ChannelEvents>) -> ScyllaSeriesRange {
        trace_fetch!("constrained_range  {:?}  {:?}", full, buf.front());
        if let Some(e) = buf.front() {
            if let Some(ts) = e.ts_min() {
                let nrange = NanoRange::from((full.beg().ns(), ts.ns()));
                ScyllaSeriesRange::from(&SeriesRange::from(nrange))
            } else {
                full.clone()
            }
        } else {
            full.clone()
        }
    }

    fn handle_first_st(&mut self, mut before: Option<ChannelEvents>, bulk: Option<ChannelEvents>) {
        trace_fetch!("handle_first_st");
        if let Some(before) = before.as_mut() {
            Self::move_latest_to_before_buf(before, &mut self.buf_before);
        }
        if let Some(bulk) = bulk {
            self.buf_st.push_back(bulk);
        }
        self.setup_first_mt();
        self.state = State::FetchFirstMt(self.setup_read_mt());
    }

    fn handle_first_mt(&mut self, mut before: Option<ChannelEvents>, bulk: Option<ChannelEvents>) {
        trace_fetch!("handle_first_mt");
        if let Some(before) = before.as_mut() {
            Self::move_latest_to_before_buf(before, &mut self.buf_before);
        }
        if let Some(bulk) = bulk {
            self.buf_mt.push_back(bulk);
        }
        self.setup_first_lt();
        self.state = State::FetchFirstLt(self.setup_read_lt());
    }

    fn handle_first_lt(&mut self, mut before: Option<ChannelEvents>, bulk: Option<ChannelEvents>) {
        trace_fetch!("handle_first_lt");
        if let Some(before) = before.as_mut() {
            Self::move_latest_to_before_buf(before, &mut self.buf_before);
        }
        if let Some(bulk) = bulk {
            self.buf_lt.push_back(bulk);
        }
    }

    fn handle_all_firsts_done(&mut self) {
        trace_switch!(
            "CONSIDERED RANGES:\nFULL {:?}\nST   {:?}\nMT   {:?}\nLT   {:?}\n",
            self.range,
            self.range_st,
            self.range_mt,
            self.range_lt
        );
        self.push_out_one_before();
        let buf = core::mem::replace(&mut self.buf_lt, VecDeque::new());
        self.state = State::ReadingLt(None, buf, self.inp_lt.take());
    }

    fn move_latest_to_before_buf(before: &mut ChannelEvents, buf: &mut Option<ChannelEvents>) {
        if let Some(tsn) = before.ts_max() {
            if buf
                .as_ref()
                .map_or(true, |buf2| buf2.ts_max().map_or(true, |x| tsn > x))
            {
                trace_fetch!("move_latest_to_before_buf  move possible before item  {tsn}");
                let n = before.len();
                match before.drain_into_new(n - 1..n) {
                    DrainIntoNewResult::Done(x) => {
                        *buf = Some(x);
                    }
                    DrainIntoNewResult::Partial(_) => panic!(),
                    DrainIntoNewResult::NotCompatible => panic!(),
                }
            }
        }
    }

    fn push_out_one_before(&mut self) {
        if let Some(buf) = self.buf_before.take() {
            trace_fetch!("push_out_one_before  len {:?}", buf.len());
            if buf.len() != 0 {
                self.out.push_back(buf);
            }
        } else {
            trace_fetch!("push_out_one_before  no buffer");
        }
    }
}

impl Stream for MergeRtsChained {
    type Item = Sitemty2<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        tracer_poll_enter!(self);
        let mut out2 = VecDeque::new();
        loop {
            tracer_loop_enter!(self);
            while let Some(x) = out2.pop_front() {
                self.out.push_back(x);
            }
            if let Some(item) = self.out.pop_front() {
                let verified = item.is_consistent();
                trace_emit!("emit item  {}  {:?}", verified, item);
                if verified == false {
                    debug!("{}bad item {:?}", "\n\n--------------------------\n", item);
                    self.state = State::Done;
                }
                if let Some(item_min) = item.ts_min() {
                    if item_min < self.ts_seen_max {
                        debug!(
                            "{}ordering error A  {}  {}",
                            "\n\n--------------------------\n", item_min, self.ts_seen_max
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::OrderMin)));
                    }
                }
                if let Some(item_max) = item.ts_max() {
                    if item_max < self.ts_seen_max {
                        debug!(
                            "{}ordering error B  {}  {}",
                            "\n\n--------------------------\n", item_max, self.ts_seen_max
                        );
                        self.state = State::Done;
                        break Ready(Some(Err(Error::OrderMax)));
                    } else {
                        self.ts_seen_max = item_max;
                    }
                }
                if let Some(ix) = item.find_highest_index_lt(self.range.beg()) {
                    trace_fetch!("see item before range  ix {ix}");
                }
                break Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))));
            }
            break match &mut self.state {
                State::Begin => {
                    self.setup_first_st();
                    self.state = State::FetchFirstSt(self.setup_read_st());
                    continue;
                }
                State::FetchFirstSt(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(x) => match x {
                                onebeforeandbulk::Output::Before(before) => {
                                    trace_fetch!("have first from ST");
                                    self.handle_first_st(Some(before), None);
                                    continue;
                                }
                                onebeforeandbulk::Output::Bulk(item) => {
                                    self.handle_first_st(None, Some(item));
                                    continue;
                                }
                            },
                            RangeCompletableItem::RangeComplete => {
                                log::info!("mark range complete for all inputs");
                                continue;
                            }
                        },
                        StreamItem::Log(x) => {
                            let item = StreamItem::Log(x);
                            Ready(Some(Ok(item)))
                        }
                        StreamItem::Stats(x) => {
                            let item = StreamItem::Stats(x);
                            Ready(Some(Ok(item)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.state = State::Done;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => {
                        trace_fetch!("no first from ST");
                        self.inp_st = None;
                        self.setup_first_mt();
                        self.state = State::FetchFirstMt(self.setup_read_mt());
                        continue;
                    }
                    Pending => Pending,
                },
                State::FetchFirstMt(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(x) => match x {
                                onebeforeandbulk::Output::Before(before) => {
                                    trace_fetch!("have first from MT");
                                    self.handle_first_mt(Some(before), None);
                                    continue;
                                }
                                onebeforeandbulk::Output::Bulk(item) => {
                                    self.handle_first_mt(None, Some(item));
                                    continue;
                                }
                            },
                            RangeCompletableItem::RangeComplete => {
                                log::info!("mark range complete for all inputs");
                                continue;
                            }
                        },
                        StreamItem::Log(x) => {
                            let item = StreamItem::Log(x);
                            Ready(Some(Ok(item)))
                        }
                        StreamItem::Stats(x) => {
                            let item = StreamItem::Stats(x);
                            Ready(Some(Ok(item)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.state = State::Done;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => {
                        trace_fetch!("no first from MT");
                        self.inp_mt = None;
                        self.setup_first_lt();
                        self.state = State::FetchFirstLt(self.setup_read_lt());
                        continue;
                    }
                    Pending => Pending,
                },
                State::FetchFirstLt(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(x) => match x {
                                onebeforeandbulk::Output::Before(before) => {
                                    trace_fetch!("have first from LT");
                                    self.handle_first_lt(Some(before), None);
                                    self.handle_all_firsts_done();
                                    continue;
                                }
                                onebeforeandbulk::Output::Bulk(item) => {
                                    self.handle_first_lt(None, Some(item));
                                    self.handle_all_firsts_done();
                                    continue;
                                }
                            },
                            RangeCompletableItem::RangeComplete => {
                                log::info!("TODO mark range complete for all inputs");
                                continue;
                            }
                        },
                        StreamItem::Log(x) => {
                            let item = StreamItem::Log(x);
                            Ready(Some(Ok(item)))
                        }
                        StreamItem::Stats(x) => {
                            let item = StreamItem::Stats(x);
                            Ready(Some(Ok(item)))
                        }
                    },
                    Ready(Some(Err(e))) => {
                        self.state = State::Done;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => {
                        trace_fetch!("no first from LT");
                        self.inp_lt = None;
                        self.handle_all_firsts_done();
                        continue;
                    }
                    Pending => Pending,
                },
                State::ReadingLt(fut, buf, inp) => {
                    if let Some(x) = buf.pop_front() {
                        out2.push_back(x);
                        continue;
                    } else if let Some(fut2) = fut {
                        match fut2.fut.poll_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                *fut = None;
                                match x {
                                    StreamItem::DataItem(x) => match x {
                                        RangeCompletableItem::Data(x) => match x {
                                            onebeforeandbulk::Output::Bulk(x) => {
                                                buf.push_back(x);
                                                continue;
                                            }
                                            onebeforeandbulk::Output::Before(_) => {
                                                self.state = State::Done;
                                                let e = Error::Logic;
                                                Ready(Some(Err(e)))
                                            }
                                        },
                                        RangeCompletableItem::RangeComplete => {
                                            log::info!("TODO mark range complete for all inputs");
                                            continue;
                                        }
                                    },
                                    StreamItem::Log(x) => {
                                        let item = StreamItem::Log(x);
                                        Ready(Some(Ok(item)))
                                    }
                                    StreamItem::Stats(x) => {
                                        let item = StreamItem::Stats(x);
                                        Ready(Some(Ok(item)))
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Done;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                *inp = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingLt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        continue;
                    } else {
                        trace_emit!("transition ReadingLt to ReadingMt");
                        let buf = core::mem::replace(&mut self.buf_mt, VecDeque::new());
                        self.state = State::ReadingMt(None, buf, self.inp_mt.take());
                        continue;
                    }
                }
                State::ReadingMt(fut, buf, inp) => {
                    if let Some(x) = buf.pop_front() {
                        out2.push_back(x);
                        continue;
                    } else if let Some(fut2) = fut {
                        match fut2.fut.poll_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                *fut = None;
                                match x {
                                    StreamItem::DataItem(x) => match x {
                                        RangeCompletableItem::Data(x) => match x {
                                            onebeforeandbulk::Output::Bulk(x) => {
                                                buf.push_back(x);
                                                continue;
                                            }
                                            onebeforeandbulk::Output::Before(_) => {
                                                self.state = State::Done;
                                                let e = Error::Logic;
                                                Ready(Some(Err(e)))
                                            }
                                        },
                                        RangeCompletableItem::RangeComplete => {
                                            log::info!("TODO mark range complete for all inputs");
                                            continue;
                                        }
                                    },
                                    StreamItem::Log(x) => {
                                        let item = StreamItem::Log(x);
                                        Ready(Some(Ok(item)))
                                    }
                                    StreamItem::Stats(x) => {
                                        let item = StreamItem::Stats(x);
                                        Ready(Some(Ok(item)))
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Done;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                *inp = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingMt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        continue;
                    } else {
                        trace_emit!("transition ReadingMt to ReadingSt");
                        let buf = core::mem::replace(&mut self.buf_st, VecDeque::new());
                        self.state = State::ReadingSt(None, buf, self.inp_st.take());
                        continue;
                    }
                }
                State::ReadingSt(fut, buf, inp) => {
                    if let Some(x) = buf.pop_front() {
                        out2.push_back(x);
                        continue;
                    } else if let Some(fut2) = fut {
                        match fut2.fut.poll_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                *fut = None;
                                match x {
                                    StreamItem::DataItem(x) => match x {
                                        RangeCompletableItem::Data(x) => match x {
                                            onebeforeandbulk::Output::Bulk(x) => {
                                                buf.push_back(x);
                                                continue;
                                            }
                                            onebeforeandbulk::Output::Before(_) => {
                                                self.state = State::Done;
                                                let e = Error::Logic;
                                                Ready(Some(Err(e)))
                                            }
                                        },
                                        RangeCompletableItem::RangeComplete => {
                                            log::info!("TODO mark range complete for all inputs");
                                            continue;
                                        }
                                    },
                                    StreamItem::Log(x) => {
                                        let item = StreamItem::Log(x);
                                        Ready(Some(Ok(item)))
                                    }
                                    StreamItem::Stats(x) => {
                                        let item = StreamItem::Stats(x);
                                        Ready(Some(Ok(item)))
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                *fut = None;
                                self.state = State::Done;
                                Ready(Some(Err(e.into())))
                            }
                            Ready(None) => {
                                *fut = None;
                                *inp = None;
                                continue;
                            }
                            Pending => Pending,
                        }
                    } else if inp.is_some() {
                        let buf = core::mem::replace(buf, VecDeque::new());
                        self.state = State::ReadingSt(Some(Self::setup_read_any(inp)), buf, inp.take());
                        continue;
                    } else {
                        trace_emit!("fully done");
                        Ready(None)
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
    let x: MergeRtsChained = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}
