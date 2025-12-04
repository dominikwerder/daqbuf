use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::DrainIntoDstResult;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use netpod::TsNano;
use netpod::log;
use netpod::stream_impl_tracer::StreamImplTracer;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }

macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }

macro_rules! trace_transition { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

macro_rules! trace_emit { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

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

macro_rules! debug_fetch { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "EventsOneBeforeAndBulk"),
    enum variants {
        Unordered,
        Logic,
        Input(Box<dyn std::error::Error + Send>),
        LimitPoll,
        LimitLoop,
    },
);

#[derive(Debug)]
pub enum Output<T> {
    Before(T),
    Bulk(T),
}

enum State {
    Begin,
    Bulk,
    Done,
}

pub struct OneBeforeAndBulk<S, T>
where
    S: Stream + Unpin,
    T: MergeableTy + Unpin,
{
    ts0: TsNano,
    inp: S,
    state: State,
    buf: Option<T>,
    out: VecDeque<T>,
    outbuf2: VecDeque<Sitemty2<Output<T>, Error>>,
    tracer: StreamImplTracer,
    seen_empty_during_begin: bool,
    seen_empty_during_bulk: bool,
    dbgname: String,
    tslast: TsNano,
    have_range_final: bool,
}

impl<S, T> OneBeforeAndBulk<S, T>
where
    S: Stream + Unpin,
    T: MergeableTy + Unpin,
{
    fn selfname() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new<E>(inp: S, ts0: TsNano, dbgname: String) -> Self
    where
        S: Stream<Item = Sitemty2<T, E>> + Unpin,
        T: MergeableTy + Unpin,
        E: std::error::Error + Send + 'static,
    {
        trace_transition!("{}::new", Self::selfname());
        Self {
            ts0,
            inp,
            state: State::Begin,
            buf: None,
            out: VecDeque::new(),
            outbuf2: VecDeque::new(),
            tracer: StreamImplTracer::new(Self::selfname().into(), 2000, 100),
            seen_empty_during_begin: false,
            seen_empty_during_bulk: false,
            dbgname,
            tslast: TsNano::from_ns(0),
            have_range_final: false,
        }
    }

    fn consume_buf_get_latest(&mut self) -> Option<T> {
        if let Some(mut buf) = self.buf.take() {
            if buf.len() == 0 {
                debug!("buf set but empty");
                None
            } else {
                match buf.drain_into_new(buf.len() - 1..buf.len()) {
                    DrainIntoNewResult::Done(ret) => Some(ret),
                    DrainIntoNewResult::Partial(_) => panic!(),
                    DrainIntoNewResult::NotCompatible => panic!(),
                }
            }
        } else {
            None
        }
    }
}

impl<S, T, E> Stream for OneBeforeAndBulk<S, T>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: MergeableTy + Unpin,
    E: std::error::Error + Send + 'static,
{
    type Item = Sitemty2<Output<T>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        tracer_poll_enter!(self);
        loop {
            tracer_loop_enter!(self);
            break if let Some(item) = self.outbuf2.pop_front() {
                Ready(Some(item))
            } else if let Some(item) = self.out.pop_front() {
                let item = Output::Bulk(item);
                let item = RangeCompletableItem::Data(item);
                let item = StreamItem::DataItem(item);
                Ready(Some(Ok(item)))
            } else {
                match &self.state {
                    State::Begin => match self.inp.poll_next_unpin(cx) {
                        Ready(Some(Ok(item))) => match item {
                            StreamItem::DataItem(item) => match item {
                                RangeCompletableItem::Data(mut item) => {
                                    if let Some(tsmin) = MergeableTy::ts_min(&item) {
                                        let tsmin = tsmin;
                                        if tsmin < self.tslast {
                                            self.state = State::Done;
                                            let e = Error::Unordered;
                                            break Ready(Some(Err(e)));
                                        } else {
                                            self.tslast = MergeableTy::ts_max(&item).unwrap();
                                        }
                                    }
                                    if item.is_consistent() == false {
                                        self.state = State::Done;
                                        let e = Error::Unordered;
                                        Ready(Some(Err(e)))
                                    } else {
                                        if item.len() == 0 {
                                            self.seen_empty_during_begin = true;
                                        } else {
                                            if self.seen_empty_during_begin {
                                                debug_fetch!(
                                                    "still in Begin  current event len {}  but seen empty before",
                                                    item.len()
                                                );
                                            }
                                        }
                                        // Separate events into before and bulk
                                        let ppp = MergeableTy::find_lowest_index_ge(&item, self.ts0);
                                        trace_transition!("partition_point  {:?}  {:?}", ppp, item.len());
                                        if let Some(pp) = ppp {
                                            if pp == 0 {
                                                // all entries are bulk
                                                trace_transition!("transition with bulk to Bulk");
                                                self.state = State::Bulk;
                                                if let Some(before) = self.consume_buf_get_latest() {
                                                    self.out.push_back(item);
                                                    let emit_len = before.len();
                                                    let item = Output::Before(before);
                                                    trace_emit!(
                                                        "State::Begin  Before  {}  emit_len {}",
                                                        self.dbgname,
                                                        emit_len
                                                    );
                                                    let item = RangeCompletableItem::Data(item);
                                                    let item = StreamItem::DataItem(item);
                                                    Ready(Some(Ok(item)))
                                                } else {
                                                    let emit_len = item.len();
                                                    let item = Output::Bulk(item);
                                                    trace_emit!(
                                                        "State::Begin  Bulk    {}  emit_len {}",
                                                        self.dbgname,
                                                        emit_len
                                                    );
                                                    let item = RangeCompletableItem::Data(item);
                                                    let item = StreamItem::DataItem(item);
                                                    Ready(Some(Ok(item)))
                                                }
                                            } else {
                                                // mixed
                                                trace_transition!("transition with mixed to Bulk");
                                                self.state = State::Bulk;
                                                match self.buf.as_mut() {
                                                    Some(buf) => match item.drain_into(buf, 0..pp) {
                                                        DrainIntoDstResult::Done => {
                                                            if let Some(before) = self.consume_buf_get_latest() {
                                                                self.out.push_back(item);
                                                                let emit_len = before.len();
                                                                let item = Output::Before(before);
                                                                trace_emit!(
                                                                    "State::Begin  Before  {}  emit_len {}",
                                                                    self.dbgname,
                                                                    emit_len
                                                                );
                                                                let item = RangeCompletableItem::Data(item);
                                                                let item = StreamItem::DataItem(item);
                                                                Ready(Some(Ok(item)))
                                                            } else {
                                                                let emit_len = item.len();
                                                                let item = Output::Bulk(item);
                                                                trace_emit!(
                                                                    "State::Begin  Bulk    {}  emit_len {}",
                                                                    self.dbgname,
                                                                    emit_len
                                                                );
                                                                let item = RangeCompletableItem::Data(item);
                                                                let item = StreamItem::DataItem(item);
                                                                Ready(Some(Ok(item)))
                                                            }
                                                        }
                                                        DrainIntoDstResult::Partial => panic!(),
                                                        DrainIntoDstResult::NotCompatible => panic!(),
                                                    },
                                                    None => match item.drain_into_new(0..pp) {
                                                        DrainIntoNewResult::Done(buf) => {
                                                            self.buf = Some(buf);
                                                            if let Some(before) = self.consume_buf_get_latest() {
                                                                self.out.push_back(item);
                                                                let emit_len = before.len();
                                                                let item = Output::Before(before);
                                                                trace_emit!(
                                                                    "State::Begin  Before  {}  emit_len {}",
                                                                    self.dbgname,
                                                                    emit_len
                                                                );
                                                                let item = RangeCompletableItem::Data(item);
                                                                let item = StreamItem::DataItem(item);
                                                                Ready(Some(Ok(item)))
                                                            } else {
                                                                let emit_len = item.len();
                                                                let item = Output::Bulk(item);
                                                                trace_emit!(
                                                                    "State::Begin  Bulk    {}  emit_len {}",
                                                                    self.dbgname,
                                                                    emit_len
                                                                );
                                                                let item = RangeCompletableItem::Data(item);
                                                                let item = StreamItem::DataItem(item);
                                                                Ready(Some(Ok(item)))
                                                            }
                                                        }
                                                        DrainIntoNewResult::Partial(_) => panic!(),
                                                        DrainIntoNewResult::NotCompatible => panic!(),
                                                    },
                                                }
                                            }
                                        } else {
                                            // all entries are before, or empty item
                                            trace_transition!("stay in Begin");
                                            trace_emit!(
                                                "State::Begin  Before  {}  all content still before  len {}",
                                                self.dbgname,
                                                item.len()
                                            );
                                            match self.buf.as_mut() {
                                                Some(buf) => match item.drain_into(buf, 0..item.len()) {
                                                    DrainIntoDstResult::Done => continue,
                                                    DrainIntoDstResult::Partial => panic!(),
                                                    DrainIntoDstResult::NotCompatible => panic!(),
                                                },
                                                None => match item.drain_into_new(0..item.len()) {
                                                    DrainIntoNewResult::Done(buf) => {
                                                        self.buf = Some(buf);
                                                        continue;
                                                    }
                                                    DrainIntoNewResult::Partial(_) => panic!(),
                                                    DrainIntoNewResult::NotCompatible => panic!(),
                                                },
                                            }
                                        }
                                    }
                                }
                                RangeCompletableItem::RangeComplete => {
                                    self.have_range_final = true;
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
                            Ready(Some(Err(Error::Input(Box::new(e)))))
                        }
                        Ready(None) => {
                            self.state = State::Done;
                            trace_transition!("transition from Begin to end of stream");
                            if let Some(before) = self.consume_buf_get_latest() {
                                let emit_len = before.len();
                                let item = Output::Before(before);
                                trace_emit!("State::Begin  EOS  {}  emit_len {}", self.dbgname, emit_len);
                                let item = RangeCompletableItem::Data(item);
                                let item = StreamItem::DataItem(item);
                                Ready(Some(Ok(item)))
                            } else {
                                trace_emit!("State::Begin  EOS  {}  emit None", self.dbgname);
                                Ready(None)
                            }
                        }
                        Pending => Pending,
                    },
                    State::Bulk => {
                        if self.buf.is_some() {
                            let n = self.buf.as_ref().map_or(0, |x| x.len());
                            error!("State::Bulk  but buf non-empty  {}", n);
                            self.state = State::Done;
                            Ready(Some(Err(Error::Logic)))
                        } else {
                            match self.inp.poll_next_unpin(cx) {
                                Ready(Some(Ok(item))) => match item {
                                    StreamItem::DataItem(item) => match item {
                                        RangeCompletableItem::Data(item) => {
                                            if let Some(tsmin) = MergeableTy::ts_min(&item) {
                                                let tsmin = tsmin;
                                                if tsmin < self.tslast {
                                                    self.state = State::Done;
                                                    let e = Error::Unordered;
                                                    break Ready(Some(Err(e)));
                                                } else {
                                                    self.tslast = MergeableTy::ts_max(&item).unwrap();
                                                }
                                            }
                                            if item.is_consistent() == false {
                                                self.state = State::Done;
                                                let e = Error::Unordered;
                                                Ready(Some(Err(e)))
                                            } else {
                                                if item.len() == 0 {
                                                    self.seen_empty_during_bulk = true;
                                                }
                                                let item_len = item.len();
                                                let item = Output::Bulk(item);
                                                trace_emit!(
                                                    "State::Bulk  data  {}  item_len {}",
                                                    self.dbgname,
                                                    item_len
                                                );
                                                let item = RangeCompletableItem::Data(item);
                                                let item = StreamItem::DataItem(item);
                                                Ready(Some(Ok(item)))
                                            }
                                        }
                                        RangeCompletableItem::RangeComplete => {
                                            self.have_range_final = true;
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
                                    Ready(Some(Err(Error::Input(Box::new(e)))))
                                }
                                Ready(None) => {
                                    trace_emit!("in bulk, input done");
                                    self.state = State::Done;
                                    trace_emit!("State::Bulk  EOS  {}  emit None", self.dbgname);
                                    Ready(None)
                                }
                                Pending => Pending,
                            }
                        }
                    }
                    State::Done => Ready(None),
                }
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
    let x: OneBeforeAndBulk<super::events::EventsStreamRt, items_2::channelevents::ChannelEvents> = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}
