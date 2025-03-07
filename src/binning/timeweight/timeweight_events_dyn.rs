use super::timeweight_events::BinnedEventsTimeweight;
use crate::binning::container_events::ContainerEvents;
use crate::binning::container_events::EventValueType;
use crate::channelevents::ChannelEvents;
use crate::log;
use daqbuf_err as err;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::sitem_data;
use items_0::timebin::BinnedEventsTimeweightTrait;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinningggContainerEventsDyn;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use items_0::timebin::EventsBoxed;
use items_0::timebin::IngestReport;
use netpod::BinnedRange;
use netpod::TsNano;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! debug_input_container { ($($arg:expr),*) => ( if true { log::debug!($($arg),*); }) }

macro_rules! debug { ($($arg:expr),*) => ( if true { log::debug!($($arg),*); }) }

macro_rules! trace_input_container { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); }) }

macro_rules! trace_emit { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); }) }

macro_rules! trace { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); }) }

autoerr::create_error_v1!(
    name(Error, "BinnedEventsTimeweightDyn"),
    enum variants {
        InnerDynMissing,
        Dummy,
        EmptyEventsBuffer,
        NoProgress,
        Binning(#[from] BinningggError),
    },
);

#[derive(Debug)]
pub struct BinnedEventsTimeweightDynbox<EVT>
where
    EVT: EventValueType,
{
    binner: BinnedEventsTimeweight<EVT>,
}

impl<EVT> BinnedEventsTimeweightDynbox<EVT>
where
    EVT: EventValueType + 'static,
{
    pub fn new(range: BinnedRange<TsNano>) -> Box<dyn BinnedEventsTimeweightTrait> {
        let ret = Self {
            binner: BinnedEventsTimeweight::new(range),
        };
        Box::new(ret)
    }
}

impl<EVT> BinnedEventsTimeweightTrait for BinnedEventsTimeweightDynbox<EVT>
where
    EVT: EventValueType,
{
    fn cnt_zero_enable(&mut self) {
        self.binner.cnt_zero_enable();
    }

    fn ingest(&mut self, evs: &EventsBoxed) -> Result<IngestReport, BinningggError> {
        match evs.as_any_ref().downcast_ref::<ContainerEvents<EVT>>() {
            Some(evs) => Ok(self.binner.ingest(evs)?),
            None => {
                let e = BinningggError::TypeMismatch {
                    have: evs.type_name().into(),
                    expect: std::any::type_name::<ContainerEvents<EVT>>().into(),
                };
                Err(e)
            }
        }
    }

    fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        Ok(self.binner.input_done_range_final()?)
    }

    fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        Ok(self.binner.input_done_range_open()?)
    }

    fn output(&mut self) -> Result<Option<BinsBoxed>, BinningggError> {
        if self.binner.output_len() == 0 {
            Ok(None)
        } else {
            let c = self.binner.output();
            Ok(Some(Box::new(c)))
        }
    }
}

#[derive(Debug)]
pub struct BinnedEventsTimeweightLazy {
    range: BinnedRange<TsNano>,
    binned_events: Option<Box<dyn BinnedEventsTimeweightTrait>>,
    enable_cnt_zero: bool,
}

impl BinnedEventsTimeweightLazy {
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        Self {
            range,
            binned_events: None,
            enable_cnt_zero: false,
        }
    }

    pub fn with_cnt_zero(mut self) -> Self {
        self.enable_cnt_zero = true;
        self
    }
}

impl BinnedEventsTimeweightTrait for BinnedEventsTimeweightLazy {
    fn cnt_zero_enable(&mut self) {
        self.enable_cnt_zero = true;
    }

    fn ingest(&mut self, evs: &EventsBoxed) -> Result<IngestReport, BinningggError> {
        self.binned_events
            .get_or_insert_with(|| {
                let mut v = evs.binned_events_timeweight_traitobj(self.range.clone());
                if self.enable_cnt_zero {
                    v.cnt_zero_enable();
                }
                v
            })
            .ingest(evs)
    }

    fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        self.binned_events
            .as_mut()
            .map(|x| x.input_done_range_final())
            .unwrap_or_else(|| {
                debug!("TODO something to do if we miss the binner here?");
                Ok(())
            })
    }

    fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        self.binned_events
            .as_mut()
            .map(|x| x.input_done_range_open())
            .unwrap_or(Ok(()))
    }

    fn output(&mut self) -> Result<Option<BinsBoxed>, BinningggError> {
        self.binned_events
            .as_mut()
            .map(|x| x.output())
            .unwrap_or(Ok(None))
    }
}

enum StreamState {
    Reading,
    Buffered(Box<dyn BinningggContainerEventsDyn>),
    Remains,
    Done,
    Invalid,
}

pub struct BinnedEventsTimeweightStream {
    state: StreamState,
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
    binned_events: BinnedEventsTimeweightLazy,
    range_final: bool,
}

impl BinnedEventsTimeweightStream {
    pub fn new(
        range: BinnedRange<TsNano>,
        inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
    ) -> Self {
        trace!("stream new");
        Self {
            state: StreamState::Reading,
            inp,
            binned_events: BinnedEventsTimeweightLazy::new(range).with_cnt_zero(),
            range_final: false,
        }
    }

    fn consume_evsbuf(
        evs: &mut Box<dyn BinningggContainerEventsDyn>,
        binner: &mut BinnedEventsTimeweightLazy,
        _cx: &mut Context,
    ) -> Result<Option<Box<dyn BinningggContainerBinsDyn>>, Error> {
        let nev = evs.len();
        if nev == 0 {
            // should not be here
            let e = Error::EmptyEventsBuffer;
            return Err(e);
        }
        match binner.ingest(evs) {
            Ok(report) => match binner.output() {
                Ok(Some(x)) => {
                    let nc = match report {
                        IngestReport::ConsumedAll => nev,
                        IngestReport::ConsumedPart(n) => n,
                    };
                    // TODO use better api which takes the number of consumed elements.
                    evs.truncate_front(nev - nc);
                    if x.len() == 0 {
                        if nc == 0 {
                            let e = Error::NoProgress;
                            Err(e)
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(Some(x))
                    }
                }
                Ok(None) => {
                    let nc = match report {
                        IngestReport::ConsumedAll => nev,
                        IngestReport::ConsumedPart(n) => n,
                    };
                    evs.truncate_front(nev - nc);
                    if nc == 0 {
                        let e = Error::NoProgress;
                        Err(e)
                    } else {
                        Ok(None)
                    }
                }
                Err(e) => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    fn handle_buffered(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use ControlFlow::*;
        use Poll::*;
        let self2 = self.get_mut();
        let evsbuf = if let StreamState::Buffered(x) = &mut self2.state {
            x
        } else {
            panic!("logic")
        };
        let binner = &mut self2.binned_events;
        if evsbuf.len() == 0 {
            // should not be here
            let e = Error::EmptyEventsBuffer;
            Break(Ready(Some(Err(daqbuf_err::Error::from_string(e)))))
        } else {
            let j = Self::consume_evsbuf(evsbuf, binner, cx);
            if evsbuf.len() == 0 {
                self2.state = StreamState::Reading;
            }
            match j {
                Ok(Some(x)) => {
                    let item = sitem_data(x);
                    Break(Ready(Some(item)))
                }
                Ok(None) => Continue(()),
                Err(e) => {
                    let e = daqbuf_err::Error::from_string(e);
                    Break(Ready(Some(Err(e))))
                }
            }
        }
    }

    fn handle_sitemty(
        mut self: Pin<&mut Self>,
        item: Sitemty<ChannelEvents>,
        _cx: &mut Context,
    ) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use ControlFlow::*;
        use Poll::*;
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        match item {
            Ok(x) => match x {
                DataItem(x) => match x {
                    Data(x) => match x {
                        ChannelEvents::Events(evs) => {
                            if evs.len() == 0 {
                            } else {
                                self.state = StreamState::Buffered(evs);
                            }
                            Continue(())
                        }
                        ChannelEvents::Status(_) => {
                            // TODO use the status
                            Continue(())
                        }
                    },
                    RangeComplete => {
                        self.range_final = true;
                        Continue(())
                    }
                },
                Log(x) => Break(Ready(Some(Ok(Log(x))))),
                Stats(x) => Break(Ready(Some(Ok(Stats(x))))),
            },
            Err(e) => {
                self.state = StreamState::Done;
                Break(Ready(Some(Err(e))))
            }
        }
    }

    fn _test1(
        self: Pin<&mut Self>,
        _cx: &mut Context,
    ) -> Result<ControlFlow<Poll<Option<<Self as Stream>::Item>>>, Error> {
        use ControlFlow::*;
        use Poll::*;
        if false {
            Ok(Break(Pending))
        } else if false {
            Ok(Continue(()))
        } else {
            let e = Error::Dummy;
            let _ = Err(e)?;
            Ok(Break(Pending))
        }
    }

    fn _test2(
        self: Pin<&mut Self>,
        _cx: &mut Context,
    ) -> ControlFlow<Result<Poll<Option<<Self as Stream>::Item>>, Error>> {
        use ControlFlow::*;
        use Poll::*;
        if false {
            Break(Ok(Pending))
        } else if false {
            Continue(())
        } else {
            let _e = Error::Dummy;
            // unfortunately can not use the `?` operator here:
            // let _ = Err(e)?;
            Break(Ok(Pending))
        }
    }

    fn handle_eos(mut self: Pin<&mut Self>, _cx: &mut Context) -> Result<(), err::Error> {
        debug_input_container!("handle_eos  range final {}", self.range_final);
        self.state = StreamState::Remains;
        if true || self.range_final {
            self.binned_events
                .input_done_range_final()
                .map_err(err::Error::from_string)?;
        } else {
            self.binned_events
                .input_done_range_open()
                .map_err(err::Error::from_string)?;
        }
        Ok(())
    }

    fn handle_remains(
        mut self: Pin<&mut Self>,
        _cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        debug_input_container!("handle_remains");
        use Poll::*;
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        debug!("handle_remains  binner {:?}", self.binned_events);
        match self
            .binned_events
            .output()
            .map_err(err::Error::from_string)?
        {
            Some(x) => {
                // trace_emit!("seeing ready bins {:?}", x);
                debug!("seeing ready bins {:?}", x);
                Ready(Some(Ok(DataItem(Data(x)))))
            }
            None => {
                debug!("no bins ready on eos");
                self.state = StreamState::Done;
                let item =
                    LogItem::from_node(888, log::Level::INFO, format!("no bins ready on eos"));
                Ready(Some(Ok(Log(item))))
            }
        }
    }

    fn handle_main(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        trace_input_container!("handle_main");
        use ControlFlow::*;
        use Poll::*;
        let ret = match &self.state {
            // TODO before attempt to read from input, check whether we have events left in buffer.
            // TODO if we do not consume events, and also not get bins out then error.
            StreamState::Reading => match self.as_mut().inp.poll_next_unpin(cx) {
                Ready(Some(x)) => self.as_mut().handle_sitemty(x, cx),
                Ready(None) => {
                    match self.as_mut().handle_eos(cx) {
                        Ok(()) => {}
                        Err(e) => return Break(Ready(Some(Err(e)))),
                    }
                    Continue(())
                }
                Pending => Break(Pending),
            },
            StreamState::Buffered(_) => self.as_mut().handle_buffered(cx),
            StreamState::Remains => Break(self.as_mut().handle_remains(cx)),
            StreamState::Done => {
                self.state = StreamState::Invalid;
                Break(Ready(None))
            }
            StreamState::Invalid => {
                panic!("StreamState::Invalid")
            }
        };
        if let Break(Ready(Some(Err(_)))) = ret {
            self.state = StreamState::Done;
        }
        if let Break(Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))))) = &ret
        {
            trace_emit!("emit item len {}", item.len());
        }
        ret
    }
}

impl Stream for BinnedEventsTimeweightStream {
    type Item = Sitemty<Box<dyn BinningggContainerBinsDyn>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use ControlFlow::*;
        loop {
            break match self.as_mut().handle_main(cx) {
                Break(x) => x,
                Continue(()) => continue,
            };
        }
    }
}
