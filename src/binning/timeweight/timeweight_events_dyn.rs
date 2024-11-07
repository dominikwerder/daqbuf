use super::timeweight_events::BinnedEventsTimeweight;
use crate::binning::container_events::ContainerEvents;
use crate::binning::container_events::EventValueType;
use crate::channelevents::ChannelEvents;
use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinnedEventsTimeweightTrait;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use items_0::timebin::EventsBoxed;
use netpod::log::*;
use netpod::BinnedRange;
use netpod::TsNano;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace_input_container { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

macro_rules! trace_emit { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

#[derive(Debug, ThisError)]
#[cstm(name = "BinnedEventsTimeweightDyn")]
pub enum Error {
    InnerDynMissing,
}

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
    fn ingest(&mut self, mut evs: EventsBoxed) -> Result<(), BinningggError> {
        // let a = (&evs as &dyn any::Any).downcast_ref::<String>();
        // evs.downcast::<String>();
        // evs.as_anybox().downcast::<ContainerEvents<f64>>();
        match evs.to_anybox().downcast::<ContainerEvents<EVT>>() {
            Ok(evs) => {
                let evs = {
                    let a = evs;
                    *a
                };
                Ok(self.binner.ingest(evs)?)
            }
            Err(_) => Err(BinningggError::TypeMismatch {
                have: evs.type_name().into(),
                expect: std::any::type_name::<ContainerEvents<EVT>>().into(),
            }),
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
}

impl BinnedEventsTimeweightLazy {
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        Self {
            range,
            binned_events: None,
        }
    }
}

impl BinnedEventsTimeweightTrait for BinnedEventsTimeweightLazy {
    fn ingest(&mut self, evs_all: EventsBoxed) -> Result<(), BinningggError> {
        self.binned_events
            .get_or_insert_with(|| evs_all.binned_events_timeweight_traitobj(self.range.clone()))
            .ingest(evs_all)
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
        self.binned_events.as_mut().map(|x| x.output()).unwrap_or(Ok(None))
    }
}

enum StreamState {
    Reading,
    Done,
    Invalid,
}

pub struct BinnedEventsTimeweightStream {
    state: StreamState,
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
    binned_events: BinnedEventsTimeweightLazy,
    range_complete: bool,
}

impl BinnedEventsTimeweightStream {
    pub fn new(range: BinnedRange<TsNano>, inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>) -> Self {
        Self {
            state: StreamState::Reading,
            inp,
            binned_events: BinnedEventsTimeweightLazy::new(range),
            range_complete: false,
        }
    }

    fn handle_sitemty(
        mut self: Pin<&mut Self>,
        item: Sitemty<ChannelEvents>,
        _cx: &mut Context,
    ) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        use ControlFlow::*;
        use Poll::*;
        match item {
            Ok(x) => match x {
                DataItem(x) => match x {
                    Data(x) => match x {
                        ChannelEvents::Events(evs) => match self.binned_events.ingest(evs.to_container_events()) {
                            Ok(()) => {
                                match self.binned_events.output() {
                                    Ok(Some(x)) => {
                                        if x.len() == 0 {
                                            Continue(())
                                        } else {
                                            Break(Ready(Some(Ok(DataItem(Data(x))))))
                                        }
                                    }
                                    Ok(None) => Continue(()),
                                    Err(e) => Break(Ready(Some(Err(err::Error::from_string(e))))),
                                }
                                // Continue(())
                            }
                            Err(e) => Break(Ready(Some(Err(err::Error::from_string(e))))),
                        },
                        ChannelEvents::Status(_) => {
                            // TODO use the status
                            Continue(())
                        }
                    },
                    RangeComplete => {
                        self.range_complete = true;
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

    fn handle_eos(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<<Self as Stream>::Item>> {
        trace_input_container!("handle_eos");
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        use Poll::*;
        self.state = StreamState::Done;
        if self.range_complete {
            self.binned_events
                .input_done_range_final()
                .map_err(err::Error::from_string)?;
        } else {
            self.binned_events
                .input_done_range_open()
                .map_err(err::Error::from_string)?;
        }
        match self.binned_events.output().map_err(err::Error::from_string)? {
            Some(x) => {
                trace_emit!("seeing ready bins {:?}", x);
                Ready(Some(Ok(DataItem(Data(x)))))
            }
            None => {
                let item = LogItem::from_node(888, Level::INFO, format!("no bins ready on eos"));
                Ready(Some(Ok(Log(item))))
            }
        }
    }

    fn handle_main(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use ControlFlow::*;
        use Poll::*;
        let ret = match &self.state {
            StreamState::Reading => match self.as_mut().inp.poll_next_unpin(cx) {
                Ready(Some(x)) => self.as_mut().handle_sitemty(x, cx),
                Ready(None) => Break(self.as_mut().handle_eos(cx)),
                Pending => Break(Pending),
            },
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
