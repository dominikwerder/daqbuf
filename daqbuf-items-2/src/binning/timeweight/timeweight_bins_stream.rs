use super::timeweight_bins_lazy::BinnedBinsTimeweightLazy;
use crate::log;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::sitem_err2_from_string;
use items_0::timebin::BinningggContainerBinsDyn;
use netpod::BinnedRange;
use netpod::TsNano;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! debug { ($($arg:tt)*) => ( if false { log::debug!($($arg)*); }) }

macro_rules! trace_input_container { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); }) }

macro_rules! trace_emit { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); }) }

autoerr::create_error_v1!(
    name(Error, "BinnedEventsTimeweightDyn"),
    enum variants {
        InnerDynMissing,
    },
);

type ItemA = Box<dyn BinningggContainerBinsDyn>;
type ItemB = Sitemty<ItemA>;

enum StreamState {
    Reading,
    Done,
}

pub struct BinnedBinsTimeweightStream {
    state: StreamState,
    inp: Pin<Box<dyn Stream<Item = ItemB> + Send>>,
    range_complete: bool,
    binned: BinnedBinsTimeweightLazy,
}

impl BinnedBinsTimeweightStream {
    pub fn new(range: BinnedRange<TsNano>, inp: Pin<Box<dyn Stream<Item = ItemB> + Send>>) -> Self {
        Self {
            state: StreamState::Reading,
            inp,
            range_complete: false,
            binned: BinnedBinsTimeweightLazy::new(range),
        }
    }

    fn handle_sitemty(
        mut self: Pin<&mut Self>,
        item: ItemB,
        _cx: &mut Context,
    ) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use ControlFlow::*;
        use Poll::*;
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        match item {
            Ok(x) => match x {
                DataItem(x) => match x {
                    Data(x) => match self.binned.ingest(&x) {
                        Ok(()) => match self.binned.output() {
                            Ok(Some(x)) => {
                                if x.len() == 0 {
                                    Continue(())
                                } else {
                                    let ret = Ok(DataItem(Data(x)));
                                    Break(Ready(Some(ret)))
                                }
                            }
                            Ok(None) => Continue(()),
                            Err(e) => {
                                let e = sitem_err2_from_string(e);
                                Break(Ready(Some(Err(e))))
                            }
                        },
                        Err(e) => {
                            let e = sitem_err2_from_string(e);
                            Break(Ready(Some(Err(e))))
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

    fn handle_eos(
        mut self: Pin<&mut Self>,
        _cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        trace_input_container!("handle_eos");
        use Poll::*;
        use items_0::streamitem::RangeCompletableItem::*;
        use items_0::streamitem::StreamItem::*;
        self.state = StreamState::Done;
        if self.range_complete {
            self.binned
                .input_done_range_final()
                .map_err(sitem_err2_from_string)?;
        } else {
            self.binned
                .input_done_range_open()
                .map_err(sitem_err2_from_string)?;
        }
        match self.binned.output().map_err(sitem_err2_from_string)? {
            Some(x) => {
                trace_emit!("seeing ready bins {:?}", x);
                Ready(Some(Ok(DataItem(Data(x)))))
            }
            None => {
                debug!("no bins ready on eos");
                let item = LogItem::from_node(log::Level::DEBUG, format!("no bins ready on eos"));
                Ready(Some(Ok(Log(item))))
            }
        }
    }

    fn handle_main(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> ControlFlow<Poll<Option<<Self as Stream>::Item>>> {
        use ControlFlow::*;
        use Poll::*;
        let ret = match &self.state {
            StreamState::Reading => match self.as_mut().inp.poll_next_unpin(cx) {
                Ready(Some(x)) => self.as_mut().handle_sitemty(x, cx),
                Ready(None) => Break(self.as_mut().handle_eos(cx)),
                Pending => Break(Pending),
            },
            StreamState::Done => Break(Ready(None)),
        };
        if let Break(Ready(Some(Err(_)))) = ret {
            self.state = StreamState::Done;
        }
        ret
    }
}

impl Stream for BinnedBinsTimeweightStream {
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
