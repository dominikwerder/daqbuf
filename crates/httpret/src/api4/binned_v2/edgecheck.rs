use futures_util::Stream;
use items_0::streamitem::LogItem;
use items_0::streamitem::StreamItem;
use items_2::binning::container_bins::ContainerBins;
use log::log_item_emit as lg;
use netpod::BinnedRange;
use netpod::TsNano;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tracing::Level;

macro_rules! info_item { ($($arg:tt)*) => { if true { lg::info!($($arg)*); } }; }
macro_rules! trace_item { ($($arg:tt)*) => { if false { lg::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Edgecheck"),
    enum variants {
        FutureComplete,
        ReadJob(#[from] streams::timebin::cached::reader::Error),
    },
);

enum State {
    Reading(Option<(TsNano, f32)>),
    Done,
}

pub struct Edgecheck<S> {
    inp: S,
    range: BinnedRange<TsNano>,
    state: State,
}

impl<S> Edgecheck<S> {
    pub fn new<E>(inp: S, range: BinnedRange<TsNano>) -> Self
    where
        S: Stream<Item = Result<StreamItem<ContainerBins<f32, f32>>, E>> + Unpin,
        E: std::error::Error,
    {
        Self {
            inp,
            range,
            state: State::Reading(None),
        }
    }
}

impl<S, E> Stream for Edgecheck<S>
where
    S: Stream<Item = Result<StreamItem<ContainerBins<f32, f32>>, E>> + Unpin,
    E: std::error::Error,
{
    type Item = Result<StreamItem<ContainerBins<f32, f32>>, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let self2 = &mut *self;
            break match &mut self2.state {
                State::Reading(last) => match Pin::new(&mut self2.inp).poll_next(cx) {
                    Ready(Some(x)) => match x {
                        Ok(x) => match x {
                            StreamItem::DataItem(x) => {
                                for (((((&ts2l, lst), &cnt), &ts1n), min), max) in x
                                    .ts2s_iter()
                                    .zip(x.lsts_iter())
                                    .zip(x.cnts_iter().skip(1))
                                    .zip(x.ts1s_iter().skip(1))
                                    .zip(x.mins_iter().skip(1))
                                    .zip(x.maxs_iter().skip(1))
                                {
                                    if ts2l != ts1n {
                                        log::info!("edge mismatch  ts2l {}  ts1n {}", ts2l, ts1n)
                                    }
                                    if cnt == 0 {
                                        if min != lst {
                                            log::info!("lst/min mismatch  ts1n {}  lst {:?}  min {:?}", ts1n, lst, min)
                                        }
                                        if max != lst {
                                            log::info!("lst/max mismatch  ts1n {}  lst {:?}  max {:?}", ts1n, lst, max)
                                        }
                                    }
                                }
                                for ((min, max), avg) in x
                                    .mins_iter()
                                    .skip(1)
                                    .zip(x.maxs_iter().skip(1))
                                    .zip(x.aggs_iter().skip(1))
                                {
                                    if min > max {
                                        log::info!("min/max mismatch  min {:?}  max {:?}", min, max);
                                    }
                                    if avg < min || avg > max {
                                        log::info!("agg out of range  avg {:?}  min {:?}  max {:?}", avg, min, max);
                                    }
                                }
                                if let Some(ll) = x.ts2_last().zip(x.lst_last()) {
                                    *last = Some(ll);
                                }
                                log::trace!("Edgecheck  passing data item with {} bins", x.len());
                                Ready(Some(Ok(StreamItem::DataItem(x))))
                            }
                            StreamItem::Log(x) => Poll::Ready(Some(Ok(StreamItem::Log(x)))),
                            StreamItem::Stats(x) => Poll::Ready(Some(Ok(StreamItem::Stats(x)))),
                        },
                        Err(e) => {
                            self.state = State::Done;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(None) => Ready(None),
                    Pending => Pending,
                },
                State::Done => {
                    lg::info!("State::Done");
                    Ready(None)
                }
            };
        }
    }
}
