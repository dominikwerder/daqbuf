use futures_util::Stream;
use items_0::streamitem::LogItem;
use items_0::streamitem::StreamItem;
use items_2::binning::container_bins::ContainerBins;
use log::log_item_emit as lg;
use netpod::BinnedRange;
use netpod::TsNano;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tracing::Level;

macro_rules! info_item { ($($arg:tt)*) => { if true { lg::info!($($arg)*); } }; }
macro_rules! trace_item { ($($arg:tt)*) => { if false { lg::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "BinnedExpand"),
    enum variants {
        FutureComplete,
        ReadJob(#[from] streams::timebin::cached::reader::Error),
    },
);

fn log_emit_item(tag: &str, x: &ContainerBins<f32, f32>) {
    if false {
        for (i, (t1, t2)) in x.ts1s_iter().zip(x.ts2s_iter()).enumerate() {
            let t1sec = t1.ns() / 1000000000;
            let t2sec = t2.ns() / 1000000000;
            let a = [1759916820 + 2820, 1759916820 + 2880, 1759916820 + 2940];
            let mark = if a.contains(&t1sec) || a.contains(&t2sec) {
                "!!!!"
            } else {
                ""
            };
            log::info!("LOG  {tag:16}   {:4}   {}   {}{mark:>10}", i, t1sec, t2sec);
        }
    }
}

enum State {
    Reading(Option<(TsNano, f32)>),
    Process(Option<(TsNano, f32)>, ContainerBins<f32, f32>),
    Expanding(TsNano, f32, ContainerBins<f32, f32>),
    ExpandingFinal(TsNano, f32),
    Done,
}

pub struct BinnedExpand<S> {
    inp: S,
    range: BinnedRange<TsNano>,
    state: State,
}

impl<S> BinnedExpand<S> {
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

impl<S, E> Stream for BinnedExpand<S>
where
    S: Stream<Item = Result<StreamItem<ContainerBins<f32, f32>>, E>> + Unpin,
    E: std::error::Error,
{
    type Item = Result<StreamItem<ContainerBins<f32, f32>>, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'main: loop {
            let self2 = &mut *self;
            break match &mut self2.state {
                State::Reading(last) => match Pin::new(&mut self2.inp).poll_next(cx) {
                    Ready(Some(x)) => match x {
                        Ok(x) => match x {
                            StreamItem::DataItem(x) => {
                                log_emit_item("RECV", &x);
                                trace_item!("BinnedExpand --------------------   RECV {}", x.len());
                                self.state = State::Process(*last, x);
                                continue;
                            }
                            StreamItem::Log(x) => Poll::Ready(Some(Ok(StreamItem::Log(x)))),
                            StreamItem::Stats(x) => Poll::Ready(Some(Ok(StreamItem::Stats(x)))),
                        },
                        Err(e) => {
                            self.state = State::Done;
                            Ready(Some(Err(e)))
                        }
                    },
                    Ready(None) => {
                        if let Some((ts2l, lst)) = &last {
                            trace_item!("State::Reading  input None  goto State::ExpandingFinal");
                            self2.state = State::ExpandingFinal(*ts2l, *lst);
                        } else {
                            trace_item!("State::Reading  input None  goto State::Done");
                            self2.state = State::Done;
                        }
                        continue;
                    }
                    Pending => Pending,
                },
                State::Process(last, inp) => {
                    match last {
                        Some((ts2l, lst)) => {
                            let ts2l = *ts2l;
                            if let Some(ts1n) = inp.ts1_first() {
                                if ts2l == ts1n {
                                    // let v = inp.ts2_last().zip(inp.lst_last());
                                    // *last = v;
                                } else if ts2l > ts1n {
                                    let item = LogItem::level_msg(
                                        Level::ERROR,
                                        format!("binned input unordered  {} > {}", ts2l, ts1n),
                                    );
                                    self2.state = State::Done;
                                    break 'main Ready(Some(Ok(StreamItem::Log(item))));
                                } else {
                                    trace_item!("State::Process  gap at beg, go to expand");
                                    self2.state = State::Expanding(ts2l, *lst, mem::replace(inp, ContainerBins::new()));
                                    continue;
                                }
                            }
                        }
                        None => {
                            // let v = inp.ts2_last().zip(inp.lst_last());
                            // *last = v;
                        }
                    }
                    if inp.len() == 0 {
                        trace_item!("State::Process  empty container in process");
                        self.state = State::Reading(*last);
                        continue;
                    } else {
                        let mut rem = None;
                        for (i, (ts2l, ts1n)) in inp.ts2s_iter().zip(inp.ts1s_iter().skip(1)).enumerate() {
                            if ts2l < ts1n {
                                {
                                    #[allow(unused)]
                                    use items_0::merge::MergeableDyn;
                                }
                                use items_0::merge::DrainIntoNewResult;
                                use items_0::merge::MergeableTy;
                                let c2 = match inp.drain_into_new(1 + i..inp.len()) {
                                    DrainIntoNewResult::Done(x) => x,
                                    DrainIntoNewResult::Partial(x) => x,
                                    DrainIntoNewResult::NotCompatible => {
                                        let item = LogItem::level_msg(
                                            Level::ERROR,
                                            format!("DrainIntoNewResult::NotCompatible"),
                                        );
                                        self2.state = State::Done;
                                        break 'main Ready(Some(Ok(StreamItem::Log(item))));
                                    }
                                };
                                rem = Some(c2);
                                break;
                            } else if ts2l > ts1n {
                                let item = LogItem::level_msg(
                                    Level::ERROR,
                                    format!("bad input overlap  ts2l {}  ts1n {}", ts2l, ts1n),
                                );
                                self2.state = State::Done;
                                break 'main Ready(Some(Ok(StreamItem::Log(item))));
                            }
                        }
                        trace_item!(
                            "State::Process  inp len {:?}  rem len {:?}",
                            inp.len(),
                            rem.as_ref().map(|x| x.len())
                        );
                        if let Some(rem) = rem {
                            let lll = inp.ts2_last().zip(inp.lst_last());
                            if let Some(lll) = lll {
                                let item = mem::replace(inp, ContainerBins::new());
                                log_emit_item("PRC-EXP-EMIT", &item);
                                log_emit_item("PRC-EXP-REM", &rem);
                                self2.state = State::Expanding(lll.0, lll.1, rem);
                                Ready(Some(Ok(StreamItem::DataItem(item))))
                            } else {
                                let item = LogItem::level_msg(
                                    Level::ERROR,
                                    format!("have rem but no last ts2/lst  inp len {}", inp.len()),
                                );
                                self2.state = State::Done;
                                break 'main Ready(Some(Ok(StreamItem::Log(item))));
                            }
                        } else {
                            let item = mem::replace(inp, ContainerBins::new());
                            trace_item!("State::Process  inp len {:?}  rem None", item.len());
                            log_emit_item("PRC-NOREM", &item);
                            let ll = item.ts2_last().zip(item.lst_last());
                            if ll.is_none() {
                                trace_item!("emit no remnant but item empty");
                            }
                            self2.state = State::Reading(ll);
                            Ready(Some(Ok(StreamItem::DataItem(item))))
                        }
                    }
                }
                State::Expanding(ts2, lst, next) => {
                    trace_item!("State::Expanding");
                    if next.ts1_first().is_none() {
                        trace_item!("BinnedExpand  empty container in expanding");
                    }
                    let next_ts1_first = next.ts1_first().unwrap_or(self2.range.nano_end());
                    let mut c = ContainerBins::new();
                    loop {
                        let tts1 = *ts2;
                        let tts2 = ts2.add_dt_nano(self2.range.bin_len_dt_ns());
                        let fnl = true;
                        c.push_back(tts1, tts2, 0, *lst, *lst, *lst, *lst, fnl);
                        *ts2 = tts2;
                        if *ts2 == next_ts1_first {
                            trace_item!("State::Expanding  goto State::Done");
                            self2.state = State::Process(Some((*ts2, *lst)), mem::replace(next, ContainerBins::new()));
                            break;
                        } else if *ts2 > next_ts1_first {
                            let item = LogItem::level_msg(Level::ERROR, format!("State::Expanding  edge mismatch"));
                            self2.state = State::Done;
                            break 'main Ready(Some(Ok(StreamItem::Log(item))));
                        } else if c.len() >= 600 {
                            break;
                        }
                    }
                    log_emit_item("EXP-MID", &c);
                    Ready(Some(Ok(StreamItem::DataItem(c))))
                }
                State::ExpandingFinal(ts2, lst) => {
                    trace_item!("State::ExpandingFinal");
                    let mut c = ContainerBins::new();
                    loop {
                        let tts1 = *ts2;
                        let tts2 = ts2.add_dt_nano(self2.range.bin_len_dt_ns());
                        let fnl = true;
                        c.push_back(tts1, tts2, 0, *lst, *lst, *lst, *lst, fnl);
                        *ts2 = tts2;
                        if *ts2 >= self2.range.nano_end() {
                            self2.state = State::Done;
                            break;
                        } else if c.len() >= 600 {
                            break;
                        }
                    }
                    log_emit_item("EXP-FIN", &c);
                    Ready(Some(Ok(StreamItem::DataItem(c))))
                }
                State::Done => {
                    trace_item!("State::Done");
                    Ready(None)
                }
            };
        }
    }
}
