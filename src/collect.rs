use crate::streamtimeout::StreamTimeout2;
use core::fmt;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::CollectableDyn;
use items_0::collect_s::CollectedDyn;
use items_0::collect_s::CollectorDyn;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use netpod::log::*;
use netpod::DiskStats;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

autoerr::create_error_v1!(
    name(Error, "CollectDyn"),
    enum variants {
        Msg(String),
        NoResultNoCollector,
    },
);

struct ErrMsg<E>(E)
where
    E: ToString;

impl<E> From<ErrMsg<E>> for Error
where
    E: ToString,
{
    fn from(value: ErrMsg<E>) -> Self {
        Self::Msg(value.0.to_string())
    }
}

#[derive(Debug)]
pub enum CollectResult<T>
where
    T: fmt::Debug,
{
    Empty,
    Timeout,
    Some(T),
}

pub struct Collect<ITEM> {
    inp: Pin<Box<dyn Stream<Item = Sitemty<ITEM>> + Send>>,
    events_max: u64,
    bytes_max: u64,
    collector: Option<Box<dyn CollectorDyn>>,
    range_final: bool,
    timeout: bool,
    timer: Pin<Box<dyn Future<Output = ()> + Send>>,
    done_input: bool,
}

impl<ITEM> Collect<ITEM>
where
    ITEM: CollectableDyn,
{
    pub fn new(
        inp: Pin<Box<dyn Stream<Item = Sitemty<ITEM>> + Send>>,
        deadline: Instant,
        events_max: u64,
        bytes_max: u64,
        timeout_provider: Box<dyn StreamTimeout2>,
    ) -> Self {
        Self {
            inp,
            events_max,
            bytes_max,
            collector: None,
            range_final: false,
            timeout: false,
            timer: timeout_provider
                .timeout_intervals(deadline.saturating_duration_since(Instant::now())),
            done_input: false,
        }
    }

    fn handle_item(&mut self, item: Sitemty<ITEM>) -> Result<(), Error> {
        match item {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {
                        self.range_final = true;
                        if let Some(coll) = self.collector.as_mut() {
                            coll.set_range_complete();
                        } else {
                            warn!("collect received RangeComplete but no collector yet");
                        }
                        Ok(())
                    }
                    RangeCompletableItem::Data(mut item) => {
                        trace!("collect sees len {}", item.len());
                        let coll = self.collector.get_or_insert_with(|| item.new_collector());
                        coll.ingest(&mut item);
                        if coll.len() as u64 >= self.events_max {
                            info!("reached events_max {} / {}", coll.len(), self.events_max);
                            self.done_input = true;
                        }
                        if coll.byte_estimate() as u64 >= self.bytes_max {
                            info!(
                                "reached bytes_max {} / {}",
                                coll.byte_estimate(),
                                self.events_max
                            );
                            self.done_input = true;
                        }
                        Ok(())
                    }
                },
                StreamItem::Log(item) => {
                    if item.level == Level::ERROR {
                        error!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::WARN {
                        warn!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::INFO {
                        info!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::DEBUG {
                        debug!("node {}  msg {}", item.node_ix, item.msg);
                    } else if item.level == Level::TRACE {
                        trace!("node {}  msg {}", item.node_ix, item.msg);
                    }
                    Ok(())
                }
                StreamItem::Stats(item) => {
                    trace!("collect stats {:?}", item);
                    match item {
                        // TODO factor and simplify the stats collection:
                        StatsItem::EventDataReadStats(_) => {}
                        StatsItem::RangeFilterStats(_) => {}
                        StatsItem::DiskStats(item) => match item {
                            DiskStats::OpenStats(_) => {
                                //total_duration += k.duration;
                            }
                            DiskStats::SeekStats(_) => {
                                //total_duration += k.duration;
                            }
                            DiskStats::ReadStats(_) => {
                                //total_duration += k.duration;
                            }
                            DiskStats::ReadExactStats(_) => {
                                //total_duration += k.duration;
                            }
                        },
                        _ => {}
                    }
                    Ok(())
                }
            },
            Err(e) => {
                // TODO  Need to use some flags to get good enough error message for remote user.
                Err(ErrMsg(e).into())
            }
        }
    }
}

impl<ITEM> Future for Collect<ITEM>
where
    ITEM: CollectableDyn,
{
    type Output = Result<CollectResult<Box<dyn CollectedDyn>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let span = tracing::span!(Level::INFO, "Collect");
        let _spg = span.enter();
        loop {
            break if self.done_input {
                if self.timeout {
                    if let Some(coll) = self.collector.as_mut() {
                        info!("Collect  call  set_timed_out");
                        coll.set_timed_out();
                    } else {
                        warn!("collect timeout but no collector yet");
                    }
                }
                // TODO use range_final and timeout in result.
                match self.collector.take() {
                    Some(mut coll) => {
                        match coll.result() {
                            Ok(res) => {
                                //info!("collect stats total duration: {:?}", total_duration);
                                Ready(Ok(CollectResult::Some(res)))
                            }
                            Err(e) => Ready(Err(ErrMsg(e).into())),
                        }
                    }
                    None => {
                        debug!("no result because no collector was created");
                        Ready(Ok(CollectResult::Empty))
                    }
                }
            } else {
                match self.timer.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = true;
                        self.done_input = true;
                        continue;
                    }
                    Pending => match self.inp.poll_next_unpin(cx) {
                        Ready(Some(item)) => match self.handle_item(item) {
                            Ok(()) => {
                                continue;
                            }
                            Err(e) => {
                                error!("Collect  {e}");
                                Ready(Err(e))
                            }
                        },
                        Ready(None) => {
                            self.done_input = true;
                            continue;
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}
