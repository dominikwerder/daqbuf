use daqbuf_err as err;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::log;
use netpod::ChConf;
use netpod::SeriesKind;
use query::api4::events::EventsSubQuery;
use scyllaconn::events2::events::EventReadOpts;
use scyllaconn::events2::mergert;
use scyllaconn::worker::ScyllaQueue;
use scyllaconn::SeriesId;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use streams::timebin::cached::reader::EventsReadProvider;
use taskrun::tokio;

macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "ScyllaChannelEventStream"),
    enum variants {
        MergeRt(#[from] mergert::Error),
    },
);

pub async fn scylla_channel_event_stream(
    evq: EventsSubQuery,
    chconf: ChConf,
    scyqueue: &ScyllaQueue,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    trace!("scylla_channel_event_stream  {evq:?}");
    // TODO depends in general on the query
    // TODO why both in PlainEventsQuery and as separate parameter? Check other usages.
    let _series = SeriesId::new(chconf.series());
    let readopts = EventReadOpts::new(
        evq.need_one_before_range(),
        evq.need_value_data(),
        evq.settings().scylla_read_queue_len(),
        scylla_opts,
    );
    let stream: Pin<Box<dyn Stream<Item = _> + Send>> = if let Some(rt) = evq.use_rt() {
        trace!("=========    SOLO {rt:?}   =====================");
        let x = scyllaconn::events2::events::EventsStreamRt::new(
            rt,
            chconf.clone(),
            evq.range().into(),
            readopts,
            scyqueue.clone(),
        )
        .map_err(|e| scyllaconn::events2::mergert::Error::Msg(e.to_string()));
        Box::pin(x)
    } else {
        trace!("=========    MERGED   =====================");
        let x =
            scyllaconn::events2::mergert::MergeRts::new(chconf.clone(), evq.range().into(), readopts, scyqueue.clone());
        Box::pin(x)
    };
    let stream = stream
        .map(move |item| match item {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(k) => match k {
                        ChannelEvents::Events(mut k) => {
                            if true {
                                let item = ChannelEvents::Events(k);
                                let item = StreamItem::DataItem(RangeCompletableItem::Data(item));
                                Ok(item)
                            } else if let SeriesKind::ChannelStatus = chconf.kind() {
                                type C1 = ContainerEvents<u64>;
                                type C2 = ContainerEvents<String>;
                                if let Some(j) = k.as_any_mut().downcast_mut::<C1>() {
                                    let mut g = C2::new();
                                    for (ts, val) in j.iter_zip() {
                                        use netpod::channelstatus as cs2;
                                        let val = match cs2::ChannelStatus::from_kind(val as _) {
                                            Ok(x) => x.to_user_variant_string(),
                                            Err(_) => format!("{}", val),
                                        };
                                        if val.len() != 0 {
                                            g.push_back(ts, val);
                                        }
                                    }
                                    let item = ChannelEvents::Events(Box::new(g));
                                    let item = StreamItem::DataItem(RangeCompletableItem::Data(item));
                                    Ok(item)
                                } else {
                                    let item = ChannelEvents::Events(k);
                                    let item = StreamItem::DataItem(RangeCompletableItem::Data(item));
                                    Ok(item)
                                }
                            } else {
                                let item = ChannelEvents::Events(k);
                                let item = StreamItem::DataItem(RangeCompletableItem::Data(item));
                                Ok(item)
                            }
                        }
                        ChannelEvents::Status(k) => {
                            let item = ChannelEvents::Status(k);
                            let item = StreamItem::DataItem(RangeCompletableItem::Data(item));
                            Ok(item)
                        }
                    },
                    RangeCompletableItem::RangeComplete => {
                        let item = StreamItem::DataItem(RangeCompletableItem::RangeComplete);
                        Ok(item)
                    }
                },
                StreamItem::Log(x) => Ok(StreamItem::Log(x)),
                StreamItem::Stats(x) => Ok(StreamItem::Stats(x)),
            },
            _ => item,
        })
        .map(move |item| match &item {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(k) => match k {
                        ChannelEvents::Events(k) => {
                            let n = k.len();
                            let d = evq.event_delay();
                            (item, n, d.clone())
                        }
                        ChannelEvents::Status(_) => (item, 1, None),
                    },
                    RangeCompletableItem::RangeComplete => (item, 1, None),
                },
                StreamItem::Log(_) | StreamItem::Stats(_) => (item, 1, None),
            },
            Err(_) => (item, 1, None),
        })
        .then(|(item, n, d)| async move {
            if let Some(d) = d {
                warn!("sleep {} times {:?}", n, d);
                tokio::time::sleep(d.saturating_mul(n as _)).await;
            }
            item
        })
        .map(|item| {
            let item = match item {
                Ok(x) => Ok(x),
                Err(e) => Err(err::Error::with_msg_no_trace(format!(
                    "{}::scylla_channel_event_stream  {e}",
                    module_path!()
                ))),
            };
            item
        });
    Ok(Box::pin(stream))
}

struct ScyllaEventsReadStream {
    fut1: Option<
        Pin<Box<dyn Future<Output = Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error>> + Send>>,
    >,
    stream: Option<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>>,
}

impl Stream for ScyllaEventsReadStream {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(fut) = self.fut1.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(Ok(x)) => {
                        self.fut1 = None;
                        self.stream = Some(x);
                        continue;
                    }
                    Ready(Err(e)) => Ready(Some(Err(err::Error::from_string(e)))),
                    Pending => Pending,
                }
            } else if let Some(fut) = self.stream.as_mut() {
                match fut.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        // let x = try_map_sitemty_data!(x, |x| match x {
                        //     ChannelEvents::Events(x) => {
                        //         let x = x.to_dim0_f32_for_binning();
                        //         Ok(ChannelEvents::Events(x))
                        //     }
                        //     ChannelEvents::Status(x) => Ok(ChannelEvents::Status(x)),
                        // });
                        Ready(Some(x))
                    }
                    Ready(None) => Ready(None),
                    Pending => Pending,
                }
            } else {
                Ready(None)
            };
        }
    }
}

pub struct ScyllaEventReadProvider {
    scyqueue: ScyllaQueue,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
}

impl ScyllaEventReadProvider {
    pub fn new(scyqueue: ScyllaQueue, scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery) -> Self {
        Self { scyqueue, scylla_opts }
    }
}

impl EventsReadProvider for ScyllaEventReadProvider {
    fn read(&self, evq: EventsSubQuery) -> streams::timebin::cached::reader::EventsReading {
        let scyqueue = self.scyqueue.clone();
        match evq.ch_conf().clone() {
            netpod::ChannelTypeConfigGen::Scylla(ch_conf) => {
                let scylla_opts = self.scylla_opts.clone();
                let fut1 = async move {
                    crate::scylla::scylla_channel_event_stream(evq, ch_conf, &scyqueue, scylla_opts).await
                };
                let stream = ScyllaEventsReadStream {
                    fut1: Some(Box::pin(fut1)),
                    stream: None,
                };
                streams::timebin::cached::reader::EventsReading::new(Box::pin(stream))
            }
            netpod::ChannelTypeConfigGen::SfDatabuffer(_) => panic!("not a scylla reader"),
        }
    }
}
