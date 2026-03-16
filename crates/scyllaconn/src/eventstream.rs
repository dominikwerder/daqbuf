use crate::worker::EventReadOpts;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::ChConf;
use netpod::SeriesKind;
use query::api4::events::EventsSubQuery;
use std::pin::Pin;
use taskrun::tokio;

autoerr::create_error_v1!(
    name(Error, "ScyllaConnEventsStream"),
    enum variants {
        Logic,
    },
);

macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }

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
        let x = crate::events2::events::EventsStreamRt::new(
            rt,
            chconf.clone(),
            evq.range().into(),
            readopts,
            scyqueue.clone(),
        )
        .map_err(|e| crate::events2::mergert::Error::Msg(e.to_string()));
        Box::pin(x)
    } else {
        trace!("=========    MERGED   =====================");
        let x = crate::events2::mergert::MergeRts::new(chconf.clone(), evq.range().into(), readopts, scyqueue.clone());
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
        .map_err(|e| {
            daqbuf_err::Error::with_msg_no_trace(format!("{}::scylla_channel_event_stream  {}", module_path!(), e))
        });
    Ok(Box::pin(stream))
}
