use futures_util::FutureExt;
use futures_util::Stream;
use items_0::streamitem::LogItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::log;
use netpod::range::evrange::NanoRange;
use netpod::ChConf;
use netpod::OneBeforeFlag;
use query::api4::events::EventsSubQuery;
use scyllaconn::events3::ks::clksmerge::cl_ks_merged;
use scyllaconn::events3::SeriesInfo;
use scyllaconn::worker::ScyllaOptsSubmit;
use scyllaconn::worker::ScyllaQueue;
use scyllaconn::SeriesId;
use std::pin::Pin;
use streams::rangefilter2::RangeFilter2;
use streams::timebin::cached::reader::EventsReadProvider;

macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "ScyllaChannelEventStream"),
    enum variants {
        ClKsMerge(#[from] scyllaconn::events3::ks::clksmerge::Error),
        NanoRangeFromSeriesRange,
    },
);

pub async fn scylla_channel_event_stream(
    evq: EventsSubQuery,
    chconf: ChConf,
    scyqueue: &ScyllaQueue,
    scyopts: ScyllaOptsSubmit,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    trace!("scylla_channel_event_stream");
    let series_info = SeriesInfo::from(&chconf);
    let one_before = OneBeforeFlag::from_bool(evq.need_one_before_range());
    let filter_rts = evq.use_rt().map(|x| vec![x]);
    let stream = cl_ks_merged(
        series_info,
        evq.range().clone(),
        one_before,
        filter_rts,
        scyqueue.clone(),
        scyopts,
    )
    .await?;
    Ok(Box::pin(stream))
}

pub struct ScyllaEventReadProvider {
    scyqu: ScyllaQueue,
    scyopts: ScyllaOptsSubmit,
}

impl ScyllaEventReadProvider {
    pub fn new(scyqu: ScyllaQueue, scyopts: ScyllaOptsSubmit) -> Self {
        Self { scyqu, scyopts }
    }
}

impl EventsReadProvider for ScyllaEventReadProvider {
    fn read(&self, evq: EventsSubQuery) -> streams::timebin::cached::reader::EventsReading {
        let scyqu = self.scyqu.clone();
        match evq.ch_conf().clone() {
            netpod::ChannelTypeConfigGen::Scylla(ch_conf) => {
                let series_info = SeriesInfo::new(
                    SeriesId::new(ch_conf.series()),
                    ch_conf.scalar_type().clone(),
                    ch_conf.shape().clone(),
                );
                let range = evq.range().clone();
                // TODO handle unwrap
                let range_ty2 = NanoRange::try_from(&range)
                    .map_err(|_| Error::NanoRangeFromSeriesRange)
                    .unwrap();
                let one_before = OneBeforeFlag::from_bool(evq.need_one_before_range());
                let filter_rts = evq.use_rt().map(|x| vec![x]);
                let stream = cl_ks_merged(series_info, range, one_before, filter_rts, scyqu, self.scyopts.clone());
                type StreamTy = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;
                let stream = stream
                    .map(move |x| match x {
                        Ok(stream) => {
                            let stream = RangeFilter2::new(stream, range_ty2.clone(), one_before);
                            Box::pin(stream) as StreamTy
                        }
                        Err(e) => {
                            let item = Err(daqbuf_err::Error::from_string(e.to_string()));
                            let stream = futures_util::stream::iter([item]);
                            Box::pin(stream) as StreamTy
                        }
                    })
                    .flatten_stream();
                streams::timebin::cached::reader::EventsReading::new(Box::pin(stream))
            }
            netpod::ChannelTypeConfigGen::SfDatabuffer(_) => {
                let msg = format!("not a sf-databuffer reader");
                let item = LogItem::info(msg);
                let it = [Ok(StreamItem::Log(item))];
                let stream = futures_util::stream::iter(it);
                streams::timebin::cached::reader::EventsReading::new(Box::pin(stream))
            }
        }
    }
}
