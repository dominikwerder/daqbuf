use crate::events2::msp::MspStreamRt;
use crate::events3::SeriesInfo;
use crate::events3::jobtrace::ReadJobTrace;
use crate::range::ScyllaSeriesRange;
use crate::worker::ReadEvents03FwdParams;
use crate::worker::ScyllaQueue;
use daqbuf_series::msp::MspU32;
use futures_util::Stream;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::channelevents::ChannelEvents;
use netpod::TsMs;
use netpod::ttl::RetentionTime;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "EventsMspFwd"),
    enum variants {
        Worker(#[from] crate::worker::Error),
    },
);

#[derive(Debug, Clone)]
pub struct EventReadOpts {
    with_values: bool,
    one_before: bool,
    qucap: u32,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
}

impl EventReadOpts {
    pub fn new(
        one_before: bool,
        with_values: bool,
        qucap: Option<u32>,
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Self {
        Self {
            one_before,
            with_values,
            qucap: qucap.unwrap_or(6),
            scylla_opts,
        }
    }

    pub fn with_values(&self) -> bool {
        self.with_values
    }
}

// Stream of events for a single msp in forward direction.
#[derive(Debug)]
pub struct EventsMspFwdOld {
    series_info: SeriesInfo,
    rt: RetentionTime,
    msp: MspU32,
    scyqueue: ScyllaQueue,
    range: ScyllaSeriesRange,
    readopts: EventReadOpts,
}

impl EventsMspFwdOld {
    fn make_read_events_fut(
        &self,
        ts_msp: TsMs,
        outbuf2: &mut VecDeque<Sitemty2<ChannelEvents, Error>>,
    ) -> Pin<Box<dyn Future<Output = Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>> + Send>> {
        let selfname = "EventsMspFwd::make_read_events_fut";
        let scyqueue = self.scyqueue.clone();
        let rt = self.rt.clone();
        let series = self.series_info.id();
        let scalar_type = self.series_info.scalar_type();
        let shape = self.series_info.shape();
        let range = self.range.clone();
        let with_values = self.readopts.with_values();
        {
            let msg = format!("{selfname}  msp {ts_msp}");
            let item = items_0::streamitem::LogItem::info(msg);
            outbuf2.push_back(Ok(StreamItem::Log(item)));
        }
        let params = ReadEvents03FwdParams {
            series,
            rt,
            scalar_type,
            shape,
            ts_msp,
            range,
            with_values,
            scylla_opts: self.readopts.scylla_opts.clone(),
        };
        let fut = async move { scyqueue.read_events_03_fwd(params).await.map_err(From::from) };
        Box::pin(fut)
    }
}

impl Stream for EventsMspFwdOld {
    type Item = Sitemty2<ChannelEvents, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let item: Box<dyn BinningggContainerEventsDyn + 'static> = netpod::todoval();
        let item = ChannelEvents::Events(item);
        let item = RangeCompletableItem::Data(item);
        let item = StreamItem::DataItem(item);
        Ready(Some(Ok(item)))
    }
}
