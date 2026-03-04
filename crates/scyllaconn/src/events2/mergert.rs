use super::events::EventsStreamRt;
use crate::events2::onebeforeandbulk::OneBeforeAndBulk;
use crate::range::ScyllaSeriesRange;
use crate::worker::EventReadOpts;
use crate::worker::ScyllaQueue;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::SitemErrTy;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::streamitem::sitem_err2_from_string;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use netpod::ChConf;
use netpod::log;
use netpod::ttl::RetentionTime;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace_init { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "EventsMergeRt"),
    enum variants {
        Msg(String),
    },
);

pub struct MergeRts {
    inp: Pin<Box<dyn Stream<Item = Sitemty2<ChannelEvents, SitemErrTy>> + Send>>,
}

impl MergeRts {
    pub fn new(ch_conf: ChConf, range: ScyllaSeriesRange, readopts: EventReadOpts, scyqueue: ScyllaQueue) -> Self {
        trace_init!("MergeRts  readopts {readopts:?}");
        let inp_st = EventsStreamRt::new(
            RetentionTime::Short,
            ch_conf.clone(),
            range.clone(),
            readopts.clone(),
            scyqueue.clone(),
        )
        .map(|x| {
            // use RangeCompletableItem::*;
            // use StreamItem::*;
            // match x {
            //     Ok(x) => Ok(DataItem(Data(x))),
            //     Err(e) => Err(daqbuf_err::Error::from_string(e)),
            // }
            x.map_err(|e| daqbuf_err::Error::from_string(e))
        });
        let inp_mt = EventsStreamRt::new(
            RetentionTime::Medium,
            ch_conf.clone(),
            range.clone(),
            readopts.clone(),
            scyqueue.clone(),
        )
        .map(|x| {
            // use RangeCompletableItem::*;
            // use StreamItem::*;
            // match x {
            //     Ok(x) => Ok(DataItem(Data(x))),
            //     Err(e) => Err(daqbuf_err::Error::from_string(e)),
            // }
            x.map_err(|e| daqbuf_err::Error::from_string(e))
        });
        let inp_lt = EventsStreamRt::new(
            RetentionTime::Long,
            ch_conf.clone(),
            range.clone(),
            readopts.clone(),
            scyqueue.clone(),
        )
        .map(|x| {
            // use RangeCompletableItem::*;
            // use StreamItem::*;
            // match x {
            //     Ok(x) => Ok(DataItem(Data(x))),
            //     Err(e) => Err(daqbuf_err::Error::from_string(e)),
            // }
            x.map_err(|e| daqbuf_err::Error::from_string(e))
        });
        let merger: Merger<ChannelEvents> =
            Merger::new(vec![Box::pin(inp_st), Box::pin(inp_mt), Box::pin(inp_lt)], None);
        let stream = merger;
        let stream = OneBeforeAndBulk::<_, ChannelEvents>::new(stream, range.beg(), "after-rt-merged".into());
        let stream = stream.map(|x| match x {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(x) => {
                        use crate::events2::onebeforeandbulk::Output;
                        match x {
                            Output::Before(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
                            Output::Bulk(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
                        }
                    }
                    RangeCompletableItem::RangeComplete => {
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                },
                StreamItem::Log(x) => Ok(StreamItem::Log(x)),
                StreamItem::Stats(x) => Ok(StreamItem::Stats(x)),
            },
            Err(e) => Err(sitem_err2_from_string(e)),
        });
        let inp = Box::pin(stream);
        Self { inp }
    }
}

impl Stream for MergeRts {
    type Item = Sitemty2<ChannelEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.inp.poll_next_unpin(cx) {
                Ready(Some(x)) => match x {
                    Ok(x) => Ready(Some(Ok(x))),
                    Err(e) => Ready(Some(Err(Error::Msg(e.to_string())))),
                },
                Ready(None) => Ready(None),
                Pending => Pending,
            };
        }
    }
}
