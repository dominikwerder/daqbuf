use super::cached::reader::EventsReadProvider;
use crate::events::convertforbinning::ConvertForBinning;
use crate::log;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinsBoxed;
use items_2::binning::timeweight::timeweight_events_dyn::BinnedEventsTimeweightStream;
use netpod::BinnedRange;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

macro_rules! trace_emit { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "ReadingBinnedFromEvents"),
    enum variants {
        ExpectTimerange,
        ExpectTimeweighted,
    },
);

pub struct BinnedFromEvents {
    stream: Pin<Box<dyn Stream<Item = Sitemty<BinsBoxed>> + Send>>,
}

impl BinnedFromEvents {
    pub fn new(
        range: BinnedRange<TsNano>,
        evq: EventsSubQuery,
        do_time_weight: bool,
        read_provider: Arc<dyn EventsReadProvider>,
    ) -> Result<Self, Error> {
        trace_emit!("new");
        if !evq.range().is_time() {
            return Err(Error::ExpectTimerange);
        }
        let stream = read_provider.read(evq);
        let stream = ConvertForBinning::new(stream);
        let stream = if do_time_weight {
            let stream = Box::pin(stream);
            BinnedEventsTimeweightStream::new(range, stream)
        } else {
            return Err(Error::ExpectTimeweighted);
        };
        let ret = Self {
            stream: Box::pin(stream),
        };
        Ok(ret)
    }
}

impl Stream for BinnedFromEvents {
    type Item = Sitemty<BinsBoxed>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}
