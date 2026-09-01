use crate::timebin::cached::reader::EventsReadProvider;
use crate::timebin::cached::reader::EventsReading;
use futures_util::Stream;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::Sitemty;
use items_2::channelevents::ChannelEvents;
use netpod::range::evrange::SeriesRange;
use query::api4::events::EventsSubQuery;
use std::pin::Pin;

fn make_stream(chname: &str, range: &SeriesRange) -> Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>> {
    let _ = &range;
    if chname == "unittest;scylla;cont;scalar;f32" {
        let e = sitem_err2_from_string(format!("unknown channel {chname}"));
        let ret = futures_util::stream::iter([Err(e)]);
        Box::pin(ret)
    } else {
        let e = sitem_err2_from_string(format!("unknown channel {chname}"));
        let ret = futures_util::stream::iter([Err(e)]);
        Box::pin(ret)
    }
}

pub struct UnitTestStream {}

impl UnitTestStream {
    pub fn new() -> Self {
        Self {}
    }
}

impl EventsReadProvider for UnitTestStream {
    fn read(&self, evq: EventsSubQuery) -> EventsReading {
        let stream = make_stream(evq.name(), evq.range());
        EventsReading::new(stream)
    }
}
