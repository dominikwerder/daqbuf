use crate::timebin::cached::reader::EventsReadProvider;
use crate::timebin::cached::reader::EventsReading;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::range::evrange::NanoRange;
use query::api4::events::EventsSubQuery;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "TestEventsReader")]
pub enum Error {}

pub struct TestEventsReader {
    range: NanoRange,
}

impl TestEventsReader {
    pub fn new(range: NanoRange) -> Self {
        Self { range }
    }
}

impl EventsReadProvider for TestEventsReader {
    fn read(&self, evq: EventsSubQuery) -> EventsReading {
        let stream = items_2::testgen::events_gen::old_events_gen_dim0_f32_v00(self.range.clone());
        let stream = stream
            .map(|x| {
                let x = Box::new(x);
                let x = ChannelEvents::Events(x);
                let x: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)));
                x
            })
            .chain({
                use RangeCompletableItem::*;
                use StreamItem::*;
                let item1 = Ok(DataItem(RangeComplete));
                futures_util::stream::iter([item1])
            });
        let stream = Box::pin(stream);
        let ret = EventsReading::new(stream);
        ret
    }
}
