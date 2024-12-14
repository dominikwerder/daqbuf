use crate::timebin::cached::reader::EventsReadProvider;
use crate::timebin::cached::reader::EventsReading;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::binning::container_events::ContainerEvents;
use items_2::binning::container_events::EventValueType;
use items_2::channelevents::ChannelEvents;
use netpod::range::evrange::NanoRange;
use query::api4::events::EventsSubQuery;

pub struct TestEventsReaderTy<GEN> {
    range: NanoRange,
    gen: GEN,
}

impl<GEN> TestEventsReaderTy<GEN> {
    pub fn new(range: NanoRange, gen: GEN) -> Self {
        Self { range, gen }
    }
}

impl<GEN, IT, TY> EventsReadProvider for TestEventsReaderTy<GEN>
where
    GEN: Fn(NanoRange) -> IT + Send + Sync,
    IT: Iterator<Item = ContainerEvents<TY>> + Send + 'static,
    TY: EventValueType,
{
    fn read(&self, evq: EventsSubQuery) -> EventsReading {
        let iter = (self.gen)(self.range.clone());
        let iter = iter
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
                [item1].into_iter()
            });
        let stream = Box::pin(futures_util::stream::iter(iter));
        let ret = EventsReading::new(stream);
        ret
    }
}

pub struct TestEventsReadProvider {}

impl TestEventsReadProvider {
    pub fn new() -> Self {
        Self {}
    }
}

impl EventsReadProvider for TestEventsReadProvider {
    fn read(&self, evq: EventsSubQuery) -> EventsReading {
        let range: NanoRange = evq.range().try_into().unwrap();
        if evq.ch_conf().series() == Some(123) && evq.name() == "test-reader-dim0-f32-00" {
            let gen = TestEventsReaderTy::new(
                range,
                items_2::testgen::events_gen::new_events_gen_dim0_f32_v00,
            );
            gen.read(evq)
        } else {
            panic!()
        }
    }
}
