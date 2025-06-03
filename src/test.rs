mod collect;
mod events;
mod events_reader;
mod framing;
mod timebin;

use futures_util::stream;
use futures_util::Stream;
use items_0::streamitem::sitem_data;
use items_0::streamitem::Sitemty;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::timeunits::SEC;
use netpod::TsNano;
use std::pin::Pin;

autoerr::create_error_v1!(
    name(Error, "StreamsTest"),
    enum variants {
        Logic,
    },
);

type BoxedEventStream = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;

// TODO use some xorshift generator.

fn inmem_test_events_d0_i32_00() -> BoxedEventStream {
    let mut evs = ContainerEvents::new();
    evs.push_back(TsNano::from_ns(SEC * 1), 10001);
    evs.push_back(TsNano::from_ns(SEC * 4), 10004);
    let cev = ChannelEvents::Events(Box::new(evs));
    let item = sitem_data(cev);
    let stream = stream::iter([item]);
    Box::pin(stream)
}

fn inmem_test_events_d0_i32_01() -> BoxedEventStream {
    let mut evs = ContainerEvents::new();
    evs.push_back(TsNano::from_ns(SEC * 2), 10002);
    let cev = ChannelEvents::Events(Box::new(evs));
    let item = sitem_data(cev);
    let stream = stream::iter([item]);
    Box::pin(stream)
}

#[test]
fn merge_mergeable_00() -> Result<(), Error> {
    let fut = async {
        let inp0 = inmem_test_events_d0_i32_00();
        let inp1 = inmem_test_events_d0_i32_01();
        let _merger = items_2::merger::Merger::new(vec![inp0, inp1], Some(4));
        Ok(())
    };
    runfut(fut)
}

fn runfut<F, T, E>(fut: F) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error,
{
    // taskrun::run(fut)
    let _ = fut;
    todo!()
}
