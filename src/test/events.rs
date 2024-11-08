use crate::cbor_stream::FramedBytesToSitemtyDynEventsStream;
use crate::firsterr::only_first_err;
use crate::frames::inmem::BoxedBytesStream;
use crate::lenframed;
use crate::plaineventscbor::plain_events_cbor_stream;
use crate::tcprawclient::OpenBoxedBytesStreams;
use crate::tcprawclient::TEST_BACKEND;
use futures_util::future;
use futures_util::Future;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::ChConf;
use netpod::ReqCtx;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::SfDbChannel;
use netpod::Shape;
use query::api4::events::EventsSubQuery;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "TestEvents")]
pub enum Error {
    InMem(#[from] crate::frames::inmem::Error),
    Generator(#[from] crate::generators::Error),
}

#[test]
fn merged_events_cbor() {
    crate::test::runfut(merged_events_inner()).unwrap();
}

async fn merged_events_inner() -> Result<(), Error> {
    let ctx = ReqCtx::for_test();
    // TODO factor out the channel config lookup such that the test code can use a similar code path,
    // except that we don't want to go over the network here.
    let ch_conf = ChConf::new(
        TEST_BACKEND,
        1,
        SeriesKind::ChannelData,
        ScalarType::I32,
        Shape::Scalar,
        "test-gen-i32-dim0-v00",
    );
    let channel = SfDbChannel::from_name(ch_conf.backend(), ch_conf.name());
    let range = SeriesRange::TimeRange(NanoRange::from_date_time(
        "2023-12-18T05:10:00Z".parse().unwrap(),
        "2023-12-18T05:12:00Z".parse().unwrap(),
    ));
    let evq = PlainEventsQuery::new(channel, range);
    let open_bytes = StreamOpener::new();
    let open_bytes = Arc::pin(open_bytes);
    let stream = plain_events_cbor_stream(&evq, ch_conf.clone().into(), &ctx, open_bytes, todo!())
        .await
        .unwrap();
    let stream = lenframed::length_framed(stream);
    let stream =
        FramedBytesToSitemtyDynEventsStream::new(stream, ch_conf.scalar_type().clone(), ch_conf.shape().clone());
    let stream = only_first_err(stream);
    stream
        .for_each(|item| {
            debug!("{item:?}");
            future::ready(())
        })
        .await;
    Ok(())
}

struct StreamOpener {}

impl StreamOpener {
    fn new() -> Self {
        Self {}
    }
}

impl OpenBoxedBytesStreams for StreamOpener {
    fn open(
        &self,
        subq: EventsSubQuery,
        _ctx: ReqCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BoxedBytesStream>, crate::tcprawclient::Error>> + Send>> {
        Box::pin(stream_opener(subq).map_err(|e| crate::tcprawclient::Error::Msg(format!("{e}"))))
    }
}

async fn stream_opener(subq: EventsSubQuery) -> Result<Vec<BoxedBytesStream>, Error> {
    let mut streams = Vec::new();
    let stream = crate::generators::make_test_channel_events_bytes_stream(subq, 1, 0)?;
    streams.push(stream);
    Ok(streams)
}
