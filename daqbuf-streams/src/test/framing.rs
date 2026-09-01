use crate::frames::frameable_stream_to_bytes_stream;
use crate::tcprawclient::container_stream_from_bytes_stream;
use futures_util::TryStreamExt;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::ByteSize;
use netpod::TsNano;

async fn framing_00_inner() -> Result<(), Box<dyn std::error::Error>> {
    let mut evs = ContainerEvents::<f32>::new();
    evs.push_back(TsNano::from_ns(1), 1.2);
    let cevs = ChannelEvents::from(evs);
    let item: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(cevs)));
    let stream = futures_util::stream::iter([item]);
    let stream = frameable_stream_to_bytes_stream(stream);
    let stream = stream.map_err(sitem_err2_from_string);
    let stream = stream.inspect_ok(|x| {
        if false {
            let a = &x[0..x.len().min(40)];
            eprintln!("byte blob in stream  {}  {:?}", x.len(), a);
        }
    });
    let stream = Box::pin(stream);
    let bufcap = ByteSize(1024 * 1024);
    let mut stream = container_stream_from_bytes_stream::<ChannelEvents>(stream, bufcap, "test".into())?;
    let mut n = 0;
    while let Some(x) = stream.try_next().await? {
        if false {
            eprintln!("{x:?}");
        }
        n += 1;
    }
    assert_eq!(n, 1);
    Ok(())
}

#[test]
fn framing_00() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(framing_00_inner())
        .unwrap()
}
