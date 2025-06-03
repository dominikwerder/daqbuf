use crate::tcpreadasbytes::TcpReadAsBytes;
use futures_util::Stream;
use futures_util::TryStreamExt;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::sitem_data;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::Sitemty;
use items_2::eventfull::EventFull;
use items_2::framable::Framable;
use items_2::frame::make_term_frame;
use netpod::log::*;
use netpod::Cluster;
use netpod::Node;
use netpod::ReqCtx;
use query::api4::events::EventsSubQuery;
use serde::de::DeserializeOwned;
use std::fmt;
use std::pin::Pin;
use streams::frames::eventsfromframes::EventsFromFrames;
use streams::frames::inmem::BoxedBytesStream;
use streams::frames::inmem::InMemoryFrameStream;
use streams::tcprawclient::make_node_command_frame;
use streams::tcprawclient::x_processed_event_blobs_stream_from_node_http;
use streams::tcprawclient::HttpSimplePost;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

autoerr::create_error_v1!(
    name(Error, "TcpRawClient"),
    enum variants {
        IO(#[from] std::io::Error),
        Frame(#[from] items_2::frame::Error),
        Framable(#[from] items_2::framable::Error),
        StreamsRawClient(#[from] streams::tcprawclient::Error),
    },
);

pub type BoxedStream<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub async fn x_processed_event_blobs_stream_from_node_tcp(
    subq: EventsSubQuery,
    node: Node,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    let addr = format!("{}:{}", node.host, node.port_raw);
    debug!("x_processed_event_blobs_stream_from_node  to: {addr}");
    let frame1 = make_node_command_frame(subq.clone())?;
    let net = TcpStream::connect(addr.clone()).await?;
    let (netin, mut netout) = net.into_split();
    let item = sitem_data(frame1);
    let buf = item.make_frame_dyn()?;
    netout.write_all(&buf).await?;
    let buf = make_term_frame()?;
    netout.write_all(&buf).await?;
    netout.flush().await?;
    netout.forget();
    let inp = TcpReadAsBytes::new(netin).map_err(sitem_err2_from_string);
    let inp = Box::pin(inp) as BoxedBytesStream;
    let frames = InMemoryFrameStream::new(inp, subq.inmem_bufcap());
    let frames = frames.map_err(sitem_err2_from_string);
    let frames = Box::pin(frames);
    let items = EventsFromFrames::new(frames, addr);
    Ok(Box::pin(items))
}

#[allow(unused)]
async fn open_event_data_streams_tcp<T>(subq: EventsSubQuery, cluster: &Cluster) -> Result<Vec<BoxedStream<T>>, Error>
where
    // TODO group bounds in new trait
    T: FrameTypeInnerStatic + DeserializeOwned + Send + Unpin + fmt::Debug + 'static,
{
    // TODO when unit tests established, change to async connect:
    let frame1 = make_node_command_frame(subq.clone())?;
    let mut streams = Vec::new();
    for node in &cluster.nodes {
        let addr = format!("{}:{}", node.host, node.port_raw);
        debug!("open_tcp_streams  to: {addr}");
        let net = TcpStream::connect(addr.clone()).await?;
        let (netin, mut netout) = net.into_split();
        let item = sitem_data(frame1.clone());
        let buf = item.make_frame_dyn()?;
        netout.write_all(&buf).await?;
        let buf = make_term_frame()?;
        netout.write_all(&buf).await?;
        netout.flush().await?;
        netout.forget();
        // TODO for images, we need larger buffer capacity
        let inp = TcpReadAsBytes::new(netin);
        let inp = inp.map_err(sitem_err2_from_string);
        let inp = Box::pin(inp) as BoxedBytesStream;
        let frames = InMemoryFrameStream::new(inp, subq.inmem_bufcap());
        let frames = frames.map_err(sitem_err2_from_string);
        let stream = EventsFromFrames::<T, _>::new(frames, addr);
        streams.push(Box::pin(stream) as _);
    }
    Ok(streams)
}

// Currently used only for the python data api3 protocol endpoint.
// TODO merge with main method.
pub async fn x_processed_event_blobs_stream_from_node(
    subq: EventsSubQuery,
    node: Node,
    post: Box<dyn HttpSimplePost>,
    ctx: ReqCtx,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    if true {
        Ok(x_processed_event_blobs_stream_from_node_http(subq, node, post, &ctx).await?)
    } else {
        x_processed_event_blobs_stream_from_node_tcp(subq, node).await
    }
}
