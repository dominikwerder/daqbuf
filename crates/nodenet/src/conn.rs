use crate::scylla::scylla_channel_event_stream;
use bytes::Bytes;
use daqbuf_err as err;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::EVENT_QUERY_JSON_STRING_FRAME;
use items_2::channelevents::ChannelEvents;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use items_2::frame::decode_frame;
use items_2::frame::make_term_frame;
use items_2::inmem::InMemoryFrame;
use netpod::histo::HistoLog2;
use netpod::log;
use netpod::NodeConfigCached;
use netpod::ReqCtxArc;
use query::api4::events::EventsSubQuery;
use query::api4::events::Frame1Parts;
use scyllaconn::worker::ScyllaQueue;
use std::net::SocketAddr;
use std::pin::Pin;
use streamio::tcpreadasbytes::TcpReadAsBytes;
use streams::frames::frameable_stream_to_bytes_stream;
use streams::frames::inmem::BoxedBytesStream;
use streams::frames::inmem::InMemoryFrameStream;
use streams::tcprawclient::TEST_BACKEND;
use taskrun::tokio;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tracing::Instrument;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }
macro_rules! log_query { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }

#[cfg(test)]
mod test;

autoerr::create_error_v1!(
    name(Error, "NodenetConn"),
    enum variants {
        BadQuery,
        Scylla(#[from] crate::scylla::Error),
        Error(#[from] err::Error),
        Io(#[from] std::io::Error),
        Items(#[from] items_2::Error),
        NotAvailable,
        DebugTest,
        Generator(#[from] streams::generators::Error),
        Framable(#[from] items_2::framable::Error),
        Frame(#[from] items_2::frame::Error),
        InMem(#[from] streams::frames::inmem::Error),
        FramedStream(#[from] streams::frames::Error),
        Netpod(#[from] netpod::Error),
    },
);

pub async fn events_service(ncc: NodeConfigCached) -> Result<(), Error> {
    let scyqueue = err::todoval();
    let addr = format!("{}:{}", ncc.node.listen(), ncc.node.port_raw);
    let lis = tokio::net::TcpListener::bind(addr).await?;
    loop {
        match lis.accept().await {
            Ok((stream, addr)) => {
                taskrun::spawn(events_conn_handler(stream, addr, scyqueue, ncc.clone()));
            }
            Err(e) => Err(e)?,
        }
    }
}

struct ConnErr {
    err: Error,
    #[allow(dead_code)]
    netout: OwnedWriteHalf,
}

impl<E: Into<Error>> From<(E, OwnedWriteHalf)> for ConnErr {
    fn from((err, netout): (E, OwnedWriteHalf)) -> Self {
        Self {
            err: err.into(),
            netout,
        }
    }
}

async fn make_channel_events_stream_data(
    subq: EventsSubQuery,
    reqctx: ReqCtxArc,
    scyqueue: Option<&ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>, Error> {
    let scylla_opts = subq.scylla_opts().clone();
    log_query!("make_channel_events_stream_data  scylla_opts  {:?}", scylla_opts);
    if subq.backend() == TEST_BACKEND {
        let node_count = ncc.node_config.cluster.nodes.len() as u64;
        let node_ix = ncc.ix as u64;
        let ret = streams::generators::make_test_channel_events_stream_data(subq, node_count, node_ix)?;
        Ok(ret)
    } else if let Some(scyqueue) = scyqueue {
        let cfg = subq.ch_conf().to_scylla()?;
        let ret = scylla_channel_event_stream(subq, cfg, scyqueue, scylla_opts).await?;
        Ok(ret)
    } else if let Some(_) = &ncc.node.channel_archiver {
        let e = Error::NotAvailable;
        Err(e)
    } else if let Some(_) = &ncc.node.archiver_appliance {
        let e = Error::NotAvailable;
        Err(e)
    } else {
        let cfg = subq.ch_conf().to_sf_databuffer()?;
        Ok(disk::raw::conn::make_event_pipe(subq, cfg, reqctx, ncc).await?)
    }
}

pub async fn create_response_bytes_stream(
    evq: EventsSubQuery,
    scyqueue: Option<&ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<BoxedBytesStream, Error> {
    debug!(
        "create_response_bytes_stream  {:?}  wasm1 {:?}",
        evq.ch_conf(),
        evq.wasm1()
    );
    let reqctx = netpod::ReqCtx::new_from_single_reqid(evq.reqid().into()).into();
    if evq.create_errors_contains("nodenet_parse_query") {
        let e = Error::DebugTest;
        return Err(e);
    }
    if evq.is_event_blobs() {
        // This is only relevant for "api-1" queries in sf-data/imagebuffer based backends.
        // TODO support event blobs as transform
        let fetch_info = evq.ch_conf().to_sf_databuffer()?;
        let stream = disk::raw::conn::make_event_blobs_pipe(&evq, &fetch_info, reqctx, ncc)?;
        let stream = stream.map(|x| x.make_frame_dyn().map(|x| x.freeze()).map_err(sitem_err2_from_string));
        let ret = Box::pin(stream) as BoxedBytesStream;
        Ok(ret)
    } else {
        let stream = make_channel_events_stream_data(evq, reqctx, scyqueue, ncc).await?;
        let stream = frameable_stream_to_bytes_stream(stream).map_err(sitem_err2_from_string);
        let ret = Box::pin(stream) as BoxedBytesStream;
        Ok(ret)
    }
}

async fn events_conn_handler_with_reqid(
    mut netout: OwnedWriteHalf,
    evq: EventsSubQuery,
    scyqueue: Option<&ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<(), ConnErr> {
    let mut stream = match create_response_bytes_stream(evq, scyqueue, ncc).await {
        Ok(x) => x,
        Err(e) => return Err((e, netout))?,
    };
    let mut buf_len_histo = HistoLog2::new(5);
    while let Some(item) = stream.next().await {
        match item {
            Ok(buf) => {
                buf_len_histo.ingest(buf.len() as u32);
                match netout.write_all(&buf).await {
                    Ok(()) => {
                        // TODO collect timing information and send as summary in a stats item.
                        // TODO especially collect a distribution over the buf lengths that were send.
                        // TODO we want to see a reasonable batch size.
                    }
                    Err(e) => return Err((e, netout))?,
                }
            }
            Err(e) => {
                error!("events_conn_handler_inner_try sees error in stream: {e:?}");
                return Err((e, netout))?;
            }
        }
    }
    {
        let item = LogItem::level_msg(log::Level::DEBUG, format!("buf_len_histo: {:?}", buf_len_histo));
        let item: Sitemty<ChannelEvents> = Ok(StreamItem::Log(item));
        let buf = match item.make_frame_dyn() {
            Ok(k) => k,
            Err(e) => return Err((e, netout))?,
        };
        match netout.write_all(&buf).await {
            Ok(()) => (),
            Err(e) => return Err((e, netout))?,
        }
    }
    let buf = match make_term_frame() {
        Ok(k) => k,
        Err(e) => return Err((e, netout))?,
    };
    match netout.write_all(&buf).await {
        Ok(()) => (),
        Err(e) => return Err((e, netout))?,
    }
    match netout.flush().await {
        Ok(()) => (),
        Err(e) => return Err((e, netout))?,
    }
    Ok(())
}

pub async fn events_get_input_frames<INP>(netin: INP) -> Result<Vec<InMemoryFrame>, Error>
where
    INP: Stream<Item = Result<Bytes, err::Error>> + Unpin,
{
    let mut h = InMemoryFrameStream::new(netin, netpod::ByteSize::from_kb(8));
    let mut frames = Vec::new();
    while let Some(k) = h
        .next()
        .instrument(log::span!(log::Level::INFO, "events_conn_handler/query-input"))
        .await
    {
        match k {
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => {
                frames.push(item);
            }
            Ok(item) => {
                debug!("ignored incoming frame {:?}", item);
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    Ok(frames)
}

pub fn events_parse_input_query(frames: Vec<InMemoryFrame>) -> Result<(EventsSubQuery,), Error> {
    if frames.len() != 1 {
        error!("{:?}", frames);
        error!("missing command frame  len {}", frames.len());
        let e = Error::BadQuery;
        return Err(e);
    }
    let query_frame = &frames[0];
    if query_frame.tyid() != EVENT_QUERY_JSON_STRING_FRAME {
        return Err(Error::BadQuery);
    }
    // TODO this does not need all variants of Sitemty.
    let qitem = match decode_frame::<Sitemty<EventQueryJsonStringFrame>>(query_frame) {
        Ok(k) => match k {
            Ok(k) => match k {
                StreamItem::DataItem(k) => match k {
                    RangeCompletableItem::Data(k) => k,
                    RangeCompletableItem::RangeComplete => return Err(Error::BadQuery),
                },
                _ => return Err(Error::BadQuery),
            },
            Err(e) => return Err(e.into()),
        },
        Err(e) => return Err(e.into()),
    };
    trace!("parsing json {:?}", qitem.str());
    let frame1: Frame1Parts = serde_json::from_str(&qitem.str()).map_err(|_e| {
        let e = Error::BadQuery;
        error!("{e}");
        error!("input was {}", qitem.str());
        e
    })?;
    Ok(frame1.parts())
}

async fn events_conn_handler_inner_try<INP>(
    netin: INP,
    netout: OwnedWriteHalf,
    addr: SocketAddr,
    scyqueue: Option<&ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<(), ConnErr>
where
    INP: Stream<Item = Result<Bytes, err::Error>> + Unpin,
{
    let _ = addr;
    let frames = match events_get_input_frames(netin).await {
        Ok(x) => x,
        Err(e) => return Err((e, netout).into()),
    };
    let (evq,) = match events_parse_input_query(frames) {
        Ok(x) => x,
        Err(e) => return Err((e, netout).into()),
    };
    debug!("events_conn_handler sees:  {evq:?}");
    let reqid = evq.reqid();
    let span = tracing::info_span!("subreq", reqid = reqid);
    events_conn_handler_with_reqid(netout, evq, scyqueue, ncc)
        .instrument(span)
        .await
}

async fn events_conn_handler_inner<INP>(
    netin: INP,
    netout: OwnedWriteHalf,
    addr: SocketAddr,
    scyqueue: Option<&ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<(), Error>
where
    INP: Stream<Item = Result<Bytes, items_0::streamitem::SitemErrTy>> + Unpin,
{
    match events_conn_handler_inner_try(netin, netout, addr, scyqueue, ncc).await {
        Ok(_) => (),
        Err(ce) => {
            let mut out = ce.netout;
            let item: Sitemty<ChannelEvents> = Err(items_0::streamitem::SitemErrTy::from_string(ce.err));
            let buf = Framable::make_frame_dyn(&item)?;
            out.write_all(&buf).await?;
        }
    }
    Ok(())
}

async fn events_conn_handler(
    stream: TcpStream,
    addr: SocketAddr,
    scyqueue: Option<&ScyllaQueue>,
    ncc: NodeConfigCached,
) -> Result<(), Error> {
    let (netin, netout) = stream.into_split();
    let inp = TcpReadAsBytes::new(netin);
    let inp = inp.map_err(sitem_err2_from_string);
    let inp = Box::new(inp);
    let span1 = log::span!(log::Level::INFO, "events_conn_handler");
    let r = events_conn_handler_inner(inp, netout, addr, scyqueue, &ncc)
        .instrument(span1)
        .await;
    match r {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("events_conn_handler sees error: {:?}", e);
            Err(e.into())
        }
    }
}
