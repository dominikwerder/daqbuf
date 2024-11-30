use crate::frames::eventsfromframes::EventsFromFrames;
use crate::frames::inmem::BoxedBytesStream;
use crate::frames::inmem::InMemoryFrameStream;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::Future;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http::Uri;
use http_body_util::BodyExt;
use items_0::framable::FrameTypeInnerStatic;
use items_0::streamitem::sitem_data;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::Sitemty;
use items_0::WithLen;
use items_2::eventfull::EventFull;
use items_2::framable::EventQueryJsonStringFrame;
use items_2::framable::Framable;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::ByteSize;
use netpod::ChannelTypeConfigGen;
use netpod::Node;
use netpod::ReqCtx;
use netpod::APP_OCTET;
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use query::api4::events::Frame1Parts;
use query::transform::TransformQuery;
use serde::de::DeserializeOwned;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

pub const TEST_BACKEND: &str = "testbackend-00";

#[derive(Debug, thiserror::Error)]
#[cstm(name = "TcpRawClient")]
pub enum Error {
    IO(#[from] std::io::Error),
    Msg(String),
    Frame(#[from] items_2::frame::Error),
    Framable(#[from] items_2::framable::Error),
    Json(#[from] serde_json::Error),
    Http(#[from] http::Error),
    // HttpClient(#[from] httpclient::Error),
    // Hyper(#[from] httpclient::hyper::Error),
    #[error("ServerError({0:?}, {1})")]
    ServerError(http::response::Parts, String),
    HttpBody(Box<dyn std::error::Error + Send>),
}

struct ErrMsg<E>(E)
where
    E: ToString;

impl<E> From<ErrMsg<E>> for Error
where
    E: ToString,
{
    fn from(value: ErrMsg<E>) -> Self {
        Self::Msg(value.0.to_string())
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::Msg(value)
    }
}

pub trait OpenBoxedBytesStreams {
    fn open(
        &self,
        subq: EventsSubQuery,
        // TODO take by Arc
        ctx: ReqCtx,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BoxedBytesStream>, Error>> + Send>>;
}

pub type OpenBoxedBytesStreamsBox = Pin<Arc<dyn OpenBoxedBytesStreams + Send + Sync>>;

pub fn make_node_command_frame(query: EventsSubQuery) -> Result<EventQueryJsonStringFrame, Error> {
    let obj = Frame1Parts::new(query);
    let ret = serde_json::to_string(&obj)?;
    Ok(EventQueryJsonStringFrame(ret))
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorBody {
    #[error("{0}")]
    Msg(String),
}

pub trait HttpSimplePost: Send {
    fn http_simple_post(
        &self,
        req: http::Request<http_body_util::Full<Bytes>>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = http::Response<
                        http_body_util::combinators::UnsyncBoxBody<Bytes, ErrorBody>,
                    >,
                > + Send,
        >,
    >;
}

pub async fn read_body_bytes<B>(mut body: B) -> Result<Bytes, Error>
where
    B: http_body::Body + Unpin,
    <B as http_body::Body>::Error: std::error::Error + Send + 'static,
{
    use bytes::BufMut;
    use http_body_util::BodyExt;
    let mut buf = BytesMut::new();
    while let Some(x) = body.frame().await {
        let mut frame = x.map_err(|e| Error::HttpBody(Box::new(e)))?;
        if let Some(x) = frame.data_mut() {
            buf.put(x);
        }
    }
    Ok(buf.freeze())
}

pub async fn x_processed_event_blobs_stream_from_node_http(
    subq: EventsSubQuery,
    node: Node,
    post: Box<dyn HttpSimplePost>,
    ctx: &ReqCtx,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<EventFull>> + Send>>, Error> {
    use http::header;
    use http::Method;
    use http::Request;
    use http::StatusCode;
    let frame1 = make_node_command_frame(subq.clone())?;
    let item = sitem_data(frame1.clone());
    let buf = item.make_frame_dyn()?.freeze();
    let url = node
        .baseurl()
        .join("/api/4/private/eventdata/frames")
        .unwrap();
    debug!("open_event_data_streams_http  post  {url}");
    let uri: Uri = url.as_str().parse().unwrap();
    let body = http_body_util::Full::new(buf);
    let req = Request::builder()
        .method(Method::POST)
        .uri(&uri)
        .header(header::HOST, uri.host().unwrap())
        .header(header::ACCEPT, APP_OCTET)
        .header(ctx.header_name(), ctx.header_value())
        .body(body)?;
    let res = post.http_simple_post(req).await;
    if res.status() != StatusCode::OK {
        let (head, body) = res.into_parts();
        error!("server error  {:?}", head);
        let buf = read_body_bytes(body).await?;
        let s = String::from_utf8_lossy(&buf);
        return Err(Error::ServerError(head, s.to_string()));
    }
    let (_head, body) = res.into_parts();
    let inp = body;
    let inp = inp.into_data_stream();
    let inp = inp.map(|x| match x {
        Ok(x) => Ok(x),
        Err(e) => Err(sitem_err2_from_string(e)),
    });
    let inp = Box::pin(inp) as BoxedBytesStream;
    let frames = InMemoryFrameStream::new(inp, subq.inmem_bufcap());
    let frames = frames.map_err(sitem_err2_from_string);
    let frames = Box::pin(frames);
    let stream = EventsFromFrames::new(frames, url.to_string());
    debug!("open_event_data_streams_http  done  {url}");
    Ok(Box::pin(stream))
}

pub fn container_stream_from_bytes_stream<T>(
    inp: BoxedBytesStream,
    bufcap: ByteSize,
    dbgdesc: String,
) -> Result<impl Stream<Item = Sitemty<T>>, Error>
where
    T: FrameTypeInnerStatic + DeserializeOwned + Send + Unpin + fmt::Debug + 'static + WithLen,
{
    let frames = InMemoryFrameStream::new(inp, bufcap);
    let frames = frames.map_err(sitem_err2_from_string);
    let frames = frames.inspect(|x| {
        if true {
            trace!("container_stream_from_bytes_stream  see frame  {:?}", x);
        }
    });
    let stream = EventsFromFrames::<T, _>::new(frames, dbgdesc);
    let stream = stream.inspect(|x| {
        if true {
            use items_0::streamitem::RangeCompletableItem::*;
            use items_0::streamitem::StreamItem::*;
            match x {
                Ok(DataItem(Data(x))) => {
                    trace!("EventsFromFrames  yields item len {}", x.len());
                }
                _ => {
                    trace!("EventsFromFrames  yields item {:?}", x);
                }
            }
        }
    });
    Ok(stream)
}

pub fn make_sub_query<SUB>(
    ch_conf: ChannelTypeConfigGen,
    range: SeriesRange,
    one_before_range: bool,
    transform: TransformQuery,
    sub: SUB,
    log_level: String,
    ctx: &ReqCtx,
) -> EventsSubQuery
where
    SUB: Into<EventsSubQuerySettings>,
{
    let mut select = EventsSubQuerySelect::new(ch_conf, range, one_before_range, transform.clone());
    if let Some(wasm1) = transform.do_wasm() {
        select.set_wasm1(wasm1.into());
    }
    let settings = sub.into();
    let subq = EventsSubQuery::from_parts(select, settings, ctx.reqid().into(), log_level);
    subq
}
