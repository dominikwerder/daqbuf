pub use http_body_util;
pub use http_body_util::Full;
pub use hyper_util;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use daqbuf_err as err;
use futures_util::Stream;
use futures_util::StreamExt;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::combinators::BoxBody;
use hyper::body::Body;
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use netpod::APP_JSON;
use netpod::ReqCtx;
use netpod::X_DAQBUF_REQID;
use netpod::log::*;
use serde::Serialize;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::net::TcpStream;
use url::Url;

#[derive(Debug)]
pub enum Error {
    NoHostInUrl,
    NoPortInUrl,
    Connection,
    IO(std::io::Error),
    Http,
    Body(Box<dyn std::error::Error + Send>),
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<http::Error> for Error {
    fn from(_: http::Error) -> Self {
        Self::Http
    }
}

impl From<hyper::Error> for Error {
    fn from(_: hyper::Error) -> Self {
        Self::Http
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl err::ToErr for Error {
    fn to_err(self) -> err::Error {
        err::Error::with_msg_no_trace(format!("self"))
    }
}

pub type BodyBox = BoxBody<Bytes, BodyError>;
pub type RespBox = Response<BodyBox>;
pub type Requ = Request<Incoming>;
pub type RespFull = Response<Full<Bytes>>;

// TODO rename: too similar.
pub type StreamBody = http_body_util::StreamBody<Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, BodyError>> + Send>>>;
pub type StreamResponse = Response<StreamBody>;

fn _assert1() {
    let body: Full<Bytes> = todoval();
    let _: &dyn Body<Data = _, Error = _> = &body;
}

fn _assert2() {
    let stream: Pin<Box<dyn futures_util::Stream<Item = Result<Frame<Bytes>, BodyError>>>> = todoval();
    let body = http_body_util::StreamBody::new(stream);
    let _: &dyn Body<Data = _, Error = _> = &body;
}

#[allow(unused)]
fn todoval<T>() -> T {
    todo!()
}

pub fn body_empty() -> StreamBody {
    // Full::new(Bytes::new()).map_err(Into::into).boxed()
    let fr = Frame::data(Bytes::new());
    let stream = futures_util::stream::iter([Ok(fr)]);
    http_body_util::StreamBody::new(Box::pin(stream))
}

pub fn body_string<S: ToString>(body: S) -> StreamBody {
    // Full::new(Bytes::from(body.to_string())).map_err(Into::into).boxed()
    let fr = Frame::data(Bytes::from(body.to_string()));
    let stream = futures_util::stream::iter([Ok(fr)]);
    http_body_util::StreamBody::new(Box::pin(stream))
}

pub fn body_bytes<D: Into<Bytes>>(body: D) -> StreamBody {
    let fr = Frame::data(body.into());
    let stream = futures_util::stream::iter([Ok(fr)]);
    http_body_util::StreamBody::new(Box::pin(stream))
}

pub fn internal_error() -> http::Response<StreamBody> {
    let mut res = http::Response::new(body_empty());
    *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    res
}

pub fn bad_request_response(msg: String, reqid: impl AsRef<str>) -> http::Response<StreamBody> {
    error_status_response(StatusCode::BAD_REQUEST, msg, reqid)
}

pub fn error_response(msg: String, reqid: impl AsRef<str>) -> http::Response<StreamBody> {
    error_status_response(StatusCode::INTERNAL_SERVER_ERROR, msg, reqid)
}

pub fn not_found_response(msg: String, reqid: impl AsRef<str>) -> http::Response<StreamBody> {
    error_status_response(StatusCode::NOT_FOUND, msg, reqid)
}

pub fn error_status_response<M: AsRef<str>>(
    status: StatusCode,
    msg: M,
    reqid: impl AsRef<str>,
) -> http::Response<StreamBody> {
    let js = serde_json::json!({
        "message": msg.as_ref(),
        "requestid": reqid.as_ref(),
    });
    if let Ok(body) = serde_json::to_string_pretty(&js) {
        match Response::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, APP_JSON)
            .body(body_string(body))
        {
            Ok(res) => res,
            Err(e) => {
                error!("can not generate http error response {e}");
                internal_error()
            }
        }
    } else {
        internal_error()
    }
}

pub trait IntoBody {
    fn into_body(self) -> StreamBody;
}

pub struct StringBody {
    body: String,
}

impl<S: ToString> From<S> for StringBody {
    fn from(value: S) -> Self {
        Self {
            body: value.to_string(),
        }
    }
}

impl IntoBody for StringBody {
    fn into_body(self) -> StreamBody {
        let fr = Frame::data(Bytes::from(self.body.as_bytes().to_vec()));
        let stream = futures_util::stream::iter([Ok(fr)]);
        http_body_util::StreamBody::new(Box::pin(stream))
    }
}

pub struct ToJsonBody {
    body: Vec<u8>,
}

impl From<Vec<u8>> for ToJsonBody {
    fn from(value: Vec<u8>) -> Self {
        Self { body: value }
    }
}

impl From<String> for ToJsonBody {
    fn from(value: String) -> Self {
        Self {
            body: value.into_bytes(),
        }
    }
}

impl<S: Serialize> From<&S> for ToJsonBody {
    fn from(value: &S) -> Self {
        Self {
            body: serde_json::to_vec(value).unwrap_or(Vec::new()),
        }
    }
}

impl IntoBody for ToJsonBody {
    fn into_body(self) -> StreamBody {
        let fr = Frame::data(Bytes::from(self.body));
        let stream = futures_util::stream::iter([Ok(fr)]);
        http_body_util::StreamBody::new(Box::pin(stream))
    }
}

pub fn body_stream<S, I, E>(stream: S) -> StreamBody
where
    S: Stream<Item = Result<I, E>> + Send + 'static,
    I: Into<Bytes> + Send,
    E: fmt::Display + Send,
{
    let stream = stream
        .inspect(|x| {
            if let Err(e) = x {
                error!("observe error in body stream: {e}");
            }
        })
        .take_while(|x| futures_util::future::ready(x.is_ok()))
        .map(|x| match x {
            Ok(x) => Ok(Frame::data(x.into())),
            Err(_e) => Err(BodyError::Bad),
        });
    StreamBody::new(Box::pin(stream))
}

pub struct StreamIncoming {
    inp: http_body_util::BodyStream<Incoming>,
}

impl StreamIncoming {
    pub fn new(inp: Incoming) -> Self {
        Self {
            inp: http_body_util::BodyStream::new(inp),
        }
    }
}

impl Stream for StreamIncoming {
    type Item = Result<Bytes, BodyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(x))) => {
                if x.is_data() {
                    Ready(Some(Ok(x.into_data().unwrap())))
                } else {
                    warn!("non-data in stream: {x:?}");
                    Ready(Some(Ok(Bytes::new())))
                }
            }
            Ready(Some(Err(e))) => {
                error!("{e}");
                Ready(Some(Err(BodyError::Bad)))
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

#[derive(Debug)]
pub enum BodyError {
    Bad,
}

impl fmt::Display for BodyError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("Bad")
    }
}

impl std::error::Error for BodyError {}

impl From<std::convert::Infallible> for BodyError {
    fn from(_value: std::convert::Infallible) -> Self {
        BodyError::Bad
    }
}

pub struct HttpResponse {
    pub head: http::response::Parts,
    pub body: Bytes,
}

pub async fn http_get(url: Url, accept: &str, ctx: &ReqCtx) -> Result<HttpResponse, Error> {
    debug!("http_get  {:?}  {:?}  {:?}", url, accept, ctx);
    let req = Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| Error::NoHostInUrl)?)
        .header(header::ACCEPT, accept)
        .header(X_DAQBUF_REQID, ctx.reqid())
        .body(body_empty())?;
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    let (head, mut body) = res.into_parts();
    debug!("http_get  head {head:?}");
    let mut buf = BytesMut::new();
    while let Some(x) = body.frame().await {
        match x {
            Ok(mut x) => {
                if let Some(x) = x.data_mut() {
                    buf.put(x);
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    let ret = HttpResponse {
        head,
        body: buf.freeze(),
    };
    Ok(ret)
}

pub async fn http_post(url: Url, accept: &str, body: String, ctx: &ReqCtx) -> Result<Bytes, Error> {
    let req = Request::builder()
        .method(http::Method::POST)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| Error::NoHostInUrl)?)
        .header(header::CONTENT_TYPE, APP_JSON)
        .header(header::ACCEPT, accept)
        .header(X_DAQBUF_REQID, ctx.reqid())
        .body(body_string(body))?;
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    if res.status() != StatusCode::OK {
        error!("Server error  {:?}", res);
        let (_head, body) = res.into_parts();
        let buf = read_body_bytes(body).await?;
        let s = String::from_utf8_lossy(&buf);
        error!("{s}");
        // TODO return error
        return Err(Error::Http);
    }
    let (head, mut body) = res.into_parts();
    debug!("http_get  head {head:?}");
    let mut buf = BytesMut::new();
    while let Some(x) = body.frame().await {
        match x {
            Ok(mut x) => {
                if let Some(x) = x.data_mut() {
                    buf.put(x);
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    let buf = read_body_bytes(body).await?;
    Ok(buf)
}

pub async fn connect_client<B>(uri: &http::Uri) -> Result<SendRequest<B>, Error>
where
    B: Body + Send + 'static,
    <B as Body>::Data: Send,
    <B as Body>::Error: Into<Box<(dyn std::error::Error + Send + Sync + 'static)>>,
{
    let scheme = uri.scheme_str().unwrap_or("http");
    let host = uri.host().ok_or_else(|| Error::NoHostInUrl)?;
    let port = uri.port_u16().unwrap_or_else(|| {
        // NOTE known issue: url::Url will "forget" the port if it was the default port.
        if scheme == "https" { 443 } else { 80 }
    });
    let stream = TcpStream::connect(format!("{host}:{port}")).await?;
    if false {
        let executor = hyper_util::rt::TokioExecutor::new();
        hyper::client::conn::http2::Builder::new(executor);
    }
    let (send_req, conn) = hyper::client::conn::http1::Builder::new()
        .handshake(hyper_util::rt::TokioIo::new(stream))
        .await?;
    // TODO would need to take greater care of this task to catch connection-level errors.
    tokio::spawn(conn);
    Ok(send_req)
}

pub async fn read_body_bytes(mut body: hyper::body::Incoming) -> Result<Bytes, Error> {
    let mut buf = BytesMut::new();
    while let Some(x) = body.frame().await {
        let mut frame = x?;
        if let Some(x) = frame.data_mut() {
            buf.put(x);
        }
    }
    Ok(buf.freeze())
}

pub struct IncomingStream {
    inp: hyper::body::Incoming,
}

impl IncomingStream {
    pub fn new(inp: hyper::body::Incoming) -> Self {
        Self { inp }
    }
}

impl futures_util::Stream for IncomingStream {
    type Item = Result<Bytes, err::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let j = &mut self.get_mut().inp;
        let k = Pin::new(j);
        match hyper::body::Body::poll_frame(k, cx) {
            Ready(Some(x)) => match x {
                Ok(x) => {
                    if let Ok(x) = x.into_data() {
                        Ready(Some(Ok(x)))
                    } else {
                        Ready(Some(Ok(Bytes::new())))
                    }
                }
                Err(e) => Ready(Some(Err(err::Error::from_string(e)))),
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
