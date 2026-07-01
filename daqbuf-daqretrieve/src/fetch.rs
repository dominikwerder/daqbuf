use futures_util::StreamExt;
use futures_util::future;
use http::Method;
use http::header;
use httpclient::IncomingStream;
use httpclient::body_empty;
use httpclient::connect_client;
use httpclient::http;
use httpclient::http::StatusCode;
use httpclient::hyper::Request;
use netpod::APP_CBOR_FRAMED;
use netpod::ScalarType;
use netpod::Shape;
use netpod::log::*;
use streams::cbor_stream::FramedBytesToChannelEventsStream;
use url::Url;

autoerr::create_error_v1!(
    name(Error, "DataFetch"),
    enum variants {
        Url(#[from] url::ParseError),
        NoHostname,
        HttpBody(#[from] http::Error),
        HttpClient(#[from] httpclient::Error),
        Hyper(#[from] httpclient::hyper::Error),
        RequestFailed(String),
    },
);

pub async fn fetch_cbor(url: &str, scalar_type: ScalarType, shape: Shape) -> Result<(), Error> {
    let url: Url = url.parse()?;
    debug!("parsed url: {url:?}");
    let req = Request::builder()
        .method(Method::GET)
        .uri(url.to_string())
        .header(header::HOST, url.host_str().ok_or_else(|| Error::NoHostname)?)
        .header(header::ACCEPT, APP_CBOR_FRAMED)
        .body(body_empty())?;
    debug!("open connection to {:?}", req.uri());
    let mut send_req = connect_client(req.uri()).await?;
    let res = send_req.send_request(req).await?;
    let (head, body) = res.into_parts();
    if head.status != StatusCode::OK {
        let buf = httpclient::read_body_bytes(body).await?;
        let s = String::from_utf8_lossy(&buf);
        let e = Error::RequestFailed(format!("request failed {:?}  {}", head, s));
        return Err(e);
    }
    debug!("fetch_cbor  head {head:?}");
    let stream = IncomingStream::new(body);
    let stream = FramedBytesToChannelEventsStream::new(stream, scalar_type, shape);
    let stream = stream
        .map(|item| {
            info!("{item:?}");
            item
        })
        .take_while({
            let mut b = true;
            move |item| {
                let ret = b;
                b = b && item.is_ok();
                future::ready(ret)
            }
        });
    stream.for_each(|_| future::ready(())).await;
    Ok(())
}
