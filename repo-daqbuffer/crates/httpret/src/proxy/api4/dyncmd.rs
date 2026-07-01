#![allow(unused_macros)]

use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use bytes::BytesMut;
use futures_util::TryStreamExt;
use http::header;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::connect_client;
use httpclient::Requ;
use httpclient::StreamIncoming;
use httpclient::StreamResponse;
use netpod::ProxyConfig;
use serde::Deserialize;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }

pub struct DynCmdHandler {}

impl DynCmdHandler {
    pub fn path() -> &'static str {
        "/api/4/private/dyncmd"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == Self::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        if req.method() != Method::POST {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        } else {
            let (req, body) = req.into_parts();
            let mut bs = http_body_util::BodyStream::new(body);
            let mut buf = BytesMut::with_capacity(1024 * 2);
            while let Some(fr) = bs.try_next().await? {
                if let Ok(b) = fr.into_data() {
                    buf.extend(b);
                }
            }
            #[derive(Deserialize)]
            struct DynCmd {
                backend: String,
            }
            let c: DynCmd = serde_json::from_slice(&buf)?;
            if let Some(backend) = proxy_config.backends.iter().filter(|x| x.name == c.backend).next() {
                req.uri
                    .path_and_query()
                    .ok_or_else(|| Error::with_msg_no_trace("uri contains no path"))?;
                let url_str = format!("{}{}", backend.url, Self::path());
                let uri: Uri = url_str.parse()?;
                let host = uri.host().ok_or_else(|| Error::with_msg_no_trace("no host in url"))?;
                let req = Request::builder()
                    .method(Method::POST)
                    .header(header::HOST, host)
                    .header(ctx.header_name(), ctx.header_value())
                    .uri(&uri)
                    .body(httpclient::body_bytes(buf))?;
                let mut client = connect_client(&uri).await?;
                let res = client.send_request(req).await?;
                let (head, body) = res.into_parts();
                let mut resb = Response::builder().status(head.status);
                for h in head.headers {
                    if let (Some(hn), hv) = h {
                        resb = resb.header(hn, hv);
                    }
                }
                let res = resb.body(body_stream(StreamIncoming::new(body)))?;
                Ok(res)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        }
    }
}
