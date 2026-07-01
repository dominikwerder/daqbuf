pub mod reqstatus;

use crate::bodystream::response;
use crate::err::Error;
use crate::ReqCtx;
use http::header;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::StatusCode;
use http::Uri;
use httpclient::body_bytes;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::connect_client;
use httpclient::read_body_bytes;
use httpclient::Requ;
use httpclient::StreamIncoming;
use httpclient::StreamResponse;
use netpod::log::*;
use netpod::query::api1::Api1Query;
use netpod::ProxyConfig;
use netpod::ACCEPT_ALL;
use netpod::X_DAQBUF_REQID;

pub struct PythonDataApi1Query {}

impl PythonDataApi1Query {
    pub fn path() -> &'static str {
        "/api/1/query"
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
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?);
        }
        let (head, body) = req.into_parts();
        let _accept = head
            .headers
            .get(http::header::ACCEPT)
            .map_or(Ok(ACCEPT_ALL), |k| k.to_str())
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?
            .to_owned();
        let body_data = read_body_bytes(body).await?;
        let qu = match serde_json::from_slice::<Api1Query>(&body_data) {
            Ok(qu) => qu,
            Err(e) => {
                let buf = &body_data[..body_data.len().min(200)];
                error!("got body_data: {:?}", String::from_utf8_lossy(buf));
                error!("can not parse: {e}");
                return Err(Error::with_msg_no_trace("can not parse query"));
            }
        };
        info!("{qu:?}");
        let back = {
            let mut ret = None;
            for b in &proxy_config.backends {
                if b.name == "sf-databuffer" {
                    ret = Some(b);
                    break;
                }
            }
            ret
        };
        if let Some(back) = back {
            // TODO remove special code, make it part of configuration
            let back = if back.url.contains("sf-daqbuf-23.psi.ch") {
                let id = 21 + rand::random::<u32>() % 13;
                let url = back.url.replace("-23.", &format!("-{id}."));
                netpod::ProxyBackend {
                    name: back.name.clone(),
                    url,
                }
            } else {
                back.clone()
            };
            let url_str = format!("{}/api/1/query", back.url);
            info!("try to ask {url_str}");
            let uri: Uri = url_str.parse()?;
            let req = Request::builder()
                .method(Method::POST)
                .header(header::HOST, uri.host().unwrap())
                .header(ctx.header_name(), ctx.header_value())
                .uri(&uri)
                .body(body_bytes(body_data))?;
            let mut client = connect_client(&uri).await?;
            let res = client.send_request(req).await?;
            let (head, body) = res.into_parts();
            if head.status != StatusCode::OK {
                error!("backend returned error: {head:?}");
                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
            } else {
                info!("backend returned OK");
                let riq_def = HeaderValue::from_static("none");
                let riq = head.headers.get(X_DAQBUF_REQID).unwrap_or(&riq_def);
                Ok(response(StatusCode::OK)
                    .header(X_DAQBUF_REQID, riq)
                    .body(body_stream(StreamIncoming::new(body)))?)
            }
        } else {
            Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
        }
    }
}
