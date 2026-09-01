use crate::ReqCtx;
use crate::bodystream::response;
use crate::err::Error;
use crate::proxy::get_query_host_for_backend;
use crate::requests::accepts_cbor_framed;
use crate::requests::accepts_json_framed;
use crate::requests::accepts_json_or_all;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use http::header;
use httpclient::Requ;
use httpclient::StreamIncoming;
use httpclient::StreamResponse;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::connect_client;
use netpod::APP_CBOR_FRAMED;
use netpod::APP_JSON_FRAMED;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::ProxyConfig;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::req_uri_to_url;
use query::api4::events::PlainEventsQuery;

pub struct EventsHandler {}

impl EventsHandler {
    pub fn path() -> &'static str {
        "/api/4/events"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == Self::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        } else {
            if accepts_cbor_framed(req.headers()) {
                self.handle_framed(req, APP_CBOR_FRAMED, ctx, proxy_config).await
            } else if accepts_json_framed(req.headers()) {
                self.handle_framed(req, APP_JSON_FRAMED, ctx, proxy_config).await
            } else if accepts_json_or_all(req.headers()) {
                Ok(crate::proxy::proxy_backend_query::<PlainEventsQuery>(req, ctx, proxy_config).await?)
            } else {
                Err(Error::with_msg_no_trace(format!("bad accept {:?}", req.headers())))
            }
        }
    }

    async fn handle_framed(
        &self,
        req: Requ,
        accept: &str,
        ctx: &ReqCtx,
        proxy_config: &ProxyConfig,
    ) -> Result<StreamResponse, Error> {
        let (head, _body) = req.into_parts();
        let url = req_uri_to_url(&head.uri)?;
        let pairs = get_url_query_pairs(&url);
        let evq = PlainEventsQuery::from_pairs(&pairs)?;
        debug!("handle_framed  {evq:?}");
        let query_host = get_query_host_for_backend(evq.backend(), proxy_config)?;
        let url_str = format!(
            "{}{}",
            query_host,
            head.uri
                .path_and_query()
                .ok_or_else(|| Error::with_msg_no_trace("uri contains no path"))?
        );
        debug!("try to contact {url_str}");
        let uri: Uri = url_str.parse()?;
        let host = uri.host().ok_or_else(|| Error::with_msg_no_trace("no host in url"))?;
        let req = Request::builder()
            .method(Method::GET)
            .header(header::HOST, host)
            .header(header::ACCEPT, accept)
            .header(ctx.header_name(), ctx.header_value())
            .uri(&uri)
            .body(body_empty())?;
        let mut client = connect_client(&uri).await?;
        let res = client.send_request(req).await?;
        let (head, body) = res.into_parts();
        if head.status != StatusCode::OK {
            warn!("backend returned error: {head:?}");
        }
        let mut resb = Response::builder().status(head.status);
        for h in head.headers {
            if let (Some(hn), hv) = h {
                resb = resb.header(hn, hv);
            }
        }
        let res = resb.body(body_stream(StreamIncoming::new(body)))?;
        Ok(res)
    }
}
