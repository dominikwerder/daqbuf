use crate::bodystream::response;
use crate::err::Error;
use crate::requests::accepts_json_or_all;
use http::Method;
use http::StatusCode;
use http::request::Parts;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::body_bytes;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::read_body_bytes;
use netpod::APP_JSON;
use netpod::ProxyConfig;
use netpod::ReqCtx;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::req_uri_to_url;

pub struct RequestStatusHandler {}

impl RequestStatusHandler {
    pub fn path_prefix() -> &'static str {
        "/api/1/requestStatus/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        let (head, body) = req.into_parts();
        if head.method != Method::GET {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        } else if !accepts_json_or_all(&head.headers) {
            Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?)
        } else {
            let _body_data = read_body_bytes(body).await?;
            self.handle_json(ctx, head, proxy_config).await
        }
    }

    async fn handle_json(
        &self,
        ctx: &ReqCtx,
        head: Parts,
        proxy_config: &ProxyConfig,
    ) -> Result<StreamResponse, Error> {
        let status_id = &head.uri.path()[Self::path_prefix().len()..];
        debug!("RequestStatusHandler  status_id {:?}", status_id);

        if false {
            let status = netpod::StatusBoardEntryUser::new_all_good();
            let s = serde_json::to_string(&status)?;
            let ret = response(StatusCode::OK).body(body_string(s))?;
            return Ok(ret);
        }

        let url = req_uri_to_url(&head.uri).map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let pairs = get_url_query_pairs(&url);
        let pn = if let Some(backend) = pairs.get("backend") {
            proxy_config
                .backends
                .iter()
                .filter(|x| x.name == *backend)
                .next()
                .ok_or_else(|| Error::with_msg_no_trace(format!("no default backend found")))?
        } else {
            proxy_config
                .backends
                .iter()
                .filter(|x| x.name == "sf-databuffer")
                .next()
                .ok_or_else(|| Error::with_msg_no_trace(format!("no default backend found")))?
        };
        let url_str = format!("{}{}{}", pn.url, Self::path_prefix(), status_id);
        debug!("try to ask {url_str}");
        let url = url_str.parse()?;
        let res = httpclient::http_get(url, APP_JSON, ctx).await?;
        let ret = response(StatusCode::OK).body(body_bytes(res.body))?;
        Ok(ret)
    }
}
