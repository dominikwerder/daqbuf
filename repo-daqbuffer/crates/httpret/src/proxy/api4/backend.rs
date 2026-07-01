use crate::bodystream::response;
use crate::err::Error;
use crate::requests::accepts_json_or_all;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::ProxyConfig;
use netpod::ReqCtx;
use std::collections::BTreeMap;

pub struct BackendListHandler {}

impl BackendListHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/backend/list" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, _ctx: &ReqCtx, cfg: &ProxyConfig) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                let mut list = Vec::new();
                if let Some(g) = &cfg.announce_backends {
                    for j in g {
                        let mut map = BTreeMap::new();
                        map.insert("name", j.clone());
                        list.push(map);
                    }
                }
                let res = serde_json::json!({
                    "backends_available": list,
                });
                let body = serde_json::to_string(&res)?;
                Ok(response(StatusCode::OK).body(body_string(body))?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }
}
