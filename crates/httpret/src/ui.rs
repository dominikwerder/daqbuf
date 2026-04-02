use crate::bodystream::response;
use crate::ReqCtx;
use crate::RetrievalError as Error;
use crate::ServiceSharedResources;
use http::StatusCode;
use httpclient::body_bytes;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::NodeConfigCached;
use serde_json::json;
use std::collections::BTreeMap;

#[allow(unused)]
macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }

pub struct UiHandler {}

impl UiHandler {
    pub fn path() -> &'static str {
        "/api/4/ui"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == Self::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        _shared_res: &ServiceSharedResources,
        _ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        let (head, _body) = req.into_parts();
        let method = head.method.as_str();
        let uri = head.uri.to_string();
        let headers: BTreeMap<_, _> = head
            .headers
            .iter()
            .map(|x| (x.0.as_str(), x.1.to_str().unwrap_or("invalid".into())))
            .collect();
        let js = json!({
            "source": "node",
            "method": serde_json::to_value(&method).unwrap(),
            "uri": serde_json::to_value(&uri).unwrap(),
            "headers": serde_json::to_value(&headers).unwrap(),
        });
        let s = serde_json::to_string(&js).unwrap();
        let buf = s.into_bytes();
        Ok(response(StatusCode::OK).body(body_bytes(buf))?)
    }
}
