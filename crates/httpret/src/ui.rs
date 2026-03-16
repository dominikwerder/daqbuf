#![allow(unused_macros)]

use crate::bodystream::response;
use crate::ReqCtx;
use crate::RetrievalError as Error;
use crate::ServiceSharedResources;
use bytes::BytesMut;
use futures_util::TryStreamExt;
use http::header;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use httpclient::body_bytes;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::connect_client;
use httpclient::Requ;
use httpclient::StreamIncoming;
use httpclient::StreamResponse;
use netpod::ttl::RetentionTime;
use netpod::NodeConfigCached;
use netpod::TsMs;
use netpod::TsNano;
use query::api4::scyllaopts::ScyllaOptsQuery;
use scyllaconn::range::ScyllaSeriesRange;
use scyllaconn::worker::ScyllaQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use series::SeriesId;
use std::collections::BTreeMap;
use std::path::PathBuf;

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
        ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        let (head, body) = req.into_parts();
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
        let s = format!("");
        let buf = s.into_bytes();
        Ok(response(StatusCode::OK).body(body_bytes(buf))?)
    }
}
