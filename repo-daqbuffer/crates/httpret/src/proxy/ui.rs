#![allow(unused_macros)]

use crate::bodystream::response;
use crate::ReqCtx;
use http::StatusCode;
use httpclient::body_bytes;
use httpclient::body_empty;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::ProxyConfig;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

pub struct UiHandler {}

impl UiHandler {
    pub fn path() -> &'static str {
        "/api/4/ui1"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        // shared_res: &ServiceSharedResources,
        // ncc: &NodeConfigCached,
        _proxy_config: &ProxyConfig,
    ) -> Result<StreamResponse, crate::err::Error> {
        if let Some(pq) = req.uri().path_and_query() {
            if pq.query().map(|x| x.contains("dbgshowreq")).unwrap_or(false) {
                let (head, _body) = req.into_parts();
                let method = head.method.as_str();
                let uri = head.uri.to_string();
                let headers: BTreeMap<_, _> = head
                    .headers
                    .iter()
                    .map(|x| (x.0.as_str(), x.1.to_str().unwrap_or("invalid".into())))
                    .collect();
                let js = json!({
                    "source": "proxy",
                    "method": serde_json::to_value(&method).unwrap(),
                    "uri": serde_json::to_value(&uri).unwrap(),
                    "headers": serde_json::to_value(&headers).unwrap(),
                });
                // let s = format!("");
                // let buf = s.into_bytes();
                let buf = serde_json::to_vec(&js).unwrap();
                Ok(response(StatusCode::OK).body(body_bytes(buf))?)
            } else {
                let base_slashed = format!("{}/", Self::path());
                let path_app = format!("{}/_app/", Self::path());
                if pq.path() == Self::path() {
                    Ok(response(StatusCode::TEMPORARY_REDIRECT)
                        .header(http::header::LOCATION, "./ui1/")
                        .body(body_empty())?)
                } else if pq.path() == base_slashed {
                    let pre = "/ui1/prerendered/daqingest/ui/ui1";
                    let path = "index.html";
                    let full = format!("{pre}/{path}");
                    match daqingest_ui::assets::get_asset(&full) {
                        Some((bytes, mime)) => {
                            let res = response(StatusCode::OK)
                                .header(http::header::CONTENT_TYPE, mime)
                                .body(body_bytes(bytes))?;
                            Ok(res)
                        }
                        None => Ok(response(StatusCode::NOT_FOUND).body(body_empty())?),
                    }
                } else if pq.path().starts_with(&path_app) {
                    let pre = "/ui1/client/daqingest/ui/ui1/_app";
                    let path = &pq.path()[path_app.len()..];
                    let path2 = path;
                    let full = format!("{pre}/{path2}");
                    info!(
                        "in _app {path_app:?}  path {path:?}  path2 {path2:?}  pq.path() {pqp:?}  full {full:?}",
                        pqp = pq.path()
                    );
                    match daqingest_ui::assets::get_asset(&full) {
                        Some((bytes, mime)) => {
                            let res = response(StatusCode::OK)
                                .header(http::header::CONTENT_TYPE, mime)
                                .body(body_bytes(bytes))?;
                            Ok(res)
                        }
                        None => Ok(response(StatusCode::NOT_FOUND).body(body_empty())?),
                    }
                } else if pq.path().starts_with(&base_slashed) {
                    let pre = "/ui1/prerendered/daqingest/ui/ui1";
                    let path = &pq.path()[base_slashed.len()..];
                    let path2 = if path == "" {
                        format!("index.html")
                    } else {
                        let p2 = PathBuf::from(&path);
                        if p2.extension().is_some() {
                            format!("{path}")
                        } else {
                            format!("{path}.html")
                        }
                    };
                    let full = format!("{pre}/{path2}");
                    info!(
                        "in base_slashed {base_slashed:?}  path {path:?}  path2 {path2:?}  pq.path() {pqp:?}  full {full:?}",
                        pqp = pq.path()
                    );
                    match daqingest_ui::assets::get_asset(&full) {
                        Some((bytes, mime)) => {
                            let res = response(StatusCode::OK)
                                .header(http::header::CONTENT_TYPE, mime)
                                .body(body_bytes(bytes))?;
                            Ok(res)
                        }
                        None => Ok(response(StatusCode::NOT_FOUND).body(body_empty())?),
                    }
                } else {
                    Ok(response(StatusCode::NOT_FOUND).body(body_empty())?)
                }
            }
        } else {
            Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
        }
    }
}
