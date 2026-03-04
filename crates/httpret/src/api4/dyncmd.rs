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
use netpod::TsNano;
use query::api4::scyllaopts::ScyllaOptsQuery;
use scyllaconn::range::ScyllaSeriesRange;
use scyllaconn::worker::ScyllaQueue;
use serde::Deserialize;
use series::SeriesId;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }

#[derive(Deserialize)]
struct ReadMsp {
    series: SeriesId,
    rt: RetentionTime,
}

#[derive(Deserialize)]
struct ReadEvents03Fwd {
    series: SeriesId,
    rt: RetentionTime,
}

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

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
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
                ty1: String,
            }
            if let Ok(cmd) = serde_json::from_slice::<DynCmd>(&buf) {
                if cmd.ty1 == "scyqu" {
                    if let Some(scyqu) = &shared_res.scyqueue {
                        #[derive(Deserialize)]
                        struct DynCmd {
                            ty1: String,
                            ty2: String,
                        }
                        if let Ok(cmd) = serde_json::from_slice::<DynCmd>(&buf) {
                            if cmd.ty2 == "read_msp" {
                                if let Ok(cmd) = serde_json::from_slice::<ReadMsp>(&buf) {
                                    let x = read_msp(cmd, scyqu).await;
                                    let buf = serde_json::to_vec(&x).unwrap();
                                    Ok(response(StatusCode::OK).body(body_bytes(buf))?)
                                } else {
                                    Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
                                }
                            } else {
                                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
                            }
                        } else {
                            Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
                        }
                    } else {
                        Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
                    }
                } else {
                    Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
                }
            } else {
                let c: serde_json::Value = serde_json::from_slice(&buf)?;
                let s = serde_json::to_string(&c).unwrap();
                let s = format!("GOT COMMAND: {s}");
                let buf = s.into_bytes();
                Ok(response(StatusCode::OK).body(httpclient::body_bytes(buf))?)
            }
        }
    }
}

async fn read_msp(cmd: ReadMsp, scyqu: &ScyllaQueue) -> serde_json::Value {
    serde_json::json!({
        "error": "no scylla",
    });
    let range = ScyllaSeriesRange::new(TsNano::from_ms(0), TsNano::from_ms(0x1fffffffffffffff));
    let scylla_opts = ScyllaOptsQuery::new();
    let x = scyqu.find_ts_msp(cmd.rt, cmd.series, range, false, scylla_opts).await;
    let x = x.map_err(|e| e.to_string());
    serde_json::to_value(&x).unwrap()
}
