#![allow(unused_macros)]

use crate::bodystream::response;
use crate::ReqCtx;
use crate::ServiceSharedResources;
use bytes::BytesMut;
use chrono::TimeZone;
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
use series::SeriesId;
use std::collections::BTreeMap;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "DynCmd"),
    enum variants {
        ErrorMsg(#[from] String),
        ScyllaConnWorker(#[from] scyllaconn::worker::Error),
        ReadMsp03(#[from] scyllaconn::events3::mspfwd::Error),
        ReadMspMismatch,
    },
);

#[derive(Debug, Serialize, Deserialize)]
struct AttachedScyllas {}

#[derive(Debug, Serialize, Deserialize)]
struct ReadMsp {
    series: String,
    rt: Option<RetentionTime>,
    limit: Option<u32>,
    ts1: String,
    ts2: String,
}

impl ReadMsp {
    fn series(&self) -> SeriesId {
        SeriesId::new(self.series.parse().unwrap())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ReadEvents03Fwd {
    series1: u32,
    series2: u32,
    msp: TsMs,
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
    ) -> Result<StreamResponse, crate::RetrievalError> {
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
                                    let buf = serde_json::to_vec(&ok_or_err_jsval(x)).unwrap();
                                    Ok(response(StatusCode::OK).body(body_bytes(buf))?)
                                } else {
                                    Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
                                }
                            } else if cmd.ty2 == "attached_scyllas" {
                                let x = attached_scyllas(scyqu).await;
                                let buf = serde_json::to_vec(&ok_or_err_jsval(x)).unwrap();
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
                let c: serde_json::Value = serde_json::from_slice(&buf)?;
                let s = serde_json::to_string(&c).unwrap();
                let s = format!("GOT COMMAND: {s}");
                let buf = s.into_bytes();
                Ok(response(StatusCode::OK).body(httpclient::body_bytes(buf))?)
            }
        }
    }
}

fn ok_or_err_jsval<E>(x: Result<serde_json::Value, E>) -> serde_json::Value
where
    E: ToString,
{
    use serde_json::json;
    match x {
        Ok(x) => x,
        Err(e) => json!({"error": e.to_string()}),
    }
}

async fn attached_scyllas(scyqu: &ScyllaQueue) -> Result<serde_json::Value, Error> {
    let clusters: Vec<_> = scyqu
        .clusters()
        .iter()
        .map(|x| {
            let keyspaces: Vec<_> = x.keyspaces().iter().map(|x| serde_json::to_value(x).unwrap()).collect();
            serde_json::json!({
                "tag": x.tag(),
                "keyspaces": keyspaces,
            })
        })
        .collect();
    let ret = serde_json::json!({
        "clusters": clusters,
    });
    Ok(ret)
}

async fn read_msp(cmd: ReadMsp, scyqu: &ScyllaQueue) -> Result<serde_json::Value, Error> {
    use netpod::FromUrl;
    use serde_json::json;
    json!({
        "error": "no scylla",
    });
    let range = netpod::query::TimeRangeQuery::from_pairs(
        &[("begDate", cmd.ts1.clone()), ("endDate", cmd.ts2.clone())]
            .map(|x| (x.0.into(), x.1))
            .into_iter()
            .collect(),
    )
    .map_err(|e| Error::from(e.to_string()))?;
    let range = netpod::range::evrange::NanoRange::from(range);
    let range = ScyllaSeriesRange::new(range.beg_ts(), range.end_ts());
    let scylla_opts = ScyllaOptsQuery::new();
    let mut ret1 = Vec::new();
    for c in scyqu.clusters() {
        let mut ret2 = Vec::new();
        for ks in c.keyspaces() {
            let mut matcher = BTreeMap::new();
            let x1 = c
                .find_ts_msp_fwd(ks.clone(), cmd.series(), range.clone(), cmd.limit, scylla_opts.clone())
                .await?;
            let x2 = c
                .read_msp_03_fwd(ks.clone(), cmd.series(), range.clone(), cmd.limit)
                .await?;
            if x1.len() != x2.len() {
                // TODO this can actually happen on concurrent db write.
                return Err(Error::ReadMspMismatch);
            }
            for x in x1.iter() {
                matcher
                    .entry(*x)
                    .and_modify(|v| {
                        *v += 1;
                    })
                    .or_insert(1u32);
            }
            for x in x2.iter() {
                matcher
                    .entry(*x)
                    .and_modify(|v| {
                        *v += 1;
                    })
                    .or_insert(1u32);
            }
            let mismatches = matcher.iter_mut().filter(|(_, v)| **v != 2).count();
            let date_zero = chrono::Utc.timestamp_millis_opt(0).unwrap();
            let dates: Vec<_> = x2
                .iter()
                .map(|x| {
                    chrono::Utc
                        .timestamp_millis_opt(x.ms() as i64)
                        .single()
                        .unwrap_or(date_zero)
                })
                .collect();
            let x = json!({
                "keyspace": ks,
                "msps": x2,
                "mismatches": mismatches,
                "dates": dates,
            });
            ret2.push(x);
        }
        let x = json!({
            "cluster": c.tag(),
            "keyspaces": ret2,
        });
        ret1.push(x);
    }
    // let x = scyqu.find_ts_msp(cmd.rt, cmd.series, range, false, scylla_opts).await;
    // let x = x.map_err(|e| e.to_string());
    let ret = serde_json::to_value(&ret1).unwrap();
    Ok(ret)
}
