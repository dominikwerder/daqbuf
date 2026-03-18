#![allow(unused_macros)]

mod create_test_data;

use crate::bodystream::response;
use crate::ReqCtx;
use crate::ServiceSharedResources;
use bytes::BytesMut;
use chrono::TimeZone;
use dbconn::worker::PgQueue;
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
use netpod::SeriesKind;
use netpod::TsMs;
use netpod::TsNano;
use query::api4::scyllaopts::ScyllaOptsQuery;
use scyllaconn::events3::msplsp::MspEv;
use scyllaconn::events3::SeriesInfo;
use scyllaconn::range::ScyllaSeriesRange;
use scyllaconn::worker::ScyllaQueue;
use serde::Deserialize;
use serde::Serialize;
use series::SeriesId;
use std::collections::BTreeMap;
use std::time::Duration;
use taskrun::tokio::time::timeout;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "DynCmd"),
    enum variants {
        Msg(#[from] String),
        ScyllaConnWorker(#[from] scyllaconn::worker::Error),
        ReadMsp03(#[from] scyllaconn::events3::mspfwd::Error),
        ReadMspMismatch,
        Read03MspBck(#[from] scyllaconn::events3::mspbck::Error),
        AsyncChannel(#[from] netpod::AsyncChannelError),
        LspLst(#[from] scyllaconn::events3::lsplst::Error),
        ChConf(#[from] dbconn::channelconfig::Error),
        PgWorker(#[from] dbconn::worker::Error),
        Search(#[from] dbconn::search::Error),
        Elapsed(#[from] taskrun::tokio::time::error::Elapsed),
    },
);

#[derive(Debug, Serialize, Deserialize)]
struct AttachedScyllas {}

#[derive(Debug, Serialize, Deserialize)]
struct ReadMsp {
    backend: String,
    series: String,
    name: String,
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
struct Read03LspLst {
    backend: String,
    series: String,
    name: String,
    rt: Option<RetentionTime>,
    limit: Option<u32>,
    ts1: String,
    ts2: String,
}

impl Read03LspLst {
    fn series(&self) -> SeriesId {
        SeriesId::new(self.series.parse().unwrap())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Read03LspForMsp {
    backend: String,
    series: String,
    name: String,
    cl: String,
    rt: RetentionTime,
    msp: MspEv,
    limit: Option<u32>,
    ts1: String,
    ts2: String,
}

impl Read03LspForMsp {
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
            info!("Received command: {}", String::from_utf8_lossy(&buf));
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
                            if cmd.ty2 == "create_test_data" {
                                let x = create_test_data::create_test_data(scyqu).await;
                                let buf = serde_json::to_vec(&ok_or_err_jsval(x)).unwrap();
                                Ok(response(StatusCode::OK).body(body_bytes(buf))?)
                            } else if cmd.ty2 == "read_msp" {
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
                            } else if cmd.ty2 == "read_03_lsp_lst" {
                                if let Ok(cmd) = serde_json::from_slice::<Read03LspLst>(&buf) {
                                    let x = lsp_lst(cmd, scyqu, shared_res.pgqueue.clone()).await;
                                    let buf = serde_json::to_vec(&ok_or_err_jsval(x)).unwrap();
                                    Ok(response(StatusCode::OK).body(body_bytes(buf))?)
                                } else {
                                    Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
                                }
                            } else if cmd.ty2 == "read_03_lsp_for_msp" {
                                if let Ok(cmd) = serde_json::from_slice::<Read03LspForMsp>(&buf) {
                                    let x = lsp_for_msp(cmd, scyqu, shared_res.pgqueue.clone()).await;
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
    let date_zero = chrono::Utc.timestamp_millis_opt(0).unwrap();
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

async fn lsp_lst(cmd: Read03LspLst, scyqu: &ScyllaQueue, pgqu: PgQueue) -> Result<serde_json::Value, Error> {
    use netpod::FromUrl;
    use serde_json::json;
    json!({
        "error": "no scylla",
    });
    let date_zero = chrono::Utc.timestamp_millis_opt(0).unwrap();
    let range = netpod::query::TimeRangeQuery::from_pairs(
        &[("begDate", cmd.ts1.clone()), ("endDate", cmd.ts2.clone())]
            .map(|x| (x.0.into(), x.1))
            .into_iter()
            .collect(),
    )
    .map_err(|e| Error::from(e.to_string()))?;
    let series_id = if cmd.series().id() == 0 {
        let qu = netpod::ChannelSearchQuery {
            backend: Some(cmd.backend.clone()),
            name_regex: cmd.name.clone(),
            source_regex: String::new(),
            description_regex: String::new(),
            icase: false,
            kind: SeriesKind::ChannelData,
            log_level: String::new(),
        };
        let mut res = pgqu.search_channel_scylla(qu).await??;
        let c1 = res
            .channels
            .pop()
            .ok_or_else(|| Error::Msg(format!("channel not found")))?;
        SeriesId::new(c1.series)
    } else {
        cmd.series()
    };
    let chi = pgqu.chconf_for_series(&cmd.backend, series_id.id()).await??;
    let si = SeriesInfo::from(&chi);
    let range = netpod::range::evrange::NanoRange::from(range);
    let range = ScyllaSeriesRange::new(range.beg_ts(), range.end_ts());
    let scylla_opts = ScyllaOptsQuery::new();
    let mut msp_bck = Vec::new();
    let quto = Duration::from_millis(2000);
    for c in scyqu.clusters() {
        let mut ret2 = Vec::new();
        for ks in c.keyspaces() {
            let mut ret3 = Vec::new();
            let msp_bck = timeout(quto, c.read_msp_03_bck(ks.clone(), series_id, range.clone())).await??;
            for msp in msp_bck.iter() {
                let msp = MspEv::from(*msp);
                let lsp_lst_opn = timeout(quto, c.read_03_lsp_lst(ks.clone(), si.clone(), msp, None)).await?;
                let end = msp.lsp(range.end());
                // TODO if end is None, we can skip the query.
                let lsp_lst_lim = timeout(quto, c.read_03_lsp_lst(ks.clone(), si.clone(), msp, end)).await?;
                if end.is_none() {
                    if let (Ok(a), Ok(b)) = (&lsp_lst_opn, &lsp_lst_lim) {
                        if a != b {
                            return Err(Error::Msg(format!("lsp_lst_lim != lsp_lst_opn")));
                        }
                    }
                }
                let msp_st = chrono::Utc
                    .timestamp_millis_opt(msp.to_ms().to_i64())
                    .single()
                    .unwrap_or(date_zero);
                let x = json!({
                    "msp": (msp, msp_st),
                    "lsp_lst_opn": lsp_lst_opn.map_err(|x| x.to_string()),
                    "lsp_lst_lim": lsp_lst_lim.map_err(|x| x.to_string()),
                });
                ret3.push(x);
            }
            let x = json!({
                "keyspace": ks,
                "msp_bck": ret3,
            });
            ret2.push(x);
        }
        let x = json!({
            "cluster": c.tag(),
            "keyspaces": ret2,
        });
        msp_bck.push(x);
    }
    let ret = json!({
        "series_id": format!("{}", series_id.id()),
        "msp_bck": msp_bck,
    });
    Ok(ret)
}

async fn lsp_for_msp(cmd: Read03LspForMsp, scyqu: &ScyllaQueue, pgqu: PgQueue) -> Result<serde_json::Value, Error> {
    use netpod::FromUrl;
    use serde_json::json;
    json!({
        "error": "no scylla",
    });
    let date_zero = chrono::Utc.timestamp_millis_opt(0).unwrap();
    let range = netpod::query::TimeRangeQuery::from_pairs(
        &[("begDate", cmd.ts1.clone()), ("endDate", cmd.ts2.clone())]
            .map(|x| (x.0.into(), x.1))
            .into_iter()
            .collect(),
    )
    .map_err(|e| Error::from(e.to_string()))?;
    let series_id = if cmd.series().id() == 0 {
        let qu = netpod::ChannelSearchQuery {
            backend: Some(cmd.backend.clone()),
            name_regex: cmd.name.clone(),
            source_regex: String::new(),
            description_regex: String::new(),
            icase: false,
            kind: SeriesKind::ChannelData,
            log_level: String::new(),
        };
        let mut res = pgqu.search_channel_scylla(qu).await??;
        let c1 = res
            .channels
            .pop()
            .ok_or_else(|| Error::Msg(format!("channel not found")))?;
        SeriesId::new(c1.series)
    } else {
        cmd.series()
    };
    let chi = pgqu.chconf_for_series(&cmd.backend, series_id.id()).await??;
    let si = SeriesInfo::from(&chi);
    let range = netpod::range::evrange::NanoRange::from(range);
    let range = ScyllaSeriesRange::new(range.beg_ts(), range.end_ts());
    let scylla_opts = ScyllaOptsQuery::new();
    let mut ret1 = Vec::new();
    let quto = Duration::from_millis(3000);
    for c in scyqu.clusters() {
        if c.tag() == cmd.cl {
            let mut ret2 = Vec::new();
            for ks in c.keyspaces() {
                if ks.rt() == cmd.rt {
                    let x = json!({
                        "keyspace": ks,
                        "msp": cmd.msp,
                    });
                    ret2.push(x);
                }
            }
            let x = json!({
                "cluster": c.tag(),
                "keyspaces": ret2,
            });
            ret1.push(x);
        }
    }
    let ret = json!({
        "series_id": format!("{}", series_id.id()),
        "result": ret1,
    });
    Ok(ret)
}
