#![allow(unused_macros)]

mod create_test_data;

use crate::bodystream::response;
use crate::ReqCtx;
use crate::ServiceSharedResources;
use bytes::BytesMut;
use chrono::TimeZone;
use dbconn::worker::PgQueue;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use futures_util::TryStreamExt;
use http::header;
use http::Method;
use http::StatusCode;
use httpclient::body_bytes;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::Requ;
use httpclient::StreamBody;
use httpclient::StreamResponse;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::StreamItem;
use items_2::binning::container_events::ContainerEvents;
use netpod::ttl::RetentionTime;
use netpod::NodeConfigCached;
use netpod::RangeExcl;
use netpod::SeriesKind;
use netpod::TsMs;
use netpod::APP_JSON_FRAMED;
use query::api4::scyllaopts::ScyllaOptsQuery;
use scyllaconn::events3::msplsp::MspEv;
use scyllaconn::events3::SeriesInfo;
use scyllaconn::range::ScyllaSeriesRange;
use scyllaconn::worker::ScyllaQueue;
use serde::Deserialize;
use serde::Serialize;
use series::SeriesId;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::time::Duration;
use streams::lenframe::bytes_chunks_to_len_framed_str;
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
        LspFwdMspMulti(#[from] scyllaconn::events3::ks::lsp_fwd_msp_multi::Error),
        Http(#[from] http::Error),
    },
);

impl crate::IntoBoxedError for Error {}

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

#[derive(Debug, Serialize, Deserialize)]
struct LspFwdMspMultiCmd {
    backend: String,
    series: String,
    name: String,
    // cl: String,
    // rt: RetentionTime,
    // msp: MspEv,
    // limit: Option<u32>,
    ts1: String,
    ts2: String,
}

impl LspFwdMspMultiCmd {
    fn series(&self) -> SeriesId {
        SeriesId::new(self.series.parse().unwrap())
    }
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
        _ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        _ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, crate::RetrievalError> {
        let (req, body) = req.into_parts();
        if req.method != Method::POST {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        } else {
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
                        #[allow(unused)]
                        #[derive(Debug, Deserialize)]
                        struct DynCmd {
                            ty1: String,
                            ty2: String,
                        }
                        if let Ok(cmd) = serde_json::from_slice::<DynCmd>(&buf) {
                            info!("{cmd:?}");
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
                            } else if cmd.ty2 == "LspFwdMspMultiCmd" {
                                if let Ok(cmd) = serde_json::from_slice::<LspFwdMspMultiCmd>(&buf) {
                                    info!("{cmd:?}");
                                    if true {
                                        let res =
                                            LspFwdMspMultiCmd::exec_stream_head(cmd, scyqu, shared_res.pgqueue.clone())
                                                .await?;
                                        Ok(res)
                                    } else {
                                        let x = LspFwdMspMultiCmd::exec(cmd, scyqu, shared_res.pgqueue.clone()).await;
                                        info!("exec yields {x:?}");
                                        let x = ok_or_err_jsval(x);
                                        info!("return  OK  {x:?}");
                                        let buf = serde_json::to_vec(&x).unwrap();
                                        Ok(response(StatusCode::OK).body(body_bytes(buf))?)
                                    }
                                } else {
                                    info!("Failed to parse LspFwdMspMultiCmd");
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
        .map(|cl| {
            let keyspaces: Vec<_> = cl
                .keyspaces()
                .iter()
                .map(|x| serde_json::to_value(x).unwrap())
                .collect();
            serde_json::json!({
                "tag": cl.tag(),
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
    for cl in scyqu.clusters() {
        let mut ret2 = Vec::new();
        for ks in cl.keyspaces() {
            let mut matcher = BTreeMap::new();
            let x1 = cl
                .find_ts_msp_fwd(ks.clone(), cmd.series(), range.clone(), cmd.limit, scylla_opts.clone())
                .await?;
            let x2 = cl
                .read_msp_03_fwd(ks.clone(), cmd.series(), range.clone(), RangeExcl::None, cmd.limit)
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
            "cluster": cl.tag(),
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
    for cl in scyqu.clusters() {
        let mut ret2 = Vec::new();
        for ks in cl.keyspaces() {
            let mut ret3 = Vec::new();
            let msp_bck = timeout(quto, cl.read_msp_03_bck(ks.clone(), series_id, range.clone())).await??;
            for msp in msp_bck.iter() {
                let msp = MspEv::from(*msp);
                let lsp_lst_opn = timeout(quto, cl.read_03_lsp_lst(ks.clone(), si.clone(), msp, None)).await?;
                let end = msp.lsp(range.end());
                // TODO if end is None, we can skip the query.
                let lsp_lst_lim = timeout(quto, cl.read_03_lsp_lst(ks.clone(), si.clone(), msp, end)).await?;
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
            "cluster": cl.tag(),
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
    for cl in scyqu.clusters() {
        if cl.tag() == cmd.cl {
            let mut ret2 = Vec::new();
            for ks in cl.keyspaces() {
                if ks.rt() == cmd.rt {
                    let x = json!({
                        "keyspace": ks,
                        "msp": cmd.msp,
                    });
                    ret2.push(x);
                }
            }
            let x = json!({
                "cluster": cl.tag(),
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

mod res_to_stream {
    use futures_util::Stream;
    use futures_util::StreamExt;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    pub struct ResultToStream<S, E1> {
        stream: Option<S>,
        errval: Option<E1>,
    }

    impl<S, E1> ResultToStream<S, E1> {
        pub fn new<T1, F>(params: Result<T1, E1>, f: F) -> Self
        where
            F: FnOnce(T1) -> S,
            S: Stream + Unpin,
        {
            match params {
                Ok(x) => Self {
                    stream: Some(f(x)),
                    errval: None,
                },
                Err(e) => Self {
                    stream: None,
                    errval: Some(e),
                },
            }
        }
    }

    impl<S, T2, E1, E2> Stream for ResultToStream<S, E1>
    where
        S: Stream<Item = Result<T2, E2>> + Unpin,
        E1: Into<E2> + Unpin,
    {
        type Item = Result<T2, E2>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            use Poll::*;
            if let Some(e) = self.as_mut().get_mut().errval.take() {
                Ready(Some(Err(e.into())))
            } else {
                if let Some(stream) = self.as_mut().get_mut().stream.as_mut() {
                    match stream.poll_next_unpin(cx) {
                        Ready(Some(Ok(x))) => Ready(Some(Ok(x))),
                        Ready(Some(Err(e))) => Ready(Some(Err(e))),
                        Ready(None) => {
                            self.stream = None;
                            Ready(None)
                        }
                        Pending => Pending,
                    }
                } else {
                    Ready(None)
                }
            }
        }
    }
}

impl LspFwdMspMultiCmd {
    async fn exec_stream_body(cmd: Self, scyqu: ScyllaQueue, pgqu: PgQueue) -> Result<StreamBody, Error> {
        use futures_util::stream::iter;
        use netpod::FromUrl;
        use serde_json::json;
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
        let series_info = SeriesInfo::from(&chi);
        let range = netpod::range::evrange::NanoRange::from(range);
        let range = ScyllaSeriesRange::new(range.beg_ts(), range.end_ts());
        // TODO this has to be done for each cl, ks
        // let msp_bck =
        //     scyllaconn::events3::mspbck::msp_bck(ks.clone(), series_info.clone(), range.beg(), cl.as_ref().clone())
        //         .await?
        //         .into_iter()
        //         .map(|x| MspEv::from(x))
        //         .collect();
        let stream = scyllaconn::events3::ks::lsp_fwd_msp_multi::LspFwdMspMultiOverClusters::new(
            series_info.clone(),
            range.clone(),
            scyllaconn::events3::ks::lsp_fwd_msp_multi::Opts::new(),
            scyqu.clone(),
        );
        let stream = iter(scyqu.into_clusters())
            .map(move |cl| {
                iter(cl.keyspaces().to_vec())
                    .then({
                        let cl = cl.clone();
                        let series_info = series_info.clone();
                        let range = range.clone();
                        move |ks| {
                            scyllaconn::events3::mspbck::msp_bck(
                                ks.clone(),
                                series_info.clone(),
                                range.beg(),
                                cl.as_ref().clone(),
                            )
                            .map_ok(|msp_bck| (ks, msp_bck))
                        }
                    })
                    .map({
                        let cl = cl.clone();
                        let series_info = series_info.clone();
                        let range = range.clone();
                        move |x| {
                            res_to_stream::ResultToStream::new(x, |(ks, msp_bck)| {
                                for x in msp_bck.iter() {
                                    info!("msp_bck  {ks:?}  {x}");
                                }
                                let cl = cl.clone();
                                let series_info = series_info.clone();
                                let range = range.clone();
                                let opts = scyllaconn::events3::ks::lsp_fwd_msp_multi::Opts::new();
                                let msps = msp_bck.into_iter().map(MspEv::from).collect();
                                let stream = scyllaconn::events3::ks::lsp_fwd_msp_multi::LspFwdMspMulti::new(
                                    ks.clone(),
                                    series_info.clone(),
                                    range.clone(),
                                    opts,
                                    cl.as_ref().clone(),
                                    msps,
                                )
                                .map_err(Error::from)
                                .map_ok(move |x| (ks.clone(), x));
                                stream
                            })
                        }
                    })
                    .flatten()
                    .map(|x| x)
                    .map_ok(move |(ks, x)| (cl.clone(), ks, x))
            })
            .flatten()
            .map(|x| x)
            .map(|x| match x {
                Ok((cl, ks, x)) => match x {
                    StreamItem::DataItem(x) => match x {
                        RangeCompletableItem::Data(x) => {
                            let x = x.to_f32_for_binning_v01();
                            if let Some(x) = x.as_any_ref().downcast_ref::<ContainerEvents<f32>>() {
                                let (tss, vals) =
                                    x.iter_zip()
                                        .fold((Vec::new(), Vec::new()), |(mut tss, mut vals), (ts, val)| {
                                            tss.push(ts.fmt().to_string());
                                            vals.push(val);
                                            (tss, vals)
                                        });
                                json!({
                                    "type": "events",
                                    "cl": cl.tag(),
                                    "ks": ks.name(),
                                    "tss:": tss,
                                    "vals": vals,
                                })
                            } else {
                                json!({
                                    "type": "error",
                                    "cl": cl.tag(),
                                    "ks": ks.name(),
                                    "error": "can not downcast",
                                })
                            }
                        }
                        RangeCompletableItem::RangeComplete => {
                            json!({
                                "type": "RangeFinal",
                                "cl": cl.tag(),
                                "ks": ks.name(),
                            })
                        }
                    },
                    StreamItem::Log(x) => {
                        json!({
                            "type": "log",
                            "cl": cl.tag(),
                            "ks": ks.name(),
                            "log": x,
                        })
                    }
                    StreamItem::Stats(x) => {
                        json!({
                            "type": "stats",
                            "cl": cl.tag(),
                            "ks": ks.name(),
                            "stats": x,
                        })
                    }
                },
                Err(e) => json!({
                    "type": "error",
                    // "cl": cl.tag(),
                    // "ks": ks.name(),
                    "error": e.to_string(),
                }),
            })
            .map(|x| Ok::<_, Error>(serde_json::to_string(&x).unwrap()));
        let stream = bytes_chunks_to_len_framed_str(stream);
        let res = body_stream(stream);
        Ok(res)
    }

    async fn exec_stream_head(cmd: Self, scyqu: &ScyllaQueue, pgqu: PgQueue) -> Result<StreamResponse, Error> {
        let body = Self::exec_stream_body(cmd, scyqu.clone(), pgqu).await?;
        let res = response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON_FRAMED)
            .body(body)?;
        Ok(res)
    }

    async fn exec(cmd: Self, scyqu: &ScyllaQueue, pgqu: PgQueue) -> Result<serde_json::Value, Error> {
        use netpod::FromUrl;
        use serde_json::json;
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
        let series_info = SeriesInfo::from(&chi);
        let range = netpod::range::evrange::NanoRange::from(range);
        let range = ScyllaSeriesRange::new(range.beg_ts(), range.end_ts());
        let mut ret1 = Vec::new();
        for cl in scyqu.clusters() {
            let mut ret2 = Vec::new();
            for ks in cl.keyspaces() {
                let opts = scyllaconn::events3::ks::lsp_fwd_msp_multi::Opts::new();
                let msps = VecDeque::new();
                let mut stream = scyllaconn::events3::ks::lsp_fwd_msp_multi::LspFwdMspMulti::new(
                    ks.clone(),
                    series_info.clone(),
                    range.clone(),
                    opts,
                    cl.as_ref().clone(),
                    msps,
                );
                let mut tss = Vec::new();
                let mut vals = Vec::new();
                let mut strs = Vec::new();
                while let Some(x) = stream.next().await {
                    let x = x?;
                    match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(x) => {
                                let x = x.to_f32_for_binning_v01();
                                if let Some(x) = x.as_any_ref().downcast_ref::<ContainerEvents<f32>>() {
                                    for (ts, val) in x.iter_zip() {
                                        tss.push(ts.fmt().to_string());
                                        vals.push(val);
                                    }
                                } else {
                                    strs.push(format!("can not downcast"));
                                }
                            }
                            RangeCompletableItem::RangeComplete => {
                                strs.push(format!("RangeComplete"));
                            }
                        },
                        StreamItem::Log(x) => {
                            strs.push(x.display_log_file().to_string());
                        }
                        StreamItem::Stats(x) => {
                            strs.push(format!("{x:?}"));
                        }
                    }
                }
                let x = json!({
                    "keyspace": ks,
                    "strs": strs,
                    "tss": tss,
                    "vals": vals,
                });
                ret2.push(x);
            }
            let x = json!({
                "cluster": cl.tag(),
                "keyspaces": ret2,
            });
            ret1.push(x);
        }
        let ret = json!({
            "type": std::any::type_name::<Self>(),
            "series_id": format!("{}", series_id.id()),
            "ret1": ret1,
        });
        Ok(ret)
    }
}
