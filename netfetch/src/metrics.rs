#![allow(unused)]
pub mod delete;
pub mod ingest;
pub mod status;
pub mod types;
pub mod ui;

use crate::ca::conn::ChannelStateInfo;
use crate::ca::connset::CaConnSetEvent;
use crate::ca::connset::ChannelStatusesRequest;
use crate::ca::connset::ChannelStatusesResponse;
use crate::ca::connset::ConnSetCmd;
use crate::ca::statemap::ChannelState;
use crate::conf::ChannelConfig;
use crate::daemon_common::ChannelName;
use crate::daemon_common::DaemonEvent;
use crate::metrics::types::MetricsPrometheusShort;
use async_channel::Receiver;
use async_channel::Sender;
use async_channel::WeakSender;
use axum::extract::Query;
use axum::http;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::response::Response;
use bytes::Bytes;
use dbpg::seriesbychannel::ChannelInfoQuery;
use err::Error;
use futures::future::ready;
use http::Request;
use http::StatusCode;
use http_body::Body;
use log;
use netpod::APP_JSON;
use scywr::config::ScyllaIngestConfig;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;
use taskrun::tokio::net::TcpListener;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

struct PublicErrorMsg(String);

trait ToPublicErrorMsg {
    fn to_public_err_msg(&self) -> PublicErrorMsg;
}

impl ToPublicErrorMsg for err::Error {
    fn to_public_err_msg(&self) -> PublicErrorMsg {
        let msg = self
            .public_msg()
            .map_or("no error message provided".into(), |x| x.join(", "));
        PublicErrorMsg(msg)
    }
}

#[derive(Serialize)]
pub struct ConnectionListV1Conn1 {
    pub ip: Ipv4Addr,
    pub port: u16,
    pub name: String,
}

#[derive(Serialize)]
pub struct ConnectionListV1 {
    pub ingest_name: String,
    pub list: Vec<ConnectionListV1Conn1>,
}

#[derive(Serialize)]
pub struct ChannelInfoV1 {
    pub name: String,
    pub state_short: String,
    pub config1: serde_json::Value,
}

#[derive(Serialize)]
pub struct ChannelsForAddrInfoV1 {
    pub channels: Vec<ChannelInfoV1>,
}

impl ChannelsForAddrInfoV1 {
    pub fn new() -> Self {
        Self { channels: Vec::new() }
    }
}

pub trait Conn2Ctrls: Send + Sync {
    fn connection_list_get_v1(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ConnectionListV1, Box<dyn std::error::Error>>> + Send>>;
    fn channels_for_addr_v1(
        &self,
        addr: SocketAddrV4,
    ) -> Pin<Box<dyn Future<Output = Result<ChannelsForAddrInfoV1, Box<dyn std::error::Error>>> + Send>>;
}

pub trait CaIngestCtrls: Send + Sync {
    fn timer_tick(&self, v: u32) -> Box<dyn Future<Output = u32>>;
    fn get_metrics(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<MetricsPrometheusShort, Box<dyn std::error::Error>>> + Send>>;
    fn channel_add(
        &self,
        conf: ChannelConfig,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>;
    fn channel_remove(
        &self,
        name: ChannelName,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>;
    fn config_reload(&self) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>;
    fn channel_states(
        &self,
        name: String,
        limit: u64,
    ) -> Pin<Box<dyn Future<Output = Result<ChannelStatusesResponse, Box<dyn std::error::Error>>> + Send>>;
    fn conn2_ctrls(&self) -> Pin<Box<dyn Future<Output = Option<Box<dyn Conn2Ctrls>>> + Send>>;
}

pub trait PostIngestCtrls: Send + Sync {
    fn resources(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<RoutesResources>, Box<dyn std::error::Error>>> + Send>>;
}

pub struct Res123 {
    content: Option<Bytes>,
}

impl http_body::Body for Res123 {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use Poll::*;
        match self.content.take() {
            Some(x) => Ready(Some(Ok(http_body::Frame::data(x)))),
            None => Ready(None),
        }
    }
}

impl IntoResponse for PublicErrorMsg {
    fn into_response(self) -> axum::response::Response {
        let msgbytes = self.0.as_bytes();
        // let body = axum::body::Bytes::from(msgbytes.to_vec());
        // let body = http_body::Frame::data(body);
        // let body = body.map_err(|_| axum::Error::new(Error::from_string("error while trying to create fixed body")));
        // let body = http_body::combinators::BoxBody::new(body);
        // let body = axum::body::Body::new(body);
        // let x = axum::response::Response::builder().status(500).body(body).unwrap();
        // return x;
        // x
        // let boddat = http_body::Empty::new();
        let res: Res123 = Res123 {
            content: Some(Bytes::from(self.0.as_bytes().to_vec())),
        };
        let bod = axum::body::Body::new(res);
        // let ret: http::Response<Bytes> = todo!();
        let ret = http::Response::builder().status(500).body(bod).unwrap();
        ret
    }
}

struct CustomErrorResponse(Response);

impl<T> From<T> for CustomErrorResponse
where
    T: ToPublicErrorMsg,
{
    fn from(value: T) -> Self {
        Self(value.to_public_err_msg().into_response())
    }
}

impl IntoResponse for CustomErrorResponse {
    fn into_response(self) -> Response {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtraInsertsConf {
    pub copies: Vec<(u64, u64)>,
}

impl ExtraInsertsConf {
    pub fn new() -> Self {
        Self { copies: Vec::new() }
    }
}

async fn always_error(params: HashMap<String, String>) -> Result<axum::Json<bool>, Response> {
    Err(Error::with_public_msg_no_trace("The-public-message")
        .to_public_err_msg()
        .into_response())
}

async fn config_reload(ca_ingest_ctrls: Arc<dyn CaIngestCtrls>) -> Result<axum::Json<serde_json::Value>, Response> {
    info!("api config reload request");
    ca_ingest_ctrls.config_reload().await.map_err(|e| {
        Error::with_public_msg_no_trace(format!("config reload error {e}"))
            .to_public_err_msg()
            .into_response()
    })?;
    let res = json!({
        "status": "ok",
    });
    let ret = serde_json::to_value(&res).unwrap();
    Ok(axum::Json(ret))
}

async fn metrics2(ca_ingest_ctrls: Arc<dyn CaIngestCtrls>) -> Result<String, Response> {
    let x = ca_ingest_ctrls.get_metrics().await.map_err(|_| {
        Error::with_public_msg_no_trace("metrics2 fail")
            .to_public_err_msg()
            .into_response()
    })?;
    let ret = x.prometheus();
    Ok(ret)
}

async fn find_channel(params: HashMap<String, String>) -> axum::Json<Vec<(String, Vec<String>)>> {
    let pattern = params.get("pattern").map_or(String::new(), |x| x.clone()).to_string();
    // TODO ask Daemon for that information.
    error!("TODO find_channel");
    let res = Vec::new();
    axum::Json(res)
}

async fn channel_add_inner(
    params: HashMap<String, String>,
    ca_ingest_ctrls: Arc<dyn CaIngestCtrls>,
) -> Result<(), Error> {
    if let Some(name) = params.get("name") {
        let conf = ChannelConfig::st_monitor(name, "api");
        let _ = ca_ingest_ctrls
            .channel_add(conf)
            .await
            .map_err(|_| Error::with_public_msg_no_trace("channel_add fail"))?;
        Ok(())
    } else {
        Err(Error::with_msg_no_trace(format!("wrong parameters given")))
    }
}

async fn channel_add(
    params: HashMap<String, String>,
    ca_ingest_ctrls: Arc<dyn CaIngestCtrls>,
) -> Result<axum::Json<bool>, Response> {
    match channel_add_inner(params, ca_ingest_ctrls).await {
        Ok(_) => Ok(axum::Json::from(true)),
        Err(e) => Err(e.to_public_err_msg().into_response()),
    }
}

async fn channel_remove(
    params: HashMap<String, String>,
    ca_ingest_ctrls: Arc<dyn CaIngestCtrls>,
) -> axum::Json<serde_json::Value> {
    use axum::Json;
    use serde_json::Value;
    let addr = if let Some(x) = params.get("addr") {
        if let Ok(addr) = x.parse::<SocketAddrV4>() {
            addr
        } else {
            return Json(Value::Bool(false));
        }
    } else {
        return Json(Value::Bool(false));
    };
    let _backend = if let Some(x) = params.get("backend") {
        x
    } else {
        return Json(Value::Bool(false));
    };
    let name = if let Some(x) = params.get("name") {
        x
    } else {
        return Json(Value::Bool(false));
    };
    error!("TODO channel_remove");
    Json(Value::Bool(false))
}

// ChannelStatusesResponse
// BTreeMap<String, ChannelState>
async fn private_channel_states(
    params: HashMap<String, String>,
    ca_ingest_ctrls: Arc<dyn CaIngestCtrls>,
) -> axum::Json<BTreeMap<String, ChannelState>> {
    let name = params.get("name").map_or(String::new(), |x| x.clone()).to_string();
    let limit = params
        .get("limit")
        .map(|x| x.parse().ok())
        .unwrap_or(None)
        .unwrap_or(40);
    let res = ca_ingest_ctrls.channel_states(name, limit).await.unwrap();
    axum::Json(res.channels_ca_conn_set)
}

async fn extra_inserts_conf_set(v: ExtraInsertsConf) -> axum::Json<bool> {
    // TODO ingest_commons is the authorative value. Should have common function outside of this metrics which
    // can update everything to a given value.
    error!("TODO extra_inserts_conf_set");
    axum::Json(true)
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
struct DummyQuery {
    name: String,
    surname: Option<String>,
    age: usize,
}

pub struct DaemonComm {
    tx: Sender<DaemonEvent>,
}

impl DaemonComm {
    pub fn new(tx: Sender<DaemonEvent>) -> Self {
        Self { tx }
    }
}

fn metricbeat() -> axum::Json<serde_json::Value> {
    let mut map = serde_json::Map::new();
    let mut ret = serde_json::Map::new();
    ret.insert("daqingest".to_string(), serde_json::Value::Object(map));
    axum::Json(serde_json::Value::Object(ret))
}

pub struct RoutesResources {
    backend: String,
    worker_tx: Sender<ChannelInfoQuery>,
    iqtx: InsertQueuesTx,
    scyconf_st: ScyllaIngestConfig,
    scyconf_mt: ScyllaIngestConfig,
    scyconf_lt: ScyllaIngestConfig,
    pgconf: netpod::Database,
}

impl RoutesResources {
    pub fn new(
        backend: String,
        worker_tx: Sender<ChannelInfoQuery>,
        series_conf_by_id_tx: Sender<()>,
        iqtx: InsertQueuesTx,
        scyconf_st: ScyllaIngestConfig,
        scyconf_mt: ScyllaIngestConfig,
        scyconf_lt: ScyllaIngestConfig,
        pgconf: netpod::Database,
    ) -> Self {
        Self {
            backend,
            worker_tx,
            iqtx,
            scyconf_st,
            scyconf_mt,
            scyconf_lt,
            pgconf,
        }
    }
}

fn make_routes_ingest(post_ingest_ctrls: Arc<dyn PostIngestCtrls>) -> axum::Router {
    use axum::Router;
    use axum::extract;
    use axum::routing::post;
    use http::StatusCode;
    Router::new().nest(
        "/write",
        Router::new()
            .route(
                "/v2",
                post({
                    let post_ingest_ctrls = post_ingest_ctrls.clone();
                    move |headers: HeaderMap, params: Query<HashMap<String, String>>, body: axum::body::Body| {
                        ingest::write_v02::write_with_fresh_msps(headers, params, body, post_ingest_ctrls)
                    }
                }),
            )
            .route(
                "/v1",
                post({
                    let post_ingest_ctrls = post_ingest_ctrls.clone();
                    move |(headers, params, body): (HeaderMap, Query<HashMap<String, String>>, axum::body::Body)| {
                        ingest::post_v01((headers, params, body), post_ingest_ctrls)
                    }
                }),
            ),
    )
}

fn make_routes_daqingest_private(
    ca_ingest_ctrls: Arc<dyn CaIngestCtrls>,
    post_ingest_ctrls: Arc<dyn PostIngestCtrls>,
) -> axum::Router {
    use axum::Router;
    use axum::extract;
    use axum::routing::get;
    Router::new()
        .nest("/channel", make_routes_private_channel(post_ingest_ctrls.clone()))
        .route(
            "/channel/states",
            get({
                let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                |Query(params): Query<HashMap<String, String>>| private_channel_states(params, ca_ingest_ctrls)
            }),
        )
        .route(
            "/debug_current_time",
            get(|| async {
                let ts = time::UtcDateTime::now();
                let format = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]Z").unwrap();
                let s = ts.format(&format).unwrap();
                axum::Json(json!({"ts":s}))
            }),
        )
        .route(
            "/conn2/connections",
            get({
                let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                || async move {
                    if let Some(c2) = ca_ingest_ctrls.conn2_ctrls().await {
                        let ret = c2.connection_list_get_v1().await.unwrap();
                        axum::Json(serde_json::to_value(&ret).unwrap())
                    } else {
                        axum::Json(json!({"error": "no ctrl"}))
                    }
                }
            }),
        )
        .route(
            "/conn2/channels_for_addr_v1/{*p1}",
            get({
                let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                |extract::Path(p1): extract::Path<String>| async move {
                    let addr = p1.parse().unwrap_or("0.0.0.0:1".parse().unwrap());
                    if let Some(c2) = ca_ingest_ctrls.conn2_ctrls().await {
                        let ret = c2.channels_for_addr_v1(addr).await.unwrap();
                        axum::Json(serde_json::to_value(&ret).unwrap())
                    } else {
                        axum::Json(json!({"error": "no ctrl"}))
                    }
                }
            }),
        )
}

fn make_routes_daqingest_ui_static(rres: Arc<RoutesResources>) -> axum::Router {
    use axum::Router;
    use axum::extract;
    use axum::routing::get;
    Router::new()
        .fallback(|| async { axum::Json(json!({ "description": "ingest ui" } )) })
        .route(
            "/allpaths",
            get({ move || async move { format!("{:?}", daqingest_ui::assets::all_asset_paths()) } }),
        )
        .route(
            "/path1/{*path}",
            get({ move |extract::Path(path): extract::Path<String>| async move { format!("{path:?}") } }),
        )
        .route(
            "/path3{*path}",
            get({ move |extract::Path(path): extract::Path<String>| async move { format!("{path:?}") } }),
        )
        .nest(
            "/path2",
            Router::new().fallback(get({
                move |extract::Path(path): extract::Path<String>| async move { format!("{path:?}") }
            })),
        )
        .nest(
            "/path3",
            Router::new().fallback(get({ move |req: extract::Request| async move { format!("{req:?}") } })),
        )
        .route(
            "/ui1/{*path}",
            get({
                let pre = "/ui1";
                move |extract::Path(path): extract::Path<String>| async move {
                    info!("pre {pre:?}  path {path:?}");
                    match daqingest_ui::assets::get_asset(&format!("{pre}/{path}")) {
                        Some((bytes, mime)) => ([(http::header::CONTENT_TYPE, mime)], bytes).into_response(),
                        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
                    }
                }
            }),
        )
}

fn make_routes_daqingest(
    ca_ingest_ctrls: Arc<dyn CaIngestCtrls>,
    post_ingest_ctrls: Arc<dyn PostIngestCtrls>,
) -> axum::Router {
    use axum::Router;
    use axum::extract;
    use axum::routing::get;
    Router::new()
        .fallback(|| async { axum::Json(json!({ "subcommands": ["channel", "metrics"] } )) })
        .nest(
            "/metrics",
            Router::new().fallback(|| async { StatusCode::NOT_FOUND }).route(
                "/",
                get({
                    let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                    || metrics2(ca_ingest_ctrls)
                }),
            ),
        )
        .nest(
            "/config",
            Router::new().route(
                "/reload",
                get({
                    let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                    || config_reload(ca_ingest_ctrls)
                }),
            ),
        )
        .nest("/channel", make_routes_channel(ca_ingest_ctrls.clone()))
        .nest("/ingest", make_routes_ingest(post_ingest_ctrls.clone()))
        .nest(
            "/private",
            make_routes_daqingest_private(ca_ingest_ctrls.clone(), post_ingest_ctrls.clone()),
        )
        .route("/metricbeat", get({ || async move { metricbeat() } }))
        .nest("/ui", ui::make_routes_daqingest_ui_node())
}

fn make_routes(ca_ingest_ctrls: Arc<dyn CaIngestCtrls>, post_ingest_ctrls: Arc<dyn PostIngestCtrls>) -> axum::Router {
    use axum::Router;
    use axum::extract;
    use axum::routing::{get, post, put};
    use http::StatusCode;
    Router::new()
        .fallback(|req: Request<axum::body::Body>| async move {
            info!("Fallback for {} {}", req.method(), req.uri());
            StatusCode::NOT_FOUND
        })
        .nest("/daqingest", make_routes_daqingest(ca_ingest_ctrls, post_ingest_ctrls))
        .route(
            "/daqingest/always-error/",
            get(|Query(params): Query<HashMap<String, String>>| always_error(params)),
        )
        .route(
            "/daqingest/find/channel",
            get({ |Query(params): Query<HashMap<String, String>>| find_channel(params) }),
        )
        .route(
            "/daqingest/store_workers_rate",
            get({ || async move { axum::Json(123) } }).put({ |v: extract::Json<u64>| async move {} }),
        )
        .route(
            "/daqingest/insert_frac",
            get({ || async move { axum::Json("unused") } })
                .put({ |v: extract::Json<u64>| async move { axum::Json("unused") } }),
        )
        .route(
            "/daqingest/extra_inserts_conf",
            get({ || async move { axum::Json(serde_json::to_value(&"TODO").unwrap()) } })
                .put({ |v: extract::Json<ExtraInsertsConf>| extra_inserts_conf_set(v.0) }),
        )
}

fn make_routes_channel(ca_ingest_ctrls: Arc<dyn CaIngestCtrls>) -> axum::Router {
    use axum::Router;
    use axum::extract;
    use axum::routing::{get, post, put};
    use http::StatusCode;
    Router::new()
        .fallback(|| async { axum::Json(json!({"subcommands":["states"]})) })
        .route(
            "/error_handler_test",
            get({ |Query(params): Query<HashMap<String, String>>| status::error_handler_test() }),
        )
        .route(
            "/states",
            get({
                let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                |Query(params): Query<HashMap<String, String>>| status::channel_states(params, ca_ingest_ctrls)
            }),
        )
        .route(
            "/add",
            get({
                let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                |Query(params): Query<HashMap<String, String>>| channel_add(params, ca_ingest_ctrls)
            }),
        )
        .route(
            "/remove",
            get({
                let ca_ingest_ctrls = ca_ingest_ctrls.clone();
                |Query(params): Query<HashMap<String, String>>| channel_remove(params, ca_ingest_ctrls)
            }),
        )
}

fn make_routes_private_channel(post_ingest_ctrls: Arc<dyn PostIngestCtrls>) -> axum::Router {
    use axum::Router;
    use axum::extract;
    use axum::routing::{get, post, put};
    use http::StatusCode;
    Router::new().route(
        "/delete",
        post({
            let post_ingest_ctrls = post_ingest_ctrls.clone();
            move |(headers, params, body): (HeaderMap, Query<HashMap<String, String>>, axum::body::Body)| {
                delete::delete((headers, params, body), post_ingest_ctrls)
            }
        }),
    )
}

pub async fn metrics_service(
    bind_to: String,
    shutdown_signal: Receiver<u32>,
    ca_ingest_ctrls: Arc<dyn CaIngestCtrls>,
    post_ingest_ctrls: Arc<dyn PostIngestCtrls>,
) -> Result<(), Error> {
    info!("metrics service start  {}", bind_to);
    let addr: SocketAddr = bind_to.parse().map_err(Error::from_string)?;
    let router = make_routes(ca_ingest_ctrls, post_ingest_ctrls)
        .layer(tower_http::compression::CompressionLayer::new().gzip(true))
        .into_make_service();
    let listener = TcpListener::bind(addr).await?;
    // into_make_service_with_connect_info
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown_signal.recv().await;
        })
        .await?;
    info!("-----------------  metrics service done");
    Ok(())
}
