#![recursion_limit = "256"]

mod api1;
mod api4;
mod ui;

pub mod bodystream;
pub mod cache;
pub mod channel_status;
pub mod channelconfig;
pub mod download;
pub mod err;
pub mod gather;
#[cfg(feature = "prometheus_endpoint")]
pub mod prometheus;
pub mod proxy;
pub mod pulsemap;
pub mod requests;
pub mod settings;

#[cfg(feature = "http3")]
pub mod http3;
#[cfg(not(feature = "http3"))]
pub mod http3_dummy;
#[cfg(not(feature = "http3"))]
use http3_dummy as http3;

use crate::bodystream::response;
use crate::err::Error;
use daqbuf_err;
use dbconn::worker::PgQueue;
use dbconn::worker::PgWorker;
use futures_util::Future;
use futures_util::FutureExt;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use net::SocketAddr;
use netpod::log::*;
use netpod::query::prebinned::PreBinnedQuery;
use netpod::req_uri_to_url;
use netpod::status_board;
use netpod::status_board_init;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::ServiceVersion;
use panic::AssertUnwindSafe;
use panic::UnwindSafe;
use pin::Pin;
use scyllaconn::worker::ScyllaQueue;
use scyllaconn::worker::ScyllaWorker;
use std::net;
use std::panic;
use std::pin;
use std::sync::Arc;
use std::task;
use task::Context;
use task::Poll;
use taskrun::tokio;
use taskrun::tokio::net::TcpListener;
use tracing::Instrument;

autoerr::create_error_v1!(
    name(RetrievalError, "Retrieval"),
    enum variants {
        Error(#[from] daqbuf_err::Error),
        Error2(#[from] crate::err::Error),
        TextError(String),
        Hyper(#[from] hyper::Error),
        Http(#[from] http::Error),
        Serde(#[from] serde_json::Error),
        Fmt(#[from] std::fmt::Error),
        Url(#[from] url::ParseError),
        Netpod(#[from] netpod::Error),
        Http3Support(#[from] crate::http3::Error),
    },
);

trait IntoBoxedError: std::error::Error {}
impl IntoBoxedError for net::AddrParseError {}
impl IntoBoxedError for tokio::task::JoinError {}
impl IntoBoxedError for api4::databuffer_tools::FindActiveError {}
impl IntoBoxedError for std::string::FromUtf8Error {}
impl IntoBoxedError for std::io::Error {}
impl IntoBoxedError for dbconn::worker::Error {}
impl IntoBoxedError for netpod::UriError {}
impl IntoBoxedError for crate::api4::binned::Error {}

impl<E> From<E> for RetrievalError
where
    E: ToString + IntoBoxedError,
{
    fn from(value: E) -> Self {
        Self::TextError(value.to_string())
    }
}

impl daqbuf_err::ToErr for RetrievalError {
    fn to_err(self) -> daqbuf_err::Error {
        daqbuf_err::Error::with_msg_no_trace(self.to_string())
    }
}

pub struct ServiceSharedResources {
    pgqueue: PgQueue,
    scyqueue: Option<ScyllaQueue>,
}

impl ServiceSharedResources {
    pub fn new(pgqueue: PgQueue, scyqueue: Option<ScyllaQueue>) -> Self {
        Self { pgqueue, scyqueue }
    }
}

pub async fn host(ncc: NodeConfigCached, service_version: ServiceVersion) -> Result<(), RetrievalError> {
    status_board_init();
    #[cfg(feature = "prometheus_endpoint")]
    if let Some(bind) = ncc.node.prometheus_api_bind {
        tokio::spawn(prometheus::host(bind));
    }
    // let rawjh = taskrun::spawn(nodenet::conn::events_service(node_config.clone()));
    let (pgqueue,) = PgWorker::new(&ncc.node_config.cluster.database).await?;
    let scyqueue = if let (Some(st), Some(mt), Some(lt)) = (
        ncc.node_config.cluster.scylla_st(),
        ncc.node_config.cluster.scylla_mt(),
        ncc.node_config.cluster.scylla_lt(),
    ) {
        // TODO use
        let (scyqueue, scylla_worker_jh) = ScyllaWorker::new(
            st.clone(),
            mt.clone(),
            lt.clone(),
            ncc.node_config.cluster.scylla_clusters(),
        )
        .await
        .map_err(|e| {
            error!("{e}");
            RetrievalError::TextError(e.to_string())
        })?;
        // TODO await the task
        let _ = scylla_worker_jh;
        Some(scyqueue)
    } else {
        None
    };
    let shared_res = ServiceSharedResources::new(pgqueue, scyqueue);
    let shared_res = Arc::new(shared_res);
    use std::str::FromStr;
    let bind_addr = SocketAddr::from_str(&format!("{}:{}", ncc.node.listen(), ncc.node.port))?;
    #[cfg(feature = "http3")]
    let http3 = http3::Http3Support::new_or_dummy(bind_addr.clone()).await?;
    // tokio::net::TcpSocket::new_v4()?.listen(200)?
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (stream, addr) = if let Ok(x) = listener.accept().await {
            x
        } else {
            break;
        };
        debug!("new connection from {}", addr);
        let node_config = ncc.clone();
        let service_version = service_version.clone();
        let io = TokioIo::new(stream);
        let shared_res = shared_res.clone();
        tokio::task::spawn(async move {
            let res = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, {
                    let conn_req_cnt = std::sync::RwLock::new(0u64);
                    service_fn(move |req| {
                        let n = conn_req_cnt.write().map_or(u64::MAX, |mut x| {
                            let n = *x;
                            *x += 1;
                            n
                        });
                        service_request(
                            req,
                            addr,
                            n,
                            node_config.clone(),
                            service_version.clone(),
                            shared_res.clone(),
                        )
                    })
                })
                .await;
            match res {
                Ok(()) => {}
                Err(e) => {
                    if e.is_body_write_aborted() {
                        info!("http conn body write abort: {}", e)
                    } else if e.is_closed() {
                        info!("http conn close: {}", e)
                    } else if e.is_canceled() {
                        info!("http conn cancel: {}", e)
                    } else if e.is_timeout() {
                        info!("http conn timeout: {}", e)
                    } else {
                        warn!("error from serve_connection: {}", e);
                    }
                }
            }
        });
    }
    info!("http service done");
    #[cfg(feature = "http3")]
    {
        let _x: () = http3.wait_idle().await;
    }
    info!("http host done");
    // rawjh.await??;
    Ok(())
}

async fn service_request(
    req: Requ,
    addr: SocketAddr,
    n: u64,
    node_config: NodeConfigCached,
    service_version: ServiceVersion,
    shared_res: Arc<ServiceSharedResources>,
) -> Result<StreamResponse, Error> {
    let ctx = ReqCtx::new_with_node(&req, &node_config);
    let reqid = ctx.reqid().into();
    let fut = the_service_fn_log_enabled(ctx, req, addr, n, node_config, service_version, shared_res);
    streams::logqueue::LogQueueFutureWrap::new(reqid, fut).await
}

async fn the_service_fn_log_enabled(
    ctx: ReqCtx,
    req: Requ,
    addr: SocketAddr,
    n: u64,
    node_config: NodeConfigCached,
    service_version: ServiceVersion,
    shared_res: Arc<ServiceSharedResources>,
) -> Result<StreamResponse, Error> {
    let reqid_span = span!(Level::INFO, "req", reqid = ctx.reqid());
    let f = http_service(req, addr, n, ctx, node_config, service_version, shared_res);
    let f = Cont { f: Box::pin(f) };
    f.instrument(reqid_span).await
}

async fn http_service(
    req: Requ,
    addr: SocketAddr,
    n: u64,
    ctx: ReqCtx,
    node_config: NodeConfigCached,
    service_version: ServiceVersion,
    shared_res: Arc<ServiceSharedResources>,
) -> Result<StreamResponse, Error> {
    info!(
        "http-request  {:?} - n:{} - {:?} - {:?} - {:?}",
        addr,
        n,
        req.method(),
        req.uri(),
        req.headers()
    );
    match http_service_try(req, ctx, &node_config, &service_version, shared_res).await {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("daqbuffer node http_service sees error from http_service_try: {}", e);
            Err(e)
        }
    }
}

// TODO move this and related stuff to separate module
struct Cont<F> {
    f: Pin<Box<F>>,
}

impl<F, I> Future for Cont<F>
where
    F: Future<Output = Result<I, Error>>,
{
    type Output = <F as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let h = std::panic::catch_unwind(AssertUnwindSafe(|| self.f.poll_unpin(cx)));
        match h {
            Ok(k) => k,
            Err(e) => {
                if let Some(v) = e.downcast_ref::<Error>() {
                    error!("Cont<F>  catch_unwind  is Error: {}", v);
                } else if let Some(v) = e.downcast_ref::<String>() {
                    error!("Cont<F>  catch_unwind  is String: {}", v);
                } else if let Some(v) = e.downcast_ref::<&str>() {
                    error!("Cont<F>  catch_unwind  is &str: {}", v);
                } else {
                    error!("Cont<F>  catch_unwind  {:?}", e);
                }
                Poll::Ready(Err(Error::with_msg_no_trace(format!("{:?}", e))))
            }
        }
    }
}

impl<F> UnwindSafe for Cont<F> {}

async fn http_service_try(
    req: Requ,
    ctx: ReqCtx,
    node_config: &NodeConfigCached,
    service_version: &ServiceVersion,
    shared_res: Arc<ServiceSharedResources>,
) -> Result<StreamResponse, Error> {
    use http::HeaderValue;
    let mut urlmarks = Vec::new();
    urlmarks.push(format!("{}:{}", req.method(), req.uri()));
    for (k, v) in req.headers() {
        if false {
            if k == netpod::PSI_DAQBUFFER_SEEN_URL {
                let s = String::from_utf8_lossy(v.as_bytes());
                urlmarks.push(s.into());
            }
        }
    }
    let mut res = http_service_inner(req, &ctx, node_config, service_version, shared_res).await?;
    let hm = res.headers_mut();
    if false {
        let asterisk = HeaderValue::from_static("*");
        hm.append(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, asterisk.clone());
        hm.append(http::header::ACCESS_CONTROL_ALLOW_HEADERS, asterisk);
    }
    for m in ctx.marks() {
        hm.append(netpod::PSI_DAQBUFFER_SERVICE_MARK, m.parse().unwrap());
    }
    hm.append(netpod::PSI_DAQBUFFER_SERVICE_MARK, ctx.mark().parse().unwrap());
    if false {
        for s in urlmarks {
            let v = HeaderValue::from_str(&s).unwrap_or_else(|_| HeaderValue::from_static("invalid"));
            hm.append(netpod::PSI_DAQBUFFER_SEEN_URL, v);
        }
    }
    Ok(res)
}

async fn http_service_inner(
    req: Requ,
    ctx: &ReqCtx,
    node_config: &NodeConfigCached,
    service_version: &ServiceVersion,
    shared_res: Arc<ServiceSharedResources>,
) -> Result<StreamResponse, RetrievalError> {
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/4/private/version" {
        if req.method() == Method::GET {
            let ret = serde_json::json!({
                "daqbuf_version": {
                    "major": service_version.major,
                    "minor": service_version.minor,
                    "patch": service_version.patch,
                    "full": service_version.to_string(),
                },
            });
            Ok(response(StatusCode::OK).body(ToJsonBody::from(&ret).into_body())?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    } else if path.starts_with("/api/4/private/logtest/") {
        if req.method() == Method::GET {
            if path.ends_with("/trace") {
                trace!("test trace log output");
            } else if path.ends_with("/debug") {
                debug!("test debug log output");
            } else if path.ends_with("/info") {
                info!("test info log output");
            } else if path.ends_with("/warn") {
                warn!("test warn log output");
            } else if path.ends_with("/error") {
                error!("test error log output");
            } else if path.ends_with("/mixed") {
                warn!("test warn log output");
                let sp_info = span!(Level::INFO, "sp_info", f1 = "v1");
                sp_info.in_scope(|| {
                    warn!("test warn log output in sp_info");
                    info!("test info log output in sp_info");
                    let sp_debug = span!(Level::DEBUG, "sp_debug", f1 = "v1");
                    sp_debug.in_scope(|| {
                        info!("test info log output in sp_info:sp_debug");
                        debug!("test debug log output in sp_info:sp_debug");
                    });
                });
            } else {
                error!("test unknown log output");
            }
            Ok(response(StatusCode::OK).body(body_empty())?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    } else if path.starts_with("/api/4/private/trigger/13/") {
        taskrun::trigger_error_worker_scylla_events(Some(true));
        Ok(response(StatusCode::OK).body(body_empty())?)
    } else if let Some(h) = api4::binwriteindex::BinWriteIndexHandler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::binned_v2::indexday1::IndexDay1::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::binned_v2::singleday::Singleday::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::binned_v2::manualparams::Manualparams::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::binned_v2::full_test1::BinnedV2Handler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::eventdata::EventDataHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config, shared_res)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?)
    } else if let Some(h) = api4::dyncmd::DynCmdHandler::handler(&req) {
        h.handle(req, ctx, &shared_res, &node_config).await
    } else if let Some(h) = api4::backend::BackendListHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config, service_version).await?)
    } else if let Some(h) = api4::status::StatusNodesRecursive::handler(&req) {
        Ok(h.handle(req, ctx, &node_config, service_version).await?)
    } else if let Some(h) = StatusBoardAllHandler::handler(&req) {
        h.handle(req, &node_config).await
    } else if let Some(h) = api4::databuffer_tools::FindActiveHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api4::search::ChannelSearchHandler::handler(&req) {
        Ok(h.handle(req, &shared_res.pgqueue, &node_config).await?)
    } else if let Some(h) = channel_status::ConnectionStatusEvents::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = channel_status::ChannelStatusEventsHandler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::events::EventsHandler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::binned::BinnedHandler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = channelconfig::ChannelConfigQuorumHandler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res.pgqueue, &node_config).await?)
    } else if let Some(h) = channelconfig::ChannelConfigsHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ChannelConfigHandler::handler(&req) {
        Ok(h.handle(req, &shared_res.pgqueue, &node_config).await?)
    } else if let Some(h) = channelconfig::SeriesConfigHandler::handler(&req) {
        Ok(h.handle(req, &shared_res.pgqueue, &node_config).await?)
    } else if let Some(h) = channelconfig::IocForChannel::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ScyllaChannelsActive::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = channelconfig::ScyllaSeriesTsMsp::handler(&req) {
        Ok(h.handle(req, &shared_res, &node_config).await?)
    } else if let Some(h) = channelconfig::AmbigiousChannelNames::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api4::accounting::AccountingIngested::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::accounting::AccountingToplistCounts::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if path == "/api/4/prebinned" {
        if req.method() == Method::GET {
            Ok(prebinned(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    } else if path == "/api/4/random/channel" {
        if req.method() == Method::GET {
            Ok(random_channel(req, ctx, &node_config).await?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    } else if let Some(h) = api4::maintenance::UpdateDbWithChannelNamesHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if let Some(h) = api4::maintenance::UpdateDbWithAllChannelConfigsHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if let Some(h) = api4::maintenance::UpdateSearchCacheHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if let Some(h) = download::DownloadHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = settings::SettingsThreadsMaxHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api1::Api1EventsBinaryHandler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseScyllaHandler::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::IndexChannelHttpFunction::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if let Some(h) = pulsemap::MarkClosedHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseLocalHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseHistoHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::MapPulseHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::Api4MapPulse2HttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = pulsemap::Api4MapPulseHttpFunction::handler(&req) {
        Ok(h.handle(req, &node_config).await?)
    } else if let Some(h) = api1::RequestStatusHandler::handler(&req) {
        Ok(h.handle(req, ctx, &node_config).await?)
    } else if let Some(h) = api4::datasearch::DataSearch::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::docs::DocsHandler::handler(&req) {
        Ok(h.handle(req, ctx).await?)
    } else if let Some(h) = ui::UiHandler::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else if let Some(h) = api4::clks::ClKsInfo::handler(&req) {
        Ok(h.handle(req, ctx, &shared_res, &node_config).await?)
    } else {
        use std::fmt::Write;
        let mut body = String::new();
        let out = &mut body;
        write!(out, "<pre>\n")?;
        write!(out, "METHOD {:?}<br>\n", req.method())?;
        write!(out, "URI    {:?}<br>\n", req.uri())?;
        write!(out, "HOST   {:?}<br>\n", req.uri().host())?;
        write!(out, "PORT   {:?}<br>\n", req.uri().port())?;
        write!(out, "PATH   {:?}<br>\n", req.uri().path())?;
        write!(out, "QUERY  {:?}<br>\n", req.uri().query())?;
        for (hn, hv) in req.headers() {
            write!(out, "HEADER {hn:?}: {hv:?}<br>\n")?;
        }
        write!(out, "</pre>\n")?;
        Ok(response(StatusCode::NOT_FOUND).body(body_string(body))?)
    }
}

pub struct StatusBoardAllHandler {}

impl StatusBoardAllHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/status/board/all" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, _req: Requ, _node_config: &NodeConfigCached) -> Result<StreamResponse, RetrievalError> {
        let sb = status_board().unwrap();
        let buf = serde_json::to_string(std::ops::Deref::deref(&sb)).unwrap();
        let res = response(StatusCode::OK).body(body_string(buf))?;
        Ok(res)
    }
}

async fn prebinned(req: Requ, ctx: &ReqCtx, node_config: &NodeConfigCached) -> Result<StreamResponse, RetrievalError> {
    match prebinned_inner(req, ctx, node_config).await {
        Ok(ret) => Ok(ret),
        Err(e) => {
            error!("fn prebinned: {e:?}");
            Ok(response(StatusCode::BAD_REQUEST).body(body_string(format!("[prebinned-error]")))?)
        }
    }
}

async fn prebinned_inner(
    req: Requ,
    _ctx: &ReqCtx,
    _node_config: &NodeConfigCached,
) -> Result<StreamResponse, RetrievalError> {
    let (head, _body) = req.into_parts();
    let url = req_uri_to_url(&head.uri)?;
    let query = PreBinnedQuery::from_url(&url)?;
    let span1 = span!(Level::INFO, "httpret::prebinned", desc = &query.patch().span_desc());
    span1.in_scope(|| {
        debug!("begin");
    });
    error!("TODO httpret prebinned_inner");
    //let fut = disk::binned::prebinned::pre_binned_bytes_for_http(node_config, &query).instrument(span1);
    todo!()
}

async fn random_channel(
    req: Requ,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<StreamResponse, RetrievalError> {
    let (_head, _body) = req.into_parts();
    let ret = dbconn::random_channel(node_config).await?;
    let ret = response(StatusCode::OK).body(body_string(ret))?;
    Ok(ret)
}

pub async fn test_log() {
    error!("------");
    warn!("------");
    info!("------");
    debug!("------");
    trace!("------");
    scyllaconn::test_log().await;
}
