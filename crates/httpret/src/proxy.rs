pub mod api1;
pub mod api4;

use crate::api1::channel_search_configs_v1;
use crate::api1::channel_search_list_v1;
use crate::api1::gather_json_2_v1;
use crate::err::Error;
use crate::gather::gather_get_json_generic;
use crate::gather::SubRes;
use crate::pulsemap::MapPulseQuery;
use crate::response;
use crate::status_board_init;
use crate::Cont;
use futures_util::pin_mut;
use futures_util::Stream;
use futures_util::StreamExt;
use http::Method;
use http::Request;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::body_string;
use httpclient::error_response;
use httpclient::http;
use httpclient::http::header;
use httpclient::read_body_bytes;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamIncoming;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use hyper::service::service_fn;
use hyper::Response;
use hyper_util::rt::TokioIo;
use itertools::Itertools;
use netpod::log::*;
use netpod::query::ChannelStateEventsQuery;
use netpod::req_uri_to_url;
use netpod::AppendToUrl;
use netpod::ChannelConfigQuery;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::ChannelSearchSingleResult;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use netpod::MapQuery;
use netpod::ProxyConfig;
use netpod::ReqCtx;
use netpod::ServiceVersion;
use netpod::APP_JSON;
use netpod::PSI_DAQBUFFER_SERVICE_MARK;
use netpod::X_DAQBUF_REQID;
use query::api4::binned::BinnedQuery;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tracing::Instrument;
use url::Url;

#[cfg(feature = "http3")]
use crate::http3;
use netpod::SeriesConfigQuery;

const DISTRI_PRE: &str = "/distri/";

pub async fn proxy(proxy_config: ProxyConfig, service_version: ServiceVersion) -> Result<(), Error> {
    status_board_init();
    use std::str::FromStr;
    let bind_addr = SocketAddr::from_str(&format!("{}:{}", proxy_config.listen, proxy_config.port))?;
    #[cfg(feature = "http3")]
    let http3 = http3::Http3Support::new_or_dummy(bind_addr.clone()).await?;
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                error!("{e}");
                break;
            }
        };
        debug!("new connection from {addr}");
        let proxy_config = proxy_config.clone();
        let service_version = service_version.clone();
        let io = TokioIo::new(stream);
        tokio::task::spawn(async move {
            let res = hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| the_service_fn(req, addr, proxy_config.clone(), service_version.clone())),
                )
                .await;
            match res {
                Ok(()) => {}
                Err(e) => {
                    error!("{e}");
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
    Ok(())
}

async fn the_service_fn(
    req: Requ,
    addr: SocketAddr,
    proxy_config: ProxyConfig,
    service_version: ServiceVersion,
) -> Result<StreamResponse, Error> {
    let ctx = ReqCtx::new_with_proxy(&req, &proxy_config);
    let reqid_span = span!(Level::INFO, "req", reqid = ctx.reqid());
    let f = proxy_http_service(req, addr, ctx, proxy_config.clone(), service_version.clone());
    let f = Cont { f: Box::pin(f) };
    f.instrument(reqid_span).await
}

async fn proxy_http_service(
    req: Requ,
    addr: SocketAddr,
    ctx: ReqCtx,
    proxy_config: ProxyConfig,
    service_version: ServiceVersion,
) -> Result<StreamResponse, Error> {
    info!(
        "http-request  {:?} - {:?} - {:?} - {:?}",
        addr,
        req.method(),
        req.uri(),
        req.headers()
    );
    match proxy_http_service_try(req, ctx, &proxy_config, &service_version).await {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("data_api_proxy sees error: {:?}", e);
            Err(e)
        }
    }
}

async fn proxy_http_service_try(
    req: Requ,
    ctx: ReqCtx,
    proxy_config: &ProxyConfig,
    service_version: &ServiceVersion,
) -> Result<StreamResponse, Error> {
    let mut res = proxy_http_service_inner(req, &ctx, proxy_config, &service_version).await?;
    let hm = res.headers_mut();
    hm.insert("Access-Control-Allow-Origin", "*".parse().unwrap());
    hm.insert("Access-Control-Allow-Headers", "*".parse().unwrap());
    for m in ctx.marks() {
        hm.append(PSI_DAQBUFFER_SERVICE_MARK, m.parse().unwrap());
    }
    hm.append(PSI_DAQBUFFER_SERVICE_MARK, ctx.mark().parse().unwrap());
    Ok(res)
}

async fn proxy_http_service_inner(
    req: Requ,
    ctx: &ReqCtx,
    proxy_config: &ProxyConfig,
    service_version: &ServiceVersion,
) -> Result<StreamResponse, Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    if path == "/api/1/channels" {
        Ok(channel_search_list_v1(req, ctx, proxy_config).await?)
    } else if path == "/api/1/channels/config" {
        Ok(channel_search_configs_v1(req, ctx, proxy_config).await?)
    } else if path.starts_with("/api/1/gather/") {
        Ok(gather_json_2_v1(req, "/api/1/gather/", proxy_config).await?)
    } else if path == "/api/4/private/version" {
        if req.method() == Method::GET {
            let ret = serde_json::json!({
                "daqbuf_version": {
                    "major": service_version.major,
                    "minor": service_version.minor,
                    "patch": service_version.patch,
                },
            });
            Ok(response(StatusCode::OK).body(ToJsonBody::from(&ret).into_body())?)
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    } else if let Some(h) = api4::StatusNodesRecursive::handler(&req) {
        h.handle(req, ctx, &proxy_config, service_version).await
    } else if let Some(h) = api4::backend::BackendListHandler::handler(&req) {
        h.handle(req, ctx, &proxy_config).await
    } else if let Some(h) = api4::ChannelSearchAggHandler::handler(&req) {
        h.handle(req, ctx, &proxy_config).await
    } else if let Some(h) = api4::events::EventsHandler::handler(&req) {
        h.handle(req, ctx, &proxy_config).await
    } else if path == "/api/4/accounting/ingested" {
        Ok(proxy_backend_query::<MapQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/accounting/toplist/counts" {
        Ok(proxy_backend_query::<MapQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/status/connection/events" {
        Ok(proxy_backend_query::<ChannelStateEventsQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/status/channel/events" {
        Ok(proxy_backend_query::<ChannelStateEventsQuery>(req, ctx, proxy_config).await?)
    } else if path.starts_with("/api/4/map/pulse-v2/") {
        Ok(proxy_backend_query::<MapPulseQuery>(req, ctx, proxy_config).await?)
    } else if path.starts_with("/api/4/map/pulse/") {
        Ok(proxy_backend_query::<MapPulseQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/binned" {
        Ok(proxy_backend_query::<BinnedQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/private/binnedv2" {
        Ok(proxy_backend_query::<BinnedQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/channel/config" {
        Ok(proxy_backend_query::<ChannelConfigQuery>(req, ctx, proxy_config).await?)
    } else if path == "/api/4/series/config" {
        Ok(proxy_backend_query::<SeriesConfigQuery>(req, ctx, proxy_config).await?)
    } else if path.starts_with("/api/4/test/http/204") {
        Ok(response(StatusCode::NO_CONTENT).body(body_string("No Content"))?)
    } else if path.starts_with("/api/4/test/http/400") {
        Ok(response(StatusCode::BAD_REQUEST).body(body_string("Bad Request"))?)
    } else if path.starts_with("/api/4/test/http/405") {
        Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_string("Method Not Allowed"))?)
    } else if path.starts_with("/api/4/test/http/406") {
        Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_string("Not Acceptable"))?)
    } else if path.starts_with("/api/4/test/log/error") {
        error!("{path}");
        Ok(response(StatusCode::OK).body(body_empty())?)
    } else if path.starts_with("/api/4/test/log/warn") {
        warn!("{path}");
        Ok(response(StatusCode::OK).body(body_empty())?)
    } else if path.starts_with("/api/4/test/log/info") {
        info!("{path}");
        Ok(response(StatusCode::OK).body(body_empty())?)
    } else if path.starts_with("/api/4/test/log/debug") {
        debug!("{path}");
        Ok(response(StatusCode::OK).body(body_empty())?)
    } else if let Some(h) = api1::PythonDataApi1Query::handler(&req) {
        h.handle(req, ctx, proxy_config).await
    } else if let Some(h) = api1::reqstatus::RequestStatusHandler::handler(&req) {
        h.handle(req, ctx, proxy_config).await
    } else if path.starts_with(DISTRI_PRE) {
        proxy_distribute_v2(req).await
    } else if let Some(h) = super::api4::docs::DocsHandler::handler(&req) {
        Ok(h.handle(req, ctx).await?)
    } else {
        let headers: std::collections::BTreeMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().map_or(String::new(), |x| x.to_string())))
            .collect();
        let res = serde_json::json!({
            "method": req.method().to_string(),
            "uri": req.uri().to_string(),
            "host": req.uri().host().unwrap_or(""),
            "port":  req.uri().port().map_or(String::new(), |x| x.to_string()),
            "path": req.uri().path(),
            "query": req.uri().query(),
            "headers": headers,
        });
        let body = serde_json::to_string(&res).unwrap_or("{}".into());
        Ok(response(StatusCode::NOT_FOUND).body(body_string(body))?)
    }
}

pub async fn proxy_distribute_v2(req: Requ) -> Result<StreamResponse, Error> {
    let path = req.uri().path();
    if path
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || ['/', '.', '-', '_'].contains(&c))
        && !path.contains("..")
    {}
    if req.method() == Method::GET {
        let s = FileStream {
            file: File::open(format!("/opt/distri/{}", &path[DISTRI_PRE.len()..])).await?,
        };
        Ok(response(StatusCode::OK).body(body_stream(s))?)
    } else {
        Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
    }
}

pub struct FileStream {
    file: File,
}

impl Stream for FileStream {
    type Item = Result<Vec<u8>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut buf = vec![0; 1024 * 8];
        let mut rb = ReadBuf::new(&mut buf);
        let f = &mut self.file;
        pin_mut!(f);
        trace!("poll_read for proxy distri");
        match f.poll_read(cx, &mut rb) {
            Ready(k) => match k {
                Ok(_) => {
                    let n = rb.filled().len();
                    if n == 0 {
                        Ready(None)
                    } else {
                        buf.truncate(n);
                        Ready(Some(Ok(buf)))
                    }
                }
                Err(e) => Ready(Some(Err(e.into()))),
            },
            Pending => Pending,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BackendsResponse {
    backends: Vec<String>,
}

pub async fn backends(_req: Requ, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
    let backends: Vec<_> = proxy_config.backends.iter().map(|k| k.name.to_string()).collect();
    let res = BackendsResponse { backends };
    let ret = response(StatusCode::OK).body(ToJsonBody::from(&res).into_body())?;
    Ok(ret)
}

pub async fn channel_search(req: Requ, ctx: &ReqCtx, proxy_config: &ProxyConfig) -> Result<StreamResponse, Error> {
    let (head, _body) = req.into_parts();
    match head.headers.get(http::header::ACCEPT) {
        Some(v) => {
            if v == APP_JSON {
                let url = req_uri_to_url(&head.uri)?;
                let query = ChannelSearchQuery::from_url(&url)?;
                let mut methods = Vec::new();
                let mut bodies = Vec::new();
                let mut urls = proxy_config
                    .backends
                    .iter()
                    .map(|sh| match Url::parse(&format!("{}/api/4/search/channel", sh.url)) {
                        Ok(mut url) => {
                            query.append_to_url(&mut url);
                            Ok(url)
                        }
                        Err(_e) => Err(Error::with_msg(format!("parse error for: {:?}", sh))),
                    })
                    .fold_ok(vec![], |mut a, x| {
                        a.push(x);
                        methods.push(http::Method::GET);
                        bodies.push(None);
                        a
                    })?;
                // TODO probably no longer needed?
                if let (Some(hosts), Some(backends)) = (None::<&Vec<String>>, None::<&Vec<String>>) {
                    #[derive(Serialize)]
                    struct QueryApi0 {
                        backends: Vec<String>,
                        regex: String,
                        #[serde(rename = "sourceRegex")]
                        source_regex: String,
                        ordering: String,
                        reload: bool,
                    }
                    hosts.iter().zip(backends.iter()).for_each(|(sh, back)| {
                        let url = Url::parse(&format!("{}/channels/config", sh)).unwrap();
                        urls.push(url);
                        let q = QueryApi0 {
                            backends: vec![back.into()],
                            ordering: "asc".into(),
                            reload: false,
                            regex: query.name_regex.clone(),
                            source_regex: query.source_regex.clone(),
                        };
                        let qs = serde_json::to_string(&q).unwrap();
                        methods.push(http::Method::POST);
                        bodies.push(Some(qs));
                    });
                }
                let tags = urls.iter().map(|k| k.to_string()).collect();
                // let nt = |tag: String, res: Response<hyper::body::Incoming>| {
                fn fn_nt(
                    tag: String,
                    res: Response<hyper::body::Incoming>,
                ) -> Pin<Box<dyn Future<Output = Result<SubRes<ChannelSearchResult>, Error>> + Send>> {
                    let fut = async {
                        let (_head, body) = res.into_parts();
                        let body = read_body_bytes(body).await?;
                        //info!("got a result {:?}", body);
                        let res: SubRes<ChannelSearchResult> =
                            match serde_json::from_slice::<ChannelSearchResult>(&body) {
                                Ok(val) => {
                                    let ret = SubRes {
                                        tag,
                                        status: StatusCode::OK,
                                        val,
                                    };
                                    ret
                                }
                                Err(_) => {
                                    #[derive(Deserialize)]
                                    struct ResItemApi0 {
                                        name: String,
                                        source: String,
                                        backend: String,
                                        #[serde(rename = "type")]
                                        ty: String,
                                    }
                                    #[derive(Deserialize)]
                                    struct ResContApi0 {
                                        #[allow(dead_code)]
                                        backend: String,
                                        channels: Vec<ResItemApi0>,
                                    }
                                    match serde_json::from_slice::<Vec<ResContApi0>>(&body) {
                                        Ok(k) => {
                                            let mut a = vec![];
                                            if let Some(g) = k.first() {
                                                for c in &g.channels {
                                                    let z = ChannelSearchSingleResult {
                                                        backend: c.backend.clone(),
                                                        description: String::new(),
                                                        name: c.name.clone(),
                                                        // TODO api 0 does not provide a series id
                                                        series: 0,
                                                        shape: vec![],
                                                        source: c.source.clone(),
                                                        ty: c.ty.clone(),
                                                        unit: String::new(),
                                                        is_api_0: Some(true),
                                                    };
                                                    a.push(z);
                                                }
                                            }
                                            let ret = ChannelSearchResult { channels: a };
                                            let ret = SubRes {
                                                tag,
                                                status: StatusCode::OK,
                                                val: ret,
                                            };
                                            ret
                                        }
                                        Err(_) => {
                                            error!("Channel search response parse failed");
                                            let ret = ChannelSearchResult { channels: vec![] };
                                            let ret = SubRes {
                                                tag,
                                                status: StatusCode::OK,
                                                val: ret,
                                            };
                                            ret
                                        }
                                    }
                                }
                            };
                        Ok(res)
                    };
                    Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
                }
                let ft = |all: Vec<(crate::gather::Tag, Result<SubRes<ChannelSearchResult>, Error>)>| {
                    let mut res = Vec::new();
                    for (_tag, j) in all {
                        match j {
                            Ok(j) => {
                                for k in j.val.channels {
                                    res.push(k);
                                }
                            }
                            Err(e) => {
                                warn!("{e}");
                            }
                        }
                    }
                    let res = ChannelSearchResult { channels: res };
                    let res = response(StatusCode::OK)
                        .header(http::header::CONTENT_TYPE, APP_JSON)
                        .body(ToJsonBody::from(&res).into_body())?;
                    Ok(res)
                };
                // TODO gather_get_json_generic must for this case accept a Method for each Request.
                // Currently it is inferred via presence of the body.
                // On the other hand, I want to gather over rather homogeneous requests.
                // So: better enforce same method.
                let ret = gather_get_json_generic(
                    http::Method::GET,
                    urls,
                    bodies,
                    tags,
                    fn_nt,
                    ft,
                    Duration::from_millis(3000),
                    ctx,
                )
                .await?;
                Ok(ret)
            } else {
                Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?)
            }
        }
        None => Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?),
    }
}

pub async fn proxy_backend_query<QT>(
    req: Requ,
    ctx: &ReqCtx,
    proxy_config: &ProxyConfig,
) -> Result<StreamResponse, Error>
where
    QT: fmt::Debug + FromUrl + AppendToUrl + HasBackend + HasTimeout,
{
    let url = req_uri_to_url(req.uri())?;
    let query = match QT::from_url(&url) {
        Ok(k) => k,
        Err(_) => {
            let msg = format!("malformed request or missing parameters  {}", req.uri());
            warn!("{msg}");
            return Ok(error_response(msg, ctx.reqid()));
        }
    };
    trace!("proxy_backend_query  {:?}  {:?}", query, req.uri());
    let timeout = Duration::from_millis(1000 * 30);
    let query = query;
    let backend = query.backend();
    let uri_path = proxy_backend_query_helper_uri_path(req.uri().path(), &url);
    trace!("uri_path {uri_path}");
    let query_host = get_query_host_for_backend(backend, proxy_config)?;
    let mut url = Url::parse(&format!("{}{}", query_host, uri_path))?;
    trace!("query_host {query_host}  uri_path {uri_path}  query {query:?}");
    query.append_to_url(&mut url);
    trace!("url {:?}", url);
    proxy_backend_query_inner(req.headers(), url, timeout, ctx, proxy_config).await
}

fn proxy_backend_query_helper_uri_path(path: &str, url: &Url) -> String {
    // TODO remove this special case
    // SPECIAL CASE:
    // Since the inner proxy is not yet handling map-pulse requests without backend,
    // we can not simply copy the original url here.
    // Instead, url needs to get parsed and formatted.
    // In general, the caller of this function should be able to provide a url, or maybe
    // better a closure so that the url can even depend on backend.
    if url.as_str().contains("/map/pulse/") {
        match MapPulseQuery::from_url(&url) {
            Ok(qu) => {
                debug!("qu {qu:?}");
                format!("/api/4/map/pulse/{}/{}", qu.backend, qu.pulse)
            }
            Err(e) => {
                error!("{e:?}");
                String::from("/BAD")
            }
        }
    } else {
        path.into()
    }
}

pub async fn proxy_backend_query_inner(
    headers: &http::HeaderMap,
    url: Url,
    timeout: Duration,
    ctx: &ReqCtx,
    _proxy_config: &ProxyConfig,
) -> Result<StreamResponse, Error> {
    let host = url
        .host_str()
        .ok_or_else(|| Error::with_msg_no_trace("no host in url"))?;
    let mut req2 = Request::builder()
        .method(http::Method::GET)
        .uri(url.to_string())
        .header(header::HOST, host)
        .header(X_DAQBUF_REQID, ctx.reqid());
    {
        let hs = req2
            .headers_mut()
            .ok_or_else(|| Error::with_msg_no_trace("can not set headers"))?;
        for (hn, hv) in headers {
            if hn == header::HOST {
            } else if hn == X_DAQBUF_REQID {
            } else {
                hs.append(hn, hv.clone());
            }
        }
    }

    debug!("send request {req2:?}");
    let req2 = req2.body(body_empty())?;

    let fut = async move {
        debug!(
            "requesting  {:?}  {:?}  {:?}",
            req2.method(),
            req2.uri(),
            req2.headers()
        );
        let mut send_req = httpclient::httpclient::connect_client(req2.uri()).await?;
        let res = send_req.send_request(req2).await?;
        Ok::<_, Error>(res)
    };

    match tokio::time::timeout(timeout, fut).await {
        Ok(res) => {
            let res = res?;
            use bytes::Bytes;
            use httpclient::http_body::Frame;
            use httpclient::BodyError;
            let (head, body) = res.into_parts();
            let body = StreamIncoming::new(body);
            let body = body.map(|x| x.map(Frame::data));
            let body: Pin<Box<dyn Stream<Item = Result<Frame<Bytes>, BodyError>> + Send>> = Box::pin(body);
            let body = http_body_util::StreamBody::new(body);
            let ret = Response::from_parts(head, body);
            Ok(ret)
        }
        Err(_) => Ok(httpclient::error_status_response(
            StatusCode::REQUEST_TIMEOUT,
            format!("request timed out at proxy, limit {} ms", timeout.as_millis() as u64),
            ctx.reqid(),
        )),
    }
}

fn get_query_host_for_backend(backend: &str, proxy_config: &ProxyConfig) -> Result<String, Error> {
    for back in &proxy_config.backends {
        if back.name == backend {
            return Ok(back.url.clone());
        }
    }
    return Err(Error::with_msg(format!("host not found for backend {:?}", backend)));
}
