use crate::bodystream::response;
use crate::channelconfig::ch_conf_from_binned;
use crate::requests::accepts_cbor_framed;
use crate::requests::accepts_json_framed;
use crate::requests::accepts_json_or_all;
use crate::requests::accepts_octets;
use crate::ServiceSharedResources;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use http::header::CONTENT_TYPE;
use http::request::Parts;
use http::Method;
use http::StatusCode;
use httpclient::bad_request_response;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::error_response;
use httpclient::error_status_response;
use httpclient::not_found_response;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log;
use netpod::req_uri_to_url;
use netpod::timeunits::SEC;
use netpod::ChannelTypeConfigGen;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::UseScylla6Workarounds;
use netpod::APP_CBOR_FRAMED;
use netpod::APP_JSON;
use netpod::APP_JSON_FRAMED;
use netpod::HEADER_NAME_REQUEST_ID;
use nodenet::client::OpenBoxedBytesViaHttp;
use nodenet::scylla::ScyllaEventReadProvider;
use query::api4::binned::BinnedQuery;
use scyllaconn::worker::ScyllaQueue;
use std::pin::Pin;
use std::sync::Arc;
use streams::collect::CollectResult;
use streams::eventsplainreader::DummyCacheReadProvider;
use streams::eventsplainreader::SfDatabufferEventReadProvider;
use streams::streamtimeout::StreamTimeout2;
use streams::timebin::cached::reader::EventsReadProvider;
use streams::timebin::CacheReadProvider;
use tracing::Instrument;
use tracing::Span;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }
macro_rules! log_query { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "Api4Binned"),
    enum variants {
        ChannelNotFound,
        BadQuery(String),
        HttpLib(#[from] http::Error),
        ChannelConfig(crate::channelconfig::Error),
        Retrieval(#[from] crate::RetrievalError),
        EventsCbor(#[from] streams::plaineventscbor::Error),
        EventsJson(#[from] streams::plaineventsjson::Error),
        ServerError,
        BinnedStream(err::Error),
        TimebinnedJson(#[from] streams::timebinnedjson::Error),
    },
);

impl From<crate::channelconfig::Error> for Error {
    fn from(value: crate::channelconfig::Error) -> Self {
        use crate::channelconfig::Error::*;
        match value {
            NotFound(_) => Self::ChannelNotFound,
            _ => Self::ChannelConfig(value),
        }
    }
}

pub struct BinnedHandler {}

impl BinnedHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/binned" {
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
        if req.method() != Method::GET {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?);
        }
        match binned(req, ctx, &shared_res.pgqueue, shared_res.scyqueue.clone(), ncc).await {
            Ok(ret) => Ok(ret),
            Err(e) => match e {
                Error::ChannelNotFound => {
                    let res = not_found_response("channel not found".into(), ctx.reqid());
                    Ok(res)
                }
                Error::BadQuery(msg) => {
                    let res = bad_request_response(msg, ctx.reqid());
                    Ok(res)
                }
                _ => {
                    error!("EventsHandler sees: {}", e);
                    Ok(error_response(e.to_string(), ctx.reqid()))
                }
            },
        }
    }
}

async fn binned(
    req: Requ,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    scyqueue: Option<ScyllaQueue>,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let url = req_uri_to_url(req.uri()).map_err(|e| Error::BadQuery(e.to_string()))?;
    if req
        .uri()
        .path_and_query()
        .map_or(false, |x| x.as_str().contains("DOERR"))
    {
        Err(Error::ServerError)?;
    }
    let reqid = ctx.reqid();
    let (head, _body) = req.into_parts();
    let query = BinnedQuery::from_url(&url).map_err(|e| {
        error!("binned_cbor_framed: {}", e);
        Error::BadQuery(e.to_string())
    })?;
    let logspan = if query.log_level() == "trace" {
        trace!("enable trace for handler");
        tracing::span!(tracing::Level::INFO, "log_span_trace")
    } else if query.log_level() == "debug" {
        debug!("enable debug for handler");
        tracing::span!(tracing::Level::INFO, "log_span_debug")
    } else {
        tracing::Span::none()
    };
    let span1 = log::span!(
        log::Level::INFO,
        "httpret::binned",
        reqid,
        beg = query.range().beg_u64() / SEC,
        end = query.range().end_u64() / SEC,
        ch = query.channel().name(),
    );
    span1.in_scope(|| {
        debug!("binned begin  {:?}", query);
    });
    binned_instrumented(head, ctx, query, pgqueue, scyqueue, ncc, logspan.clone())
        .instrument(logspan)
        .instrument(span1)
        .await
}

async fn binned_instrumented(
    head: Parts,
    ctx: &ReqCtx,
    query: BinnedQuery,
    pgqueue: &PgQueue,
    scyqueue: Option<ScyllaQueue>,
    ncc: &NodeConfigCached,
    logspan: Span,
) -> Result<StreamResponse, Error> {
    let res2 = HandleRes2::new(ctx, logspan, query.clone(), pgqueue, scyqueue, ncc).await?;
    {
        let obj = serde_json::json!({
            "query_type": "binned",
            "channel": {
                "name": res2.ch_conf.name(),
                "series": res2.ch_conf.series(),
            }
        });
        let js = serde_json::to_string(&obj).unwrap();
        info!("api4-request-binned {js}");
    }
    if accepts_cbor_framed(&head.headers) {
        Ok(binned_cbor_framed(res2, ctx, ncc).await?)
    } else if accepts_json_framed(&head.headers) {
        Ok(binned_json_framed(res2, ctx, ncc).await?)
    } else if accepts_json_or_all(&head.headers) {
        Ok(binned_json_single(res2, ctx, ncc).await?)
    } else if accepts_octets(&head.headers) {
        Ok(error_response(
            format!("binary binned data not yet available"),
            ctx.reqid(),
        ))
    } else {
        let ret = error_response(format!("Unsupported Accept: {:?}", &head.headers), ctx.reqid());
        Ok(ret)
    }
}

fn make_read_provider(
    chname: &str,
    use_scylla6_workarounds: UseScylla6Workarounds,
    scyqueue: Option<ScyllaQueue>,
    open_bytes: Pin<Arc<OpenBoxedBytesViaHttp>>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> (Arc<dyn EventsReadProvider>, Arc<dyn CacheReadProvider>) {
    let events_read_provider = if chname.starts_with("unittest") {
        let x = streams::teststream::UnitTestStream::new();
        Arc::new(x)
    } else if ncc.node_config.cluster.scylla_lt().is_some() {
        scyqueue
            .clone()
            .map(|qu| ScyllaEventReadProvider::new(qu, use_scylla6_workarounds.clone()))
            .map(|x| Arc::new(x) as Arc<dyn EventsReadProvider>)
            .expect("scylla queue")
    } else if ncc.node.sf_databuffer.is_some() {
        // TODO do not clone the request. Pass an Arc up to here.
        let x = SfDatabufferEventReadProvider::new(Arc::new(ctx.clone()), open_bytes);
        Arc::new(x)
    } else {
        panic!("unexpected backend")
    };
    let cache_read_provider = if ncc.node_config.cluster.scylla_lt().is_some() {
        scyqueue
            .clone()
            .map(|qu| scyllaconn::bincache::ScyllaPrebinnedReadProvider::new(use_scylla6_workarounds, qu))
            .map(|x| Arc::new(x) as Arc<dyn CacheReadProvider>)
            .expect("scylla queue")
    } else if ncc.node.sf_databuffer.is_some() {
        let x = DummyCacheReadProvider::new();
        Arc::new(x)
    } else {
        panic!("unexpected backend")
    };
    (events_read_provider, cache_read_provider)
}

async fn binned_json_single(
    res2: HandleRes2<'_>,
    ctx: &ReqCtx,
    _ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    // TODO unify with binned_json_framed
    debug!("binned_json_single");
    let res = streams::timebinnedjson::timebinned_json(
        res2.query,
        res2.ch_conf,
        ctx,
        res2.cache_read_provider,
        res2.events_read_provider,
        res2.timeout_provider,
    )
    .await?;
    match res {
        CollectResult::Some(item) => {
            let ret = response(StatusCode::OK)
                .header(CONTENT_TYPE, APP_JSON)
                .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
                .body(ToJsonBody::from(item.into_bytes()).into_body())?;
            Ok(ret)
        }
        CollectResult::Empty => {
            let ret = error_status_response(StatusCode::NO_CONTENT, format!("no content"), ctx.reqid());
            Ok(ret)
        }
        CollectResult::Timeout => {
            let ret = error_status_response(
                StatusCode::GATEWAY_TIMEOUT,
                format!("no content within timeout"),
                ctx.reqid(),
            );
            Ok(ret)
        }
    }
}

async fn binned_json_framed(
    res2: HandleRes2<'_>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    debug!("binned_json_framed");
    let use_scylla6_workarounds = res2
        .query
        .use_scylla6_workarounds()
        .map(From::from)
        .unwrap_or(ncc.node_config.cluster.use_scylla6_workarounds());
    log_query!("binned_json_framed  {:?}  {:?}", res2.query, use_scylla6_workarounds);
    // TODO handle None case better and return 404
    let ch_conf = ch_conf_from_binned(&res2.query, ctx, res2.pgqueue, ncc)
        .await?
        .ok_or_else(|| Error::ChannelNotFound)?;
    let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
    let (events_read_provider, cache_read_provider) = make_read_provider(
        ch_conf.name(),
        use_scylla6_workarounds.clone(),
        res2.scyqueue,
        open_bytes,
        ctx,
        ncc,
    );
    let timeout_provider = streamio::streamtimeout::StreamTimeout::arced();
    let stream = streams::timebinnedjson::timebinned_json_framed(
        res2.query,
        ch_conf,
        ctx,
        cache_read_provider,
        events_read_provider,
        timeout_provider,
    )
    .await?;
    let stream = streams::lenframe::bytes_chunks_to_len_framed_str(stream);
    let stream = streams::instrument::InstrumentStream::new(stream, res2.logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}

async fn binned_cbor_framed(
    res2: HandleRes2<'_>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    debug!("binned_cbor_framed");
    let use_scylla6_workarounds = res2
        .query
        .use_scylla6_workarounds()
        .map(From::from)
        .unwrap_or(ncc.node_config.cluster.use_scylla6_workarounds());
    log_query!("binned_cbor_framed  {:?}  {:?}", res2.query, use_scylla6_workarounds);
    // TODO handle None case better and return 404
    let ch_conf = ch_conf_from_binned(&res2.query, ctx, res2.pgqueue, ncc)
        .await?
        .ok_or_else(|| Error::ChannelNotFound)?;
    let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
    let (events_read_provider, cache_read_provider) = make_read_provider(
        ch_conf.name(),
        use_scylla6_workarounds,
        res2.scyqueue,
        open_bytes,
        ctx,
        ncc,
    );
    let timeout_provider = streamio::streamtimeout::StreamTimeout::arced();
    let stream = streams::timebinnedjson::timebinned_cbor_framed(
        res2.query,
        ch_conf,
        ctx,
        cache_read_provider,
        events_read_provider,
        timeout_provider,
    )
    .await?;
    let stream = streams::lenframe::bytes_chunks_to_framed(stream);
    let stream = streams::instrument::InstrumentStream::new(stream, res2.logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_CBOR_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}

pub struct HandleRes2<'a> {
    logspan: Span,
    query: BinnedQuery,
    ch_conf: ChannelTypeConfigGen,
    events_read_provider: Arc<dyn EventsReadProvider>,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    timeout_provider: Arc<dyn StreamTimeout2>,
    pgqueue: &'a PgQueue,
    scyqueue: Option<ScyllaQueue>,
}

impl<'a> HandleRes2<'a> {
    async fn new(
        ctx: &ReqCtx,
        logspan: Span,
        query: BinnedQuery,
        pgqueue: &'a PgQueue,
        scyqueue: Option<ScyllaQueue>,
        ncc: &NodeConfigCached,
    ) -> Result<Self, Error> {
        let ch_conf = ch_conf_from_binned(&query, ctx, pgqueue, ncc)
            .await?
            .ok_or_else(|| Error::ChannelNotFound)?;
        let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
        let (events_read_provider, cache_read_provider) = make_read_provider(
            ch_conf.name(),
            query
                .use_scylla6_workarounds()
                .map(From::from)
                .unwrap_or(ncc.node_config.cluster.use_scylla6_workarounds()),
            scyqueue.clone(),
            open_bytes,
            ctx,
            ncc,
        );
        let timeout_provider = streamio::streamtimeout::StreamTimeout::arced();
        let ret = Self {
            logspan,
            query,
            ch_conf,
            events_read_provider,
            cache_read_provider,
            timeout_provider,
            pgqueue,
            scyqueue,
        };
        Ok(ret)
    }
}
