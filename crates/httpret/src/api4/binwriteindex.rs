use crate::bodystream::response;
use crate::channelconfig::ch_conf_from_binned;
use crate::requests::accepts_json_or_all;
use crate::ServiceSharedResources;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use futures_util::StreamExt;
use http::header::CONTENT_TYPE;
use http::request::Parts;
use http::Method;
use http::StatusCode;
use httpclient::bad_request_response;
use httpclient::body_empty;
use httpclient::error_response;
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
use netpod::APP_JSON;
use netpod::HEADER_NAME_REQUEST_ID;
use nodenet::client::OpenBoxedBytesViaHttp;
use query::api4::binned::BinWriteIndexQuery;
use query::api4::binned::BinnedQuery;
use query::api4::scyllaopts::ScyllaOptsQuery;
use scyllaconn::worker::ScyllaQueue;
use series::SeriesId;
use std::pin::Pin;
use std::sync::Arc;
use streams::streamtimeout::StreamTimeout2;
use streams::timebin::cached::reader::EventsReadProvider;
use streams::timebin::CacheReadProvider;
use tracing::Instrument;
use tracing::Span;
use url::Url;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "Api4BinWriteIndex"),
    enum variants {
        ChannelNotFound,
        BadQuery(String),
        HttpLib(#[from] http::Error),
        ChannelConfig(crate::channelconfig::Error),
        Retrieval(#[from] crate::RetrievalError),
        EventsCbor(#[from] super::events::plaineventscbor::Error),
        EventsJson(#[from] super::events::plaineventsjson::Error),
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

impl From<Error> for crate::RetrievalError {
    fn from(value: Error) -> Self {
        crate::RetrievalError::TextError(value.to_string())
    }
}

pub struct BinWriteIndexHandler {}

impl BinWriteIndexHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/binwriteindex" {
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
        match handle_request(req, ctx, &shared_res.pgqueue, shared_res.scyqueue.clone(), ncc).await {
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

async fn handle_request(
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
    let query = BinWriteIndexQuery::from_url(&url).map_err(|e| {
        error!("handle_request: {}", e);
        Error::BadQuery(e.to_string())
    })?;
    info!("{:?}", query);
    let logspan = if query.log_level() == "trace" {
        trace!("enable trace for handler");
        tracing::span!(tracing::Level::INFO, "log_span_trace")
    } else if query.log_level() == "debug" {
        debug!("enable debug for handler");
        tracing::span!(tracing::Level::INFO, "log_span_debug")
    } else {
        tracing::Span::none()
    };
    let span1 = tracing::span!(
        tracing::Level::INFO,
        "binwriteindex",
        reqid,
        beg = query.range().beg_u64() / SEC,
        end = query.range().end_u64() / SEC,
        ch = query.channel().name(),
    );
    span1.in_scope(|| {
        debug!("binned begin  {:?}", query);
    });
    binned_instrumented(head, ctx, url, query, pgqueue, scyqueue, ncc, logspan.clone())
        .instrument(logspan)
        .instrument(span1)
        .await
}

async fn binned_instrumented(
    head: Parts,
    ctx: &ReqCtx,
    url: Url,
    query: BinWriteIndexQuery,
    pgqueue: &PgQueue,
    scyqueue: Option<ScyllaQueue>,
    ncc: &NodeConfigCached,
    logspan: Span,
) -> Result<StreamResponse, Error> {
    let res2 = HandleRes2::new(ctx, logspan, url, query.clone(), pgqueue, scyqueue, ncc).await?;
    if accepts_json_or_all(&head.headers) {
        Ok(binned_json_single(res2, ctx, ncc).await?)
    } else {
        let ret = error_response(format!("unsupported accept: {:?}", &head.headers), ctx.reqid());
        Ok(ret)
    }
}

fn make_read_provider(
    chname: &str,
    scylla_opts: ScyllaOptsQuery,
    scyqueue: Option<ScyllaQueue>,
    open_bytes: Pin<Arc<OpenBoxedBytesViaHttp>>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> (Arc<dyn EventsReadProvider>, Arc<dyn CacheReadProvider>) {
    todo!()
}

async fn binned_json_single(
    res2: HandleRes2<'_>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    // TODO unify with binned_json_framed
    debug!("binned_json_single");
    let rt1 = res2.query.retention_time();
    let pbp = res2.query.prebinned_partitioning();
    // let rts = [RetentionTime::Short, RetentionTime::Medium, RetentionTime::Long];
    // for rt in rts {
    let mut strings = Vec::new();
    {
        let mut stream = scyllaconn::binwriteindex::BinWriteIndexRtStream::new(
            rt1,
            SeriesId::new(res2.ch_conf.series().unwrap()),
            pbp.clone(),
            res2.query.range().to_time().unwrap(),
            todo!(),
            res2.scyqueue.clone().unwrap(),
        );
        while let Some(x) = stream.next().await {
            strings.push(format!("{:?}", x));
        }
    }
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(ToJsonBody::from(&strings).into_body())?;
    Ok(ret)
}

struct HandleRes2<'a> {
    logspan: Span,
    url: Url,
    query: BinWriteIndexQuery,
    scylla_opts: ScyllaOptsQuery,
    ch_conf: ChannelTypeConfigGen,
    events_read_provider: Arc<dyn EventsReadProvider>,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    timeout_provider: Box<dyn StreamTimeout2>,
    pgqueue: &'a PgQueue,
    scyqueue: Option<ScyllaQueue>,
}

impl<'a> HandleRes2<'a> {
    async fn new(
        ctx: &ReqCtx,
        logspan: Span,
        url: Url,
        query: BinWriteIndexQuery,
        pgqueue: &'a PgQueue,
        scyqueue: Option<ScyllaQueue>,
        ncc: &NodeConfigCached,
    ) -> Result<Self, Error> {
        todo!("unify with similar code");
        let scylla_opts = ScyllaOptsQuery::new();
        let q2 = BinnedQuery::new(query.channel().clone(), query.range().clone(), 100);
        let ch_conf = ch_conf_from_binned(&q2, ctx, pgqueue, ncc)
            .await?
            .ok_or_else(|| Error::ChannelNotFound)?;
        let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
        let (events_read_provider, cache_read_provider) =
            make_read_provider(ch_conf.name(), scylla_opts, scyqueue.clone(), open_bytes, ctx, ncc);
        let timeout_provider = streamio::streamtimeout::StreamTimeout::boxed();
        let ret = Self {
            logspan,
            url,
            query,
            scylla_opts,
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
