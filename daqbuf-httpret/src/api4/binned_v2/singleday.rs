use super::tools::HandleRes2;
use crate::bodystream::response;
use crate::requests::accepts_json_or_all;
use crate::ServiceSharedResources;
use bytes::Bytes;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use futures_util::future::ready;
use futures_util::StreamExt;
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
use httpclient::Requ;
use httpclient::StreamResponse;
use items_0::streamitem::LogItem;
use items_0::streamitem::StreamItem;
use netpod::log;
use netpod::req_uri_to_url;
use netpod::timeunits::SEC;
use netpod::ttl::RetentionTime;
use netpod::DtMs;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::APP_JSON;
use netpod::HEADER_NAME_REQUEST_ID;
use query::api4::binned::BinnedQuery;
use scyllaconn::binwriteindex::BinWriteIndexRtStream;
use scyllaconn::worker::ScyllaOptsSubmit;
use scyllaconn::worker::ScyllaQueue;
use series::msp::PrebinnedPartitioning;
use series::SeriesId;
use tracing::Instrument;
use tracing::Span;
use url::Url;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "BinnedSingleday"),
    enum variants {
        ChannelNotFound,
        BadQuery(String),
        HttpLib(#[from] http::Error),
        ChannelConfig(crate::channelconfig::Error),
        Retrieval(#[from] crate::RetrievalError),
        EventsCbor(#[from] super::super::events::plaineventscbor::Error),
        EventsJson(#[from] super::super::events::plaineventsjson::Error),
        ServerError,
        BinnedStream(err::Error),
        TimebinnedJson(#[from] streams::timebinnedjson::Error),
        ReadAllCoarse(#[from] scyllaconn::binwriteindex::read_all_coarse::Error),
        Binned2FromBinned(#[from] scyllaconn::binned2::frombinned::Error),
        BinnedQuery(#[from] query::api4::binned::Error),
        BadRange,
        Msg(String),
        BinnedV2Tools(#[from] super::tools::Error),
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

pub struct Singleday {}

impl Singleday {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/binnedv2/singleday" {
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
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        } else {
            handle_get(req, ctx, shared_res, ncc).await
        }
    }
}

async fn handle_get(
    req: Requ,
    ctx: &ReqCtx,
    shared_res: &ServiceSharedResources,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
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
    let query = BinnedQuery::from_url(&url).map_err(|e| {
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
    instrumented(head, ctx, url, query, pgqueue, scyqueue, ncc, logspan.clone())
        .instrument(logspan)
        .instrument(span1)
        .await
}

async fn instrumented(
    head: Parts,
    ctx: &ReqCtx,
    url: Url,
    query: BinnedQuery,
    pgqueue: &PgQueue,
    scyqueue: Option<ScyllaQueue>,
    ncc: &NodeConfigCached,
    logspan: Span,
) -> Result<StreamResponse, Error> {
    let res2 = HandleRes2::new(ctx, logspan, url, query.clone(), pgqueue, scyqueue, ncc).await?;
    if accepts_json_or_all(&head.headers) {
        Ok(deliver_json(res2, ctx, ncc).await?)
    } else {
        let ret = error_status_response(StatusCode::NOT_ACCEPTABLE, "", ctx.reqid());
        Ok(ret)
    }
}

async fn deliver_json(res2: HandleRes2<'_>, ctx: &ReqCtx, _ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
    let series = SeriesId::new(res2.ch_conf.series().unwrap());
    let range = res2.query.range().to_time().unwrap();
    let scyqueue = res2.scyqueue.as_ref().unwrap();
    let rt_opt = res2.query.use_rt();
    let pbp1_opt = res2.query.pbp1();
    info!("deliver_json  {rt_opt:?}  {pbp1_opt:?}  scyfix {:?}", res2.scylla_opts);
    let rt = rt_opt.clone().map_or(RetentionTime::Long, |x| x.clone());
    let pbp1 = pbp1_opt.map_or(PrebinnedPartitioning::Day1, |x| x.clone());
    info!("deliver_json  BinWriteIndexRtStream::new  {rt:?}  {pbp1:?}");
    let stream = BinWriteIndexRtStream::new(
        rt.clone(),
        series.clone(),
        pbp1.clone(),
        range.clone(),
        ScyllaOptsSubmit::no_choice(),
        scyqueue.clone(),
    );
    let stream = stream.then(|item| {
        //
        match item {
            Ok(x) => match x {
                StreamItem::DataItem(x) => {
                    let gg = x.entries.iter().fold(Vec::new(), |mut a, x| {
                        let pbp2 = PrebinnedPartitioning::from_binlen(DtMs::from_ms_u64(x.binlen.to_u32() as u64));
                        let tt1 = range.beg_ts().to_ts_ms();
                        let msp_lsp_res = pbp2.as_ref().ok().map(|x| x.msp_lsp(tt1));
                        a.push((pbp2, msp_lsp_res));
                        a
                    });
                    ready(Ok(StreamItem::Log(LogItem::info(format!(
                        "TODO handle item {x:?}  {gg:?}"
                    )))))
                }
                x => ready(Ok(x)),
            },
            _ => ready(item),
        }
    });
    let stream = streams::logqueue::LogItemMux::new(stream, ctx.reqid().into());
    let (objs,) = stream
        .fold((Vec::new(),), |mut a, x| {
            a.0.push(format!("{x:?}"));
            ready(a)
        })
        .await;
    let js = serde_json::json!({
        "rt": rt_opt,
        "pbp1": pbp1_opt,
        "objs": objs,
        "uses_index_min10": pbp1.uses_index_min10(),
        "range": range,
        "series": series,
    });
    let bb = serde_json::to_vec(&js).unwrap();
    let stream = futures_util::stream::iter([Bytes::from(bb)]).map(|x| Ok::<_, Error>(x));
    let stream = streams::instrument::InstrumentStream::new(stream, res2.logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}
