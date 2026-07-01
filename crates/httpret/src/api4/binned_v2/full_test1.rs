use super::tools::HandleRes2;
use crate::bodystream::response;
use crate::requests::accepts_json_framed;
use crate::ServiceSharedResources;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http::header::CONTENT_TYPE;
use http::request::Parts;
use http::Method;
use http::StatusCode;
use httpclient::bad_request_response;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::error_response;
use httpclient::not_found_response;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::log;
use netpod::req_uri_to_url;
use netpod::timeunits::SEC;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::APP_JSON_FRAMED;
use netpod::HEADER_NAME_REQUEST_ID;
use query::api4::binned::BinnedQuery;
use scyllaconn::worker::ScyllaQueue;
use series::SeriesId;
use std::time::Duration;
use streams::streamtimeout::TimeoutableStream;
use streams::timebinnedjson::timeoutable_collectable_stream_to_json_bytes;
use tracing::Instrument;
use tracing::Span;
use url::Url;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "Api4BinnedV2"),
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

pub struct BinnedV2Handler {}

impl BinnedV2Handler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/binnedv2/binned" {
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
    if accepts_json_framed(&head.headers) {
        Ok(deliver_json_framed(res2, ctx, ncc).await?)
    } else {
        let ret = error_response(format!("unsupported accept: {:?}", &head.headers), ctx.reqid());
        Ok(ret)
    }
}
async fn deliver_json_framed(
    res2: HandleRes2<'_>,
    ctx: &ReqCtx,
    _ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    info!("binned_json_framed  V2 prebinned");
    let series = SeriesId::new(res2.ch_conf.series().unwrap());
    let _range = res2.query.range().to_time().unwrap();
    let scyqueue = res2.scyqueue.as_ref().unwrap();
    let stream = if false {
        // let stream = scyllaconn::binwriteindex::read_all_coarse::ReadAllCoarse::new(series, range, scyqueue.clone());
        // let stream = stream.map_ok(to_debug).map_err(Error::from);
        // let msg = format!("{}", res2.url.as_str());
        // let stream = futures_util::stream::iter([Ok(msg)]).chain(stream);
        // Box::pin(stream) as Pin<Box<dyn Stream<Item = _> + Send>>
        todo!("testpart=read_all_coarse disabled")
    } else if true {
        let binrange = res2
            .query
            .covering_range()?
            .binned_range_time()
            .ok_or_else(|| Error::BadRange)?;
        let stream = scyllaconn::binned2::frombinned::FromBinned::new(
            series,
            binrange.clone(),
            res2.scylla_opts.clone(),
            scyqueue,
            res2.cache_read_provider,
        );
        let stream = stream.map_err(Error::from);
        let stream = stream.map(|x| x);
        // use items_2::binning::timeweight::timeweight_bins::BinnedBinsTimeweight;
        use items_0::streamitem::StreamItem;
        use items_2::binning::timeweight::timeweight_bins_lazy::BinnedBinsTimeweightLazy;
        let mut rebinner = BinnedBinsTimeweightLazy::new(binrange).set_cnt_zero();
        let stream = stream.filter_map(move |x| {
            let ret = match x {
                Ok(StreamItem::DataItem(x)) => match rebinner.ingest(&x) {
                    Ok(()) => match rebinner.output() {
                        Ok(Some(x)) => Some(Ok(items_0::streamitem::StreamItem::DataItem(x))),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                },
                Ok(StreamItem::Log(x)) => Some(Ok(StreamItem::Log(x))),
                Ok(StreamItem::Stats(x)) => Some(Ok(StreamItem::Stats(x))),
                Err(e) => Some(Err(items_0::timebin::BinningggError::Dyn(Box::new(e)))),
            };
            futures_util::future::ready(ret)
        });
        let stream = stream.map(|item| {
            use items_0::streamitem::RangeCompletableItem;
            use items_0::streamitem::StreamItem;
            // use items_0::timebin::BinsBoxed;
            match item {
                Ok(StreamItem::DataItem(mut x)) => {
                    x.fix_numerics();
                    let ret = x.boxed_into_collectable_box();
                    Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))
                }
                Ok(StreamItem::Log(x)) => Ok(StreamItem::Log(x)),
                Ok(StreamItem::Stats(x)) => Ok(StreamItem::Stats(x)),
                Err(e) => Err(e),
            }
        });
        let stream = stream.map(|x| x);
        let stream = stream.map_err(|e| daqbuf_err::Error::from_string(e));
        let timeout_content_base = res2
            .query
            .timeout_content()
            .unwrap_or(Duration::from_millis(2000))
            .min(Duration::from_millis(8000))
            .max(Duration::from_millis(334));
        let stream = stream.map(|x| x);
        let stream = streams::logqueue::LogItemMux::new(stream, ctx.reqid().into());
        let timeout_content_2 = timeout_content_base * 2 / 3;
        let stream = stream.map(|x| Some(x)).chain(futures_util::stream::iter([None]));
        let stream = TimeoutableStream::new(timeout_content_base, res2.timeout_provider, stream);
        let stream = stream.map(|x| x);
        let stream = Box::pin(stream);
        let stream = timeoutable_collectable_stream_to_json_bytes(stream, timeout_content_2, true);
        // let stream = stream.map(|x| Ok(format!("dummy82749827348932")));
        // Box::pin(stream) as Pin<Box<dyn Stream<Item = _> + Send>>
        stream
    } else {
        // let msg = format!("UNKNOWN  {}", res2.url.as_str());
        // let stream = futures_util::stream::iter([Ok(msg)]);
        // Box::pin(stream)
        let e = streams::json_stream::Error::Msg(format!("unknown testpart AA in url: {}", res2.url.as_str()));
        let stream = futures_util::stream::iter([Err(e)]);
        Box::pin(stream)
    };
    let stream = streams::lenframe::bytes_chunks_to_len_framed_str(stream);
    let stream = streams::instrument::InstrumentStream::new(stream, res2.logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
    // let ret = response(StatusCode::OK)
    //     .header(CONTENT_TYPE, APP_JSON)
    //     .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
    //     .body(ToJsonBody::from(&strings).into_body())?;
    // Ok(ret)
}
