use super::tools::HandleRes2;
use crate::api4::binned_v2::binexpand::BinnedExpand;
use crate::api4::binned_v2::edgecheck::Edgecheck;
use crate::bodystream::response;
use crate::requests::accepts_cbor_framed;
use crate::requests::accepts_json_framed;
use crate::requests::accepts_json_or_all;
use crate::ServiceSharedResources;
use bytes::Bytes;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use futures_util::future::ready;
use futures_util::Stream;
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
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use items_0::collect_s::CollectableDyn;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::StreamItem;
use items_2::binning::container_bins::ContainerBins;
use items_2::jsonbytes::JsonBytes;
use netpod::log;
use netpod::req_uri_to_url;
use netpod::timeunits::SEC;
use netpod::ttl::RetentionTime;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::APP_JSON;
use netpod::APP_JSON_FRAMED;
use netpod::HEADER_NAME_REQUEST_ID;
use query::api4::binned::BinnedQuery;
use scyllaconn::binned2::binnedrtpbp::BinnedRtPbpStream;
use scyllaconn::binwriteindex::BinWriteIndexRtStream;
use scyllaconn::worker::ScyllaQueue;
use series::msp::PrebinnedPartitioning;
use series::SeriesId;
use std::time::Duration;
use std::time::Instant;
use streams::collect::Collect;
use streams::collect::CollectResult;
use tracing::Instrument;
use tracing::Span;
use url::Url;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }
macro_rules! log_query { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "BinnedManualParams"),
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
        ReadAllCoarse(#[from] scyllaconn::binwriteindex::read_all_coarse::Error),
        Binned2FromBinned(#[from] scyllaconn::binned2::frombinned::Error),
        BinnedQuery(#[from] query::api4::binned::Error),
        BadRange,
        Msg(String),
        BinnedV2Tools(#[from] super::tools::Error),
        Collect(#[from] streams::collect::Error),
        BinnedRtPbp(#[from] scyllaconn::binned2::binnedrtpbp::Error),
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

pub struct Manualparams {}

impl Manualparams {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/binnedv2/manualparams" {
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
        // beg = query.range().beg_u64() / SEC,
        // end = query.range().end_u64() / SEC,
        // ch = query.channel().name(),
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
    {
        let do_json_single = accepts_json_or_all(&head.headers);
        let do_json_framed = accepts_json_framed(&head.headers);
        let do_cbor_framed = accepts_cbor_framed(&head.headers);
        info!(
            "do_json_single {}  do_json_framed {}  do_cbor_framed {}  {:?}",
            do_json_single,
            do_json_framed,
            do_cbor_framed,
            head.headers.get(http::header::ACCEPT)
        );
    }
    if accepts_json_framed(&head.headers) {
        Ok(deliver_json_framed(res2, ctx, ncc).await?)
    } else if accepts_json_or_all(&head.headers) {
        Ok(deliver_json(res2, ctx, ncc).await?)
    } else {
        let ret = error_status_response(StatusCode::NOT_ACCEPTABLE, "", ctx.reqid());
        Ok(ret)
    }
}

fn build_stream(
    res2: HandleRes2<'_>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> impl Stream<Item = Result<StreamItem<ContainerBins<f32, f32>>, Error>> {
    let series = SeriesId::new(res2.ch_conf.series().unwrap());
    let range = res2.query.range().to_time().unwrap();
    let scyqueue = res2.scyqueue.as_ref().unwrap();
    let rt_opt = res2.query.use_rt();
    let pbp1_opt = res2.query.pbp1();
    let use_pbp_opt = res2.query.use_pbp();
    info!(
        "deliver_json  {rt_opt:?}  pbp1_opt {pbp1_opt:?}  scyfix {:?}  use_pbp_opt {use_pbp_opt:?}",
        res2.scylla_opts
    );
    let rt = rt_opt.clone().map_or(RetentionTime::Long, |x| x.clone());
    let use_pbp = use_pbp_opt.map_or(PrebinnedPartitioning::Day1, |x| x.clone());
    let brange = BinnedRange::from_nano_range(range, use_pbp.bin_len());
    info!("deliver_json  {rt:?}  {use_pbp:?}  {brange:?}");
    let stream = BinnedRtPbpStream::new(series, rt, use_pbp, brange.clone(), res2.scylla_opts, scyqueue.clone());
    let stream = BinnedExpand::new(stream, brange.clone());
    let stream = Edgecheck::new(stream, brange.clone());
    // let stream = futures_util::stream::iter([Ok::<StreamItem<String>, Error>(netpod::todoval())]);
    let stream = streams::logqueue::LogItemMux::new(stream, ctx.reqid().into());
    let stream = stream.map(|x| match x {
        Err(e) => Err(Error::from(e)),
        Ok(x) => Ok(x),
    });
    stream
}

async fn deliver_json_framed(
    res2: HandleRes2<'_>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<StreamResponse, Error> {
    let timeout_provider = res2.timeout_provider.clone();
    let rt_opt = res2.query.use_rt();
    let pbp1_opt = res2.query.pbp1();
    let use_pbp_opt = res2.query.use_pbp();
    let logspan = res2.logspan.clone();
    let stream = build_stream(res2, ctx, ncc);
    let stream = stream.map(|e| match e {
        Ok(StreamItem::Log(x)) => Ok(StreamItem::Log(x)),
        Ok(StreamItem::Stats(x)) => Ok(StreamItem::Stats(x)),
        Ok(StreamItem::DataItem(x)) => {
            let x = Box::new(x) as Box<dyn CollectableDyn>;
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
        }
        Err(e) => Err(err::Error::from_string(e)),
    });
    let stream = stream.map(|x| Some(Some(x)));
    let stream = Box::pin(stream);
    let timeout = Duration::from_millis(90000);
    let stream = streams::timebinnedjson::timeoutable_collectable_stream_to_json_bytes(stream, timeout, false);
    let stream = streams::lenframe::bytes_chunks_to_len_framed_str(stream);
    let stream = streams::instrument::InstrumentStream::new(stream, logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}

async fn deliver_json(res2: HandleRes2<'_>, ctx: &ReqCtx, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
    let timeout_provider = res2.timeout_provider.clone();
    let rt_opt = res2.query.use_rt();
    let pbp1_opt = res2.query.pbp1();
    let use_pbp_opt = res2.query.use_pbp();
    let stream = build_stream(res2, ctx, ncc);
    let stream = stream.map(|e| {
        match e {
            Ok(StreamItem::Log(x)) => {
                // log::info!("got LogItem {:?}", x);
                // let v = serde_json::to_value(&x).unwrap();
                // a.0.push(v);
                Ok(StreamItem::Log(x))
            }
            Ok(StreamItem::Stats(x)) => Ok(StreamItem::Stats(x)),
            Ok(StreamItem::DataItem(x)) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
            Err(e) => Err(err::Error::from_string(e)),
        }
    });
    let stream = Box::pin(stream);
    let deadline = Instant::now() + Duration::from_millis(12000);
    let collect_max = 60000;
    let bytes_max = 1024 * 1024 * 8;
    let collected = Collect::new(stream, deadline, collect_max, bytes_max, timeout_provider);
    let collected = Box::pin(collected);
    let collres = collected.await?;
    let res = match collres {
        CollectResult::Some(res) => {
            let val = res.into_user_facing_api_type_box().into_serializable_json();
            let jsval = serde_json::to_string(&val).unwrap();
            CollectResult::Some(JsonBytes::new(jsval))
        }
        CollectResult::Empty => CollectResult::Empty,
        CollectResult::Timeout => CollectResult::Timeout,
    };
    return match res {
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
    };
    let js = serde_json::json!({
        "rt": rt_opt,
        "pbp1": pbp1_opt,
        "objs": (),
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
