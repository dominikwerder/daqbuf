pub mod dyneventsstream;
pub mod plaineventscbor;
pub mod plaineventsjson;

use crate::channelconfig::chconf_from_events_quorum;
use crate::requests::accepts_cbor_framed;
use crate::requests::accepts_json_framed;
use crate::requests::accepts_json_or_all;
use crate::response;
use crate::ServiceSharedResources;
use dbconn::worker::PgQueue;
use http::header::CONTENT_TYPE;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::error_response;
use httpclient::error_status_response;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamBody;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::ChannelTypeConfigGen;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::APP_CBOR_FRAMED;
use netpod::APP_JSON;
use netpod::APP_JSON_FRAMED;
use netpod::HEADER_NAME_REQUEST_ID;
use nodenet::client::OpenBoxedBytesViaHttp;
use plaineventscbor::plain_events_cbor_stream;
use plaineventsjson::plain_events_json_stream;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;
use std::sync::Arc;
use streams::collect::CollectResult;
use streams::instrument::InstrumentStream;
use streams::lenframe::bytes_chunks_to_framed;
use streams::lenframe::bytes_chunks_to_len_framed_str;
use streams::streamtimeout::StreamTimeout2;
use tracing::Instrument;
use tracing::Span;

autoerr::create_error_v1!(
    name(Error, "Api4Events"),
    enum variants {
        ChannelNotFound,
        HttpLib(#[from] http::Error),
        ChannelConfig(crate::channelconfig::Error),
        Retrieval(#[from] crate::RetrievalError),
        EventsCbor(#[from] plaineventscbor::Error),
        EventsJson(#[from] plaineventsjson::Error),
    },
);

impl Error {
    pub fn user_message(&self) -> String {
        match self {
            Error::ChannelNotFound => format!("channel not found"),
            _ => self.to_string(),
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::ChannelNotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn response(&self, reqid: &str) -> http::Response<StreamBody> {
        let js = serde_json::json!({
            "message": self.user_message(),
            "requestid": reqid,
        });
        if let Ok(body) = serde_json::to_string_pretty(&js) {
            match http::Response::builder()
                .status(self.status_code())
                .header(http::header::CONTENT_TYPE, APP_JSON)
                .body(httpclient::body_string(body))
            {
                Ok(res) => res,
                Err(e) => {
                    error!("can not generate http error response {e}");
                    httpclient::internal_error()
                }
            }
        } else {
            httpclient::internal_error()
        }
    }
}

impl From<crate::channelconfig::Error> for Error {
    fn from(value: crate::channelconfig::Error) -> Self {
        use crate::channelconfig::Error::*;
        match value {
            NotFound(_) => Self::ChannelNotFound,
            _ => Self::ChannelConfig(value),
        }
    }
}

pub struct EventsHandler {}

impl EventsHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/events" {
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
    ) -> Result<StreamResponse, crate::err::Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?);
        }
        let self_name = "handle";
        let url = req_uri_to_url(req.uri())?;
        let evq = PlainEventsQuery::from_url(&url)?;
        debug!("{self_name}  evq {evq:?}");
        let logspan = if evq.log_level() == "trace" {
            trace!("enable trace for handler");
            tracing::span!(tracing::Level::INFO, "log_span_trace")
        } else if evq.log_level() == "debug" {
            debug!("enable debug for handler");
            tracing::span!(tracing::Level::INFO, "log_span_debug")
        } else {
            tracing::Span::none()
        };
        match plain_events_prep(
            req,
            evq,
            ctx,
            shared_res.scyqueue.clone(),
            &shared_res.pgqueue,
            ncc,
            logspan.clone(),
        )
        .instrument(logspan)
        .await
        {
            Ok(ret) => Ok(ret),
            Err(e) => Ok(e.response(ctx.reqid())),
        }
    }
}

async fn plain_events_prep(
    req: Requ,
    evq: PlainEventsQuery,
    ctx: &ReqCtx,
    scyqu: Option<scyllaconn::worker::ScyllaQueue>,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
    logspan: Span,
) -> Result<StreamResponse, Error> {
    let res2 = HandleRes2::new(ctx, logspan, evq.clone(), scyqu, pgqueue, ncc).await?;
    {
        let obj = serde_json::json!({
            "query_type": "events",
            "channel": {
                "name": res2.ch_conf.name(),
                "series": res2.ch_conf.series(),
            }
        });
        let js = serde_json::to_string(&obj).unwrap();
        info!("api4-request-events {js}");
        info!("");
    }
    plain_events(res2, req).await
}

async fn plain_events(res2: HandleRes2<'_>, req: Requ) -> Result<StreamResponse, Error> {
    if accepts_cbor_framed(req.headers()) {
        Ok(plain_events_cbor_framed(req, res2).await?)
    } else if accepts_json_framed(req.headers()) {
        Ok(plain_events_json_framed(req, res2).await?)
    } else if accepts_json_or_all(req.headers()) {
        Ok(plain_events_json(req, res2).await?)
    } else {
        let ret = error_response(format!("unsupported accept"), res2.ctx.reqid());
        Ok(ret)
    }
}

async fn plain_events_cbor_framed(req: Requ, res2: HandleRes2<'_>) -> Result<StreamResponse, Error> {
    debug!("plain_events_cbor_framed  {:?}  {:?}", res2.ch_conf, req);
    let stream = plain_events_cbor_stream(
        &res2.evq,
        res2.ch_conf,
        res2.ctx,
        res2.open_bytes,
        res2.scyqu,
        res2.timeout_provider,
    )
    .await?;
    let stream = bytes_chunks_to_framed(stream);
    let stream = InstrumentStream::new(stream, res2.logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_CBOR_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, res2.ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}

async fn plain_events_json_framed(req: Requ, res2: HandleRes2<'_>) -> Result<StreamResponse, Error> {
    debug!("plain_events_json_framed  {:?}  {:?}", res2.ch_conf, req);
    let stream = plain_events_json_stream(
        &res2.evq,
        res2.ch_conf,
        res2.ctx,
        res2.open_bytes,
        res2.scyqu.clone(),
        res2.timeout_provider,
    )
    .await?;
    let stream = bytes_chunks_to_len_framed_str(stream);
    let stream = InstrumentStream::new(stream, res2.logspan);
    let ret = response(StatusCode::OK)
        .header(CONTENT_TYPE, APP_JSON_FRAMED)
        .header(HEADER_NAME_REQUEST_ID, res2.ctx.reqid())
        .body(body_stream(stream))?;
    Ok(ret)
}

async fn plain_events_json(req: Requ, res2: HandleRes2<'_>) -> Result<StreamResponse, Error> {
    let self_name = "plain_events_json";
    debug!("{self_name}  {:?}  {:?}", res2.ch_conf, req);
    let (_head, _body) = req.into_parts();
    let item = plaineventsjson::plain_events_json(
        &res2.evq,
        res2.ch_conf,
        res2.ctx,
        &res2.ncc.node_config.cluster,
        res2.open_bytes,
        res2.scyqu.clone(),
        res2.timeout_provider,
    )
    .await;
    debug!("{self_name}  returned  {}", item.is_ok());
    let item = match item {
        Ok(item) => item,
        Err(e) => {
            error!("{self_name}  got error from streams::plaineventsjson::plain_events_json {e}");
            return Err(e.into());
        }
    };
    match item {
        CollectResult::Some(item) => {
            let ret = response(StatusCode::OK)
                .header(CONTENT_TYPE, APP_JSON)
                .header(HEADER_NAME_REQUEST_ID, res2.ctx.reqid())
                .body(ToJsonBody::from(item.into_bytes()).into_body())?;
            debug!("{self_name}  response created");
            Ok(ret)
        }
        CollectResult::Empty => {
            let ret = error_status_response(StatusCode::NO_CONTENT, format!("no content"), res2.ctx.reqid());
            Ok(ret)
        }
        CollectResult::Timeout => {
            let ret = error_status_response(
                StatusCode::GATEWAY_TIMEOUT,
                format!("no content within timeout"),
                res2.ctx.reqid(),
            );
            Ok(ret)
        }
    }
}

struct HandleRes2<'a> {
    logspan: Span,
    evq: PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    open_bytes: Pin<Arc<OpenBoxedBytesViaHttp>>,
    scyqu: Option<scyllaconn::worker::ScyllaQueue>,
    timeout_provider: Arc<dyn StreamTimeout2>,
    #[allow(unused)]
    pgqueue: &'a PgQueue,
    ctx: &'a ReqCtx,
    ncc: &'a NodeConfigCached,
}

impl<'a> HandleRes2<'a> {
    async fn new(
        ctx: &'a ReqCtx,
        logspan: Span,
        evq: PlainEventsQuery,
        scyqu: Option<scyllaconn::worker::ScyllaQueue>,
        pgqueue: &'a PgQueue,
        ncc: &'a NodeConfigCached,
    ) -> Result<Self, Error> {
        let ch_conf = chconf_from_events_quorum(&evq, ctx, pgqueue, ncc)
            .await?
            .ok_or_else(|| Error::ChannelNotFound)?;
        let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
        let timeout_provider = streamio::streamtimeout::StreamTimeout::arced();
        let ret = Self {
            logspan,
            evq,
            ch_conf,
            open_bytes,
            scyqu,
            timeout_provider,
            pgqueue,
            ctx,
            ncc,
        };
        Ok(ret)
    }
}
