use crate::response;
use crate::ReqCtx;
use crate::ServiceSharedResources;
use daqbuf_err as err;
use err::PublicError;
use err::ToPublicError;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::error_response;
use httpclient::read_body_bytes;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::log::*;
use netpod::NodeConfigCached;
use std::sync::Arc;
use streams::instrument::InstrumentStream;

autoerr::create_error_v1!(
    name(EventDataError, "EventData"),
    enum variants {
        QueryParse,
        Error(Box<dyn ToPublicError>),
        InternalError,
    },
);

impl ToPublicError for EventDataError {
    fn to_public_error(&self) -> PublicError {
        match self {
            EventDataError::QueryParse => format!("{self}").into(),
            EventDataError::Error(e) => e.to_public_error(),
            EventDataError::InternalError => format!("{self}").into(),
        }
    }
}

pub struct EventDataHandler {}

impl EventDataHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().eq("/api/4/private/eventdata/frames") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        ncc: &NodeConfigCached,
        shared_res: Arc<ServiceSharedResources>,
    ) -> Result<StreamResponse, EventDataError> {
        if req.method() != Method::POST {
            Ok(response(StatusCode::NOT_ACCEPTABLE)
                .body(body_empty())
                .map_err(|_| EventDataError::InternalError)?)
        } else {
            match Self::handle_req(req, ncc, shared_res).await {
                Ok(ret) => Ok(ret),
                Err(e) => {
                    error!("{e}");
                    let res = error_response(e.to_public_error().to_string(), ctx.reqid());
                    Ok(res)
                }
            }
        }
    }

    async fn handle_req(
        req: Requ,
        ncc: &NodeConfigCached,
        shared_res: Arc<ServiceSharedResources>,
    ) -> Result<StreamResponse, EventDataError> {
        let (_head, body) = req.into_parts();
        let body = read_body_bytes(body).await.map_err(|_| EventDataError::InternalError)?;
        let inp = futures_util::stream::iter([Ok(body)]);
        let frames = nodenet::conn::events_get_input_frames(inp)
            .await
            .map_err(|_| EventDataError::InternalError)?;
        let (evsubq,) = nodenet::conn::events_parse_input_query(frames).map_err(|_| EventDataError::QueryParse)?;
        // info!("{:?}", evsubq);
        let logspan = if evsubq.log_level() == "trace" {
            trace!("emit trace span");
            tracing::span!(tracing::Level::INFO, "log_span_trace")
        } else if evsubq.log_level() == "debug" {
            debug!("emit debug span");
            tracing::span!(tracing::Level::INFO, "log_span_debug")
        } else {
            tracing::Span::none()
        };
        use tracing::Instrument;
        let stream = nodenet::conn::create_response_bytes_stream(evsubq, shared_res.scyqueue.as_ref(), ncc)
            .instrument(logspan.clone())
            .await
            .map_err(|e| EventDataError::Error(Box::new(err::Error::from_string(e))))?;
        let stream = InstrumentStream::new(stream, logspan);
        let ret = response(StatusCode::OK)
            .body(body_stream(stream))
            .map_err(|_| EventDataError::InternalError)?;
        Ok(ret)
    }
}
