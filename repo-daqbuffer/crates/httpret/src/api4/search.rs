use crate::bodystream::response;
use crate::bodystream::ToPublicResponse;
use crate::err::Error;
use dbconn::worker::PgQueue;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use tracing::Instrument;

pub struct ChannelSearchHandler {}

impl ChannelSearchHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/search/channel" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, pgqueue: &PgQueue, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                match channel_search(req, pgqueue, ncc).await {
                    Ok(item) => Ok(response(StatusCode::OK).body(ToJsonBody::from(&item).into_body())?),
                    Err(e) => {
                        warn!("handle: got error from channel_search: {e:?}");
                        Ok(e.to_public_response())
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }
}

async fn channel_search(req: Requ, pgqueue: &PgQueue, ncc: &NodeConfigCached) -> Result<ChannelSearchResult, Error> {
    let url = req_uri_to_url(req.uri())?;
    let query = ChannelSearchQuery::from_url(&url)?;
    let logspan = if query.log_level() == "trace" {
        trace!("enable trace for handler");
        tracing::span!(tracing::Level::INFO, "log_span_trace")
    } else if query.log_level() == "debug" {
        debug!("enable debug for handler");
        tracing::span!(tracing::Level::INFO, "log_span_debug")
    } else {
        tracing::Span::none()
    };
    let res = dbconn::search::search_channel(query, pgqueue, ncc)
        .instrument(logspan)
        .await
        .map_err(|e| Error::from_to_string(e))?;
    Ok(res)
}
