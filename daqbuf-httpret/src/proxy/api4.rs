pub mod backend;
pub mod dyncmd;
pub mod events;

use crate::ReqCtx;
use crate::bodystream::ToPublicResponse;
use crate::err::Error;
use crate::gather::SubRes;
use crate::gather::Tag;
use crate::gather::gather_get_json_generic;
use crate::requests::accepts_json_or_all;
use crate::response;
use futures_util::Future;
use http::Method;
use http::Response;
use http::StatusCode;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use httpclient::body_empty;
use httpclient::read_body_bytes;
use hyper::body::Incoming;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::NodeStatus;
use netpod::NodeStatusSub;
use netpod::ProxyConfig;
use netpod::ServiceVersion;
use netpod::log::*;
use netpod::req_uri_to_url;
use serde_json::Value as JsVal;
use std::collections::VecDeque;
use std::pin::Pin;
use std::time::Duration;
use url::Url;

// TODO model channel search according to StatusNodesRecursive.
// Make sure that backend handling is correct:
// The aggregator asks all backends, except if the user specifies some backend
// in which case it should only go to the matching backends.
// The aggregators and leaf nodes behind should as well not depend on backend,
// but simply answer all matching.

pub async fn channel_search(req: Requ, ctx: &ReqCtx, proxy_config: &ProxyConfig) -> Result<ChannelSearchResult, Error> {
    let (head, _body) = req.into_parts();
    let inpurl = req_uri_to_url(&head.uri)?;
    let query = ChannelSearchQuery::from_url(&inpurl)?;
    let mut urls = Vec::new();
    let mut tags = Vec::new();
    let mut bodies = Vec::new();
    for pb in &proxy_config.backends {
        if query.backend.as_ref().map_or(true, |b| pb.name.eq(b)) {
            match Url::parse(&format!("{}/api/4/search/channel", pb.url)) {
                Ok(mut url) => {
                    let tag = url.to_string();
                    info!("search  forward to  tag {tag}  url {url}");
                    query.append_to_url(&mut url);
                    tags.push(tag);
                    bodies.push(None);
                    urls.push(url);
                }
                Err(_) => return Err(Error::with_msg(format!("parse error for: {:?}", pb))),
            }
        }
    }
    let nt = |tag: String, res: Response<Incoming>| {
        let fut = async {
            let (_head, body) = res.into_parts();
            let fut = read_body_bytes(body);
            let body = fut.await?;
            // info!("got a result {:?}", body);
            let res: ChannelSearchResult = match serde_json::from_slice(&body) {
                Ok(k) => k,
                Err(_) => {
                    let msg = format!("can not parse result  tag {}  {}", tag, String::from_utf8_lossy(&body));
                    error!("{}", msg);
                    return Err(Error::with_msg_no_trace(msg));
                }
            };
            info!("search  from {}  len {}  {:?}", tag, res.channels.len(), res.channels);
            let ret = SubRes {
                tag,
                status: StatusCode::OK,
                val: res,
            };
            Ok(ret)
        };
        Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
    };
    let ft = |all: Vec<(Tag, Result<SubRes<ChannelSearchResult>, Error>)>| {
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
        Ok(res)
    };
    let ret = gather_get_json_generic(
        http::Method::GET,
        urls,
        bodies,
        tags,
        nt,
        ft,
        Duration::from_millis(1100),
        ctx,
    )
    .await?;
    Ok(ret)
}

pub struct ChannelSearchAggHandler {}

impl ChannelSearchAggHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/search/channel" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ctx: &ReqCtx, node_config: &ProxyConfig) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match channel_search(req, ctx, node_config).await {
                    Ok(item) => Ok(response(StatusCode::OK).body(ToJsonBody::from(&item).into_body())?),
                    Err(e) => {
                        warn!("ChannelConfigHandler::handle: got error from channel_config: {e:?}");
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

pub struct StatusNodesRecursive {}

impl StatusNodesRecursive {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == crate::api4::status::StatusNodesRecursive::path() {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        proxy_config: &ProxyConfig,
        service_version: &ServiceVersion,
    ) -> Result<StreamResponse, Error> {
        match self.status(req, ctx, proxy_config, service_version).await {
            Ok(status) => {
                let ret = response(StatusCode::OK).body(ToJsonBody::from(&status).into_body())?;
                Ok(ret)
            }
            Err(e) => {
                error!("StatusNodesRecursive sees: {e}");
                let ret = crate::bodystream::ToPublicResponse::to_public_response(&e);
                Ok(ret)
            }
        }
    }

    async fn status(
        &self,
        _req: Requ,
        ctx: &ReqCtx,
        proxy_config: &ProxyConfig,
        service_version: &ServiceVersion,
    ) -> Result<NodeStatus, Error> {
        let path = crate::api4::status::StatusNodesRecursive::path();
        let mut bodies = Vec::new();
        let mut urls = Vec::new();
        let mut tags = Vec::new();
        for sub in &proxy_config.status_subs {
            match Url::parse(&format!("{}{}", sub.url, path)) {
                Ok(url) => {
                    bodies.push(None);
                    tags.push(sub.url.to_string());
                    urls.push(url);
                }
                Err(e) => return Err(Error::with_msg_no_trace(format!("parse error for: {sub:?}  {e:?}"))),
            }
        }
        let nt = |tag: String, res: Response<Incoming>| {
            let fut = async {
                let (_head, body) = res.into_parts();
                let body = read_body_bytes(body).await?;
                let res: JsVal = match serde_json::from_slice(&body) {
                    Ok(k) => k,
                    Err(e) => {
                        error!("{e}");
                        let msg = format!(
                            "gather sub responses, can not parse result: {}  {}",
                            String::from_utf8_lossy(&body),
                            e,
                        );
                        error!("{}", msg);
                        return Err(Error::with_msg_no_trace(msg));
                    }
                };
                let ret = SubRes {
                    tag,
                    status: StatusCode::OK,
                    val: res,
                };
                Ok(ret)
            };
            Box::pin(fut) as Pin<Box<dyn Future<Output = _> + Send>>
        };
        let ft = |all: Vec<(Tag, Result<SubRes<JsVal>, Error>)>| {
            let mut subs = VecDeque::new();
            for (tag, sr) in all {
                match sr {
                    Ok(sr) => {
                        let s: Result<NodeStatus, _> =
                            serde_json::from_value(sr.val).map_err(|e| netpod::NodeStatusSubError::Msg(e.to_string()));
                        let sub = NodeStatusSub { url: tag.0, status: s };
                        subs.push_back(sub);
                    }
                    Err(e) => {
                        let sub = NodeStatusSub {
                            url: tag.0,
                            status: Err(netpod::NodeStatusSubError::Msg(e.to_string())),
                        };
                        subs.push_back(sub);
                    }
                }
            }
            let ret = NodeStatus {
                name: format!("{}:{}", proxy_config.name, proxy_config.port),
                version: service_version.to_string(),
                is_sf_databuffer: false,
                is_archiver_engine: false,
                is_archiver_appliance: false,
                database_size: None,
                archiver_appliance_status: None,
                subs,
            };
            Ok(ret)
        };
        let ret = gather_get_json_generic(
            http::Method::GET,
            urls,
            bodies,
            tags,
            nt,
            ft,
            Duration::from_millis(1200),
            ctx,
        )
        .await?;
        Ok(ret)
    }
}
