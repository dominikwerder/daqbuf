use crate::bodystream::response;
use crate::err::Error;
use crate::requests::accepts_json_or_all;
use crate::ReqCtx;
use crate::ServiceSharedResources;
use futures_util::StreamExt;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::error_status_response;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use items_0::Empty;
use items_0::Extendable;
use items_2::channelevents::ChannelStatusEvents;
use items_2::channelevents::ConnStatusEvent;
use netpod::log::*;
use netpod::query::ChannelStateEventsQuery;
use netpod::req_uri_to_url;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;

pub struct ConnectionStatusEvents {}

impl ConnectionStatusEvents {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/status/connection/events" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                let url = req_uri_to_url(req.uri())?;
                let q = ChannelStateEventsQuery::from_url(&url)?;
                match self.fetch_data(&q, shared_res, ncc).await {
                    Ok(k) => {
                        let body = ToJsonBody::from(&k).into_body();
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => {
                        error!("{e}");
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(body_string(format!("{:?}", e.public_msg())))?)
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn fetch_data(
        &self,
        q: &ChannelStateEventsQuery,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<Vec<ConnStatusEvent>, Error> {
        let scyco = ncc
            .node_config
            .cluster
            .scylla_st()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("no scylla configured")))?;
        let _scy = scyllaconn::conn::create_scy_session(scyco).await?;
        let _chconf =
            nodenet::channelconfig::channel_config(q.range().clone(), q.channel().clone(), &shared_res.pgqueue, ncc)
                .await?;
        let _do_one_before_range = true;
        let ret = Vec::new();
        if true {
            return Err(Error::with_msg_no_trace("TODO dedicated connection status?"));
        }
        // let mut stream =
        //     scyllaconn::status::StatusStreamScylla::new(series, q.range().clone(), do_one_before_range, scy);
        // while let Some(item) = stream.next().await {
        //     let item = item?;
        //     ret.push(item);
        // }
        Ok(ret)
    }
}

pub struct ChannelStatusEventsHandler {}

impl ChannelStatusEventsHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/status/channel/events" {
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
            Ok(error_status_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "expect a GET request",
                ctx.reqid(),
            ))
        } else if !accepts_json_or_all(req.headers()) {
            Ok(error_status_response(
                StatusCode::NOT_ACCEPTABLE,
                "server can only deliver json",
                ctx.reqid(),
            ))
        } else {
            let url = req_uri_to_url(req.uri())?;
            let q = ChannelStateEventsQuery::from_url(&url)?;
            match self.fetch_data(&q, shared_res, ncc).await {
                Ok(k) => {
                    let body = ToJsonBody::from(&k).into_body();
                    Ok(response(StatusCode::OK).body(body)?)
                }
                Err(e) => {
                    error!("{e}");
                    Ok(response(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(body_string(format!("{:?}", e.public_msg())))?)
                }
            }
        }
    }

    async fn fetch_data(
        &self,
        q: &ChannelStateEventsQuery,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<ChannelStatusEvents, Error> {
        let scyco = ncc
            .node_config
            .cluster
            .scylla_st()
            .ok_or_else(|| Error::with_public_msg_no_trace(format!("no scylla configured")))?;
        let scy = scyllaconn::conn::create_scy_session(scyco).await?;
        let do_one_before_range = true;
        if false {
            let chconf = nodenet::channelconfig::channel_config(
                q.range().clone(),
                q.channel().clone(),
                &shared_res.pgqueue,
                ncc,
            )
            .await?
            .ok_or_else(|| Error::with_msg_no_trace("channel config not found"))?;
            use netpod::ChannelTypeConfigGen;
            match chconf {
                ChannelTypeConfigGen::Scylla(_x) => todo!(),
                ChannelTypeConfigGen::SfDatabuffer(_x) => todo!(),
            }
        }
        let mut stream = scyllaconn::status::StatusStreamScylla::new(
            q.channel().series().unwrap(),
            q.range().clone(),
            do_one_before_range,
            scy,
        );
        let mut ret = ChannelStatusEvents::empty();
        while let Some(item) = stream.next().await {
            let mut item = item?;
            ret.extend_from(&mut item);
        }
        Ok(ret)
    }
}
