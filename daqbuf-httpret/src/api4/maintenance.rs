use crate::RetrievalError;
use crate::bodystream::response;
use crate::err::Error;
use bytes::Bytes;
use futures_util::StreamExt;
use http::Method;
use http::StatusCode;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::body_empty;
use httpclient::body_stream;
use httpclient::body_string;
use netpod::APP_JSON;
use netpod::APP_JSON_LINES;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::log;

macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }

pub struct UpdateDbWithChannelNamesHandler {}

impl UpdateDbWithChannelNamesHandler {
    pub fn self_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn prefix() -> &'static str {
        "/api/4/maintenance/update_db_with_channel_names"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        info!("{}", Self::self_name());
        let (head, _body) = req.into_parts();
        let _dry = match head.uri.query() {
            Some(q) => q.contains("dry"),
            None => false,
        };
        let res =
            dbconn::scan::update_db_with_channel_names(node_config.clone(), &node_config.node_config.cluster.database)
                .await;
        match res {
            Ok(res) => {
                let stream = res.map(|k| match serde_json::to_string(&k) {
                    Ok(mut item) => {
                        item.push('\n');
                        Ok(Bytes::from(item))
                    }
                    Err(e) => Err(e),
                });
                let stream = streams::print_on_done::PrintOnDone::new(
                    stream,
                    Box::pin(|ts| {
                        let dt = ts.elapsed();
                        info!("{}  stream done  {:.0} ms", Self::self_name(), 1e3 * dt.as_secs_f32());
                    }),
                );
                let ret = response(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
                    .body(body_stream(stream))?;
                let dt = ctx.ts_ctor().elapsed();
                info!("{}  response dt {:.0} ms", Self::self_name(), 1e3 * dt.as_secs_f32());
                Ok(ret)
            }
            Err(e) => {
                let p = serde_json::to_string(&e)?;
                let res = response(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
                    .body(body_string(p))?;
                Ok(res)
            }
        }
    }
}

pub struct UpdateDbWithAllChannelConfigsHandler {}

impl UpdateDbWithAllChannelConfigsHandler {
    pub fn self_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn prefix() -> &'static str {
        "/api/4/maintenance/update_db_with_all_channel_configs"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        info!("{}", Self::self_name());
        let (head, _body) = req.into_parts();
        let _dry = match head.uri.query() {
            Some(q) => q.contains("dry"),
            None => false,
        };
        let res = dbconn::scan::update_db_with_all_channel_configs(node_config.clone()).await?;
        let stream = res.map(|k| match serde_json::to_string(&k) {
            Ok(mut item) => {
                item.push('\n');
                Ok(Bytes::from(item))
            }
            Err(e) => Err(e),
        });
        let stream = streams::print_on_done::PrintOnDone::new(
            stream,
            Box::pin(|ts| {
                let dt = ts.elapsed();
                info!("{}  stream done  {:.0} ms", Self::self_name(), 1e3 * dt.as_secs_f32());
            }),
        );
        let ret = response(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
            .body(body_stream(stream))?;
        let dt = ctx.ts_ctor().elapsed();
        info!("{}  response dt {:.0} ms", Self::self_name(), 1e3 * dt.as_secs_f32());
        Ok(ret)
    }
}

pub struct UpdateSearchCacheHandler {}

impl UpdateSearchCacheHandler {
    pub fn self_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn prefix() -> &'static str {
        "/api/4/maintenance/update_search_cache"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        info!("{}", Self::self_name());
        let (head, _body) = req.into_parts();
        let _dry = match head.uri.query() {
            Some(q) => q.contains("dry"),
            None => false,
        };
        let res = dbconn::scan::update_search_cache(node_config).await?;
        let ret = response(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, APP_JSON)
            .body(body_string(serde_json::to_string(&res)?))?;
        let dt = ctx.ts_ctor().elapsed();
        info!("{}  response dt {:.0} ms", Self::self_name(), 1e3 * dt.as_secs_f32());
        Ok(ret)
    }
}

#[allow(unused)]
async fn update_db_with_channel_names_3(
    req: Requ,
    _ctx: &ReqCtx,
    node_config: &NodeConfigCached,
) -> Result<StreamResponse, RetrievalError> {
    let (head, _body) = req.into_parts();
    let _dry = match head.uri.query() {
        Some(q) => q.contains("dry"),
        None => false,
    };
    let res = dbconn::scan::update_db_with_channel_names_3(node_config);
    let stream = res.map(|k| match serde_json::to_string(&k) {
        Ok(mut item) => {
            item.push('\n');
            Ok(Bytes::from(item))
        }
        Err(e) => Err(e),
    });
    let ret = response(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, APP_JSON_LINES)
        .body(body_stream(stream))?;
    Ok(ret)
}
