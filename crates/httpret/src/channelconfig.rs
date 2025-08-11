use crate::requests::accepts_json_or_all;
use crate::response;
use crate::ServiceSharedResources;
use dbconn::create_connection;
use dbconn::worker::PgQueue;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::query::prebinned::PreBinnedQuery;
use netpod::query::PulseRangeQuery;
use netpod::query::TimeRangeQuery;
use netpod::range::evrange::SeriesRange;
use netpod::req_uri_to_url;
use netpod::timeunits::*;
use netpod::ttl::RetentionTime;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::ChannelTypeConfigGen;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::ScalarType;
use netpod::SeriesConfigQuery;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::UseScylla6Workarounds;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use nodenet::configquorum::find_config_basics_quorum;
use query::api4::binned::BinnedQuery;
use query::api4::events::PlainEventsQuery;
use serde::Deserialize;
use serde::Serialize;
use series::SeriesId;
use std::collections::BTreeMap;
use url::Url;

autoerr::create_error_v1!(
    name(Error, "ChannelConfigError"),
    enum variants {
        NotFound(SfDbChannel),
        ConfigQuorum(#[from] nodenet::configquorum::Error),
        ConfigNode(#[from] nodenet::channelconfig::Error),
        Http(#[from] crate::Error),
        HttpCrate(#[from] http::Error),
        // TODO create dedicated error type for query parsing
        BadQuery(#[from] daqbuf_err::Error),
        MissingBackend,
        MissingScalarType,
        MissingShape,
        MissingShapeKind,
        MissingEdge,
        MissingTimerange,
        MissingChannelName,
        Uri(#[from] netpod::UriError),
        ChannelConfigQuery(daqbuf_err::Error),
        ExpectScyllaBackend,
        Pg(#[from] dbconn::pg::Error),
        Scylla(String),
        Join,
        OtherErr(daqbuf_err::Error),
        PgWorker(#[from] dbconn::worker::Error),
        Async(#[from] netpod::AsyncChannelError),
        ChannelConfig(#[from] dbconn::channelconfig::Error),
        Netpod(#[from] netpod::Error),
        ScyllaConn(#[from] scyllaconn::conn::Error),
        ScyllaExecution(#[from] scyllaconn::scylla::errors::ExecutionError),
        ScyllaPagerExecution(#[from] scyllaconn::scylla::errors::PagerExecutionError),
        ScyllanextRow(#[from] scyllaconn::scylla::errors::NextRowError),
        ScyllaTypeCheck(#[from] scyllaconn::scylla::deserialize::TypeCheckError),
    },
);

fn other_err_error(e: daqbuf_err::Error) -> Error {
    Error::OtherErr(e)
}

impl From<taskrun::tokio::task::JoinError> for Error {
    fn from(_e: taskrun::tokio::task::JoinError) -> Self {
        Self::Join
    }
}

impl From<Error> for crate::err::Error {
    fn from(e: Error) -> Self {
        Self::with_msg_no_trace(format!("{e} TODO add public message"))
    }
}

impl Error {
    fn to_public_response(self) -> http::Response<httpclient::StreamBody> {
        use httpclient::internal_error;
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        let js = serde_json::json!({
            "message": self.to_string(),
        });
        if let Ok(body) = serde_json::to_string_pretty(&js) {
            match response(status)
                .header(http::header::CONTENT_TYPE, APP_JSON)
                .body(body_string(body))
            {
                Ok(res) => res,
                Err(e) => {
                    error!("can not generate http error response {e}");
                    internal_error()
                }
            }
        } else {
            internal_error()
        }
    }
}

impl crate::IntoBoxedError for Error {}

pub async fn chconf_from_events_quorum(
    q: &PlainEventsQuery,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    let ret = find_config_basics_quorum(q.channel().clone(), q.range().clone(), ctx, pgqueue, ncc).await?;
    Ok(ret)
}

pub async fn chconf_from_prebinned(
    q: &PreBinnedQuery,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    let ret = find_config_basics_quorum(q.channel().clone(), q.patch().patch_range(), ctx, pgqueue, ncc).await?;
    Ok(ret)
}

pub async fn ch_conf_from_binned(
    q: &BinnedQuery,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    let ret = find_config_basics_quorum(q.channel().clone(), q.range().clone(), ctx, pgqueue, ncc).await?;
    Ok(ret)
}

pub struct ChannelConfigHandler {}

impl ChannelConfigHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/channel/config" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        pgqueue: &PgQueue,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match self.channel_config(req, pgqueue, &node_config).await {
                    Ok(k) => Ok(k),
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

    async fn channel_config(
        &self,
        req: Requ,
        pgqueue: &PgQueue,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        let url = req_uri_to_url(req.uri())?;
        let q = ChannelConfigQuery::from_url(&url)?;
        let conf =
            nodenet::channelconfig::channel_config(q.range.clone(), q.channel.clone(), pgqueue, node_config).await?;
        match conf {
            Some(conf) => {
                let res: ChannelConfigResponse = conf.into();
                let ret = response(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, APP_JSON)
                    .body(ToJsonBody::from(&res).into_body())?;
                Ok(ret)
            }
            None => {
                let ret = response(StatusCode::NOT_FOUND)
                    .header(http::header::CONTENT_TYPE, APP_JSON)
                    .body(body_empty())?;
                Ok(ret)
            }
        }
    }
}

pub struct ChannelConfigsHandler {}

impl ChannelConfigsHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/channel/configs" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                match self.channel_configs(req, &node_config).await {
                    Ok(k) => Ok(k),
                    Err(e) => {
                        warn!("got error from channel_config: {e}");
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

    async fn channel_configs(&self, req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        info!("channel_configs");
        let url = req_uri_to_url(req.uri())?;
        let q = ChannelConfigQuery::from_url(&url)?;
        info!("channel_configs  for q {q:?}");
        let ch_confs = nodenet::channelconfig::channel_configs(q.channel, ncc).await?;
        let ret = response(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, APP_JSON)
            .body(ToJsonBody::from(&ch_confs).into_body())?;
        Ok(ret)
    }
}

pub struct ChannelConfigQuorumHandler {}

impl ChannelConfigQuorumHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/channel/config/quorum" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        pgqueue: &PgQueue,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept.contains(APP_JSON) || accept.contains(ACCEPT_ALL) {
                match self.channel_config_quorum(req, ctx, pgqueue, &node_config).await {
                    Ok(k) => Ok(k),
                    Err(e) => {
                        warn!("from channel_config_quorum: {e}");
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

    async fn channel_config_quorum(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        pgqueue: &PgQueue,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        info!("channel_config_quorum");
        let url = req_uri_to_url(req.uri())?;
        let q = ChannelConfigQuery::from_url(&url)?;
        info!("channel_config_quorum  for q {q:?}");
        let ch_confs =
            nodenet::configquorum::find_config_basics_quorum(q.channel, q.range.into(), ctx, pgqueue, ncc).await?;
        let ret = response(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, APP_JSON)
            .body(ToJsonBody::from(&ch_confs).into_body())?;
        Ok(ret)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigsHisto {
    scalar_types: Vec<(ScalarType, Vec<(Shape, u32)>)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelsWithTypeQuery {
    scalar_type: ScalarType,
    shape: Shape,
}

impl FromUrl for ChannelsWithTypeQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let s = pairs.get("scalar_type").ok_or_else(|| Error::MissingScalarType)?;
        //let scalar_type = ScalarType::from_bsread_str(s)?;
        let scalar_type: ScalarType =
            serde_json::from_str(&format!("\"{s}\"")).map_err(|_| Error::MissingScalarType)?;
        let s = pairs.get("shape").ok_or_else(|| Error::MissingShape)?;
        let shape = Shape::from_dims_str(s)?;
        Ok(Self { scalar_type, shape })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScyllaChannelEventSeriesIdQuery {
    backend: String,
    #[serde(rename = "channelName")]
    name: String,
    #[serde(rename = "scalarType")]
    scalar_type: ScalarType,
    shape: Shape,
    #[serde(rename = "doCreate", skip_serializing_if = "bool_false")]
    do_create: bool,
}

fn bool_false(x: &bool) -> bool {
    *x == false
}

impl FromUrl for ScyllaChannelEventSeriesIdQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Error> {
        let backend = pairs.get("backend").ok_or_else(|| Error::MissingBackend)?.into();
        let name = pairs
            .get("channelName")
            .ok_or_else(|| Error::MissingChannelName)?
            .into();
        let s = pairs.get("scalarType").ok_or_else(|| Error::MissingScalarType)?;
        let scalar_type: ScalarType =
            serde_json::from_str(&format!("\"{s}\"")).map_err(|_| Error::MissingScalarType)?;
        let s = pairs.get("shape").ok_or_else(|| Error::MissingShape)?;
        let shape = Shape::from_dims_str(s)?;
        let do_create = pairs.get("doCreate").map_or("false", |x| x.as_str()) == "true";
        Ok(Self {
            backend,
            name,
            scalar_type,
            shape,
            do_create,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScyllaChannelEventSeriesIdResponse {
    #[serde(rename = "seriesId")]
    series: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScyllaChannelsActiveQuery {
    tsedge: u64,
    #[serde(rename = "shapeKind")]
    shape_kind: u32,
    #[serde(rename = "scalarType")]
    scalar_type: ScalarType,
}

impl FromUrl for ScyllaChannelsActiveQuery {
    type Error = daqbuf_err::Error;

    fn from_url(url: &Url) -> Result<Self, daqbuf_err::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, daqbuf_err::Error> {
        let s = pairs
            .get("tsedge")
            .ok_or_else(|| daqbuf_err::Error::with_public_msg_no_trace("missing tsedge"))?;
        let tsedge: u64 = s.parse()?;
        let s = pairs
            .get("shapeKind")
            .ok_or_else(|| daqbuf_err::Error::with_public_msg_no_trace("missing shapeKind"))?;
        let shape_kind: u32 = s.parse()?;
        let s = pairs
            .get("scalarType")
            .ok_or_else(|| daqbuf_err::Error::with_public_msg_no_trace("missing scalarType"))?;
        let scalar_type: ScalarType = serde_json::from_str(&format!("\"{s}\""))?;
        info!("parsed scalar type  inp: {s:?}  val: {scalar_type:?}");
        Ok(Self {
            tsedge,
            scalar_type,
            shape_kind,
        })
    }
}

pub struct ScyllaChannelsActive {}

impl ScyllaChannelsActive {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/channels/active" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                let url = req_uri_to_url(req.uri())?;
                let q = ScyllaChannelsActiveQuery::from_url(&url).map_err(|e| Error::BadQuery(e))?;
                let res = self.get_channels(&q, node_config).await?;
                let body = ToJsonBody::from(&res).into_body();
                Ok(response(StatusCode::OK).body(body)?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn get_channels(
        &self,
        q: &ScyllaChannelsActiveQuery,
        node_config: &NodeConfigCached,
    ) -> Result<Vec<u64>, Error> {
        let scyco = node_config
            .node_config
            .cluster
            .scylla_st()
            .ok_or_else(|| Error::ExpectScyllaBackend)?;
        let scy = scyllaconn::conn::create_scy_session(scyco).await?;
        // Database stores tsedge/ts_msp in units of (10 sec), and we additionally map to the grid.
        let tsedge = q.tsedge / 10 / (6 * 2) * (6 * 2);
        info!(
            "ScyllaChannelsActive::get_channels  tsedge {} (10s)  {} (s)",
            tsedge,
            tsedge * 10
        );
        let mut ret = Vec::new();
        for part in 0..256 {
            let mut res = scy
            .query_iter(
                "select series from series_by_ts_msp where part = ? and ts_msp = ? and shape_kind = ? and scalar_type = ?",
                (part as i32, tsedge as i32, q.shape_kind as i32, q.scalar_type.to_scylla_i32()),
            )
            .await?.rows_stream::<(i64,)>()?;
            while let Some(row) = res.try_next().await? {
                ret.push(row.0 as u64);
            }
        }
        Ok(ret)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct IocForChannelQuery {
    #[serde(rename = "backend")]
    backend: String,
    #[serde(rename = "channelName")]
    name: String,
}

impl FromUrl for IocForChannelQuery {
    type Error = daqbuf_err::Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let backend = pairs
            .get("backend")
            .ok_or_else(|| daqbuf_err::Error::with_public_msg_no_trace("missing backend"))?
            .into();
        let name = pairs
            .get("channelName")
            .ok_or_else(|| daqbuf_err::Error::with_public_msg_no_trace("missing channelName"))?
            .into();
        Ok(Self { backend, name })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IocForChannelRes {
    #[serde(rename = "iocAddr")]
    ioc_addr: String,
}

pub struct IocForChannel {}

impl IocForChannel {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/channel/ioc" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                let url = req_uri_to_url(req.uri())?;
                let q = IocForChannelQuery::from_url(&url).map_err(|e| Error::BadQuery(e))?;
                match self.find(&q, node_config).await {
                    Ok(k) => {
                        let body = ToJsonBody::from(&k).into_body();
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => Ok(e.to_public_response()),
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn find(
        &self,
        q: &IocForChannelQuery,
        node_config: &NodeConfigCached,
    ) -> Result<Option<IocForChannelRes>, Error> {
        let dbconf = &node_config.node_config.cluster.database;
        let (pg_client, pgjh) = create_connection(dbconf).await.map_err(other_err_error)?;
        let rows = pg_client
            .query(
                "select addr from ioc_by_channel where facility = $1 and channel = $2",
                &[&q.backend, &q.name],
            )
            .await?;
        drop(pg_client);
        pgjh.await?.map_err(other_err_error)?;
        if let Some(row) = rows.first() {
            let ioc_addr = row.get(0);
            let ret = IocForChannelRes { ioc_addr };
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ScyllaSeriesTsMspQuery {
    channel: SfDbChannel,
    range: SeriesRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    use_scylla6_workarounds: Option<u32>,
}

impl FromUrl for ScyllaSeriesTsMspQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let channel = SfDbChannel::from_pairs(pairs)?;
        let range = if let Ok(x) = TimeRangeQuery::from_pairs(pairs) {
            SeriesRange::TimeRange(x.into())
        } else if let Ok(x) = PulseRangeQuery::from_pairs(pairs) {
            SeriesRange::PulseRange(x.into())
        } else {
            return Err(Error::MissingTimerange);
        };
        Ok(Self {
            channel,
            range,
            use_scylla6_workarounds: pairs.get("use_scylla6_workarounds").and_then(|x| x.parse().ok()),
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScyllaSeriesTsMspResponse {
    st_ts_msp_ms: Vec<String>,
    mt_ts_msp_ms: Vec<String>,
    lt_ts_msp_ms: Vec<String>,
}

pub struct ScyllaSeriesTsMsp {}

impl ScyllaSeriesTsMsp {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/scylla/series/tsMsp" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        shared_res: &ServiceSharedResources,
        _ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                let url = req_uri_to_url(req.uri())?;
                let q = ScyllaSeriesTsMspQuery::from_url(&url)?;
                match self.get_ts_msps(&q, shared_res).await {
                    Ok(k) => {
                        let body = ToJsonBody::from(&k).into_body();
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => Ok(e.to_public_response()),
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn get_ts_msps(
        &self,
        q: &ScyllaSeriesTsMspQuery,
        shared_res: &ServiceSharedResources,
    ) -> Result<ScyllaSeriesTsMspResponse, Error> {
        // TODO also use the cluster config default
        let use_scylla6_workarounds = q
            .use_scylla6_workarounds
            .map(From::from)
            .unwrap_or(UseScylla6Workarounds::production_default());
        let nano_range = if let SeriesRange::TimeRange(x) = q.range.clone() {
            x
        } else {
            todo!()
        };
        let chconf = shared_res
            .pgqueue
            .chconf_best_matching_name_range(q.channel.clone(), nano_range)
            .await??;
        use scyllaconn::SeriesId;
        let sid = SeriesId::new(chconf.series());
        let scyqueue = shared_res.scyqueue.clone().unwrap();
        let mut st_ts_msp_ms = Vec::new();
        let mut msp_stream = scyllaconn::events2::msp::MspStreamRt::new(
            RetentionTime::Short,
            sid,
            (&q.range).into(),
            use_scylla6_workarounds.clone(),
            scyqueue.clone(),
        );
        use chrono::TimeZone;
        while let Some(x) = msp_stream.next().await {
            let v = x.unwrap().ms();
            let st = chrono::Utc.timestamp_millis_opt(v as _).earliest().unwrap();
            let s = st.format(netpod::DATETIME_FMT_0MS).to_string();
            st_ts_msp_ms.push(s);
        }

        let mut mt_ts_msp_ms = Vec::new();
        let mut msp_stream = scyllaconn::events2::msp::MspStreamRt::new(
            RetentionTime::Medium,
            sid,
            (&q.range).into(),
            use_scylla6_workarounds.clone(),
            scyqueue.clone(),
        );
        while let Some(x) = msp_stream.next().await {
            let v = x.unwrap().ms();
            let st = chrono::Utc.timestamp_millis_opt(v as _).earliest().unwrap();
            let s = st.format(netpod::DATETIME_FMT_0MS).to_string();
            mt_ts_msp_ms.push(s);
        }

        let mut lt_ts_msp_ms = Vec::new();
        let mut msp_stream = scyllaconn::events2::msp::MspStreamRt::new(
            RetentionTime::Long,
            sid,
            (&q.range).into(),
            use_scylla6_workarounds.clone(),
            scyqueue.clone(),
        );
        while let Some(x) = msp_stream.next().await {
            let v = x.unwrap().ms();
            let st = chrono::Utc.timestamp_millis_opt(v as _).earliest().unwrap();
            let s = st.format(netpod::DATETIME_FMT_0MS).to_string();
            lt_ts_msp_ms.push(s);
        }

        let ret = ScyllaSeriesTsMspResponse {
            st_ts_msp_ms,
            mt_ts_msp_ms,
            lt_ts_msp_ms,
        };
        Ok(ret)
    }
}

#[derive(Serialize)]
pub struct AmbigiousChannel {
    series: u64,
    name: String,
    scalar_type: ScalarType,
    shape: Shape,
}

#[derive(Serialize)]
pub struct AmbigiousChannelNamesResponse {
    ambigious: Vec<AmbigiousChannel>,
}

pub struct AmbigiousChannelNames {}

impl AmbigiousChannelNames {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/channels/ambigious" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                match self.process(ncc).await {
                    Ok(k) => {
                        let body = ToJsonBody::from(&k).into_body();
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => Ok(e.to_public_response()),
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn process(&self, ncc: &NodeConfigCached) -> Result<AmbigiousChannelNamesResponse, Error> {
        let dbconf = &ncc.node_config.cluster.database;
        let (pg_client, pgjh) = create_connection(dbconf).await.map_err(other_err_error)?;
        let rows = pg_client
            .query(
                "select t2.series, t2.channel, t2.scalar_type, t2.shape_dims, t2.agg_kind from series_by_channel t1, series_by_channel t2 where t2.channel = t1.channel and t2.series != t1.series",
                &[],
            )
            .await?;
        drop(pg_client);
        pgjh.await?.map_err(other_err_error)?;
        let mut ret = AmbigiousChannelNamesResponse { ambigious: Vec::new() };
        for row in rows {
            let g = AmbigiousChannel {
                series: row.get::<_, i64>(0) as u64,
                name: row.get(1),
                scalar_type: ScalarType::from_scylla_i32(row.get(2))?,
                shape: Shape::from_scylla_shape_dims(&row.get::<_, Vec<i32>>(3))?,
            };
            ret.ambigious.push(g);
        }
        Ok(ret)
    }
}

struct Msps(Vec<u64>);
struct Lsps(Vec<u64>);
struct Pulses(Vec<u64>);
struct ValsF64(Vec<f64>);

fn test_data_f64_01() -> (Msps, Lsps, Pulses, ValsF64) {
    let mut msps = Msps(Vec::new());
    let mut lsps = Lsps(Vec::new());
    let mut pulses = Pulses(Vec::new());
    let mut vals = ValsF64(Vec::new());
    let mut msp = 0;
    let mut i1 = 0;
    for i in 0..2000 {
        let ts = SEC * 1600000000 + MIN * 2 * i;
        let pulse = 10000 + i;
        if msp == 0 || i1 >= 40 {
            msp = ts / MIN * MIN;
            i1 = 0;
        }
        msps.0.push(msp);
        lsps.0.push(ts - msp);
        pulses.0.push(pulse);
        vals.0.push(pulse as f64 + 0.4 + 0.2 * (pulse as f64).sin());
        i1 += 1;
    }
    (msps, lsps, pulses, vals)
}

pub struct GenerateScyllaTestData {}

impl GenerateScyllaTestData {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/test/generate/scylla" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            let accept_def = APP_JSON;
            let accept = req
                .headers()
                .get(http::header::ACCEPT)
                .map_or(accept_def, |k| k.to_str().unwrap_or(accept_def));
            if accept == APP_JSON || accept == ACCEPT_ALL {
                match self.process(node_config).await {
                    Ok(k) => {
                        let body = ToJsonBody::from(&k).into_body();
                        Ok(response(StatusCode::OK).body(body)?)
                    }
                    Err(e) => Ok(e.to_public_response()),
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn process(&self, node_config: &NodeConfigCached) -> Result<(), Error> {
        let scyconf = node_config.node_config.cluster.scylla_st().unwrap();
        let scy = scyllaconn::conn::create_scy_session(scyconf).await?;
        let series: u64 = 42001;
        // TODO query `ts_msp` for all MSP values und use that to delete from event table first.
        // Only later delete also from the `ts_msp` table.
        let mut it = scy
            .query_iter("select ts_msp from ts_msp where series = ?", (series as i64,))
            .await?
            .rows_stream::<(i64,)>()?;
        while let Some(row) = it.try_next().await? {
            let values = (series as i64, row.0);
            scy.query_unpaged("delete from events_scalar_f64 where series = ? and ts_msp = ?", values)
                .await?;
        }
        scy.query_unpaged("delete from ts_msp where series = ?", (series as i64,))
            .await?;

        // Generate
        let (msps, lsps, pulses, vals) = test_data_f64_01();
        let mut last = 0;
        for msp in msps.0.iter().map(|x| *x) {
            if msp != last {
                scy.query_unpaged(
                    "insert into ts_msp (series, ts_msp) values (?, ?)",
                    (series as i64, msp as i64),
                )
                .await?;
            }
            last = msp;
        }
        for (((msp, lsp), pulse), val) in msps.0.into_iter().zip(lsps.0).zip(pulses.0).zip(vals.0) {
            scy.query_unpaged(
                "insert into events_scalar_f64 (series, ts_msp, ts_lsp, pulse, value) values (?, ?, ?, ?, ?)",
                (series as i64, msp as i64, lsp as i64, pulse as i64, val),
            )
            .await?;
        }
        Ok(())
    }
}

pub struct SeriesConfigHandler {}

impl SeriesConfigHandler {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/series/config" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        pgqueue: &PgQueue,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match self.config(req, pgqueue, &node_config).await {
                    Ok(k) => Ok(k),
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

    async fn config(
        &self,
        req: Requ,
        pgqueue: &PgQueue,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        let url = req_uri_to_url(req.uri())?;
        let q = SeriesConfigQuery::from_url(&url)?;
        let conf = nodenet::channelconfig::scylla_chconf_from_series(q.backend, SeriesId::new(q.series), pgqueue).await;
        match conf {
            Ok(conf) => {
                let res: ChannelConfigResponse = conf.into();
                let ret = response(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, APP_JSON)
                    .body(ToJsonBody::from(&res).into_body())?;
                Ok(ret)
            }
            Err(e) => {
                let ret = response(StatusCode::NOT_FOUND)
                    .header(http::header::CONTENT_TYPE, APP_JSON)
                    .body(body_empty())?;
                Ok(ret)
            }
        }
    }
}
