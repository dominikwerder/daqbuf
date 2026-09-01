use dbconn::worker::PgQueue;
use httpclient::url::Url;
use netpod::APP_JSON;
use netpod::AppendToUrl;
use netpod::ByteOrder;
use netpod::ChConf;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::ChannelTypeConfigGen;
use netpod::DtNano;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::ScalarType;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::DAY;
use scyllaconn::SeriesId;
use serde::Serialize;

autoerr::create_error_v1!(
    name(Error, "ChannelConfigNode"),
    enum variants {
        NotFoundChannel(SfDbChannel),
        ChannelConfig(dbconn::channelconfig::Error),
        DbWorker(#[from] dbconn::worker::Error),
        DiskConfig(#[from] disk::channelconfig::ConfigError),
        BackendConfigError,
        BadTestSetup,
        HttpReqError,
        HttpClient(#[from] httpclient::Error),
        ConfigParse(#[from] disk::parse::channelconfig::ConfigParseError),
        JsonParse(#[from] serde_json::Error),
        SearchWithGivenSeries,
        AsyncSend,
        AsyncRecv,
        Todo,
    },
);

impl From<async_channel::RecvError> for Error {
    fn from(_value: async_channel::RecvError) -> Self {
        Error::AsyncRecv
    }
}

impl From<netpod::AsyncChannelError> for Error {
    fn from(value: netpod::AsyncChannelError) -> Self {
        match value {
            netpod::AsyncChannelError::Send => Self::AsyncSend,
            netpod::AsyncChannelError::Recv => Self::AsyncRecv,
        }
    }
}

impl From<dbconn::channelconfig::Error> for Error {
    fn from(value: dbconn::channelconfig::Error) -> Self {
        use dbconn::channelconfig::Error::*;
        match value {
            NotFound(chn, _) => Self::NotFoundChannel(chn),
            SeriesNotFound(backend, series) => Self::NotFoundChannel(SfDbChannel::from_full(
                backend,
                Some(series),
                "",
                netpod::SeriesKind::ChannelData,
            )),
            _ => Self::ChannelConfig(value),
        }
    }
}

const TEST_BACKEND: &str = "testbackend-00";

fn channel_config_test_backend(channel: SfDbChannel) -> Result<ChannelTypeConfigGen, Error> {
    let backend = channel.backend();
    let ret = if channel.name() == "scalar-i32-be" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "wave-f64-be-n21" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            3,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::F64,
            Shape::Wave(21),
        );
        ret
    } else if channel.name() == "const-regular-scalar-i32-be" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "test-gen-i32-dim0-v00" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "test-gen-i32-dim0-v01" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            2,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::I32,
            Shape::Scalar,
        );
        ret
    } else if channel.name() == "test-gen-f64-dim1-v00" {
        let ret = SfChFetchInfo::new(
            backend,
            channel.name(),
            3,
            DtNano::from_ns(DAY),
            ByteOrder::Big,
            ScalarType::F64,
            Shape::Wave(21),
        );
        ret
    } else {
        error!("no test information");
        return Err(Error::NotFoundChannel(channel));
    };
    Ok(ChannelTypeConfigGen::SfDatabuffer(ret))
}

pub async fn channel_config(
    range: NanoRange,
    channel: SfDbChannel,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    if channel.backend() == TEST_BACKEND {
        Ok(Some(channel_config_test_backend(channel)?))
    } else if ncc.node_config.is_backend_scylla() {
        debug!("try to get ChConf for scylla type backend");
        let ret = scylla_chconf_from_sf_db_channel(range, channel, pgqueue).await?;
        Ok(Some(ChannelTypeConfigGen::Scylla(ret)))
    } else if ncc.node.sf_databuffer.is_some() {
        debug!("channel_config  channel {channel:?}");
        let k = disk::channelconfig::channel_config_best_match(range, channel.clone(), ncc).await?;
        match k {
            Some(config) => {
                debug!("channel_config  config  {config:?}");
                let ret = SfChFetchInfo::new(
                    config.channel.backend(),
                    config.channel.name(),
                    config.keyspace,
                    config.time_bin_size,
                    config.byte_order,
                    config.scalar_type,
                    config.shape,
                );
                let ret = ChannelTypeConfigGen::SfDatabuffer(ret);
                Ok(Some(ret))
            }
            None => Ok(None),
        }
    } else {
        Err(Error::BackendConfigError)
    }
}

pub async fn series_config(
    range: NanoRange,
    channel: SfDbChannel,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    if channel.backend() == TEST_BACKEND {
        Ok(Some(channel_config_test_backend(channel)?))
    } else if ncc.node_config.is_backend_scylla() {
        debug!("try to get ChConf for scylla type backend");
        let ret = scylla_chconf_from_sf_db_channel(range, channel, pgqueue).await?;
        Ok(Some(ChannelTypeConfigGen::Scylla(ret)))
    } else if ncc.node.sf_databuffer.is_some() {
        Err(Error::BackendConfigError)
    } else {
        Err(Error::BackendConfigError)
    }
}

#[derive(Debug, Serialize)]
pub enum ChannelConfigsGen {
    Scylla(ChConf),
    SfDatabuffer(disk::parse::channelconfig::ChannelConfigs),
}

pub async fn channel_configs(channel: SfDbChannel, ncc: &NodeConfigCached) -> Result<ChannelConfigsGen, Error> {
    if channel.backend() == TEST_BACKEND {
        let ret = match channel_config_test_backend(channel)? {
            ChannelTypeConfigGen::Scylla(x) => ChannelConfigsGen::Scylla(x),
            ChannelTypeConfigGen::SfDatabuffer(_) => {
                // ChannelConfigsGen::SfDatabuffer(todo!())
                let e = Error::BadTestSetup;
                warn!("{e}");
                return Err(e);
            }
        };
        Ok(ret)
    } else if ncc.node_config.is_backend_scylla() {
        debug!("try to get ChConf for scylla type backend");
        let ret = scylla_all_chconf_from_sf_db_channel(&channel, ncc)
            .await
            .map_err(Error::from)?;
        Ok(ChannelConfigsGen::Scylla(ret))
    } else if ncc.node.sf_databuffer.is_some() {
        debug!("channel_config  channel {channel:?}");
        let configs = disk::channelconfig::channel_configs(channel.clone(), ncc).await?;
        Ok(ChannelConfigsGen::SfDatabuffer(configs))
    } else {
        Err(Error::BackendConfigError)
    }
}

pub async fn http_get_channel_config(
    qu: ChannelConfigQuery,
    baseurl: Url,
    ctx: &ReqCtx,
) -> Result<Option<ChannelConfigResponse>, Error> {
    let url = baseurl;
    let mut url = url.join("/api/4/channel/config").unwrap();
    qu.append_to_url(&mut url);
    let res = httpclient::http_get(url, APP_JSON, ctx).await?;
    use httpclient::http::StatusCode;
    if res.head.status == StatusCode::NOT_FOUND {
        Ok(None)
    } else if res.head.status == StatusCode::OK {
        let ret: ChannelConfigResponse = serde_json::from_slice(&res.body)?;
        Ok(Some(ret))
    } else {
        // let b = &res.body;
        // let s = String::from_utf8_lossy(&b[0..b.len().min(256)]);
        Err(Error::HttpReqError)
    }
}

async fn scylla_chconf_from_sf_db_channel(
    range: NanoRange,
    channel: SfDbChannel,
    pgqueue: &PgQueue,
) -> Result<ChConf, Error> {
    trace!("scylla_chconf_from_sf_db_channel  {:?}", channel);
    if let Some(series) = channel.series() {
        let ret = pgqueue.chconf_for_series(channel.backend(), series).await??;
        Ok(ret)
    } else {
        // TODO let called function allow to return None instead of error-not-found
        let ret = pgqueue.chconf_best_matching_name_range(channel, range).await??;
        Ok(ret)
    }
}

pub async fn scylla_chconf_from_series(backend: String, series: SeriesId, pgqueue: &PgQueue) -> Result<ChConf, Error> {
    trace!("scylla_chconf_from_series  {:?}", series);
    let ret = pgqueue.chconf_for_series(&backend, series.id()).await??;
    Ok(ret)
}

async fn scylla_all_chconf_from_sf_db_channel(channel: &SfDbChannel, _ncc: &NodeConfigCached) -> Result<ChConf, Error> {
    if let Some(_) = channel.series() {
        Err(Error::SearchWithGivenSeries)
    } else {
        Err(Error::Todo)
    }
}
