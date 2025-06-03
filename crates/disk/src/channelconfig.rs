use crate::SfDbChConf;
use daqbuf_err as err;
use err::*;
#[allow(unused)]
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::DAY;
use netpod::ByteOrder;
use netpod::DtNano;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::SfDbChannel;
use netpod::TsNano;
use parse::channelconfig::extract_matching_config_entry;
use parse::channelconfig::parse_config;
use parse::channelconfig::ChannelConfigs;
use parse::channelconfig::ConfigEntry;
use parse::channelconfig::ConfigParseError;
use std::time::Duration;
use std::time::SystemTime;
use streams::tcprawclient::TEST_BACKEND;
use taskrun::tokio;

autoerr::create_error_v1!(
    name(ConfigError, "ChannelConfig"),
    enum variants {
        ParseError(ConfigParseError),
        NotFound,
        Error,
    },
);

impl From<ConfigParseError> for ConfigError {
    fn from(value: ConfigParseError) -> Self {
        match value {
            ConfigParseError::FileNotFound => ConfigError::NotFound,
            x => ConfigError::ParseError(x),
        }
    }
}

pub async fn config_entry_best_match(
    range: &NanoRange,
    channel: SfDbChannel,
    ncc: &NodeConfigCached,
) -> Result<Option<ConfigEntry>, ConfigError> {
    let channel_config = match read_local_config(channel.clone(), ncc.clone()).await {
        Ok(x) => x,
        Err(e) => match e {
            ConfigParseError::FileNotFound => return Ok(None),
            e => return Err(e.into()),
        },
    };
    let entry_res = match extract_matching_config_entry(range, &channel_config) {
        Ok(k) => k,
        Err(e) => return Err(e)?,
    };
    match entry_res.best() {
        None => Ok(None),
        Some(x) => Ok(Some(x.clone())),
    }
}

async fn read_local_config_real(
    channel: SfDbChannel,
    ncc: &NodeConfigCached,
) -> Result<ChannelConfigs, ConfigParseError> {
    use std::io::ErrorKind;
    let path = ncc
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| ConfigParseError::NotSupportedOnNode)?
        .data_base_path
        .join("config")
        .join(channel.name())
        .join("latest")
        .join("00000_Config");
    match tokio::fs::read(&path).await {
        Ok(buf) => parse_config(&buf),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => Err(ConfigParseError::FileNotFound),
            ErrorKind::PermissionDenied => Err(ConfigParseError::PermissionDenied(path.to_string_lossy().into())),
            e => {
                error!("read_local_config_real {e:?}");
                Err(ConfigParseError::IO)
            }
        },
    }
}

async fn read_local_config_test(
    channel: SfDbChannel,
    ncc: &NodeConfigCached,
) -> Result<ChannelConfigs, ConfigParseError> {
    if channel.name() == "test-gen-i32-dim0-v00" {
        let ts = 0;
        let ret = ChannelConfigs {
            format_version: 0,
            channel_name: channel.name().into(),
            entries: vec![ConfigEntry {
                ts: TsNano::from_ns(ts),
                ts_human: SystemTime::UNIX_EPOCH + Duration::from_nanos(ts as u64),
                pulse: 0,
                ks: 2,
                bs: DtNano::from_ns(DAY),
                split_count: ncc.node_config.cluster.nodes.len() as _,
                status: -1,
                bb: -1,
                modulo: -1,
                offset: -1,
                precision: -1,
                scalar_type: ScalarType::I32,
                is_compressed: false,
                is_shaped: false,
                is_array: false,
                byte_order: ByteOrder::Big,
                compression_method: None,
                shape: None,
                source_name: None,
                unit: None,
                description: None,
                optional_fields: None,
                value_converter: None,
            }],
        };
        Ok(ret)
    } else if channel.name() == "test-gen-i32-dim0-v01" {
        let ts = 0;
        let ret = ChannelConfigs {
            format_version: 0,
            channel_name: channel.name().into(),
            entries: vec![ConfigEntry {
                ts: TsNano::from_ns(ts),
                ts_human: SystemTime::UNIX_EPOCH + Duration::from_nanos(ts as u64),
                pulse: 0,
                ks: 2,
                bs: DtNano::from_ns(DAY),
                split_count: ncc.node_config.cluster.nodes.len() as _,
                status: -1,
                bb: -1,
                modulo: -1,
                offset: -1,
                precision: -1,
                scalar_type: ScalarType::I32,
                is_compressed: false,
                is_shaped: false,
                is_array: false,
                byte_order: ByteOrder::Big,
                compression_method: None,
                shape: None,
                source_name: None,
                unit: None,
                description: None,
                optional_fields: None,
                value_converter: None,
            }],
        };
        Ok(ret)
    } else {
        Err(ConfigParseError::NotSupported)
    }
}

// TODO can I take parameters as ref, even when used in custom streams?
async fn read_local_config(channel: SfDbChannel, ncc: NodeConfigCached) -> Result<ChannelConfigs, ConfigParseError> {
    if channel.backend() == TEST_BACKEND {
        read_local_config_test(channel, &ncc).await
    } else {
        read_local_config_real(channel, &ncc).await
    }
}

pub async fn channel_configs(
    channel: SfDbChannel,
    node_config: &NodeConfigCached,
) -> Result<ChannelConfigs, ConfigParseError> {
    read_local_config(channel.clone(), node_config.clone()).await
}

pub async fn channel_config_best_match(
    range: NanoRange,
    channel: SfDbChannel,
    ncc: &NodeConfigCached,
) -> Result<Option<SfDbChConf>, ConfigError> {
    let best = config_entry_best_match(&range, channel.clone(), ncc).await?;
    match best {
        None => Ok(None),
        Some(entry) => {
            let shape = match entry.to_shape() {
                Ok(k) => k,
                // TODO pass error to caller
                Err(_e) => return Err(ConfigError::Error)?,
            };
            let channel_config = SfDbChConf {
                channel: channel.clone(),
                keyspace: entry.ks as u8,
                time_bin_size: entry.bs.clone(),
                shape,
                scalar_type: entry.scalar_type.clone(),
                byte_order: entry.byte_order.clone(),
                array: entry.is_array,
                compression: entry.is_compressed,
            };
            Ok(Some(channel_config))
        }
    }
}
