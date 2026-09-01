use crate::channelconfig::http_get_channel_config;
use dbconn::worker::PgQueue;
use netpod::ChConf;
use netpod::ChannelConfigQuery;
use netpod::ChannelConfigResponse;
use netpod::ChannelTypeConfigGen;
use netpod::DtNano;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::SfChFetchInfo;
use netpod::SfDbChannel;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use std::collections::BTreeMap;
use std::time::Duration;
use taskrun::tokio;

autoerr::create_error_v1!(
    name(Error, "ConfigQuorum"),
    enum variants {
        NotFound(SfDbChannel),
        MissingTimeRange,
        Timeout,
        ChannelConfig(crate::channelconfig::Error),
        ExpectSfDatabufferBackend,
        UnsupportedBackend,
        BadTimeRange,
        DbWorker(#[from] dbconn::worker::Error),
        FindChannel(#[from] dbconn::FindChannelError),
    },
);

impl From<crate::channelconfig::Error> for Error {
    fn from(value: crate::channelconfig::Error) -> Self {
        use crate::channelconfig::Error::*;
        match value {
            NotFoundChannel(chn) => Self::NotFound(chn),
            _ => Self::ChannelConfig(value),
        }
    }
}

fn decide_sf_ch_config_quorum(inp: Vec<ChannelConfigResponse>) -> Result<Option<ChannelTypeConfigGen>, Error> {
    let mut histo = BTreeMap::new();
    for item in inp {
        let item = match item {
            ChannelConfigResponse::SfDatabuffer(k) => ChannelTypeConfigGen::SfDatabuffer(SfChFetchInfo::new(
                k.backend,
                k.name,
                k.keyspace,
                DtNano::from_ms(k.timebinsize),
                k.byte_order,
                k.scalar_type,
                k.shape,
            )),
            ChannelConfigResponse::Daqbuf(k) => {
                ChannelTypeConfigGen::Scylla(ChConf::new(k.backend, k.series, k.kind, k.scalar_type, k.shape, k.name))
            }
        };
        if histo.contains_key(&item) {
            *histo.get_mut(&item).unwrap() += 1;
        } else {
            histo.insert(item, 0u32);
        }
    }
    let mut v: Vec<_> = histo.into_iter().collect();
    v.sort_unstable_by_key(|x| x.1);
    match v.pop() {
        Some((x, _)) => Ok(Some(x)),
        None => Ok(None),
    }
}

async fn find_sf_ch_config_quorum(
    channel: SfDbChannel,
    range: SeriesRange,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> Result<Option<SfChFetchInfo>, Error> {
    let range = match range {
        SeriesRange::TimeRange(x) => x,
        SeriesRange::PulseRange(_) => return Err(Error::MissingTimeRange),
    };
    let mut all = Vec::new();
    for node in &ncc.node_config.cluster.nodes {
        // TODO add a baseurl function to struct Node
        let qu = ChannelConfigQuery {
            channel: channel.clone(),
            range: range.clone(),
            // TODO
            expand: false,
        };
        let res = tokio::time::timeout(
            Duration::from_millis(4000),
            http_get_channel_config(qu, node.baseurl(), ctx),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        all.push(res);
    }
    let all: Vec<_> = all.into_iter().filter_map(|x| x).collect();
    let qu = decide_sf_ch_config_quorum(all)?;
    match qu {
        Some(item) => match item {
            ChannelTypeConfigGen::Scylla(_) => Err(Error::ExpectSfDatabufferBackend),
            ChannelTypeConfigGen::SfDatabuffer(item) => Ok(Some(item)),
        },
        None => Ok(None),
    }
}

pub async fn find_config_basics_quorum(
    channel: SfDbChannel,
    range: SeriesRange,
    ctx: &ReqCtx,
    pgqueue: &PgQueue,
    ncc: &NodeConfigCached,
) -> Result<Option<ChannelTypeConfigGen>, Error> {
    trace!("find_config_basics_quorum");
    if let Some(_cfg) = &ncc.node.sf_databuffer {
        let channel = if channel.name().is_empty() {
            if let Some(_) = channel.series() {
                pgqueue.find_sf_channel_by_series(channel).await??
            } else {
                channel
            }
        } else {
            channel
        };
        match find_sf_ch_config_quorum(channel, range, ctx, ncc).await? {
            Some(x) => Ok(Some(ChannelTypeConfigGen::SfDatabuffer(x))),
            None => Ok(None),
        }
    } else if ncc.node_config.is_backend_scylla() {
        let range = netpod::range::evrange::NanoRange::try_from(&range).map_err(|_| Error::BadTimeRange)?;
        let ret = crate::channelconfig::channel_config(range, channel, pgqueue, ncc).await?;
        Ok(ret)
    } else {
        Err(Error::UnsupportedBackend)
    }
}
