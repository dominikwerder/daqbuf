//! Adapters which expose the `ConnSet` command interface to the metrics http service.

use crate::ca::conn2::conn::StatusDetail;
use crate::ca::connset2::connset::ConnSetCmder;
use crate::ca::connset2::connset::StatusV1Req;
use crate::metrics::ChannelsForAddrInfoV1;
use crate::metrics::ChannelsForAddrInfoV2;
use crate::metrics::ConnectionListV1;
use crate::metrics::status_v1;
use crate::metrics::status_v1::ChannelSelector;
use crate::metrics::status_v1::StatusFull;
use crate::metrics::status_v1::StatusLight;
use crate::metrics::types::MetricsPrometheusShort;
use regex::Regex;
use std::net::SocketAddrV4;
use std::pin::Pin;

type BoxErr = Box<dyn std::error::Error>;
type FutBox<T> = Pin<Box<dyn Future<Output = Result<T, BoxErr>> + Send>>;

/// Implements the conn2 part of the http control interface on top of a [`ConnSetCmder`].
#[derive(Clone, Debug)]
pub struct ConnSetConn2Ctrls {
    cmder: ConnSetCmder,
}

impl ConnSetConn2Ctrls {
    pub fn new(cmder: ConnSetCmder) -> Self {
        Self { cmder }
    }
}

impl crate::metrics::Conn2Ctrls for ConnSetConn2Ctrls {
    fn status_light_v1(&self, sel: ChannelSelector) -> FutBox<StatusLight> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let req = StatusV1Req {
                sel,
                detail: StatusDetail::Light,
            };
            let ret = cmder.status_v1(req).await?;
            Ok(status_v1::assemble_light(ret))
        };
        Box::pin(fut)
    }

    fn status_full_v1(&self, sel: ChannelSelector) -> FutBox<StatusFull> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let req = StatusV1Req {
                sel,
                detail: StatusDetail::Full,
            };
            let ret = cmder.status_v1(req).await?;
            Ok(status_v1::assemble_full(ret))
        };
        Box::pin(fut)
    }

    fn connection_list_get_v1(&self) -> FutBox<ConnectionListV1> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.connection_list_get_v1().await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channels_for_addr_v1(&self, addr: SocketAddrV4) -> FutBox<ChannelsForAddrInfoV1> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channels_for_addr_v1(addr).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channels_for_addr_v2(&self, addr: SocketAddrV4, name: String) -> FutBox<ChannelsForAddrInfoV2> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channels_for_addr_v2(addr, name).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn cmd_dyn_v1(&self, cmd: String) -> FutBox<serde_json::Value> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.cmd_dyn_v1(cmd).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channel_add_v1(&self, name: String) -> FutBox<()> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channel_add_v1(name).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channel_remove_v1(&self, name: String) -> FutBox<()> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channel_remove_v1(name).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn get_metrics(&self) -> FutBox<MetricsPrometheusShort> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.metrics_get_v1().await?;
            Ok(ret)
        };
        Box::pin(fut)
    }
}
