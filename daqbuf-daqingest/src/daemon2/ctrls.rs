//! Implements the control interface which the metrics http service uses.

use super::cmd::DaemonCmder;
use netfetch::ca::connset2::connset::ConnSetCmder;
use netfetch::ca::connset2::ctrls::ConnSetConn2Ctrls;
use netfetch::ca::connset::ChannelStatusesResponse;
use netfetch::conf::ChannelConfig;
use netfetch::daemon_common::ChannelName;
use netfetch::metrics::CaIngestCtrls;
use netfetch::metrics::Conn2Ctrls;
use netfetch::metrics::PostIngestCtrls;
use netfetch::metrics::RoutesResources;
use netfetch::metrics::types::MetricsPrometheusShort;
use std::pin::Pin;
use std::sync::Arc;

autoerr::create_error_v1!(
    name(Error, "Daemon2Ctrls"),
    enum variants {
        NotAvailable(String),
        NoScyllaConfigured,
    },
);

type BoxErr = Box<dyn std::error::Error>;
type FutBox<T> = Pin<Box<dyn Future<Output = Result<T, BoxErr>> + Send>>;

pub struct CaIngestCtrlsV2 {
    cmder: ConnSetCmder,
    daemon: DaemonCmder,
}

impl CaIngestCtrlsV2 {
    pub fn new(cmder: ConnSetCmder, daemon: DaemonCmder) -> Self {
        Self { cmder, daemon }
    }
}

impl CaIngestCtrls for CaIngestCtrlsV2 {
    fn timer_tick(&self, v: u32) -> Box<dyn Future<Output = u32>> {
        Box::new(async move { v })
    }

    fn get_metrics(&self) -> FutBox<MetricsPrometheusShort> {
        let daemon = self.daemon.clone();
        let fut = async move {
            let ret = daemon.get_metrics().await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channel_add(&self, conf: ChannelConfig) -> FutBox<()> {
        let cmder = self.cmder.clone();
        let fut = async move {
            cmder.channel_add(conf).await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn channel_remove(&self, name: ChannelName) -> FutBox<()> {
        let cmder = self.cmder.clone();
        let fut = async move {
            cmder.channel_remove(name.name().to_string()).await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn config_reload(&self) -> FutBox<()> {
        let daemon = self.daemon.clone();
        let fut = async move {
            daemon.config_reload().await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn shutdown(&self) -> FutBox<()> {
        let daemon = self.daemon.clone();
        let fut = async move {
            daemon.shutdown().await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn channel_states(&self, name: String, limit: u64) -> FutBox<ChannelStatusesResponse> {
        let _ = (name, limit);
        // `ChannelStatusesResponse` is produced by the old connset only. The conn2 equivalent is
        // reachable via `cmd_dyn_v1` with `channel_details_v00`.
        let fut = async move { Err(Box::new(Error::NotAvailable(format!("channel_states, use cmd_dyn_v1 with channel_details_v00"))) as _) };
        Box::pin(fut)
    }

    fn conn2_ctrls(&self) -> Pin<Box<dyn Future<Output = Option<Box<dyn Conn2Ctrls>>> + Send>> {
        let ctrls = ConnSetConn2Ctrls::new(self.cmder.clone());
        let fut = async move { Some(Box::new(ctrls) as _) };
        Box::pin(fut)
    }
}

pub struct PostIngestCtrlsV2 {
    res: Option<Arc<RoutesResources>>,
}

impl PostIngestCtrlsV2 {
    pub fn new(res: Option<Arc<RoutesResources>>) -> Self {
        Self { res }
    }
}

impl PostIngestCtrls for PostIngestCtrlsV2 {
    fn resources(&self) -> FutBox<Arc<RoutesResources>> {
        let res = self.res.clone();
        let fut = async move {
            match res {
                Some(x) => Ok(x),
                None => Err(Box::new(Error::NoScyllaConfigured) as _),
            }
        };
        Box::pin(fut)
    }
}
