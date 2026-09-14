use super::Error;
use async_channel::Receiver;
use async_channel::Sender;
use netfetch::metrics::status_v1::ScyllaStatus;
use netfetch::metrics::types::MetricsPrometheusShort;

#[derive(Debug)]
pub enum DaemonCmd {
    GetMetrics(Sender<MetricsPrometheusShort>),
    ScyllaStatus(Sender<ScyllaStatus>),
    ConfigReload(Sender<Result<(), String>>),
    Shutdown(Sender<()>),
}

impl DaemonCmd {
    pub fn name(&self) -> &'static str {
        match self {
            DaemonCmd::GetMetrics(_) => "GetMetrics",
            DaemonCmd::ScyllaStatus(_) => "ScyllaStatus",
            DaemonCmd::ConfigReload(_) => "ConfigReload",
            DaemonCmd::Shutdown(_) => "Shutdown",
        }
    }
}

/// Handle to send commands to the daemon run loop.
#[derive(Clone, Debug)]
pub struct DaemonCmder {
    tx: Sender<DaemonCmd>,
}

impl DaemonCmder {
    pub fn new(tx: Sender<DaemonCmd>) -> Self {
        Self { tx }
    }

    async fn send<T>(&self, cmd: DaemonCmd, rx: Receiver<T>) -> Result<T, Error> {
        self.tx.send(cmd).await.map_err(|_| Error::ChannelSend)?;
        let ret = rx.recv().await.map_err(|_| Error::ChannelRecv)?;
        Ok(ret)
    }

    pub async fn get_metrics(&self) -> Result<MetricsPrometheusShort, Error> {
        let (tx, rx) = async_channel::bounded(1);
        self.send(DaemonCmd::GetMetrics(tx), rx).await
    }

    pub async fn scylla_status(&self) -> Result<ScyllaStatus, Error> {
        let (tx, rx) = async_channel::bounded(1);
        self.send(DaemonCmd::ScyllaStatus(tx), rx).await
    }

    pub async fn config_reload(&self) -> Result<(), Error> {
        let (tx, rx) = async_channel::bounded(1);
        self.send(DaemonCmd::ConfigReload(tx), rx).await?.map_err(Error::Msg)
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        let (tx, rx) = async_channel::bounded(1);
        self.send(DaemonCmd::Shutdown(tx), rx).await
    }
}
