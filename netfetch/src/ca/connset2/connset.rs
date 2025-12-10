mod cmder;
mod futs;
mod streamtask;

use crate::ca::conn2::asynchan;
use crate::conf::CaIngestOpts;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
pub use futs::FutShutdown;
use futures_util::Stream;
use scywr::insertqueues::InsertQueuesTx;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "ConnSet"),
    enum variants {
        Logic,
    },
);

#[derive(Debug, Clone)]
pub struct ChannelAdd {
    ch_cfg: crate::conf::ChannelConfig,
    restx: asynchan::Sender<Result<(), Error>>,
}

impl ChannelAdd {
    pub fn name(&self) -> &str {
        self.ch_cfg.name()
    }
}

#[derive(Debug)]
enum ConnSetCmdKind {
    ChannelAdd(ChannelAdd),
    Shutdown,
}

#[derive(Debug)]
pub struct ConnSetCmd {
    kind: ConnSetCmdKind,
}

impl ConnSetCmd {
    fn shutdown() -> Self {
        Self {
            kind: ConnSetCmdKind::Shutdown,
        }
    }
}

#[derive(Debug)]
pub struct ConnSet {
    backend: String,
    local_epics_hostname: String,
    cmd_tx: asynchan::Sender<ConnSetCmd>,
    cmd_rx: asynchan::Receiver<ConnSetCmd>,
}

impl ConnSet {
    pub fn new(
        backend: String,
        local_epics_hostname: String,
        iqtx: InsertQueuesTx,
        // TODO this used async channel Sender before. Rework into specialized type?
        // This seems to be for when I already know the type and shape. But what about the status series?
        channel_info_query_tx: ChannelInfoQuerySender,
        ingest_opts: CaIngestOpts,
    ) -> Self {
        // streamtask::run_in_task();
        let (find_ioc_res_tx, find_ioc_res_rx) = async_channel::bounded(400);
        let (find_ioc_query_tx, ioc_finder_jh) =
            crate::ca::finder::start_finder(find_ioc_res_tx.clone(), backend.clone(), ingest_opts).unwrap();
        ConnSet {
            backend,
            local_epics_hostname,
            cmd_tx,
            cmd_rx,
        }
    }

    pub async fn shutdown(&self) -> FutShutdown {
        todo!()
    }

    pub fn create_cmder(&self) -> cmder::ConnSetCmder {
        cmder::ConnSetCmder::new(self.cmd_tx.clone())
    }
}

impl Stream for ConnSet {
    type Item = Result<(), Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
