mod channels;
mod cmder;
mod futs;
mod streamtask;

use crate::ca::conn2::asynchan;
use crate::ca::connset2::connset::cmder::ConnSetCmder;
use crate::ca::findioc::FindIocRes;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::CaIngestOpts;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
pub use futs::FutShutdown;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use scywr::insertqueues::InsertQueuesTx;
use std::collections::VecDeque;
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
    cmder: ConnSetCmder,
    cmd_rx: asynchan::Receiver<ConnSetCmd>,
    cmd_fut: Option<ErasedFuture<(), 4>>,
    find_ioc_res_rx: Pin<Box<async_channel::Receiver<VecDeque<FindIocRes>>>>,
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
        let (cmd_tx, cmd_rx) = asynchan::bounded(100, "ConnSetCmder");
        let cmder = ConnSetCmder::new(cmd_tx);
        ConnSet {
            backend,
            local_epics_hostname,
            cmder,
            cmd_rx,
            cmd_fut: None,
            find_ioc_res_rx: Box::pin(find_ioc_res_rx),
        }
    }

    pub async fn shutdown(&self) -> FutShutdown {
        todo!()
    }

    pub fn cmder(&self) -> &ConnSetCmder {
        &self.cmder
    }
}

macro_rules! poll_next_mark {
    ($fut:expr, $cx:expr, $hpp:expr) => {{
        let fut = $fut;
        let cx = $cx;
        let hpp = $hpp;
        match fut {
            Some(fut) => match fut.poll_unpin(cx) {
                Ready(()) => {
                    self.cmd_fut = None;
                    hpp.mark_progress();
                }
                Pending => {
                    hpp.mark_pending();
                }
            },
            None => match self.cmd_rx.poll_next_unpin(cx) {
                Ready(x) => match x {
                    Some(x) => {
                        hpp.mark_progress();
                    }
                    None => {}
                },
                Pending => {
                    hpp.mark_pending();
                }
            },
        }
    }};
}

impl Stream for ConnSet {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match &mut self.cmd_fut {
                Some(fut) => match fut.poll_unpin(cx) {
                    Ready(()) => {
                        self.cmd_fut = None;
                        hpp.mark_progress();
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                None => match self.cmd_rx.poll_next_unpin(cx) {
                    Ready(x) => match x {
                        Some(x) => {
                            hpp.mark_progress();
                        }
                        None => {}
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                },
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}
