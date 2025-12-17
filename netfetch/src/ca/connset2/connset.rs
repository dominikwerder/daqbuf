mod channels;
mod cmder;
mod futs;
mod streamtask;

use crate::ca::conn2;
use crate::ca::conn2::asynchan;
use crate::ca::conn2::conn::CaConn;
use crate::ca::conn2::conn::CaConnComm;
use crate::ca::connset2::connset::channels::pollcstm;
use crate::ca::connset2::connset::channels::pollcstm::PollCstm;
use crate::ca::connset2::connset::channels::pollcstm::PollRess;
use crate::ca::connset2::connset::cmder::ConnSetCmder;
use crate::ca::finder::FinderHandleV02;
use crate::ca::findioc::FindIocRes;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::CaIngestOpts;
use crate::conf::ChannelConfig;
use crate::misc::todoval;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
pub use futs::FutShutdown;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use scywr::insertqueues::InsertQueuesTx;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio;
use taskrun::tokio::task::JoinHandle;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if true { log::info!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ConnSet"),
    enum variants {
        DbPgSeriesByChannel(#[from] dbpg::seriesbychannel::Error),
        Channel(#[from] channels::channel::Error),
        Conn(#[from] conn2::conn::Error),
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
pub struct ChannelCat {
    channel: channels::channel::Channel,
    cmd_tx: asynchan::Sender<pollcstm::Cmd>,
}

#[derive(Debug)]
pub struct ConnSet {
    backend: String,
    local_epics_hostname: String,
    cmder: ConnSetCmder,
    cmd_rx: asynchan::Receiver<ConnSetCmd>,
    cmd_fut: Option<ErasedFuture<Result<(), Error>, 0x200>>,
    finder_handle: FinderHandleV02,
    channels: VecDeque<ChannelCat>,
    ch_info_tx: ChannelInfoQuerySender,
    ca_conns: BTreeMap<SocketAddrV4, (CaConnComm, JoinHandle<Result<(), Error>>)>,
}

impl ConnSet {
    pub async fn new(
        backend: String,
        local_epics_hostname: String,
        // iqtx: InsertQueuesTx,
        // TODO this used async channel Sender before. Rework into specialized type?
        // This seems to be for when I already know the type and shape. But what about the status series?
        // channel_info_query_tx: ChannelInfoQuerySender,
        ingest_opts: CaIngestOpts,
    ) -> Result<Self, Error> {
        // streamtask::run_in_task();
        // let (find_ioc_res_tx, find_ioc_res_rx) = async_channel::bounded(400);
        // let (find_ioc_query_tx, ioc_finder_jh) =
        //     crate::ca::finder::start_finder(find_ioc_res_tx.clone(), backend.clone(), ingest_opts).unwrap();
        let (finder_handle, finder_jh) =
            crate::ca::finder::start_finder_handle_v02(backend.clone(), ingest_opts.clone());
        let (cmd_tx, cmd_rx) = asynchan::bounded(100, "ConnSetCmder");
        let cmder = ConnSetCmder::new(cmd_tx);
        let conf = ChannelConfig::st_monitor("TEST:SLOW:SCALAR:F32:000000", "TEST");
        let channels = {
            let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelCmd");
            let e = ChannelCat {
                channel: channels::channel::Channel::new(backend.clone(), conf, cmd_rx),
                cmd_tx,
            };
            [e].into()
        };
        let ch_info_tx = {
            let (channel_info_query_tx, jhs, jh) = dbpg::seriesbychannel::start_lookup_workers::<
                dbpg::seriesbychannel::SalterRandom,
            >(2, ingest_opts.postgresql_config())
            .await?;
            ChannelInfoQuerySender::new(channel_info_query_tx)
        };
        let ret = ConnSet {
            backend,
            local_epics_hostname,
            cmder,
            cmd_rx,
            cmd_fut: None,
            finder_handle,
            channels,
            ch_info_tx,
            ca_conns: BTreeMap::new(),
        };
        Ok(ret)
    }

    pub async fn shutdown(&self) -> FutShutdown {
        todo!()
    }

    pub fn cmder(&self) -> &ConnSetCmder {
        &self.cmder
    }

    fn poll_channels(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        // TODO rework return type
        let self2 = self.get_mut();
        for ch in self2.channels.iter_mut() {
            let mut ress = PollRess::new(&self2.ch_info_tx, &self2.finder_handle);
            match ch.channel.poll_unpin(&mut ress, cx) {
                Ready(Some(x)) => match x {
                    Ok(x) => {
                        use channels::channel::ChannelActionItem;
                        match x {
                            ChannelActionItem::AddToCaConn(conf, addr) => {
                                trace!("poll_channels  ChannelActionItem::AddToCaConn  {addr}  {conf:?}");
                                if let Some((comm, jh)) = self2.ca_conns.get_mut(&addr) {
                                    let fut = async move {
                                        comm.channel_add(conf).await?;
                                        Ok(())
                                    };
                                    self2.cmd_fut = Some(ErasedFuture::new(fut));
                                } else {
                                    let conn =
                                        CaConn::new(self2.backend.clone(), addr, self2.local_epics_hostname.clone());
                                    let mut comm = conn.comm();
                                    let jh = {
                                        let fut = async move {
                                            let mut conn = conn;
                                            while let Some(x) = conn.next().await {
                                                trace!("ConnSet CaConn item {x:?}");
                                            }
                                            Ok::<_, Error>(())
                                        };
                                        tokio::spawn(fut)
                                    };
                                    self2.ca_conns.insert(addr, (comm.clone(), jh));
                                    let fut = async move {
                                        comm.channel_add(conf).await?;
                                        Ok(())
                                    };
                                    self2.cmd_fut = Some(ErasedFuture::new(fut));
                                }
                            }
                        }
                        return Ready(Ok(()));
                    }
                    Err(e) => {
                        return Ready(Err(e.into()));
                    }
                },
                Ready(None) => {
                    trace!("Channel is done  TODO status event, clean up");
                }
                Pending => {}
            }
        }
        Pending
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
                    Ready(x) => {
                        self.cmd_fut = None;
                        hpp.mark_progress();
                        match x {
                            Ok(()) => {}
                            Err(e) => {
                                break Ready(Some(Err(e)));
                            }
                        }
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
            match self.as_mut().poll_channels(cx) {
                Ready(x) => match x {
                    Ok(()) => {
                        hpp.mark_progress();
                    }
                    Err(e) => {
                        hpp.mark_progress();
                        break Ready(Some(Err(e)));
                    }
                },
                Pending => {
                    hpp.mark_pending();
                }
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
