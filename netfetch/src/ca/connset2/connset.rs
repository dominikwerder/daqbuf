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
use futures::TryFutureExt;
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
        Send,
        Logic,
    },
);

impl<T> From<asynchan::SendError<T>> for Error {
    fn from(_value: asynchan::SendError<T>) -> Self {
        Self::Send
    }
}

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
    cmd_fut_channel: Option<ErasedFuture<Result<(), Error>, 0x200>>,
    cmd_fut_comm: Option<ErasedFuture<Result<(), Error>, 0x200>>,
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
            cmd_fut_channel: None,
            cmd_fut_comm: None,
            finder_handle,
            channels: VecDeque::new(),
            ch_info_tx,
            ca_conns: BTreeMap::new(),
        };
        {
            let chname = "TEST:SLOW:SCALAR:F32:000000";
            warn!("SETTING UP A FIXED CHANNEL");
        }
        Ok(ret)
    }

    pub async fn shutdown(&self) -> FutShutdown {
        todo!()
    }

    pub fn cmder(&self) -> &ConnSetCmder {
        &self.cmder
    }

    async fn channel_add(&mut self, conf: ChannelConfig) -> Result<(), Error> {
        TODO impl via the cmder;
        let channels = {
            let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelCmd");
            let e = ChannelCat {
                channel: channels::channel::Channel::new(backend.clone(), conf, cmd_rx),
                cmd_tx,
            };
            [e].into()
        };
        // ret.cmder.channel_add(conf).await?;

        let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelCmd");
        let e = ChannelCat {
            channel: channels::channel::Channel::new(self.backend.clone(), conf, cmd_rx),
            cmd_tx,
        };
        Ok(())
    }

    async fn channel_remove(&mut self, conf: ChannelConfig) -> Result<(), Error> {
        //
        Ok(())
    }

    fn poll_channels(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<Option<(ErasedFuture<Result<(), Error>, 512>,)>, Error>> {
        use Poll::*;
        // TODO caller wants to handle only one potential future at a time.
        let mut hpp = HaveProgressPending::new();
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
                                if let Some((comm, _jh)) = self2.ca_conns.get_mut(&addr) {
                                    let fut = async move {
                                        comm.channel_add(conf).await?;
                                        Ok(())
                                    };
                                    hpp.mark_progress();
                                    return Ready(Ok(Some((ErasedFuture::new(fut),))));
                                } else {
                                    let conn =
                                        CaConn::new(self2.backend.clone(), addr, self2.local_epics_hostname.clone());
                                    let mut comm = conn.comm();
                                    let jh = tokio::spawn(conn.into_task().map_err(Error::from));
                                    self2.ca_conns.insert(addr, (comm.clone(), jh));
                                    let fut = async move {
                                        comm.channel_add(conf).await?;
                                        Ok(())
                                    };
                                    hpp.mark_progress();
                                    return Ready(Ok(Some((ErasedFuture::new(fut),))));
                                }
                            }
                            ChannelActionItem::RemoveFromCaConn(conf, reminfo, mut done_tx) => {
                                // TODO send a command to the CaConn to remove the channel, wait for confirmation.
                                if let Some(addr) = reminfo.addr {
                                    if let Some((comm, _jh)) = self2.ca_conns.get_mut(&addr) {
                                        let fut = async move {
                                            comm.channel_remove(conf).await?;
                                            let _ = done_tx.send(0).await;
                                            Ok(())
                                        };
                                        hpp.mark_progress();
                                        return Ready(Ok(Some((ErasedFuture::new(fut),))));
                                    } else {
                                    }
                                } else {
                                    // Channel has no address (yet) so it can not be assigned to a CaConn yet.
                                }
                            }
                        }
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
        if hpp.have_progress() {
            // TODO return type does not allow yet to indicate progress without future to execute.
            let fut = async move { Ok(()) };
            Ready(Ok(Some((ErasedFuture::new(fut),))))
        } else if hpp.have_pending() {
            Pending
        } else {
            Ready(Ok(None))
        }
    }

    fn poll_conn_comm(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_conn_comm";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            for (_, (comm, _)) in self.ca_conns.iter_mut() {
                match comm.poll_next_unpin(cx) {
                    Ready(x) => match x {
                        Some(x) => match x {
                            Ok(x) => {
                                trace!("TODO handle item from CaConn {x:?}");
                                match x {
                                    conn2::conn::CaConnItem::StatusInfo(e1) => match e1.state {
                                        conn2::conn::StatusState::Connecting => {}
                                        conn2::conn::StatusState::Connected(e2) => match e2.status {
                                            conn2::conn::connected::StatusInfoState::Init => {}
                                            conn2::conn::connected::StatusInfoState::Handshake => {}
                                            conn2::conn::connected::StatusInfoState::ActiveCa(e3) => match e3.state {
                                                conn2::conn::activeca::StatusInfoState::Running(e4) => {
                                                    for e5 in e4.handlers {
                                                        match e5.status {
                                                            conn2::conn::channelheap::StatusChannelHandlerState::Active(e6) => {
                                                                if e6.counters.event_add_res_cnt > 6 {
                                                                    // TODO clean up reduce indent
                                                                    trace!("{selfname}  channel counter reach limit");
                                                                    // TODO send command to CaConn to remove channel
                                                                    // TODO observe that CaConn shuts down after a few seconds.
                                                                    if let Some(ch) = self.channels.iter().filter(|x| {
                                                                        x.channel.name() == &e5.name
                                                                    }).next() {
                                                                        let mut tx = ch.cmd_tx.clone();
                                                                        let fut = async move {
                                                                            let item = pollcstm::Cmd::Remove;
                                                                            let _ = tx.send(item).await?;
                                                                            Ok(())
                                                                        };
                                                                        self.cmd_fut_comm = Some(ErasedFuture::new(fut));
                                                                    }
                                                                }
                                                            },
                                                            conn2::conn::channelheap::StatusChannelHandlerState::Done => {},
                                                        }
                                                    }
                                                }
                                                conn2::conn::activeca::StatusInfoState::Done => {}
                                            },
                                            conn2::conn::connected::StatusInfoState::Done => {}
                                        },
                                        conn2::conn::StatusState::Done => {}
                                    },
                                }
                                return Ready(Some(Ok(())));
                            }
                            Err(e) => {
                                trace!("{selfname}  ERROR from CaConnComm  {e}");
                                todo!("{selfname}  ERROR from CaConnComm  {e}");
                            }
                        },
                        None => {}
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
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
                        match x {
                            Ok(()) => {
                                hpp.mark_progress();
                            }
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
                            trace!("TODO handle ConnSetCmd {x:?}");
                            hpp.mark_progress();
                        }
                        None => {}
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                },
            }
            {
                let opt = &mut self.cmd_fut_channel;
                if let Some(fut) = opt {
                    match fut.poll_unpin(cx) {
                        Ready(x) => match x {
                            Ok(()) => {
                                *opt = None;
                                hpp.mark_progress();
                            }
                            Err(e) => {
                                break Ready(Some(Err(e)));
                            }
                        },
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
            }
            {
                let opt = &mut self.cmd_fut_comm;
                if let Some(fut) = opt {
                    match fut.poll_unpin(cx) {
                        Ready(x) => match x {
                            Ok(()) => {
                                *opt = None;
                                hpp.mark_progress();
                            }
                            Err(e) => {
                                break Ready(Some(Err(e)));
                            }
                        },
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
            }
            if self.cmd_fut_channel.is_none() {
                match self.as_mut().poll_channels(cx) {
                    Ready(x) => match x {
                        Ok(Some((fut,))) => {
                            self.cmd_fut_channel = Some(fut);
                            hpp.mark_progress();
                        }
                        Ok(None) => {}
                        Err(e) => {
                            hpp.mark_progress();
                            break Ready(Some(Err(e)));
                        }
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if self.cmd_fut_comm.is_none() {
                match self.as_mut().poll_conn_comm(cx) {
                    Ready(Some(x)) => match x {
                        Ok(()) => {
                            hpp.mark_progress();
                        }
                        Err(e) => {
                            break Ready(Some(Err(e)));
                        }
                    },
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
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
