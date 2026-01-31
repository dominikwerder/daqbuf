mod channels;
mod cmder;
mod futs;
mod streamtask;

use crate::ca::conn2;
use crate::ca::conn2::asynchan;
use crate::ca::conn2::asynchan::SendPoll;
use crate::ca::conn2::conn::CaConn;
use crate::ca::conn2::conn::CaConnComm;
use crate::ca::conn2::timeoutable;
use crate::ca::conn2::timeoutable::Timeoutable;
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
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use crate::misc::todoval;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
pub use futs::FutShutdown;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::future::ready;
use scywr::insertqueues::InsertQueuesTx;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;
use taskrun::tokio::task::JoinHandle;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
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
        Timeout(#[from] timeoutable::TimeoutError),
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
    done_tx: asynchan::Sender<Result<(), Error>>,
}

impl ChannelAdd {
    pub fn name(&self) -> &str {
        self.ch_cfg.name()
    }
}

#[derive(Debug, Clone)]
pub struct ChannelRemove {
    name: String,
    done_tx: asynchan::Sender<Result<(), Error>>,
}

impl ChannelRemove {
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
enum ConnSetCmdKind {
    ChannelAdd(ChannelAdd),
    ChannelRemove(ChannelRemove),
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
    remove_on_shutdown_sent: bool,
}

#[derive(Debug)]
struct CaConnReg {
    comm: CaConnComm,
    rx: asynchan::Receiver<Result<conn2::conn::CaConnItem, conn2::conn::Error>>,
    jh: JoinHandle<Result<(), Error>>,
    shutting_down: bool,
}

#[derive(Debug)]
enum State {
    Running,
    Shutdown,
    Done,
}

impl State {
    fn name(&self) -> &'static str {
        match self {
            State::Running => "Running",
            State::Shutdown => "Shutdown",
            State::Done => "Done",
        }
    }
}

const EF1: usize = 0x500;
const EF2: usize = 0x500;
const EF3: usize = 0x500;

#[derive(Debug)]
pub struct ConnSet {
    backend: String,
    local_epics_hostname: String,
    state: State,
    cmder: ConnSetCmder,
    cmd_rx: asynchan::Receiver<ConnSetCmd>,
    cmder_cmd_fut: Option<ErasedFuture<Result<(), Error>, EF1>>,
    cmd_fut_channel: Option<ErasedFuture<Result<(), Error>, EF2>>,
    cmd_fut_comm: Option<ErasedFuture<Result<(), Error>, EF3>>,
    finder_handle: FinderHandleV02,
    channels: VecDeque<ChannelCat>,
    ch_info_tx: ChannelInfoQuerySender,
    ca_conns: BTreeMap<SocketAddrV4, CaConnReg>,
    int_rx: asynchan::Receiver<u32>,
    shutdown_fut: Option<FutDbg<Result<(), Error>>>,
    conn_idle_disconnect_futs: VecDeque<FutDbg<Result<(), Error>>>,
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
        int_rx: asynchan::Receiver<u32>,
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
            state: State::Running,
            cmder,
            cmd_rx,
            cmder_cmd_fut: None,
            cmd_fut_channel: None,
            cmd_fut_comm: None,
            finder_handle,
            channels: VecDeque::new(),
            ch_info_tx,
            ca_conns: BTreeMap::new(),
            int_rx,
            shutdown_fut: None,
            conn_idle_disconnect_futs: VecDeque::new(),
        };
        Ok(ret)
    }

    fn is_accept_cmds(&self) -> bool {
        match &self.state {
            State::Running => true,
            State::Shutdown => false,
            State::Done => false,
        }
    }

    pub fn cmder(&self) -> &ConnSetCmder {
        &self.cmder
    }

    fn trigger_shutdown(mut self: Pin<&mut Self>) {
        // TODO move Self state such that it is clear that we proceed to shut down.
        // Channels must not get added.
        // Also not removed, we remove all on our own anyway.
        // Trigger the ConnSet channel handlers to go into shut down mode.
        // TODO go through Self fields to check what needs to get cleaned.
        match self.state {
            State::Running => {
                self.state = State::Shutdown;
                self.shutdown_fut = Some(Self::shutdown_task().box2());
            }
            State::Shutdown => {}
            State::Done => {}
        }
    }

    async fn shutdown_task() -> Result<(), Error> {
        // Can not do much here.
        Ok(())
    }

    fn poll_channels(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<Option<(ErasedFuture<Result<(), Error>, EF2>,)>, Error>> {
        use Poll::*;
        // TODO caller wants to handle only one potential future at a time.
        let mut hpp = HaveProgressPending::new();
        let self2 = self.get_mut();
        for ch in self2.channels.iter_mut() {
            let mut ress = PollRess::new(&self2.ch_info_tx, &self2.finder_handle);
            match ch.channel.poll_unpin(&mut ress, cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match x {
                        Ok(x) => {
                            use channels::channel::ChannelActionItem;
                            match x {
                                ChannelActionItem::AddToCaConn(conf, addr) => {
                                    trace!("poll_channels  ChannelActionItem::AddToCaConn  {addr}  {conf:?}");
                                    if let Some(conn_reg) = self2.ca_conns.get_mut(&addr) {
                                        if conn_reg.shutting_down {
                                            error!("TODO backoff channel, existing CaConn shutting down");
                                            // TODO
                                            todo!();
                                        } else {
                                            let fut = async move {
                                                conn_reg.comm.channel_add(conf).await?;
                                                Ok(())
                                            };
                                            return Ready(Ok(Some((ErasedFuture::new(fut),))));
                                        }
                                    } else {
                                        let conn = CaConn::new(
                                            self2.backend.clone(),
                                            addr,
                                            self2.local_epics_hostname.clone(),
                                        );
                                        let mut comm = conn.comm();
                                        let (conn, ca_conn_rx) = conn.into_task();
                                        let jh = tokio::spawn(conn.map_err(Error::from));
                                        self2.ca_conns.insert(
                                            addr,
                                            CaConnReg {
                                                comm: comm.clone(),
                                                rx: ca_conn_rx,
                                                jh,
                                                shutting_down: false,
                                            },
                                        );
                                        let fut = async move {
                                            comm.channel_add(conf).await?;
                                            Ok(())
                                        };
                                        return Ready(Ok(Some((ErasedFuture::new(fut),))));
                                    }
                                }
                                ChannelActionItem::RemoveFromCaConn(conf, reminfo, mut done_tx) => {
                                    // TODO send a command to the CaConn to remove the channel, wait for confirmation.
                                    if let Some(addr) = reminfo.addr() {
                                        if let Some(conn_reg) = self2.ca_conns.get_mut(&addr) {
                                            let fut = async move {
                                                conn_reg.comm.channel_remove(conf).await?;
                                                let _ = done_tx.send(0).await;
                                                Ok(())
                                            };
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
                    }
                }
                Ready(None) => {
                    error!("Channel is done  TODO status event, clean up");
                }
                Pending => {
                    hpp.mark_pending();
                }
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

    fn handle_conn_comm_status_info(
        e1: conn2::conn::StatusInfo,
        cmder: &ConnSetCmder,
        cx: &mut Context,
    ) -> Result<Option<ErasedFuture<Result<(), Error>, EF3>>, Error> {
        let selfname = "handle_conn_comm_status_info";
        match e1.state {
            conn2::conn::StatusState::Connecting => {}
            conn2::conn::StatusState::Connected(e2) => match e2.status {
                conn2::conn::connected::StatusInfoState::Init => {}
                conn2::conn::connected::StatusInfoState::Handshake => {}
                conn2::conn::connected::StatusInfoState::ActiveCa(e3) => match e3.state {
                    conn2::conn::activeca::StatusInfoState::Running(e4) => {
                        for e5 in e4.handlers {
                            match e5.status {
                                conn2::conn::channelheap::StatusChannelHandlerState::Active(e6) => {
                                    if false {
                                        if e6.counters.event_add_res_cnt > 6 {
                                            trace!("{selfname}  channel counter reach limit");
                                            let fut = async move {
                                                cmder.channel_remove(&e5.name).await;
                                                trace!("{selfname}  channel removed  {}", e5.name);
                                                Ok(())
                                            };
                                            return Ok(Some(ErasedFuture::new(fut)));
                                        }
                                    }
                                }
                                conn2::conn::channelheap::StatusChannelHandlerState::Done => {}
                            }
                        }
                    }
                    conn2::conn::activeca::StatusInfoState::Done => {}
                },
                conn2::conn::connected::StatusInfoState::Done => {}
            },
            conn2::conn::StatusState::Done => {}
        }
        Ok(None)
    }

    fn poll_conn_comm(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_conn_comm";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            for (_, conn_reg) in self2.ca_conns.iter_mut() {
                // match comm.poll_next_unpin(cx) {
                match conn_reg.rx.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => {
                                trace!("TODO handle item from CaConn {x:?}");
                                match x {
                                    conn2::conn::CaConnItem::StatusInfo(e1) => {
                                        match Self::handle_conn_comm_status_info(e1, &self2.cmder, cx) {
                                            Ok(x) => match x {
                                                Some(fut) => {
                                                    self2.cmd_fut_comm = Some(fut);
                                                }
                                                None => {}
                                            },
                                            Err(e) => {
                                                return Ready(Some(Err(e)));
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                trace!("{selfname}  ERROR from CaConnComm  {e}");
                                todo!("{selfname}  ERROR from CaConnComm  {e}");
                            }
                        }
                    }
                    Ready(None) => {
                        error!("{selfname}  TODO  Conn seems done, must process this here");
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
                // TODO factor this better.
                // We must abort the loop because a precondition is that the fut slot is available.
                if self2.cmd_fut_comm.is_some() {
                    return Ready(Some(Ok(())));
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

    fn handle_cmder_cmd(mut self: Pin<&mut Self>, cmd: ConnSetCmd, cx: &mut Context) {
        let selfname = "handle_cmder_cmd";
        match cmd.kind {
            ConnSetCmdKind::ChannelAdd(mut cmd) => {
                trace4!("{selfname}  ConnSetCmdKind::ChannelAdd");
                let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelCmd");
                let e = ChannelCat {
                    channel: channels::channel::Channel::new(self.backend.clone(), cmd.ch_cfg, cmd_rx),
                    cmd_tx,
                    remove_on_shutdown_sent: false,
                };
                self.channels.push_back(e);
                // TODO expect it to succeed immediately, should use dedicated api.
                if cmd.done_tx.try_send(Ok(())).is_err() {
                    error!("{selfname}  ConnSetCmdKind::ChannelAdd  done_tx.try_send failed");
                }
                info!("{selfname}  ConnSetCmdKind::ChannelAdd  added channel");
                /*
                match cmd.done_tx.poll_send_unpin(Ok(()), cx) {
                    Ok(()) => {
                        info!("{selfname}  ConnSetCmdKind::ChannelAdd  done_tx.send succeeded");
                    }
                    Err(e) => {
                        info!(
                            "{selfname}  ConnSetCmdKind::ChannelAdd  done_tx.send failed {}",
                            e.reason_str()
                        );
                        // TODO count for metrics
                    }
                }
                */
            }
            ConnSetCmdKind::ChannelRemove(cmd) => {
                // regular channel remove initiated by ConnSet.
                let txs: Vec<_> = self
                    .channels
                    .iter()
                    .filter(|ch| cmd.name() == ch.channel.name())
                    .map(|ch| ch.cmd_tx.clone())
                    .collect();
                let mut done_tx_1 = cmd.done_tx;
                let fut = async move {
                    for tx in txs {
                        let (tx3, mut rx3) = asynchan::bounded(4, "ConnSetCmdKind::ChannelRemove");
                        let cmd2 = pollcstm::Cmd::Remove(pollcstm::Remove { done_tx: tx3 });
                        let _ = tx.clone().send(cmd2).await;
                        let _ = rx3.recv().await;
                    }
                    let _ = done_tx_1.send(Ok(())).await;
                    Ok(())
                };
                // TODO maybe better return the future from here and let caller place it.
                self.cmder_cmd_fut = Some(ErasedFuture::new(fut));
                // TODO connection tear down logic.
                // self.ca_conns;
            }
            ConnSetCmdKind::Shutdown => todo!("TODO handle shutdown"),
        }
    }

    fn poll_cmder_rx(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_cmder_rx";
        use Poll::*;
        trace4!("{selfname}  begin");
        let mut hpp = HaveProgressPending::new();
        match self.cmd_rx.poll_next_unpin(cx) {
            Ready(x) => match x {
                Some(x) => match self.handle_cmder_cmd(x, cx) {
                    () => {
                        hpp.mark_progress();
                    }
                },
                None => {}
            },
            Pending => {
                hpp.mark_pending();
            }
        }
        if hpp.have_progress() {
            trace4!("{selfname}  have_progress");
            Ready(Some(Ok(())))
        } else if hpp.have_pending() {
            trace4!("{selfname}  have_pending");
            Pending
        } else {
            trace4!("{selfname}  have nothing");
            Ready(None)
        }
    }

    fn poll_cmder(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_cmder";
        use Poll::*;
        trace4!("{selfname}  begin");
        let mut hpp = HaveProgressPending::new();
        match &mut self.cmder_cmd_fut {
            Some(fut) => match fut.poll_unpin(cx) {
                Ready(x) => {
                    self.cmder_cmd_fut = None;
                    match x {
                        Ok(()) => {
                            hpp.mark_progress();
                        }
                        Err(e) => {
                            return Ready(Some(Err(e)));
                        }
                    }
                }
                Pending => {
                    hpp.mark_pending();
                }
            },
            None => {
                if self.is_accept_cmds() {
                    match self.poll_cmder_rx(cx) {
                        Ready(Some(x)) => match x {
                            Ok(()) => {
                                hpp.mark_progress();
                            }
                            Err(e) => {
                                return Ready(Some(Err(e)));
                            }
                        },
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                } else {
                    // TODO ?
                }
            }
        }
        if hpp.have_progress() {
            trace4!("{selfname}  have_progress");
            Ready(Some(Ok(())))
        } else if hpp.have_pending() {
            trace4!("{selfname}  have_pending");
            Pending
        } else {
            trace4!("{selfname}  have nothing");
            Ready(None)
        }
    }

    fn check_idle_caconn(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<()>> {
        let selfname = "check_idle_caconn";
        info!("{selfname} =======================");
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        // TODO
        // Find the channels which already have their address resolved. Can be done sync here.
        // Count the channels associated with any registered CaConn. Is Sync.
        // Mark those CaConn without associated channels as ShuttingDown.
        // Schedule those CaConn a command.
        // In the logic which tries to find an existing CaConn for some channel, if we find
        // a CaConn in shutting down, ignore and schedule this channel with a backoff.
        // Actually, this logic is probably a command handler which handles the assign command
        // from some channel sub future.
        let mut ch_by_addr: BTreeMap<_, _> = self.ca_conns.keys().map(|x| (*x, 0u32)).collect();
        for ch in self.channels.iter() {
            if let Some(addr) = ch.channel.addr() {
                if let Some(e) = ch_by_addr.get_mut(&addr) {
                    *e += 1;
                } else {
                    // it can be that the channel is resolved, but not yet assigned.
                }
            } else {
                // channel addr not yet resolved.
            }
        }
        for (addr, cc) in ch_by_addr.into_iter() {
            if self.conn_idle_disconnect_futs.len() >= 1 {
                break;
            } else if cc == 0 {
                if let Some(cr) = self.ca_conns.get_mut(&addr) {
                    if cr.shutting_down == false {
                        cr.shutting_down = true;
                        let mut tx2 = cr.comm.clone();
                        let fut = async move {
                            tx2.trigger_disconnect_on_idle()
                                .timeout(Duration::from_millis(3000))
                                .then(|x| match x {
                                    Ok(x) => ready(x.map_err(Error::from)),
                                    Err(e) => ready(Err(e.into())),
                                })
                                .await
                        };
                        hpp.mark_progress();
                        self.conn_idle_disconnect_futs.push_back(fut.box2());
                    }
                } else {
                    panic!("logic");
                }
            }
        }
        if hpp.have_progress() {
            Ready(Some(()))
        } else if hpp.have_pending() {
            Pending
        } else {
            Ready(None)
        }
    }

    fn poll_conn_idle_disconnect_futs(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<()>> {
        let selfname = "poll_conn_idle_disconnect_futs";
        info!("{selfname} =======================");
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        if let Some(fut) = self.conn_idle_disconnect_futs.get_mut(0) {
            match fut.poll_unpin(cx) {
                Ready(x) => {
                    hpp.mark_progress();
                    self.conn_idle_disconnect_futs.pop_front();
                    match x {
                        Ok(()) => {
                            error!(
                                "{selfname}  TODO  idle disconnect triggered, but we must also await that it gets done"
                            );
                        }
                        Err(e) => {
                            error!("{selfname}  TODO")
                        }
                    }
                }
                Pending => {
                    hpp.mark_pending();
                }
            }
        }
        if hpp.have_progress() {
            Ready(Some(()))
        } else if hpp.have_pending() {
            Pending
        } else {
            Ready(None)
        }
    }

    fn poll_common_running_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<()>> {
        let selfname = "poll_common_running_shutdown";
        info!("{selfname} =======================");
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        match self.as_mut().check_idle_caconn(cx) {
            Ready(Some(())) => {
                hpp.mark_progress();
            }
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        match self.as_mut().poll_conn_idle_disconnect_futs(cx) {
            Ready(Some(())) => {
                hpp.mark_progress();
            }
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        if hpp.have_progress() {
            Ready(Some(()))
        } else if hpp.have_pending() {
            Pending
        } else {
            Ready(None)
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
            trace4!("ConnSet  poll_next  loop begin  {}", self.state.name());
            let mut hpp = HaveProgressPending::new();
            match self.as_mut().poll_cmder(cx) {
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
            match &mut self.state {
                State::Running => {
                    match self.int_rx.poll_next_unpin(cx) {
                        Ready(Some(_)) => {
                            self.as_mut().trigger_shutdown();
                            info!("shutdown triggered");
                            hpp.mark_progress();
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match self.as_mut().poll_common_running_shutdown(cx) {
                        Ready(Some(())) => {
                            hpp.mark_progress();
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Shutdown => {
                    match self.as_mut().poll_common_running_shutdown(cx) {
                        Ready(Some(())) => {
                            hpp.mark_progress();
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    if let Some(fut) = &mut self.shutdown_fut {
                        match fut.poll_unpin(cx) {
                            Ready(x) => {
                                self.shutdown_fut = None;
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
                        }
                    } else {
                        // Either find something new to do for shutdown_fut for transition to Done.
                        let self2 = self.as_mut().get_mut();
                        for ch in self2.channels.iter_mut() {
                            if ch.remove_on_shutdown_sent == false {
                                ch.remove_on_shutdown_sent = true;
                                let (done_tx, mut done_rx) = asynchan::bounded(4, "ConnSetShutdownRemove");
                                let mut tx = ch.cmd_tx.clone();
                                let fut = async move {
                                    tx.send(pollcstm::Cmd::Remove(pollcstm::Remove { done_tx })).await?;
                                    let _ = done_rx.recv().await;
                                    Ok(())
                                };
                                self2.shutdown_fut = Some(fut.box2());
                            }
                        }
                        if self.shutdown_fut.is_none() {
                            let self2 = self.as_mut().get_mut();
                            // Wait for all channels to be removed.
                            let fut = async {
                                tokio::time::sleep(Duration::from_millis(1000)).await;
                                info!("waiting for shutdown...");
                                Ok(())
                            };
                            self2.shutdown_fut = Some(fut.box2());
                        }
                        if self.shutdown_fut.is_none() {
                            // TODO emit all final metrics.
                        }
                        if self.shutdown_fut.is_none() {
                            self.state = State::Done;
                            eprintln!("shutdown done");
                        }
                        hpp.mark_progress();
                    }
                }
                State::Done => {}
            }
            break if hpp.have_progress() {
                trace2!("ConnSet  poll_next  Done");
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                trace2!("ConnSet  poll_next  Done");
                Ready(None)
            };
        }
    }
}
