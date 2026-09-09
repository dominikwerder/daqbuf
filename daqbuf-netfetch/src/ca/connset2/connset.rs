const INP_BUF_CAP: usize = 128;

pub mod channels;
pub mod channeltrace;
mod cmd_handler;
mod cmder;
mod futs;
pub mod gather;
mod streamtask;

use crate::asynbuf::AsynBuf;
use crate::asynchan;
use crate::ca::conn2;
use crate::ca::connset2::connset::channels::pollcstm;
use crate::ca::connset2::connset::channels::pollcstm::PollCstm;
use crate::ca::connset2::connset::channels::pollcstm::PollRess;
use crate::ca::finder::FinderHandleV02;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use crate::misc::todoval;
use async_channel::Sender;
pub use cmder::ConnSetCmder;
use conn2::conn::CaConn;
use conn2::conn::CaConnComm;
use conn2::locallog;
use conn2::locallog::LocalLog;
use conn2::timeoutable;
use conn2::timeoutable::Timeoutable;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
pub use futs::FutShutdown;
use futures::Future;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::future::ready;
use regex::Regex;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use scywr::senderpolling::SenderPolling;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::net::Ipv4Addr;
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
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { log::trace!("{}  Pending", format_args!($($arg)*)); } }; }
macro_rules! trace_hpp_flags { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! todo_shutdown { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ConnSet"),
    enum variants {
        DbPgSeriesByChannel(#[from] dbpg::seriesbychannel::Error),
        Channel(#[from] channels::channel::Error),
        Conn(#[from] conn2::conn::Error),
        Send,
        Timeout(#[from] timeoutable::TimeoutError),
        Command(String),
        ConnSetCmderBox(Box<dyn std::error::Error + Send>),
        Logic,
        Json(#[from] serde_json::Error),
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
    pub fn new(ch_cfg: crate::conf::ChannelConfig, done_tx: asynchan::Sender<Result<(), Error>>) -> Self {
        Self { ch_cfg, done_tx }
    }

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
    pub fn new(name: String, done_tx: asynchan::Sender<Result<(), Error>>) -> Self {
        Self { name, done_tx }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub struct StatusV1Req {
    pub sel: crate::metrics::status_v1::ChannelSelector,
    pub detail: conn2::conn::StatusDetail,
}

#[derive(Debug)]
pub enum ConnsetChannels {
    Light(Vec<channels::channel::ChannelStatusLight>),
    Full(Vec<(String, channels::channel::ChannelInfo)>),
}

#[derive(Debug)]
pub struct StatusV1Res {
    pub ts: String,
    pub ingest_name: String,
    pub conn_count_total: u32,
    pub conn_count_matched: u32,
    pub connset_channel_count_total: u32,
    pub connset_channels: ConnsetChannels,
    pub conns: Vec<(SocketAddrV4, Result<conn2::conn::StatusInfo, gather::GatherError>)>,
}

#[derive(Debug)]
enum ConnSetCmdKind {
    ChannelAdd(ChannelAdd),
    ChannelRemove(ChannelRemove),
    Shutdown,
    ConnectionListGetV1(asynchan::Sender<crate::metrics::ConnectionListV1>),
    ChannelsForAddrInfoV1(SocketAddrV4, asynchan::Sender<crate::metrics::ChannelsForAddrInfoV1>),
    ChannelsForAddrInfoV2(
        SocketAddrV4,
        String,
        asynchan::Sender<crate::metrics::ChannelsForAddrInfoV2>,
    ),
    CmdDynV1(String, asynchan::Sender<serde_json::Value>),
    StatusV1(StatusV1Req, asynchan::Sender<StatusV1Res>),
    MetricsGetV1(asynchan::Sender<crate::metrics::types::MetricsPrometheusShort>),
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
    rx: asynchan::Receiver<Result<AsynBuf<conn2::conn::CaConnItem>, conn2::conn::Error>>,
    jh: JoinHandle<Result<(), Error>>,
    shutting_down: bool,
}

#[derive(Debug)]
struct Shutdown {
    timeout: FutDbg<()>,
}

#[derive(Debug)]
pub struct TestValue {
    pub val: f32,
    pub dttrig: f32,
    pub dtcmd: f32,
    pub dtreg: f32,
    pub dtdisp: f32,
}

impl fmt::Display for TestValue {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "val {:8.3}  dt  {:5.1}  {:5.1}  {:5.1}  {:5.1}",
            self.val, self.dttrig, self.dtcmd, self.dtreg, self.dtdisp
        )
    }
}

#[derive(Debug)]
pub enum ConnSetItem {
    TestValue(TestValue),
    ChannelEventValue(conn2::ChannelEventValue),
}

#[derive(Debug)]
enum State {
    Running,
    Shutdown(Shutdown),
    Shutdown2,
    Done,
}

impl State {
    fn name(&self) -> &'static str {
        match self {
            State::Running => "Running",
            State::Shutdown(_) => "Shutdown",
            State::Shutdown2 => "Shutdown2",
            State::Done => "Done",
        }
    }
}

#[derive(Debug)]
pub struct ConnSet {
    backend: String,
    local_epics_hostname: String,
    state: State,
    cmder: ConnSetCmder,
    cmd_rx: asynchan::Receiver<ConnSetCmd>,
    cmder_cmd_fut: Option<FutDbg<Result<(), Error>>>,
    cmd_fut_channel: Option<FutDbg<Result<(), Error>>>,
    cmd_fut_comm: Option<FutDbg<Result<(), Error>>>,
    finder_handle: FinderHandleV02,
    channels: BTreeMap<String, ChannelCat>,
    ch_info_tx: ChannelInfoQuerySender,
    ca_conns: BTreeMap<SocketAddrV4, CaConnReg>,
    shutdown_fut: Option<FutDbg<Result<(), Error>>>,
    conn_idle_disconnect_futs: VecDeque<FutDbg<Result<(), Error>>>,
    out_buf: AsynBuf<ConnSetItem>,
    llog: LocalLog,
    chtrace: channeltrace::ChannelTraceStash,
    write_staging: VecDeque<QueryItem>,
    write_sender: Pin<Box<SenderPolling<VecDeque<QueryItem>>>>,
    /// Accumulates the metrics of this ConnSet and of all CaConn below it.
    /// This is the root of the v2 metrics tree, so it is never reset: the
    /// counters are handed out to prometheus as cumulative counters.
    mett: stats::mett::ConnSet2Metrics,
}

impl ConnSet {
    pub async fn new(
        backend: String,
        local_epics_hostname: String,
        ch_info_tx: ChannelInfoQuerySender,
        finder_handle: FinderHandleV02,
        insert_input: Sender<VecDeque<QueryItem>>,
    ) -> Result<Self, Error> {
        let (cmd_tx, cmd_rx) = asynchan::bounded(100, "ConnSetCmder");
        let cmder = ConnSetCmder::new(cmd_tx);
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
            channels: BTreeMap::new(),
            ch_info_tx,
            ca_conns: BTreeMap::new(),
            shutdown_fut: None,
            conn_idle_disconnect_futs: VecDeque::new(),
            out_buf: AsynBuf::new(INP_BUF_CAP),
            llog: LocalLog::new(),
            chtrace: channeltrace::ChannelTraceStash::new(),
            write_staging: VecDeque::new(),
            write_sender: Box::pin(SenderPolling::new(insert_input)),
            mett: stats::mett::ConnSet2Metrics::new(),
        };
        Ok(ret)
    }

    fn is_accept_cmds(&self) -> bool {
        match &self.state {
            State::Running => true,
            State::Shutdown(_) => false,
            State::Shutdown2 => false,
            State::Done => false,
        }
    }

    pub fn cmder(&self) -> &ConnSetCmder {
        &self.cmder
    }

    fn trigger_shutdown(mut self: Pin<&mut Self>) {
        match self.state {
            State::Running => {
                self.state = State::Shutdown(Shutdown {
                    timeout: tokio::time::sleep(Duration::from_millis(10000)).box2(),
                });
            }
            State::Shutdown(_) => {}
            State::Shutdown2 => {}
            State::Done => {}
        }
    }

    fn poll_on_shutdown_timeout_1(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        let conn_comms: Vec<_> = self.ca_conns.iter().map(|x| x.1.comm.clone()).collect();
        for mut comm in conn_comms {
            // TODO
            comm.channel_remove(todo!());
            comm.trigger_disconnect_on_idle();
        }
        Ready(Ok(()))
    }

    fn poll_channels(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<Option<(FutDbg<Result<(), Error>>,)>, Error>> {
        use Poll::*;
        let selfname = "poll_channels";
        // TODO caller wants to handle only one potential future at a time.
        let mut hpp = HaveProgressPending::new();
        let self2 = self.get_mut();
        let mut remove_names = Vec::new();
        let mut it1 = self2.channels.iter_mut();
        let item = loop {
            if let Some((name, ch)) = it1.next() {
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
                                                let mut comm = conn_reg.comm.clone();
                                                let fut = async move {
                                                    comm.channel_add(conf).await?;
                                                    Ok(())
                                                };
                                                break Ready(Ok(Some((fut.box2(),))));
                                            }
                                        } else {
                                            self2.mett.ca_conn_create().inc();
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
                                            break Ready(Ok(Some((fut.box2(),))));
                                        }
                                    }
                                    ChannelActionItem::RemoveFromCaConn(conf, reminfo, mut done_tx) => {
                                        // TODO send a command to the CaConn to remove the channel, wait for confirmation.
                                        if let Some(addr) = reminfo.addr() {
                                            if let Some(conn_reg) = self2.ca_conns.get_mut(&addr) {
                                                let mut comm = conn_reg.comm.clone();
                                                let fut = async move {
                                                    comm.channel_remove(conf).await?;
                                                    let _ = done_tx.send(0).await;
                                                    Ok(())
                                                };
                                                break Ready(Ok(Some((fut.box2(),))));
                                            } else {
                                            }
                                        } else {
                                            // Channel has no address (yet) so it can not be assigned to a CaConn yet.
                                        }
                                    }
                                    ChannelActionItem::LocalLog(llog) => {
                                        hpp.mark_progress();
                                        self2.llog.push_entry(llog);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("{selfname}  recv error {e}");
                                break Ready(Err(e));
                            }
                        }
                    }
                    Ready(None) => {
                        todo_shutdown!("channel is done  TODO status event");
                        remove_names.push(name.clone());
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else {
                break Ready(Ok(None));
            }
        };
        for x in remove_names {
            self2.channels.remove(&x);
        }
        if let Ready(Ok(Some(x))) = item {
            Ready(Ok(Some(x)))
        } else {
            if hpp.have_progress() {
                // TODO return type does not allow yet to indicate progress without future to execute.
                let fut = async move { Ok(()) };
                Ready(Ok(Some((fut.box2(),))))
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(Ok(None))
            }
        }
    }

    fn handle_conn_comm_status_info(
        e1: conn2::conn::StatusInfo,
        cmder: &ConnSetCmder,
        _cx: &mut Context,
    ) -> Result<Option<FutDbg<Result<(), Error>>>, Error> {
        let selfname = "handle_conn_comm_status_info";
        // TODO process periodic status info
        match e1.state {
            conn2::conn::StatusState::Connecting => {}
            conn2::conn::StatusState::Connected(e2) => match e2.state {
                conn2::conn::connected::StatusInfoState::Init => {}
                conn2::conn::connected::StatusInfoState::Handshake => {}
                conn2::conn::connected::StatusInfoState::ActiveCa(e3) => match e3.state {
                    conn2::conn::activeca::StatusInfoState::Running(st1, e4) => {
                        for e5 in e4.handlers {
                            match e5.state {
                                conn2::conn::channelheap::StatusChannelHandlerState::Active(e6) => {
                                    if false {
                                        if e6.counters.event_add_res_cnt > 6 {
                                            trace!("{selfname}  channel counter reach limit");
                                            let cmder = cmder.clone();
                                            let fut = async move {
                                                cmder
                                                    .channel_remove(&e5.name)
                                                    .await
                                                    .map_err(|e| Error::ConnSetCmderBox(Box::new(e)))?;
                                                trace!("{selfname}  channel removed  {}", e5.name);
                                                Ok(())
                                            };
                                            return Ok(Some(fut.box2()));
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

    fn poll_write_sender(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<()>> {
        use Poll::*;
        let selfname = "poll_write_sender";
        let self2 = self.as_mut().get_mut();
        let mut progress = false;
        if self2.write_sender.is_sending() {
            match self2.write_sender.as_mut().poll(cx) {
                Ready(Ok(())) => progress = true,
                Ready(Err(e)) => {
                    error!("{selfname}  write_sender closed  {e:?}");
                    self2.mett.write_sender_closed().inc();
                    progress = true;
                }
                Pending => {}
            }
        }
        if self2.write_sender.is_idle() && !self2.write_staging.is_empty() {
            let batch = std::mem::take(&mut self2.write_staging);
            self2.mett.write_sender_batch_send().inc();
            self2.mett.write_sender_batch_len().push_val(batch.len() as u32);
            self2.write_sender.as_mut().send_pin(batch);
            progress = true;
        }
        if progress {
            Ready(Some(()))
        } else if self2.write_sender.is_sending() {
            Pending
        } else {
            Ready(None)
        }
    }

    fn poll_conn_comm(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_conn_comm";
        use Poll::*;
        'outer: loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(fut) = self.cmd_fut_comm.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(x) => {
                        hpp.mark_progress();
                        self.cmd_fut_comm = None;
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
                let mut addr_found_done = Vec::new();
                let mut addr_found_error = Vec::new();
                let self2 = self.as_mut().get_mut();
                // TODO introduce fairness for congested case
                for (addr, conn_reg) in self2.ca_conns.iter_mut() {
                    // match comm.poll_next_unpin(cx) {
                    match conn_reg.rx.poll_next_unpin(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(items) => {
                                    //
                                    for item in items {
                                        self2.mett.conn_item_recv().inc();
                                        match item {
                                            conn2::conn::CaConnItem::Metrics(m) => {
                                                self2.mett.conn_metrics_recv().inc();
                                                self2.mett.ca_conn().ingest(m);
                                            }
                                            conn2::conn::CaConnItem::StatusInfo(e1) => {
                                                self2.mett.conn_status_info().inc();
                                                match Self::handle_conn_comm_status_info(e1, &self2.cmder, cx) {
                                                    Ok(x) => match x {
                                                        Some(fut) => {
                                                            self2.cmd_fut_comm = Some(fut);
                                                        }
                                                        None => {}
                                                    },
                                                    Err(e) => {
                                                        break 'outer Ready(Some(Err(e)));
                                                    }
                                                }
                                            }
                                            conn2::conn::CaConnItem::ChannelInfoQuery(item) => {
                                                self2.mett.conn_channel_info_query().inc();
                                                let mut tx = self2.ch_info_tx.clone();
                                                let fut = async move {
                                                    match tx.send(item).await {
                                                        Ok(()) => {}
                                                        Err(e) => {
                                                            error!("ChannelInfoQuery channel send error");
                                                        }
                                                    }
                                                    Ok(())
                                                };
                                                self2.cmd_fut_comm = Some(fut.box2());
                                            }
                                            conn2::conn::CaConnItem::TestValue(x) => {
                                                self2.mett.conn_test_value().inc();
                                                self2.out_buf.push_back_force(ConnSetItem::TestValue(x));
                                            }
                                            conn2::conn::CaConnItem::LocalLog(x) => {
                                                self2.mett.conn_local_log().inc();
                                                self2.llog.push_entry(x);
                                            }
                                            conn2::conn::CaConnItem::ChannelEventValue(x) => {
                                                self2.mett.conn_channel_event_value().inc();
                                                self2.out_buf.push_back_force(ConnSetItem::ChannelEventValue(x));
                                            }
                                            conn2::conn::CaConnItem::ChannelWriteItems(x) => {
                                                self2.mett.conn_channel_write_items().add(x.len() as u32);
                                                self2.write_staging.extend(x);
                                            }
                                            conn2::conn::CaConnItem::ChannelTrace(x) => {
                                                self2.mett.conn_channel_trace().inc();
                                                self2.chtrace.push(x);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("{selfname}  recv error  {e}");
                                    self2.mett.conn_recv_error().inc();
                                    use conn2::conn::Error as E2;
                                    match e {
                                        E2::IO(e) => {
                                            error!("{selfname}  check IO error  {e}");
                                        }
                                        _ => {}
                                    }
                                    // TODO ??
                                    // self.conn_idle_disconnect_futs;
                                    addr_found_error.push(*addr);
                                }
                            }
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            let addr = *addr;
                            debug!("{selfname}  CaConn done {addr}");
                            self2.mett.conn_done().inc();
                            addr_found_done.push(addr);
                            let fut = async move {
                                warn!("{selfname}  TODO  CaConn {addr} done, emit status event.");
                                // TODO need to emit status event.
                                Ok(())
                            };
                            self2.cmd_fut_comm = Some(fut.box2());
                            break;
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                let mut signal_channels_on_address = Vec::with_capacity(addr_found_done.len() + addr_found_error.len());
                for addr in addr_found_done {
                    signal_channels_on_address.push(addr);
                    if let Some(conn) = self.ca_conns.remove(&addr) {
                        // TODO await the jh
                        let jh = conn.jh;
                    } else {
                        self.mett.conn_done_not_in_registry().inc();
                        error!("finished connection not in registry  {addr}");
                    }
                }
                for addr in addr_found_error {
                    signal_channels_on_address.push(addr);
                    if let Some(conn) = self.ca_conns.remove(&addr) {
                        // TODO await the jh
                        let jh = conn.jh;
                    } else {
                        self.mett.conn_error_not_in_registry().inc();
                        error!("connection with error not in registry  {addr}");
                    }
                }
                for addr in signal_channels_on_address {
                    for (chn, cc) in self.channels.iter_mut() {
                        if cc.channel.addr().map_or(false, |x| x == addr) {
                            cc.channel.signal_ca_conn_down(addr, chn);
                        }
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

    fn handle_cmd_dyn_v1(
        self: Pin<&mut Self>,
        cmd: String,
        mut tx: asynchan::Sender<serde_json::Value>,
        cx: &mut Context,
    ) -> Option<FutDbg<Result<(), Error>>> {
        match serde_json::from_str::<crate::metrics::CmdType>(&cmd) {
            Ok(cmdty) => {
                if cmdty.ty == "ChannelsByRegexV1" {
                    match serde_json::from_str::<crate::metrics::CmdChannelsByRegex>(&cmd) {
                        Ok(cmd) => match regex::Regex::new(&cmd.regex) {
                            Ok(re1) => {
                                if cmd.src == "CaConn" {
                                    let cmdtxs: Vec<_> = self
                                        .ca_conns
                                        .iter()
                                        .filter(|(addr, _)| true || addr.port() == 123)
                                        .map(|(addr, reg)| (addr.clone(), reg.comm.clone()))
                                        .collect();
                                    let fut = async move {
                                        let mut a = Vec::new();
                                        for (addr, mut cmdtx) in cmdtxs {
                                            let r =
                                                cmdtx.channels_by_regex_v1(cmd.kind.clone(), cmd.regex.clone()).await?;
                                            for x in r {
                                                let name = if let Some(x) = x.get("name") {
                                                    x.as_str().unwrap_or("(nil)")
                                                } else {
                                                    "(nil)"
                                                };
                                                let x = crate::metrics::ChannelInfoV3 {
                                                    name: name.into(),
                                                    addr: Some(addr.into()),
                                                    chinfo_a: x,
                                                };
                                                a.push(x);
                                            }
                                        }
                                        let val = serde_json::json!({
                                            "channels": a,
                                        });
                                        let _ = tx.send(val).await;
                                        Ok(())
                                    };
                                    Some(fut.box2())
                                } else {
                                    let channels: Vec<_> = self
                                        .channels
                                        .iter()
                                        .filter(|(name, _)| re1.is_match(&name))
                                        .map(|(name, ch)| {
                                            let chi = ch.channel.channel_info();
                                            crate::metrics::ChannelInfoV3 {
                                                name: ch.channel.name().into(),
                                                addr: ch.channel.addr().map(Into::into),
                                                chinfo_a: serde_json::to_value(&chi).unwrap(),
                                            }
                                        })
                                        .collect();
                                    let val = serde_json::json!({
                                        "channels": channels,
                                    });
                                    let fut = async move {
                                        let _ = tx.send(val).await;
                                        Ok(())
                                    };
                                    Some(fut.box2())
                                }
                            }
                            Err(e) => {
                                let val = serde_json::json!({
                                    "type": "error",
                                    "msg": format!("{e}"),
                                });
                                let _ = tx.try_send(val);
                                None
                            }
                        },
                        Err(e) => {
                            let val = serde_json::json!({
                                "type": "error",
                                "msg": format!("{e}"),
                            });
                            let _ = tx.try_send(val);
                            None
                        }
                    }
                } else if cmdty.ty == "dyn_cmd_v03" {
                    self.handle_dyn_cmd_v03(cmd, tx, cx)
                } else if cmdty.ty == "ConnSetLlogV1" {
                    let local_log = self.llog.to_vec_string();
                    let val = serde_json::json!({
                        "llog": local_log,
                    });
                    let fut = async move {
                        let _ = tx.send(val).await;
                        Ok(())
                    };
                    Some(fut.box2())
                } else {
                    let val = serde_json::json!({
                        "type": "error",
                        "msg": format!("unknown: {}", cmdty.ty),
                    });
                    let _ = tx.try_send(val);
                    None
                }
            }
            Err(e) => {
                let val = serde_json::json!({
                    "type": "error",
                    "msg": format!("{e}"),
                });
                let _ = tx.try_send(val);
                None
            }
        }
    }

    // TODO return error via return type, let caller handle done_tx
    fn handle_channel_add(&mut self, conf: ChannelConfig, mut done_tx: asynchan::Sender<Result<(), Error>>) {
        let selfname = "handle_channel_add";
        self.mett.cmd_channel_add().inc();
        if self.channels.contains_key(conf.name()) {
            self.mett.cmd_channel_add_exists().inc();
            let e = Error::Command(format!("channel already added"));
            let _ = done_tx.try_send(Err(e));
        } else {
            trace4!("{selfname}  ConnSetCmdKind::ChannelAdd");
            let name = conf.name().to_string();
            let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelCmd");
            let e = ChannelCat {
                channel: channels::channel::Channel::new(self.backend.clone(), conf, cmd_rx),
                cmd_tx,
                remove_on_shutdown_sent: false,
            };
            trace2!("{selfname}  ConnSetCmdKind::ChannelAdd  added  {name}");
            self.channels.insert(name, e);
            // TODO expect it to succeed immediately, should use dedicated api.
            if done_tx.try_send(Ok(())).is_err() {
                error!("{selfname}  ConnSetCmdKind::ChannelAdd  done_tx.try_send failed");
            }
        }
    }

    fn handle_cmder_cmd(mut self: Pin<&mut Self>, cmd: ConnSetCmd, cx: &mut Context) {
        let selfname = "handle_cmder_cmd";
        assert!(self.cmder_cmd_fut.is_none());
        match cmd.kind {
            ConnSetCmdKind::ChannelAdd(mut cmd) => {
                self.handle_channel_add(cmd.ch_cfg, cmd.done_tx);
            }
            ConnSetCmdKind::ChannelRemove(cmd) => {
                self.mett.cmd_channel_remove().inc();
                // regular channel remove initiated by ConnSet.
                let txs: Vec<_> = self
                    .channels
                    .iter()
                    .filter(|(name, _)| *name == cmd.name())
                    .map(|(_, ch)| ch.cmd_tx.clone())
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
                self.cmder_cmd_fut = Some(fut.box2());
                error!("TODO connection tear down logic");
            }
            ConnSetCmdKind::Shutdown => {
                self.mett.cmd_shutdown().inc();
                self.as_mut().trigger_shutdown();
                info!("{lf}shutdown triggered{lf}", lf = "\n\n\n");
            }
            ConnSetCmdKind::ConnectionListGetV1(mut tx) => {
                self.mett.cmd_connection_list_v1().inc();
                let mut ret = crate::metrics::ConnectionListV1 {
                    ingest_name: ingest_linux::net::local_hostname(),
                    list: Vec::new(),
                };
                for (addr, conn) in self.ca_conns.iter() {
                    ret.list.push(crate::metrics::ConnectionListV1Conn1 {
                        ip: addr.ip().clone(),
                        port: addr.port(),
                        name: format!("todo-resolve"),
                    });
                }
                let fut = async move {
                    let z: Vec<_> = ret.list.iter().map(|c| c.ip).collect();
                    let args = ["hosts".to_string()]
                        .into_iter()
                        .chain(z.iter().take(20).map(ToString::to_string));
                    let d = tokio::process::Command::new("getent")
                        .args(args)
                        .output()
                        .await
                        .unwrap();
                    let s2 = String::from_utf8_lossy(&d.stdout);
                    let m: BTreeMap<_, _> = s2
                        .split("\n")
                        .into_iter()
                        .filter_map({
                            let re1 = regex::Regex::new(r"([^ ]+) +([^ ]+)").unwrap();
                            move |line| {
                                re1.captures_iter(line)
                                    .map(|c| c.iter().map(|x| x.map(|x| x.as_str().to_string())).collect::<Vec<_>>())
                                    .filter_map(|x| {
                                        let mut it2 = x.into_iter();
                                        it2.next();
                                        it2.next().zip(it2.next()).map(|x| x.0.into_iter().zip(x.1).next())
                                    })
                                    .map(|x| x)
                                    .next()
                            }
                        })
                        .filter_map(|x| x)
                        .filter_map(|(addr, name)| {
                            if let Ok(x) = addr.parse::<Ipv4Addr>() {
                                Some((x, name))
                            } else {
                                None
                            }
                        })
                        .collect();
                    for e in ret.list.iter_mut() {
                        if let Some(y) = m.get(&e.ip) {
                            e.name = y.to_string();
                        }
                    }
                    if tx.send(ret).await.is_err() {
                        error!("could not send response");
                    }
                    Ok(())
                };
                // TODO maybe better return the future from here and let caller place it.
                self.cmder_cmd_fut = Some(fut.box2());
            }
            ConnSetCmdKind::ChannelsForAddrInfoV1(addr, mut tx) => {
                self.mett.cmd_channels_for_addr_v1().inc();
                let cmdtxs: Vec<_> = self
                    .ca_conns
                    .iter()
                    .filter(|x| *x.0 == addr)
                    .map(|x| x.1.comm.clone())
                    .collect();
                let fut = async move {
                    let mut a = crate::metrics::ChannelsForAddrInfoV1 { channels: Vec::new() };
                    for mut cmdtx in cmdtxs {
                        let r = cmdtx.channels_info_v1().await?;
                        for x in r.channels {
                            a.channels.push(x);
                        }
                    }
                    if tx.send(a).await.is_err() {
                        error!("could not send response");
                    }
                    Ok(())
                };
                // TODO maybe better return the future from here and let caller place it.
                self.cmder_cmd_fut = Some(fut.box2());
            }
            ConnSetCmdKind::StatusV1(req, mut tx) => {
                self.mett.cmd_status_v1().inc();
                let conn_count_total = self.ca_conns.len() as u32;
                let cmdtxs: Vec<_> = self
                    .ca_conns
                    .iter()
                    .filter(|(addr, _)| req.sel.matches_addr(addr))
                    .map(|(addr, reg)| (addr.clone(), reg.comm.clone()))
                    .collect();
                let conn_count_matched = cmdtxs.len() as u32;
                let connset_channel_count_total = self.channels.len() as u32;
                let sel = req.sel.status_sel(req.detail);
                let connset_channels = match req.detail {
                    conn2::conn::StatusDetail::Light => ConnsetChannels::Light(
                        self.channels
                            .iter()
                            .filter(|(name, _)| sel.matches(name))
                            .map(|(_, ch)| ch.channel.status_light())
                            .collect(),
                    ),
                    conn2::conn::StatusDetail::Full => ConnsetChannels::Full(
                        self.channels
                            .iter()
                            .filter(|(name, _)| sel.matches(name))
                            .map(|(name, ch)| (name.clone(), ch.channel.channel_info()))
                            .collect(),
                    ),
                };
                let ingest_name = ingest_linux::net::local_hostname();
                let fut = async move {
                    let conns = gather::gather(
                        cmdtxs,
                        gather::GATHER_CONCURRENCY,
                        gather::GATHER_TIMEOUT,
                        |mut comm| {
                            let sel = sel.clone();
                            async move { comm.status_v1(sel).await }
                        },
                    )
                    .await;
                    let res = StatusV1Res {
                        ts: chrono::Utc::now().to_rfc3339(),
                        ingest_name,
                        conn_count_total,
                        conn_count_matched,
                        connset_channel_count_total,
                        connset_channels,
                        conns,
                    };
                    if tx.send(res).await.is_err() {
                        error!("could not send response");
                    }
                    Ok(())
                };
                self.cmder_cmd_fut = Some(fut.box2());
            }
            ConnSetCmdKind::ChannelsForAddrInfoV2(addr, name, mut tx) => {
                self.mett.cmd_channels_for_addr_v2().inc();
                let cmdtxs: Vec<_> = self
                    .ca_conns
                    .iter()
                    .filter(|x| *x.0 == addr)
                    .map(|x| x.1.comm.clone())
                    .collect();
                let fut = async move {
                    let mut a = crate::metrics::ChannelsForAddrInfoV2 { channels: Vec::new() };
                    for mut cmdtx in cmdtxs {
                        let r = cmdtx.channels_info_v2(name.clone()).await?;
                        for x in r.channels {
                            a.channels.push(x);
                        }
                    }
                    if tx.send(a).await.is_err() {
                        error!("could not send response");
                    }
                    Ok(())
                };
                // TODO maybe better return the future from here and let caller place it.
                self.cmder_cmd_fut = Some(fut.box2());
            }
            ConnSetCmdKind::CmdDynV1(cmd, tx) => {
                self.mett.cmd_dyn_v1().inc();
                // TODO maybe better return the future from here and let caller place it.
                if let Some(fut) = self.as_mut().handle_cmd_dyn_v1(cmd, tx, cx) {
                    self.cmder_cmd_fut = Some(fut.box2());
                }
            }
            ConnSetCmdKind::MetricsGetV1(mut tx) => {
                let ret = self.as_mut().metrics_prometheus();
                let fut = async move {
                    let _ = tx.send(ret).await;
                    Ok(())
                };
                // TODO maybe better return the future from here and let caller place it.
                self.cmder_cmd_fut = Some(fut.box2());
            }
        }
    }

    /// Render the accumulated metrics of the v2 code path in the prometheus
    /// text exposition format.
    fn metrics_prometheus(mut self: Pin<&mut Self>) -> crate::metrics::types::MetricsPrometheusShort {
        self.mett.metrics_request().inc();
        let n = self.ca_conns.len() as u32;
        self.mett.ca_conn_count().set(n);
        let n = self.channels.len() as u32;
        self.mett.channel_count().set(n);
        let n = self.write_staging.len() as u32;
        self.mett.write_staging_len().set(n);
        (&self.mett).into()
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
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(()) => {}
                                Err(e) => {
                                    return Ready(Some(Err(e)));
                                }
                            }
                        }
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
        trace4!("{selfname}");
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
        for (_, ch) in self.channels.iter() {
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
        let mut idle_disconnect_trigger = 0;
        for (addr, cc) in ch_by_addr.into_iter() {
            if self.conn_idle_disconnect_futs.len() >= 1 {
                break;
            } else if cc == 0 {
                if let Some(cr) = self.ca_conns.get_mut(&addr) {
                    if cr.shutting_down == false {
                        cr.shutting_down = true;
                        idle_disconnect_trigger += 1;
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
        if idle_disconnect_trigger != 0 {
            self.mett.channel_idle_disconnect_trigger().add(idle_disconnect_trigger);
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
        trace4!("{selfname}");
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        if let Some(fut) = self.conn_idle_disconnect_futs.front_mut() {
            match fut.poll_unpin(cx) {
                Ready(x) => {
                    hpp.mark_progress();
                    let _ = self.conn_idle_disconnect_futs.pop_front();
                    match x {
                        Ok(()) => {
                            error!(
                                "{selfname}  TODO  idle disconnect triggered, but we must also await that it gets done"
                            );
                        }
                        Err(e) => match e {
                            Error::Conn(e2) => match e2 {
                                conn2::conn::Error::ChanRecv => {
                                    // can be because racy.
                                    self.mett.channel_idle_disconnect_err().inc();
                                }
                                _ => {
                                    error!("{selfname}  TODO  ERROR  {e2}");
                                }
                            },
                            _ => {
                                error!("{selfname}  TODO  ERROR  {e}");
                            }
                        },
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

    fn poll_channels_outer(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_channels_outer";
        trace4!("{selfname}");
        use Poll::*;
        if false {
            for (name, ch) in self.channels.iter() {
                debug!("{selfname}  {}  {:?}", name, ch.channel.channel_info());
            }
        }
        // let mut hpp = HaveProgressPending::new();
        let opt = &mut self.cmd_fut_channel;
        if let Some(fut) = opt {
            match fut.poll_unpin(cx) {
                Ready(x) => match x {
                    Ok(()) => {
                        *opt = None;
                        // hpp.mark_progress();
                        Ready(Some(Ok(())))
                    }
                    Err(e) => Ready(Some(Err(e))),
                },
                Pending => {
                    // hpp.mark_pending();
                    Pending
                }
            }
        } else {
            match self.as_mut().poll_channels(cx) {
                Ready(x) => match x {
                    Ok(Some((fut,))) => {
                        self.cmd_fut_channel = Some(fut);
                        // hpp.mark_progress();
                        Ready(Some(Ok(())))
                    }
                    Ok(None) => Ready(None),
                    Err(e) => {
                        // hpp.mark_progress();
                        Ready(Some(Err(e)))
                    }
                },
                Pending => {
                    // hpp.mark_pending();
                    Pending
                }
            }
        }
    }

    fn poll_common(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Option<Result<(), Error>>>> {
        let selfname = "poll_common";
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        let poll_input = match &self.state {
            State::Running => true,
            State::Shutdown(_) => true,
            State::Shutdown2 => false,
            State::Done => false,
        };
        if poll_input {
            match self.as_mut().poll_cmder(cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match x {
                        Ok(()) => {}
                        Err(e) => {
                            return Ready(Some(Some(Err(e))));
                        }
                    }
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
        }
        if !poll_input {
            trace_hpp_flags!("FLAGS  {}  {}", hpp.have_progress(), hpp.have_pending());
        }
        match self.as_mut().poll_channels_outer(cx) {
            Ready(Some(x)) => {
                hpp.mark_progress();
                match x {
                    Ok(()) => {}
                    Err(e) => {
                        self.state = State::Done;
                        return Ready(Some(Some(Err(e))));
                    }
                }
            }
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        if !poll_input {
            trace_hpp_flags!("FLAGS  {}  {}", hpp.have_progress(), hpp.have_pending());
        }
        match self.as_mut().poll_conn_comm(cx) {
            Ready(Some(x)) => match x {
                Ok(x) => {
                    hpp.mark_progress();
                    return Ready(Some(Some(Ok(x))));
                }
                Err(e) => {
                    return Ready(Some(Some(Err(e))));
                }
            },
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        if !poll_input {
            trace_hpp_flags!("FLAGS  {}  {}", hpp.have_progress(), hpp.have_pending());
        }
        match self.as_mut().check_idle_caconn(cx) {
            Ready(Some(())) => {
                hpp.mark_progress();
            }
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        if !poll_input {
            trace_hpp_flags!("FLAGS  {}  {}", hpp.have_progress(), hpp.have_pending());
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
        match self.as_mut().poll_write_sender(cx) {
            Ready(Some(())) => {
                hpp.mark_progress();
            }
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        if hpp.have_progress() {
            Ready(Some(None))
        } else if hpp.have_pending() {
            Pending
        } else {
            Ready(None)
        }
    }

    fn poll_shutdown_issue_all_removes(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_shutdown_issue_all_removes";
        let n1 = self.channels.len();
        let n2 = self.channels.iter().filter(|(_, x)| x.remove_on_shutdown_sent).count();
        trace!("{selfname}  {n1}  {n2}");
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        if let Some(fut) = &mut self.shutdown_fut {
            match fut.poll_unpin(cx) {
                Ready(x) => {
                    self.shutdown_fut = None;
                    hpp.mark_progress();
                    match x {
                        Ok(()) => {}
                        Err(e) => {
                            return Ready(Some(Err(e)));
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
            for (_, ch) in self2.channels.iter_mut() {
                if !ch.remove_on_shutdown_sent {
                    hpp.mark_progress();
                    ch.remove_on_shutdown_sent = true;
                    let (done_tx, mut done_rx) = asynchan::bounded(4, "ConnSetShutdownRemove");
                    let mut tx = ch.cmd_tx.clone();
                    let fut = async move {
                        tx.send(pollcstm::Cmd::Remove(pollcstm::Remove { done_tx })).await?;
                        let _ = done_rx.recv().await;
                        Ok(())
                    };
                    self2.shutdown_fut = Some(fut.box2());
                    break;
                }
            }
        }
        if hpp.have_progress() {
            Ready(Some(Ok(())))
        } else if hpp.have_pending() {
            Pending
        } else {
            Ready(None)
        }
    }
}

macro_rules! poll_a {
    ($poll:expr, $hpp:expr) => {{
        use Poll::*;
        match $poll {
            Ready(Some(x)) => {
                $hpp.mark_progress();
                match x {
                    Some(Ok(())) => {}
                    Some(Err(e)) => {
                        //
                        break Ready(Some(Err(e)));
                    }
                    None => {}
                }
            }
            Ready(None) => {}
            Pending => {
                $hpp.mark_pending();
            }
        }
    }};
}

macro_rules! poll_break {
    ($poll:expr, $hpp:expr) => {{
        use Poll::*;
        match $poll {
            Ready(Some(x)) => {
                $hpp.mark_progress();
                match x {
                    Some(x) => match x {
                        Ok(x) => {
                            break Ready(Some(Ok(x)));
                        }
                        Err(e) => {
                            //
                            break Ready(Some(Err(e)));
                        }
                    },
                    None => {}
                }
            }
            Ready(None) => {}
            Pending => {
                $hpp.mark_pending();
            }
        }
    }};
}

macro_rules! poll_stream_map_ok {
    ($poll:expr, $hpp:expr, $self2:expr, $map:expr, $streamdone:tt) => {{
        use Poll::*;
        match $poll {
            Ready(Some(x)) => {
                $hpp.mark_progress();
                match $map(x) {
                    Ok(Some(x)) => {
                        //
                        break Ready(Some(Ok(x)));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        // TODO
                        $self2.state = State::Done;
                        break Ready(Some(Err(e)));
                    }
                }
            }
            Ready(None) => $streamdone,
            Pending => {
                $hpp.mark_pending();
            }
        }
    }};
}

macro_rules! poll_opt_fut_map {
    ($futopt:expr, $cx:expr, $hpp:expr, $self2:expr, $cf1:ident, $map:expr, $futnone:tt) => {{
        use Poll::*;
        if let Some(fut) = $futopt.as_mut() {
            match fut.poll_unpin($cx) {
                Ready(x) => {
                    $hpp.mark_progress();
                    match $map(x) {
                        Ok(Some(x)) => {
                            $cf1 Poll::Ready(Some(Ok(x)));
                        }
                        Ok(None) => {}
                        Err(e) => {
                            // TODO
                            $self2.state = State::Done;
                            $cf1 Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                Pending => {
                    $hpp.mark_pending();
                }
            }
        } else {
            $futnone
        }
    }};
}

impl Stream for ConnSet {
    type Item = Result<AsynBuf<ConnSetItem>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            trace4!("ConnSet  poll_next  loop begin  {}", self.state.name());
            let mut hpp = HaveProgressPending::new();
            if self.out_buf.len() != 0 {
                break Ready(Some(Ok(self.out_buf.take())));
            }
            match &mut self.state {
                State::Running => {
                    poll_a!(self.as_mut().poll_common(cx), hpp);
                }
                State::Shutdown(st2) => {
                    match st2.timeout.poll_unpin(cx) {
                        Ready(()) => {
                            error!("TODO  shutdown timeout");
                            st2.timeout = tokio::time::sleep(Duration::from_millis(10000)).box2();
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    poll_a!(self.as_mut().poll_common(cx), hpp);
                    if false {
                        poll_opt_fut_map!(self.shutdown_fut, cx, hpp, self, break, |x| { Ok(None) }, {});
                    }
                    poll_stream_map_ok!(
                        self.as_mut().poll_shutdown_issue_all_removes(cx),
                        hpp,
                        self,
                        |x: Result<(), Error>| x.map_err(|e| e).map(|_| None),
                        {
                            trace2!("Shutdown --> Shutdown2");
                            hpp.mark_progress();
                            self.state = State::Shutdown2;
                        }
                    );
                }
                State::Shutdown2 => {
                    poll_a!(self.as_mut().poll_common(cx), hpp);
                    if !hpp.have_progress() && !hpp.have_pending() {
                        trace2!("Shutdown2 --> Done");
                        hpp.mark_progress();
                        self.state = State::Done;
                    }
                }
                State::Done => {}
            }
            break if hpp.have_progress() {
                trace4!("ConnSet  poll_next  Done");
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                trace!("HPP:Done");
                Ready(None)
            };
        }
    }
}
