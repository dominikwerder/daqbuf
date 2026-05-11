use crate::asynchan;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::channelheap;
use crate::ca::conn2::conn::channelheap::ChannelHeap;
use crate::ca::conn2::conn::ctchan::CtChan;
use crate::ca::conn2::conn::ctchan::CtChanRc;
use crate::ca::conn2::locallog;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use asynchan::Receiver;
use asynchan::SendPoll;
use asynchan::Sender;
use ca_proto::ca::proto::CaMsg;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use serde::Serialize;
use stats::mett::CaConnConnectedMetrics;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use taskrun::tokio::time::Sleep;
use taskrun::tokio::time::sleep;
use taskrun::tokio::time::sleep_until;
use tokio::time::error::Elapsed;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ActiveCa"),
    enum variants {
        IO(#[from] std::io::Error),
        ProtoTxClosed,
        ChannelHeap(#[from] channelheap::Error),
        Send,
        Logic,
        IocEchoTimeout,
    },
);

#[derive(Debug)]
pub struct CaCommand {
    kind: CaCommandKind,
}

impl CaCommand {
    pub fn channel_add(conf: ChannelConfig, done_tx: asynchan::Sender<u32>) -> Self {
        Self {
            kind: CaCommandKind::ChannelAdd(conf, done_tx),
        }
    }

    pub fn channel_remove<S: Into<String>>(name: S, done_tx: asynchan::Sender<u32>) -> Self {
        Self {
            kind: CaCommandKind::ChannelRemove(name.into(), done_tx),
        }
    }

    pub fn disconnect_on_idle(done_tx: asynchan::Sender<u32>) -> Self {
        Self {
            kind: CaCommandKind::DisconnectOnIdle(done_tx),
        }
    }
}

#[derive(Debug)]
enum CaCommandKind {
    ChannelAdd(ChannelConfig, asynchan::Sender<u32>),
    ChannelRemove(String, asynchan::Sender<u32>),
    DisconnectOnIdle(asynchan::Sender<u32>),
}

struct Sleep2 {
    until: Instant,
    fut: Pin<Box<Sleep>>,
}

impl Sleep2 {
    fn new_until(until: Instant) -> Self {
        Self {
            until,
            fut: Box::pin(sleep_until(until.into())),
        }
    }

    fn new_dur(dur: Duration) -> Self {
        let until = Instant::now() + dur;
        Self {
            until,
            fut: Box::pin(sleep_until(until.into())),
        }
    }

    fn until(&self) -> Instant {
        self.until
    }
}

impl Future for Sleep2 {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

impl fmt::Debug for Sleep2 {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tsnow = Instant::now();
        let dt = self.until.saturating_duration_since(tsnow);
        let dt = humantime::format_duration(dt);
        fmt.debug_struct("Sleep2").field("until", &dt).finish()
    }
}

#[derive(Debug)]
enum PingPong {
    Idle(Sleep2),
    Send(Option<CaMsg>, Sleep2),
    Wait(Sleep2),
}

impl PingPong {
    fn new_idle() -> Self {
        let dur = Duration::from_millis(10000);
        Self::Idle(Sleep2::new_dur(dur))
    }

    fn new_send(item: CaMsg) -> Self {
        let dur = Duration::from_millis(4000);
        Self::Send(Some(item), Sleep2::new_dur(dur))
    }

    fn new_wait() -> Self {
        let dur = Duration::from_millis(10000);
        Self::Wait(Sleep2::new_dur(dur))
    }

    fn status_info(&self) -> PingPongInfo {
        match self {
            PingPong::Idle(x) => PingPongInfo::Idle(x.until()),
            PingPong::Send(_, x) => PingPongInfo::Send(x.until()),
            PingPong::Wait(x) => PingPongInfo::Wait(x.until()),
        }
    }
}

use ca_proto::ca::proto::CaMsgTy;
use serde_helper::serde_instant::serde_Instant_elapsed_ms::serialize as inser3;

#[derive(Debug, Serialize)]
enum PingPongInfo {
    Idle(#[serde(serialize_with = "inser3")] Instant),
    Send(#[serde(serialize_with = "inser3")] Instant),
    Wait(#[serde(serialize_with = "inser3")] Instant),
}

#[derive(Debug)]
struct Running {
    pingpong: PingPong,
}

impl Running {
    fn new() -> Self {
        Self {
            pingpong: PingPong::new_idle(),
        }
    }

    fn status_info(&self) -> RunningInfo {
        RunningInfo {
            pingpong: self.pingpong.status_info(),
        }
    }

    pub(super) fn dump_state_poll(&self) -> serde_json::Value {
        use serde_json::json;
        let js = json!({
            "pingpong": "TODO",
        });
        js
    }
}

#[derive(Debug, Serialize)]
pub struct RunningInfo {
    pingpong: PingPongInfo,
}

#[derive(Debug)]
enum State {
    Running(Running),
    Done,
}

impl State {
    fn new() -> Self {
        Self::Running(Running::new())
    }
}

struct CommandFut(Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>);

impl fmt::Debug for CommandFut {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("CommandFut").finish()
    }
}

#[derive(Debug)]
pub enum ItemInner {
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelEventValue(ChannelEventValue),
}

#[derive(Debug)]
pub struct ActiveCaItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

#[derive(Debug, Serialize)]
pub enum StatusInfoState {
    Running(RunningInfo, channelheap::StatusInfo),
    Done,
}

#[derive(Debug, Serialize)]
pub struct StatusInfo {
    pub state: StatusInfoState,
}

type StreamItem = Result<ActiveCaItem, Error>;

#[derive(Debug)]
pub struct ActiveCa {
    backend: String,
    tsbeg: Instant,
    addr: SocketAddrV4,
    state: State,
    chanheap: ChannelHeap,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_rx: asynchan::Receiver<CaMsg>,
    proto_rx_buf: VecDeque<CaMsg>,
    proto_rx_buf_done: bool,
    proto_2_tx: asynchan::Sender<CaMsg>,
    cmd_fut: Option<CommandFut>,
    chanheap_cmd_tx: asynchan::Sender<channelheap::Cmd>,
    chanheap_cmd_rx: asynchan::Receiver<channelheap::Cmd>,
    mett: CaConnConnectedMetrics,
}

impl ActiveCa {
    pub fn new(
        backend: String,
        proto_rx: asynchan::Receiver<CaMsg>,
        proto_tx: asynchan::Sender<CaMsg>,
        tsnow: Instant,
        addr: SocketAddrV4,
    ) -> Self {
        let (proto_2_tx, proto_2_rx) = asynchan::bounded(120, "ActiveCa-proto2");
        let (chanheap_cmd_tx, chanheap_cmd_rx) = asynchan::bounded(16, "ActiveCa-ChannelHeap-cmd");
        Self {
            chanheap: ChannelHeap::new(backend.clone(), proto_tx.clone(), proto_2_rx),
            proto_tx,
            backend,
            tsbeg: tsnow,
            addr,
            state: State::new(),
            proto_rx,
            proto_rx_buf: VecDeque::with_capacity(16),
            proto_rx_buf_done: false,
            proto_2_tx,
            cmd_fut: None,
            chanheap_cmd_tx,
            chanheap_cmd_rx,
            mett: CaConnConnectedMetrics::new(),
        }
    }

    pub fn status_info(&self) -> StatusInfo {
        match &self.state {
            State::Running(st) => StatusInfo {
                state: StatusInfoState::Running(st.status_info(), self.chanheap.status_info()),
            },
            State::Done => StatusInfo {
                state: StatusInfoState::Done,
            },
        }
    }

    pub fn mett_take(&mut self) -> CaConnConnectedMetrics {
        self.chanheap.mett_take()
    }

    pub(super) fn check_flow_state(&self) {
        let selfname = "check_flow_state";
        match &self.state {
            State::Running(st) => {
                warn!("{selfname}  State::Running");
            }
            State::Done => {}
        }
    }

    pub(super) fn dump_state_poll(&self) -> serde_json::Value {
        use serde_json::json;
        let st = match &self.state {
            State::Running(st) => json!({"Running": st.dump_state_poll()}),
            State::Done => json!({"Done": {}}),
        };
        let js = json!({
            "state": st,
            "chanheap": self.chanheap.dump_state_poll(),
        });
        js
    }

    pub fn handle_dyn_cmd_v03(&mut self, cmd: serde_json::Value) -> impl Future<Output = serde_json::Value> + use<> {
        use futures::future::ready;
        use serde_json::json;
        match &mut self.state {
            State::Running(st) => self.chanheap.handle_dyn_cmd_v03(cmd).box2(),
            State::Done => ready(json!({
                "error": "ActiveCa  State::Done",
            }))
            .box2(),
        }
    }

    fn handle_command(&mut self, cmd: CaCommand, cx: &mut Context) -> CommandFut {
        let selfname = "handle_command";
        match cmd.kind {
            CaCommandKind::ChannelAdd(conf, mut done_tx) => {
                trace!("{selfname}  ChannelAdd");
                self.chanheap.channel_add(conf, cx);
                let fut = async move {
                    let _ = done_tx.send(0).await;
                    Ok(())
                }
                .boxed();
                CommandFut(Box::pin(fut))
            }
            CaCommandKind::ChannelRemove(name, mut done_tx) => {
                trace!("{selfname}  ChannelRemove");
                let mut chanheap_cmd_tx = self.chanheap_cmd_tx.clone();
                let fut = async move {
                    let (done_2_tx, mut done_2_rx) = asynchan::bounded(2, "ChannelHeap-Done");
                    let cmd = channelheap::Cmd::RemoveChannel(name, done_2_tx);
                    let ff = chanheap_cmd_tx.send(cmd);
                    match ff.await {
                        Ok(()) => {
                            trace!("{selfname} ChannelRemove Future: sent RemoveChannel command");
                            if done_2_rx.recv().await.is_err() {
                                error!("{selfname}  done_2_rx  recv  fail")
                            }
                            if done_tx.send(0).await.is_err() {
                                error!("{selfname}  done_tx  send  fail")
                            }
                            Ok(())
                        }
                        Err(e) => {
                            error!("{selfname}  TODO  ChannelRemove Future: failed to send RemoveChannel command");
                            Err(Error::Send)
                        }
                    }
                }
                .boxed();
                CommandFut(Box::pin(fut))
            }
            CaCommandKind::DisconnectOnIdle(mut done_tx) => {
                trace!("{selfname}  DisconnectOnIdle");
                self.chanheap.disconnect_on_idle();
                let fut = async move {
                    let _ = done_tx.send(0).await;
                    Ok(())
                }
                .boxed();
                CommandFut(Box::pin(fut))
            }
        }
    }

    fn poll_command_input(
        mut self: Pin<&mut Self>,
        cmd_rx: &mut CtChan<CaCommand>,
        cx: &mut Context,
        hpp: &mut HaveProgressPending,
    ) -> Option<Error> {
        use Poll::*;
        let self2 = self.get_mut();
        if let Some(fut) = self2.cmd_fut.as_mut() {
            match fut.0.as_mut().poll(cx) {
                Ready(Ok(())) => {
                    trace!("CmdFut:Ready:Ok");
                    self2.cmd_fut = None;
                    hpp.mark_progress();
                    None
                }
                Ready(Err(e)) => {
                    trace!("CmdFut:Ready:Err {e}");
                    hpp.mark_progress();
                    Some(e)
                }
                Pending => {
                    trace_pending!("CmdFut");
                    hpp.mark_pending();
                    None
                }
            }
        } else {
            match cmd_rx.poll_next_unpin(cx) {
                Ready(Some(cmd)) => {
                    trace!("CmdRx:Some");
                    trace!("---------------------------------------------------     CmdRx:Some");
                    hpp.mark_progress();
                    let fut = self2.handle_command(cmd, cx);
                    self2.cmd_fut = Some(fut);
                    None
                }
                Ready(None) => None,
                Pending => {
                    trace_pending!("CmdRx");
                    hpp.mark_pending();
                    None
                }
            }
        }
    }

    fn poll_dispatch(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        let selfname = "poll_dispatch";
        trace4!("{selfname}");
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(item) = self.proto_rx_buf.pop_front() {
                let dispatch = item.cid().is_some() || item.subid().is_some() || item.ioid().is_some();
                if dispatch {
                    use asynchan::SendPoll;
                    use asynchan::SendPollError;
                    match self.proto_2_tx.poll_send_unpin(item, cx) {
                        Ok(()) => {
                            trace!("{selfname}  Sent");
                            hpp.mark_progress();
                        }
                        Err(e) => match e {
                            SendPollError::Full(item) => {
                                trace_pending!("{selfname}  Pending");
                                hpp.mark_pending();
                                self.proto_rx_buf.push_front(item);
                            }
                            SendPollError::Closed(item) => {
                                trace!("{selfname}  Closed");
                                error!("{selfname}  TODO handle Closed better?");
                            }
                        },
                    }
                } else {
                    match item.ty {
                        CaMsgTy::Echo => {
                            hpp.mark_progress();
                            if let State::Running(st1) = &mut self.state {
                                match &mut st1.pingpong {
                                    PingPong::Idle(..) => {}
                                    PingPong::Send(..) => {}
                                    PingPong::Wait(..) => {
                                        st1.pingpong = PingPong::new_idle();
                                    }
                                }
                            } else {
                                // TODO metrics
                            }
                        }
                        _ => {
                            hpp.mark_progress();
                            error!("TODO handle incoming item internally: {item:?}");
                        }
                    }
                }
            } else if self.proto_rx_buf_done {
            } else {
                hpp.mark_pending();
            }
            break if hpp.have_progress() {
                trace4!("{selfname}  HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("{selfname}  HPP");
                Pending
            } else {
                trace!("{selfname}  HPP:Done");
                Ready(None)
            };
        }
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        cmd_rx: &mut CtChan<CaCommand>,
        cx: &mut Context,
    ) -> Poll<Option<StreamItem>> {
        use Poll::*;
        let selfname = "ActiveCa::poll_next";
        trace4!("{selfname}");
        loop {
            let mut hpp = HaveProgressPending::new();
            match &mut self.state {
                State::Running(st1) => {
                    match self.as_mut().poll_command_input(cmd_rx, cx, &mut hpp) {
                        Some(e) => {
                            hpp.mark_progress();
                            break Ready(Some(Err(e)));
                        }
                        None => {}
                    }
                    let self2 = self.as_mut().get_mut();
                    if self2.proto_rx_buf.len() < self2.proto_rx_buf.capacity() {
                        match self2.proto_rx.poll_next_unpin(cx) {
                            Ready(x) => match x {
                                Some(item) => {
                                    trace!("ActiveCa:ProtoRx:Some");
                                    self2.proto_rx_buf.push_back(item);
                                    hpp.mark_progress();
                                }
                                None => {
                                    trace!("ActiveCa:ProtoRx:Error");
                                    error!("TODO clean shutdown, remote seems gone");
                                    self2.state = State::Done;
                                    hpp.mark_progress();
                                }
                            },
                            Pending => {
                                trace_pending!("ActiveCa:ProtoRx");
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        warn!("{selfname}  SKIP proto_rx.poll_next_unpin  BLOCKED BY proto_rx_buf");
                    }
                    match self.as_mut().poll_dispatch(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(()) => {}
                                Err(e) => {
                                    break Ready(Some(Err(e)));
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    let self2 = self.as_mut().get_mut();
                    match self2.chanheap.poll_next_unpin(&mut self2.chanheap_cmd_rx, cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(item) => {
                                    trace!("ActiveCa:ChannelHeap:Some");
                                    match item.inner {
                                        channelheap::ItemInner::ChannelInfoQuery(item2) => {
                                            let item = ActiveCaItem {
                                                ts_create: item.ts_create,
                                                inner: ItemInner::ChannelInfoQuery(item2),
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                        channelheap::ItemInner::TestValue(x) => {
                                            let item = ActiveCaItem {
                                                ts_create: item.ts_create,
                                                inner: ItemInner::TestValue(x),
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                        channelheap::ItemInner::LocalLog(x) => {
                                            let item = ActiveCaItem {
                                                ts_create: item.ts_create,
                                                inner: ItemInner::LocalLog(x),
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                        channelheap::ItemInner::ChannelEventValue(x) => {
                                            let item = ActiveCaItem {
                                                ts_create: item.ts_create,
                                                inner: ItemInner::ChannelEventValue(x),
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("ActiveCa:ChannelHeap:Error {e}");
                                    error!("ActiveCa:ChannelHeap:Error  TODO clean shutdown");
                                    self2.state = State::Done;
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {
                            error!("ActiveCa:ChannelHeap:Done  TODO clean shutdown");
                            hpp.mark_progress();
                            self2.state = State::Done;
                        }
                        Pending => {
                            trace_pending!("ActiveCa:ChannelHeap");
                            hpp.mark_pending();
                        }
                    }
                    let self2 = self.as_mut().get_mut();
                    if let State::Running(st1) = &mut self2.state {
                        match &mut st1.pingpong {
                            PingPong::Idle(to) => match to.poll_unpin(cx) {
                                Ready(()) => {
                                    hpp.mark_progress();
                                    let tsnow = Instant::now();
                                    let item = CaMsg::from_ty_ts(CaMsgTy::Echo, tsnow);
                                    st1.pingpong = PingPong::new_send(item);
                                }
                                Pending => {
                                    hpp.mark_pending();
                                }
                            },
                            PingPong::Send(item, to) => match to.poll_unpin(cx) {
                                Ready(()) => {
                                    hpp.mark_progress();
                                    let tsnow = Instant::now();
                                    let item = CaMsg::from_ty_ts(CaMsgTy::Echo, tsnow);
                                    st1.pingpong = PingPong::new_send(item);
                                }
                                Pending => {
                                    hpp.mark_pending();
                                    if let Some(item2) = item.take() {
                                        match self2.proto_tx.poll_send_unpin(item2, cx) {
                                            Ok(()) => {
                                                hpp.mark_progress();
                                                st1.pingpong = PingPong::new_wait();
                                            }
                                            Err(e) => match e {
                                                asynchan::SendPollError::Full(x) => {
                                                    hpp.mark_pending();
                                                    *item = Some(x);
                                                }
                                                asynchan::SendPollError::Closed(_) => {
                                                    self2.state = State::Done;
                                                    break Ready(Some(Err(Error::ProtoTxClosed)));
                                                }
                                            },
                                        }
                                    } else {
                                        self2.state = State::Done;
                                        break Ready(Some(Err(Error::Logic)));
                                    }
                                }
                            },
                            PingPong::Wait(to) => match to.poll_unpin(cx) {
                                Ready(()) => {
                                    hpp.mark_progress();
                                    self2.state = State::Done;
                                    break Ready(Some(Err(Error::IocEchoTimeout)));
                                }
                                Pending => {
                                    hpp.mark_pending();
                                }
                            },
                        }
                    } else {
                        self2.state = State::Done;
                        break Ready(Some(Err(Error::Logic)));
                    }
                }
                State::Done => {
                    error!(
                        "{selfname}  State::Done  {}  {}",
                        hpp.have_progress(),
                        hpp.have_pending()
                    );
                    // TODO when in Done, we should no longer be stuck with Pending on something.
                }
            }
            break if hpp.have_progress() {
                trace4!("HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("HPP");
                Pending
            } else {
                trace!("HPP:Done");
                Ready(None)
            };
        }
    }

    pub fn poll_next_unpin(&mut self, cmd_rx: &mut CtChan<CaCommand>, cx: &mut Context) -> Poll<Option<StreamItem>> {
        Pin::new(self).poll_next(cmd_rx, cx)
    }

    pub fn channel_info_v1(&mut self) -> crate::metrics::ChannelsForAddrInfoV1 {
        self.chanheap.channel_info_v1()
    }

    pub fn channel_info_v2(&mut self, name: String) -> crate::metrics::ChannelsForAddrInfoV2 {
        self.chanheap.channel_info_v2(name)
    }

    pub fn channels_by_regex_v1(&mut self, kind: String, reg: String) -> Vec<serde_json::Value> {
        self.chanheap.channels_by_regex_v1(kind, reg)
    }
}
