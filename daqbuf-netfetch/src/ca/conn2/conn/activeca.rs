const INP_BUF_CAP: usize = 3;
const FWD_BUF_CAP: usize = 1;
const LOOP_MAX_PUSH_TO_CHANHEAP: usize = 200;

//

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
macro_rules! trace_blocked { ($($arg:tt)*) => { if super::TRACE_BLOCK { log::trace!($($arg)*); } }; }
macro_rules! trace_command { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ActiveCa"),
    enum variants {
        IO(#[from] std::io::Error),
        ProtoTxClosed,
        ChannelHeap(#[from] channelheap::Error),
        BufPushGuardIssue,
        Send,
        PingNoItem,
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

use crate::asynbuf;
use crate::asynbuf::AsynBuf;
use crate::asynbuf::TsMark;
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

#[derive(Debug)]
struct CommandFut(FutDbg<Result<(), Error>>);

#[derive(Debug)]
pub enum ItemInner {
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelEventValue(ChannelEventValue),
    ProtoOut(CaMsg),
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

type StreamItem = Result<AsynBuf<ActiveCaItem>, Error>;

#[derive(Debug)]
pub enum InpItem {
    CaMsg(CaMsg),
    Cmd(CaCommand),
}

#[derive(Debug)]
pub struct ActiveCa {
    tsbeg: Instant,
    addr: SocketAddrV4,
    state: State,
    chanheap: ChannelHeap,
    backend: String,
    inp_buf: asynbuf::AsynBuf<InpItem>,
    inp_buf_done: bool,
    inp_cmd_buf: asynbuf::AsynBuf<CaCommand>,
    inp_msg_buf: asynbuf::AsynBuf<CaMsg>,
    buf_for_chanheap: asynbuf::AsynBuf<channelheap::InpItem>,
    ts_mark_proto_rx: TsMark,
    cmd_fut: Option<CommandFut>,
    mett: CaConnConnectedMetrics,
}

impl ActiveCa {
    pub fn new(backend: String, tsnow: Instant, addr: SocketAddrV4) -> Self {
        Self {
            tsbeg: tsnow,
            addr,
            state: State::new(),
            chanheap: ChannelHeap::new(backend.clone()),
            backend,
            inp_buf: asynbuf::AsynBuf::new(INP_BUF_CAP),
            inp_buf_done: false,
            inp_cmd_buf: asynbuf::AsynBuf::new(FWD_BUF_CAP),
            inp_msg_buf: asynbuf::AsynBuf::new(FWD_BUF_CAP),
            buf_for_chanheap: asynbuf::AsynBuf::new(FWD_BUF_CAP),
            ts_mark_proto_rx: TsMark::new("proto_rx".into()),
            cmd_fut: None,
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
            "inp_buf": {
                "len": self.inp_buf.len(),
                "cap": self.inp_buf.cap(),
            },
            "ts_mark_proto_rx": &self.ts_mark_proto_rx,
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
        let self2 = self;
        assert_eq!(self2.buf_for_chanheap.is_space(), true);
        match cmd.kind {
            CaCommandKind::ChannelAdd(conf, mut done_tx) => {
                trace_command!("{selfname}  ChannelAdd");
                let s2 = format!("ChannelAdd {conf:?}");
                self2.chanheap.channel_add(conf, cx);
                let fut = async move {
                    let _ = done_tx.send(0).await;
                    trace_command!("COMMAND DONE {s2}");
                    Ok(())
                };
                CommandFut(fut.box2())
            }
            CaCommandKind::ChannelRemove(name, mut done_tx) => {
                trace_command!("{selfname}  ChannelRemove");
                let s2 = format!("ChannelRemove {name}");
                let (done_2_tx, mut done_2_rx) = asynchan::bounded(2, "ChannelHeap-Done");
                let cmd = channelheap::Cmd::RemoveChannel(name, done_2_tx);
                // guarded
                self2.buf_for_chanheap.push_back_force(channelheap::InpItem::Cmd(cmd));
                let fut = async move {
                    if done_2_rx.recv().await.is_err() {
                        error!("{selfname}  done_2_rx  recv  fail")
                    }
                    if done_tx.send(0).await.is_err() {
                        error!("{selfname}  done_tx  send  fail")
                    }
                    trace_command!("COMMAND DONE {s2}");
                    Ok(())
                };
                CommandFut(fut.box2())
            }
            CaCommandKind::DisconnectOnIdle(mut done_tx) => {
                trace_command!("{selfname}  DisconnectOnIdle");
                let s2 = format!("DisconnectOnIdle");
                self2.chanheap.disconnect_on_idle();
                let fut = async move {
                    let _ = done_tx.send(0).await;
                    trace_command!("COMMAND DONE {s2}");
                    Ok(())
                };
                CommandFut(fut.box2())
            }
        }
    }

    // Has no EOS return.
    fn poll_command_input(self: Pin<&mut Self>, cx: &mut Context) -> Option<Poll<Option<Error>>> {
        use Poll::*;
        let selfname = "poll_command_input";
        let self2 = self.get_mut();
        if let Some(fut) = self2.cmd_fut.as_mut() {
            match fut.0.poll_unpin(cx) {
                Ready(Ok(())) => {
                    trace!("CmdFut:Ready:Ok");
                    self2.cmd_fut = None;
                    Some(Ready(None))
                }
                Ready(Err(e)) => {
                    trace!("CmdFut:Ready:Err {e}");
                    Some(Ready(Some(e)))
                }
                Pending => {
                    trace_pending!("CmdFut");
                    Some(Pending)
                }
            }
        } else if self2.buf_for_chanheap.is_space() {
            if let Some(item) = self2.inp_cmd_buf.pop_front() {
                let fut = self2.handle_command(item, cx);
                self2.cmd_fut = Some(fut);
                Some(Ready(None))
            } else {
                None
            }
        } else {
            None
        }
    }

    fn poll_dispatch(mut self: Pin<&mut Self>, cx: &mut Context) -> Option<Poll<Result<(), Error>>> {
        use Poll::*;
        let selfname = "poll_dispatch";
        trace4!("{selfname}");
        loop {
            let mut hpp = HaveProgressPending::new();
            if self.buf_for_chanheap.is_space() {
                if let Some(item) = self.inp_msg_buf.pop_front() {
                    hpp.mark_progress();
                    let dispatch = item.cid().is_some() || item.subid().is_some() || item.ioid().is_some();
                    if dispatch {
                        let item = channelheap::InpItem::CaMsg(item);
                        self.buf_for_chanheap.push_back_force(item);
                    } else {
                        match item.ty {
                            CaMsgTy::Echo => {
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
                                error!("TODO handle incoming item internally: {item:?}");
                            }
                        }
                    }
                } else {
                }
            } else {
                trace_blocked!("{selfname}  SKIP  BLOCKED BY buf_for_chanheap");
            }
            break if hpp.have_progress() {
                trace4!("{selfname}  HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("{selfname}  HPP");
                Some(Pending)
            } else {
                trace!("{selfname}  HPP:Done");
                None
            };
        }
    }

    pub fn inp_push_try(&mut self, item: InpItem, cx: &mut Context<'_>) -> asynbuf::PushRes<InpItem> {
        let self2 = self;
        let v = &mut self2.inp_buf;
        // let w1 = &mut self2.waker_1;
        // let w2 = &mut self2.waker_2;
        let x = v.push_back(item);
        match &x {
            asynbuf::PushRes::First => {
                // if let Some(w) = w1.take() {
                //     w.wake();
                // }
            }
            asynbuf::PushRes::Done => {}
            asynbuf::PushRes::Full(_) => {
                // *w2 = Some(cx.waker().clone());
            }
        }
        x
    }

    pub fn inp_is_space(&self) -> bool {
        self.inp_buf.is_space()
    }

    fn poll_inp_push_to_chanheap(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        let selfname = "ActiveCa::poll_inp_push_to_chanheap";
        let self2 = self.as_mut().get_mut();
        let mut i = 0;
        loop {
            let mut hpp = HaveProgressPending::new();
            if i > LOOP_MAX_PUSH_TO_CHANHEAP {
            } else if self2.chanheap.inp_is_space()
                && let Some(x) = self2.buf_for_chanheap.pop_front()
            {
                // guarded
                match self2.chanheap.inp_push_try(x, cx) {
                    asynbuf::PushRes::First => {
                        hpp.mark_progress();
                    }
                    asynbuf::PushRes::Done => {
                        hpp.mark_progress();
                    }
                    asynbuf::PushRes::Full(x) => {
                        self2.buf_for_chanheap.push_front(x);
                        // TODO metrics should not happen
                        //
                        // TODO
                        // Should we mark HPP?
                        //
                    }
                }
            } else {
                trace_blocked!("SKIP self2.chanheap.inp_push_try  BLOCKED YB self2.chanheap.inp_is_space");
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<StreamItem>> {
        use Poll::*;
        let selfname = "ActiveCa::poll_next";
        trace4!("{selfname}");
        loop {
            let tsnow = Instant::now();
            let mut hpp = HaveProgressPending::new();
            match &mut self.state {
                State::Running(st1) => {
                    let self2 = self.as_mut().get_mut();
                    if self2.inp_cmd_buf.is_space() && self2.inp_msg_buf.is_space() {
                        if let Some(item) = self2.inp_buf.pop_front() {
                            match item {
                                InpItem::CaMsg(x) => {
                                    if self2.inp_msg_buf.push_back(x).is_fail() {
                                        self2.state = State::Done;
                                        break Ready(Some(Err(Error::BufPushGuardIssue)));
                                    }
                                }
                                InpItem::Cmd(x) => {
                                    if self2.inp_cmd_buf.push_back(x).is_fail() {
                                        self2.state = State::Done;
                                        break Ready(Some(Err(Error::BufPushGuardIssue)));
                                    }
                                }
                            }
                        } else {
                        }
                    } else {
                        warn!("{selfname}  SKIP inp_buf pop");
                    }
                    match self.as_mut().poll_command_input(cx) {
                        Some(x) => match x {
                            Ready(Some(e)) => {
                                hpp.mark_progress();
                                self.state = State::Done;
                                break Ready(Some(Err(e.into())));
                            }
                            Ready(None) => {
                                hpp.mark_progress();
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        },
                        None => {}
                    }
                    match self.as_mut().poll_dispatch(cx) {
                        Some(x) => match x {
                            Ready(x) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(()) => {}
                                    Err(e) => {
                                        self.state = State::Done;
                                        break Ready(Some(Err(e)));
                                    }
                                }
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        },
                        None => {}
                    }
                    match self.as_mut().poll_inp_push_to_chanheap(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(()) => {}
                                Err(e) => {
                                    self.state = State::Done;
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
                    match self2.chanheap.poll_next_unpin(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(items) => {
                                    trace!("ActiveCa:ChannelHeap:Some");
                                    let items = items
                                        .into_iter()
                                        .map(|item| match item.inner {
                                            channelheap::ItemInner::ChannelInfoQuery(item2) => {
                                                let item = ActiveCaItem {
                                                    ts_create: item.ts_create,
                                                    inner: ItemInner::ChannelInfoQuery(item2),
                                                };
                                                item
                                            }
                                            channelheap::ItemInner::TestValue(x) => {
                                                let item = ActiveCaItem {
                                                    ts_create: item.ts_create,
                                                    inner: ItemInner::TestValue(x),
                                                };
                                                item
                                            }
                                            channelheap::ItemInner::LocalLog(x) => {
                                                let item = ActiveCaItem {
                                                    ts_create: item.ts_create,
                                                    inner: ItemInner::LocalLog(x),
                                                };
                                                item
                                            }
                                            channelheap::ItemInner::ChannelEventValue(x) => {
                                                let item = ActiveCaItem {
                                                    ts_create: item.ts_create,
                                                    inner: ItemInner::ChannelEventValue(x),
                                                };
                                                item
                                            }
                                            channelheap::ItemInner::ProtoOut(x) => {
                                                let item = ActiveCaItem {
                                                    ts_create: item.ts_create,
                                                    inner: ItemInner::ProtoOut(x),
                                                };
                                                item
                                            }
                                        })
                                        .collect::<VecDeque<_>>();
                                    break Ready(Some(Ok(AsynBuf::from_deque(items))));
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
                                    error!("error emit ping item");
                                    st1.pingpong = PingPong::new_idle();
                                }
                                Pending => {
                                    hpp.mark_pending();
                                    if let Some(item2) = item.take() {
                                        let t2 = ActiveCaItem {
                                            ts_create: tsnow,
                                            inner: ItemInner::ProtoOut(item2),
                                        };
                                        hpp.mark_progress();
                                        st1.pingpong = PingPong::new_wait();
                                        let x = AsynBuf::from_deque([t2].into());
                                        break Ready(Some(Ok(x)));
                                    } else {
                                        self2.state = State::Done;
                                        break Ready(Some(Err(Error::PingNoItem)));
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
                        // nothing to do
                    }
                }
                State::Done => {
                    if hpp.have_pending() || hpp.have_progress() {
                        error!(
                            "{selfname}  State::Done  {}  {}",
                            hpp.have_progress(),
                            hpp.have_pending()
                        );
                    }
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

    pub fn poll_next_unpin(&mut self, cx: &mut Context) -> Poll<Option<StreamItem>> {
        Pin::new(self).poll_next(cx)
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
