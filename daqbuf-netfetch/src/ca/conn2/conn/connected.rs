const INP_BUF_CAP: usize = 128;

pub const LOOP_MAX_PROTOWRAP_POLL: usize = 130;
pub const LOOP_MAX_PASS_INP: usize = 128;
pub const LOOP_MAX_PASS_CMD: usize = 16;

//

use super::handshake::Handshake;
use crate::asynbuf;
use crate::asynbuf::AsynBuf;
use crate::asynchan;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::activeca;
use crate::ca::conn2::conn::activeca::ActiveCa;
use crate::ca::conn2::conn::ctchan::CtChan;
use crate::ca::conn2::locallog;
use crate::ca::conn2::protowrap;
use crate::ca::connset2::connset::channeltrace::ChannelTraceItem;
use crate::ca::connset2::connset::channeltrace::ChannelTraceL1Item;
use crate::ca::progpend::HaveProgressPending;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use netpod::futdbg::FutDbgBox;
use serde::Serialize;
use stats::mett::CaConnConnectedMetrics;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }
macro_rules! trace_transition { ($($arg:tt)*) => { if false { log::debug!($($arg)*); } }; }
macro_rules! trace_blocked { ($($arg:tt)*) => { if super::TRACE_BLOCK { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Connected"),
    enum variants {
        IO(#[from] std::io::Error),
        Protowrap(#[from] protowrap::Error),
        Handshake(#[from] super::handshake::Error),
        ActiveCa(#[from] super::activeca::Error),
        NoProgressNoPending,
        ProtoOutputClosed,
        Logic,
    },
);

#[derive(Debug)]
enum State {
    Init(),
    Handshake(Handshake),
    ActiveCa(ActiveCa),
    Done,
}

#[derive(Debug)]
pub enum ItemInner {
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelEventValue(ChannelEventValue),
    ChannelTrace(ChannelTraceL1Item),
}

#[derive(Debug)]
pub struct ConnectedItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

#[derive(Debug, Serialize)]
pub enum StatusInfoState {
    Init,
    Handshake,
    ActiveCa(activeca::StatusInfo),
    Done,
}

#[derive(Debug, Serialize)]
pub struct StatusInfo {
    pub state: StatusInfoState,
    pub socket_state: serde_json::Value,
}

#[derive(Debug)]
pub struct Connected {
    backend: String,
    tsbeg: Instant,
    addr: SocketAddrV4,
    protowrap: protowrap::ProtoPusher,
    state: State,
    inp_buf: asynbuf::AsynBuf<CaMsg>,
    inp_cmd_buf: asynbuf::AsynBuf<activeca::CaCommand>,
    mett: CaConnConnectedMetrics,
}

impl Connected {
    pub fn new(backend: String, tcp: TcpStream, addr: SocketAddrV4, tsnow: Instant) -> Self {
        let raw_socket_fd = tcp.as_raw_fd();
        // TODO take from options
        let array_truncate = 1024 * 1024 * 10;
        let proto = CaProto::new(
            TcpAsyncWriteRead::from(tcp),
            Some(raw_socket_fd),
            addr.to_string(),
            array_truncate,
        );
        let protowrap = protowrap::ProtoPusher::new(proto);
        Self {
            backend,
            tsbeg: tsnow,
            addr,
            protowrap,
            state: State::Init(),
            inp_buf: asynbuf::AsynBuf::new(INP_BUF_CAP),
            inp_cmd_buf: asynbuf::AsynBuf::new(INP_BUF_CAP),
            mett: CaConnConnectedMetrics::new(),
        }
    }

    pub fn inp_cmd_buf(&mut self) -> &mut asynbuf::AsynBuf<activeca::CaCommand> {
        &mut self.inp_cmd_buf
    }

    pub fn status_info(&mut self) -> StatusInfo {
        let ss = self.status_socket();
        match &self.state {
            State::Init(..) => StatusInfo {
                state: StatusInfoState::Init,
                socket_state: ss,
            },
            State::Handshake(..) => StatusInfo {
                state: StatusInfoState::Handshake,
                socket_state: ss,
            },
            State::ActiveCa(st) => StatusInfo {
                state: StatusInfoState::ActiveCa(st.status_info()),
                socket_state: ss,
            },
            State::Done => StatusInfo {
                state: StatusInfoState::Done,
                socket_state: ss,
            },
        }
    }

    pub fn mett_take(&mut self) -> CaConnConnectedMetrics {
        // std::mem::replace(&mut self.mett, CaConnConnectedMetrics::new())
        match &mut self.state {
            State::Init(..) => {
                // TODO
                CaConnConnectedMetrics::new()
            }
            State::Handshake(..) => {
                // TODO
                CaConnConnectedMetrics::new()
            }
            State::ActiveCa(st) => st.mett_take(),
            State::Done => {
                // TODO
                CaConnConnectedMetrics::new()
            }
        }
    }

    pub fn addr(&self) -> SocketAddrV4 {
        self.addr
    }

    fn goto_state_done(&mut self) {
        self.state = State::Done;
    }

    pub fn channel_info_v1(&mut self) -> crate::metrics::ChannelsForAddrInfoV1 {
        let empty = crate::metrics::ChannelsForAddrInfoV1::new();
        match &mut self.state {
            State::Init(..) => empty,
            State::Handshake(..) => empty,
            State::ActiveCa(st, ..) => st.channel_info_v1(),
            State::Done => empty,
        }
    }

    pub fn channel_info_v2(&mut self, name: String) -> crate::metrics::ChannelsForAddrInfoV2 {
        let empty = crate::metrics::ChannelsForAddrInfoV2::new();
        match &mut self.state {
            State::Init(..) => empty,
            State::Handshake(..) => empty,
            State::ActiveCa(st, ..) => st.channel_info_v2(name),
            State::Done => empty,
        }
    }

    pub fn channels_by_regex_v1(&mut self, kind: String, reg: String) -> Vec<serde_json::Value> {
        let empty = Vec::new();
        match &mut self.state {
            State::Init(..) => empty,
            State::Handshake(..) => empty,
            State::ActiveCa(st, ..) => st.channels_by_regex_v1(kind, reg),
            State::Done => empty,
        }
    }

    pub fn status_socket(&mut self) -> serde_json::Value {
        self.protowrap.status_socket()
    }

    pub fn handle_dyn_cmd_v03(&mut self, cmd: serde_json::Value) -> impl Future<Output = serde_json::Value> + use<> {
        use futures::future::ready;
        use serde_json::json;
        match &mut self.state {
            State::Init(..) => ready(json!({
                "error": "CaConn  Connected  State::Init",
            }))
            .box2(),
            State::Handshake(..) => ready(json!({
                "error": "CaConn  Connected  State::Handshake",
            }))
            .box2(),
            State::ActiveCa(st, ..) => {
                let ss = self.protowrap.status_socket();
                let aca = st.handle_dyn_cmd_v03(cmd);
                async move {
                    json!({
                        "proto": {
                            "ss": ss,
                        },
                        "ActiveCa": aca.await,
                    })
                }
                .box2()
            }
            State::Done => ready(json!({
                "error": "CaConn  Connected  State::Done",
            }))
            .box2(),
        }
    }

    pub(super) fn check_flow_state(&self) {
        match &self.state {
            State::Init(..) => {}
            State::Handshake(_) => {}
            State::ActiveCa(st) => {
                st.check_flow_state();
            }
            State::Done => {}
        }
    }

    pub(super) fn dump_state_poll(&self) -> serde_json::Value {
        use serde_json::json;
        let st = match &self.state {
            State::Init(..) => json!({"Init": {}}),
            State::Handshake(_) => json!({"Handshake": {}}),
            State::ActiveCa(st) => json!({
                "ActiveCa": st.dump_state_poll(),
            }),
            State::Done => json!({"Done": {}}),
        };
        let js = json!({
            "state": st,
            "protowrap": self.protowrap.dump_state_poll(),
        });
        js
    }

    pub fn health_check(&self) -> bool {
        let mut healthy = true;
        if self.protowrap.out_len() > 1000 {
            warn!("protowrap len");
            healthy = false;
        }
        if self.inp_buf.len() > 10 * INP_BUF_CAP {
            warn!("inp_buf len");
            healthy = false;
        }
        if self.inp_cmd_buf.len() > 10 * INP_BUF_CAP {
            warn!("inp_cmd_buf len");
            healthy = false;
        }
        // TODO
        match &self.state {
            State::Init() => {}
            State::Handshake(st) => {}
            State::ActiveCa(st) => {}
            State::Done => {}
        };
        healthy
    }
}

impl Stream for Connected {
    type Item = Result<AsynBuf<ConnectedItem>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfname = "Connected::poll_next";
        trace4!("Connected:poll_next");
        'outer: loop {
            trace4!("{selfname}  loop");
            let tsnow = Instant::now();
            let mut self2 = self.as_mut().get_mut();
            let mut hpp = HaveProgressPending::new();
            match &mut self2.state {
                State::Done => {}
                _ => {
                    trace4!(
                        "{selfname}  PROTOWRAP  self2.inp_buf.len() {n}",
                        n = self2.inp_buf.len()
                    );
                    match Pin::new(&mut self2.protowrap).poll_outbound(cx) {
                        Ready(Some(x)) => match x {
                            Ok(()) => {
                                hpp.mark_progress();
                            }
                            Err(e) => break Ready(Some(Err(e.into()))),
                        },
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    let mut i = 0;
                    loop {
                        i += 1;
                        break if i > LOOP_MAX_PROTOWRAP_POLL {
                        } else if self2.inp_buf.is_space() {
                            match Pin::new(&mut self2).protowrap.poll_next_unpin(cx) {
                                Ready(Some(Ok(x))) => {
                                    hpp.mark_progress();
                                    match x {
                                        CaItem::Msg(x) => {
                                            trace4!("{selfname}  PROTOWRAP  Msg");
                                            self2.inp_buf.push_back_force(x);
                                        }
                                        CaItem::Empty => {
                                            trace2!("{selfname}  PROTOWRAP  Empty");
                                        }
                                    }
                                }
                                Ready(Some(Err(e))) => {
                                    hpp.mark_progress();
                                    debug!("{selfname}  PROTOWRAP  error  {e}");
                                    self2.goto_state_done();
                                    break 'outer Ready(Some(Err(e.into())));
                                }
                                Ready(None) => {
                                    trace!("{selfname}  PROTOWRAP  Done");
                                }
                                Pending => {
                                    trace4!("{selfname}  PROTOWRAP  Pending");
                                    hpp.mark_pending();
                                }
                            }
                        } else {
                            trace_blocked!("{selfname}  SKIP  protowrap.poll_next_unpin  BLOCKED BY inp_buf");
                        };
                    }
                }
            }
            match &mut self2.state {
                State::Init(..) => {
                    let stn = Handshake::new(tsnow, self.addr.clone());
                    self.state = State::Handshake(stn);
                    hpp.mark_progress();
                }
                State::Handshake(st1) => {
                    if let Some(x) = self2.inp_buf.pop_front() {
                        match st1.inp_push_try(x, cx) {
                            asynbuf::PushRes::First => {
                                hpp.mark_progress();
                            }
                            asynbuf::PushRes::Done => {
                                hpp.mark_progress();
                            }
                            asynbuf::PushRes::Full(x) => {
                                self2.inp_buf.push_front(x);
                            }
                        }
                    } else {
                        // TODO metrics
                    }
                    if self2.protowrap.is_space() {
                        match st1.poll_next_unpin(cx) {
                            Ready(Some(x)) => match x {
                                Ok(x) => match x {
                                    crate::ca::conn2::conn::handshake::Item::CaMsg(x) => {
                                        hpp.mark_progress();
                                        self2.protowrap.push_back_or_drop(x);
                                    }
                                    crate::ca::conn2::conn::handshake::Item::HandshakeDone => {
                                        trace!("HandshakeDone");
                                        hpp.mark_progress();
                                        let st1 = std::mem::replace(st1, st1.to_dummy());
                                        let (buf,) = st1.dismantle();
                                        if buf.len() != 0 {
                                            warn!("{selfname}  TODO recover any leftover CaMsg items {}", buf.len());
                                        }
                                        trace_transition!("ActiveCa::new");
                                        let stn = ActiveCa::new(self2.backend.clone(), tsnow, self2.addr);
                                        self.state = State::ActiveCa(stn);
                                    }
                                },
                                Err(e) => {
                                    trace!("Handshake:Error");
                                    self2.goto_state_done();
                                    hpp.mark_progress();
                                    break Ready(Some(Err(e.into())));
                                }
                            },
                            Ready(None) => {}
                            Pending => {
                                trace_pending!("Handshake");
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        trace_blocked!("{selfname}  SKIP Handshake::poll_next_unpin  BLOCKED BY proto out full");
                    }
                }
                State::ActiveCa(st1) => {
                    let mut i = 0;
                    loop {
                        i += 1;
                        break if i > LOOP_MAX_PASS_CMD {
                        } else if self2.inp_cmd_buf.len() != 0 {
                            if st1.inp_is_space() {
                                if let Some(x) = self2.inp_cmd_buf.pop_front() {
                                    if st1.inp_push_try(activeca::InpItem::Cmd(x), cx).is_fail() {
                                        self2.state = State::Done;
                                        break 'outer Ready(Some(Err(Error::Logic)));
                                    } else {
                                        hpp.mark_progress();
                                    }
                                } else {
                                    break;
                                }
                            } else {
                                trace_blocked!("{selfname}  SKIP ActiveCa cmd push  BLOCKED BY inp full");
                            }
                        };
                    }
                    let mut i = 0;
                    loop {
                        i += 1;
                        break if i > LOOP_MAX_PASS_INP {
                        } else if let Some(x) = self2.inp_buf.pop_front() {
                            match st1.inp_push_try(activeca::InpItem::CaMsg(x), cx) {
                                asynbuf::PushRes::First => {
                                    hpp.mark_progress();
                                }
                                asynbuf::PushRes::Done => {
                                    hpp.mark_progress();
                                }
                                asynbuf::PushRes::Full(x) => {
                                    if let activeca::InpItem::CaMsg(x) = x {
                                        self2.inp_buf.push_front(x);
                                    } else {
                                        break 'outer Ready(Some(Err(Error::Logic)));
                                    }
                                }
                            }
                        } else {
                            break;
                        };
                    }
                    if self2.protowrap.is_space() {
                        match st1.poll_next_unpin(cx) {
                            Ready(Some(x)) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(items) => {
                                        let items = items
                                            .into_iter()
                                            .filter_map(|item| match item.inner {
                                                activeca::ItemInner::ChannelInfoQuery(item2) => {
                                                    let item = ConnectedItem {
                                                        ts_create: item.ts_create,
                                                        inner: ItemInner::ChannelInfoQuery(item2),
                                                    };
                                                    Some(item)
                                                }
                                                activeca::ItemInner::TestValue(x) => {
                                                    let item = ConnectedItem {
                                                        ts_create: item.ts_create,
                                                        inner: ItemInner::TestValue(x),
                                                    };
                                                    Some(item)
                                                }
                                                activeca::ItemInner::LocalLog(x) => {
                                                    let item = ConnectedItem {
                                                        ts_create: item.ts_create,
                                                        inner: ItemInner::LocalLog(x),
                                                    };
                                                    Some(item)
                                                }
                                                activeca::ItemInner::ChannelEventValue(x) => {
                                                    let item = ConnectedItem {
                                                        ts_create: item.ts_create,
                                                        inner: ItemInner::ChannelEventValue(x),
                                                    };
                                                    Some(item)
                                                }
                                                activeca::ItemInner::ProtoOut(x) => {
                                                    self2.protowrap.push_back_force(x);
                                                    None
                                                }
                                                activeca::ItemInner::ChannelTrace(x) => {
                                                    let item = ConnectedItem {
                                                        ts_create: item.ts_create,
                                                        inner: ItemInner::ChannelTrace(x),
                                                    };
                                                    Some(item)
                                                }
                                            })
                                            .collect::<VecDeque<_>>();
                                        break Ready(Some(Ok(AsynBuf::from_deque(items))));
                                    }
                                    Err(e) => {
                                        trace!("ActiveCa:Error");
                                        self2.goto_state_done();
                                        break Ready(Some(Err(e.into())));
                                    }
                                }
                            }
                            Ready(None) => {
                                trace!("ActiveCa:Done");
                                hpp.mark_progress();
                                self2.goto_state_done();
                            }
                            Pending => {
                                trace_pending!("ActiveCa");
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        warn!("{selfname}  SKIP ActiveCa::poll_next_unpin  BLOCKED BY proto out no space");
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
}
