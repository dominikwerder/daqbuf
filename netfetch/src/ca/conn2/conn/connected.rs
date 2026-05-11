use super::handshake::Handshake;
use crate::asynchan;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::activeca;
use crate::ca::conn2::conn::activeca::ActiveCa;
use crate::ca::conn2::conn::ctchan::CtChan;
use crate::ca::conn2::locallog;
use crate::ca::conn2::protowrap;
use crate::ca::progpend::HaveProgressPending;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use netpod::futdbg::FutDbgBox;
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
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "Connected"),
    enum variants {
        IO(#[from] std::io::Error),
        Protowrap(#[from] protowrap::Error),
        Handshake(#[from] super::handshake::Error),
        ActiveCa(#[from] super::activeca::Error),
        NoProgressNoPending,
        ProtoOutputClosed,
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
}

#[derive(Debug)]
pub struct ConnectedItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

#[derive(Debug)]
pub struct StatusChannel {
    pub name: String,
    pub test_monitor_recv_cnt: u64,
}

#[derive(Debug)]
pub struct StatusChannels {
    pub status_channels: Vec<StatusChannel>,
}

#[derive(Debug)]
pub enum StatusInfoState {
    Init,
    Handshake,
    ActiveCa(activeca::StatusInfo),
    Done,
}

#[derive(Debug)]
pub struct StatusInfo {
    pub status: StatusInfoState,
}

#[derive(Debug)]
pub struct Connected {
    backend: String,
    tsbeg: Instant,
    addr: SocketAddrV4,
    protowrap: protowrap::ProtoPusher,
    state: State,
    inp_buf: VecDeque<CaMsg>,
    msg_a_chan: CtChan<activeca::CaCommand>,
    mett: CaConnConnectedMetrics,
}

impl Connected {
    pub fn new(
        backend: String,
        tcp: TcpStream,
        addr: SocketAddrV4,
        tsnow: Instant,
        ca_cmd_rx: asynchan::Receiver<activeca::CaCommand>,
    ) -> Self {
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
            inp_buf: VecDeque::with_capacity(32),
            msg_a_chan: CtChan::new(),
            mett: CaConnConnectedMetrics::new(),
        }
    }

    pub fn status_info(&self) -> StatusInfo {
        match &self.state {
            State::Init(..) => StatusInfo {
                status: StatusInfoState::Init,
            },
            State::Handshake(..) => StatusInfo {
                status: StatusInfoState::Handshake,
            },
            State::ActiveCa(st) => StatusInfo {
                status: StatusInfoState::ActiveCa(st.status_info()),
            },
            State::Done => StatusInfo {
                status: StatusInfoState::Done,
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
}

impl Stream for Connected {
    type Item = Result<ConnectedItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfname = "Connected::poll_next";
        trace4!("Connected:poll_next");
        loop {
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
                    if self2.inp_buf.len() < self2.inp_buf.capacity() {
                        match Pin::new(&mut self2).protowrap.poll_next_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                hpp.mark_progress();
                                match x {
                                    CaItem::Msg(x) => {
                                        trace3!("{selfname}  PROTOWRAP  Msg");
                                        self2.inp_buf.push_back(x);
                                    }
                                    CaItem::Empty => {
                                        trace3!("{selfname}  PROTOWRAP  Empty");
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                hpp.mark_progress();
                                trace3!("{selfname}  PROTOWRAP  error  {e}");
                                self2.goto_state_done();
                                break Ready(Some(Err(e.into())));
                            }
                            Ready(None) => {
                                trace3!("{selfname}  PROTOWRAP  Done");
                            }
                            Pending => {
                                trace4!("{selfname}  PROTOWRAP  Pending");
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        warn!("{selfname}  SKIP  protowrap.poll_next_unpin  BLOCKED BY inp_buf");
                    }
                }
            }
            match &mut self2.state {
                State::Init(..) => {
                    let stn = Handshake::new(tsnow, self.addr.clone());
                    self.state = State::Handshake(stn);
                    hpp.mark_progress();
                }
                State::Handshake(st1) => match st1.poll_next_unpin(cx) {
                    Ready(Some(x)) => match x {
                        Ok(x) => match x {
                            crate::ca::conn2::conn::handshake::Item::CaMsg(ca_msg) => todo!(),
                            crate::ca::conn2::conn::handshake::Item::HandshakeDone => {
                                warn!("{selfname}  HandshakeDone");
                                let st1 = std::mem::replace(st1, st1.to_dummy());
                                let (buf,) = st1.dismantle();

                                warn!("{selfname}  TODO recover any leftover CaMsg items");
                                // TODO recover any leftover CaMsg items
                                // TODO Rewrite ActiveCa such that it also takes messages by push.

                                let stn = ActiveCa::new(self2.backend.clone(), tsnow, self2.addr);
                                self.state = State::ActiveCa(stn);
                                hpp.mark_progress();
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
                },
                State::ActiveCa(st1) => {
                    let ctchan = &mut self2.msg_a_chan;
                    if ctchan.has_space() {
                        match rx.poll_next_unpin(cx) {
                            Ready(Some(item)) => {
                                hpp.mark_progress();
                                // We checked for space before.
                                // TODO add api for reserved slot.
                                #[allow(unused)]
                                ctchan.poll_send_unpin(item, cx);
                            }
                            Ready(None) => {}
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        warn!("{selfname}  SKIP  CtChan no space");
                    }

                    error!("TODO  poll only when we have buffer");
                    netpod::todoval();

                    match st1.poll_next_unpin(&mut self2.msg_a_chan, cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(item) => match item.inner {
                                    activeca::ItemInner::ChannelInfoQuery(item2) => {
                                        let item = ConnectedItem {
                                            ts_create: item.ts_create,
                                            inner: ItemInner::ChannelInfoQuery(item2),
                                        };
                                        break Ready(Some(Ok(item)));
                                    }
                                    activeca::ItemInner::TestValue(x) => {
                                        let item = ConnectedItem {
                                            ts_create: item.ts_create,
                                            inner: ItemInner::TestValue(x),
                                        };
                                        break Ready(Some(Ok(item)));
                                    }
                                    activeca::ItemInner::LocalLog(x) => {
                                        let item = ConnectedItem {
                                            ts_create: item.ts_create,
                                            inner: ItemInner::LocalLog(x),
                                        };
                                        break Ready(Some(Ok(item)));
                                    }
                                    activeca::ItemInner::ChannelEventValue(x) => {
                                        let item = ConnectedItem {
                                            ts_create: item.ts_create,
                                            inner: ItemInner::ChannelEventValue(x),
                                        };
                                        break Ready(Some(Ok(item)));
                                    }
                                    activeca::ItemInner::ProtoOut(x) => {
                                        error!("TODO  put proto item in buffer");
                                        netpod::todoval();
                                    }
                                },
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
