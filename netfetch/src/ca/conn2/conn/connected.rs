use super::handshake::Handshake;
use crate::ca::conn2::asynchan;
use crate::ca::conn2::conn::activeca;
use crate::ca::conn2::conn::activeca::ActiveCa;
use crate::ca::conn2::conn::ctchan::CtChan;
use crate::ca::conn2::protowrap;
use crate::ca::progpend::HaveProgressPending;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
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
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
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
    Init(asynchan::Receiver<CaMsg>, asynchan::Receiver<activeca::CaCommand>),
    Handshake(Handshake),
    ActiveCa(ActiveCa, asynchan::Receiver<activeca::CaCommand>),
    Done,
}

#[derive(Debug)]
pub enum ItemInner {
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    ScyllaWrite,
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
    out_tx: asynchan::Sender<CaMsg>,
    inp_buf: VecDeque<CaMsg>,
    inp_tx_main: asynchan::Sender<CaMsg>,
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
        let (inp_tx, inp_rx) = asynchan::bounded(16, "Connected-inp");
        let (out_tx, out_rx) = asynchan::bounded(16, "Connected-out");
        let protowrap = protowrap::ProtoPusher::new(proto, out_rx);

        // TODO poll the protowrap input and distribute to sub state.
        // Only poll the proto if I have space in the buffer.

        // But then: when and how to deliver the input?
        // There are N channels, one for each Cid (which can be General).

        Self {
            backend,
            tsbeg: tsnow,
            addr,
            protowrap,
            state: State::Init(inp_rx, ca_cmd_rx),
            out_tx,
            inp_buf: VecDeque::with_capacity(32),
            inp_tx_main: inp_tx,
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
            State::ActiveCa(st, _rx) => StatusInfo {
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
            State::Handshake(st) => {
                // TODO
                CaConnConnectedMetrics::new()
            }
            State::ActiveCa(st, _) => st.mett_take(),
            State::Done => {
                // TODO
                CaConnConnectedMetrics::new()
            }
        }
    }

    fn goto_state_done(&mut self) {
        {
            let (tx, _rx) = asynchan::bounded(16, "Connected-CaMsg-dummy1");
            self.inp_tx_main = tx;
        }
        {
            let (tx, _rx) = asynchan::bounded(16, "Connected-CaMsg-dummy2");
            self.out_tx = tx;
        }
        self.protowrap.close();
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
}

impl Stream for Connected {
    type Item = Result<ConnectedItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        trace4!("Connected:poll_next");
        loop {
            trace4!("Connected:poll_next  loop");
            let tsnow = Instant::now();
            let mut self2 = self.as_mut().get_mut();
            let mut hpp = HaveProgressPending::new();
            match &mut self2.state {
                State::Done => {}
                _ => {
                    trace4!("PROTOWRAP  self2.inp_buf.len() {n}", n = self2.inp_buf.len());
                    if self2.inp_buf.len() < self2.inp_buf.capacity() {
                        match Pin::new(&mut self2).protowrap.poll_next_unpin(cx) {
                            Ready(Some(Ok(x))) => {
                                hpp.mark_progress();
                                match x {
                                    CaItem::Msg(x) => {
                                        trace3!("PROTOWRAP  Msg");
                                        self2.inp_buf.push_back(x);
                                    }
                                    CaItem::Empty => {
                                        trace3!("PROTOWRAP  Empty");
                                    }
                                }
                            }
                            Ready(Some(Err(e))) => {
                                hpp.mark_progress();
                                trace3!("PROTOWRAP  error  {e}");
                                self2.goto_state_done();
                                break Ready(Some(Err(e.into())));
                            }
                            Ready(None) => {
                                trace3!("PROTOWRAP  Done");
                            }
                            Pending => {
                                trace4!("PROTOWRAP  Pending");
                                hpp.mark_pending();
                            }
                        }
                    }
                    if let Some(item) = self2.inp_buf.pop_front() {
                        use asynchan::SendPoll;
                        use asynchan::SendPollError;
                        match self2.inp_tx_main.poll_send_unpin(item, cx) {
                            Ok(()) => {
                                hpp.mark_progress();
                            }
                            Err(e) => match e {
                                SendPollError::Full(item) => {
                                    hpp.mark_pending();
                                    self2.inp_buf.push_front(item);
                                }
                                SendPollError::Closed(item) => {
                                    hpp.mark_progress();
                                    self2.inp_buf.push_front(item);
                                    self2.goto_state_done();
                                    break Ready(Some(Err(Error::ProtoOutputClosed)));
                                }
                            },
                        }
                    }
                }
            }
            match &mut self2.state {
                State::Init(st1, ca_cmd_rx) => {
                    let inp_rx = std::mem::replace(st1, asynchan::bounded(1, "Connected-dummy").1);
                    let ca_cmd_rx = std::mem::replace(ca_cmd_rx, asynchan::bounded(1, "Connected-dummy-cacmd").1);
                    let stn = Handshake::new(inp_rx, self.out_tx.clone(), tsnow, self.addr.clone(), ca_cmd_rx);
                    self.state = State::Handshake(stn);
                    if false {
                        // check whether this can be useful or not
                        // self.inp_tx_main.set_waker(cx.waker());
                    }
                    hpp.mark_progress();
                }
                State::Handshake(st1) => match st1.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        trace!("Handshake:Done");
                        let ca_cmd_rx =
                            std::mem::replace(&mut st1.ca_cmd_rx, asynchan::bounded(1, "Connected-dummy-cacmd").1);
                        let st1 = std::mem::replace(st1, st1.to_dummy());
                        let tx = self2.out_tx.clone();
                        let (rx,) = st1.dismantle();
                        let stn = ActiveCa::new(self2.backend.clone(), rx, tx, tsnow, self2.addr, cx);
                        self.state = State::ActiveCa(stn, ca_cmd_rx);
                        hpp.mark_progress();
                    }
                    Ready(Err(e)) => {
                        trace!("Handshake:Error");
                        self2.goto_state_done();
                        hpp.mark_progress();
                        break Ready(Some(Err(e.into())));
                    }
                    Pending => {
                        trace_pending!("Handshake");
                        hpp.mark_pending();
                    }
                },
                State::ActiveCa(st1, rx) => {
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
                    }
                    match st1.poll_next_unpin(&mut self2.msg_a_chan, cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(item) => {
                                    let item = match item.inner {
                                        activeca::ItemInner::ChannelInfoQuery(item2) => ConnectedItem {
                                            ts_create: item.ts_create,
                                            inner: ItemInner::ChannelInfoQuery(item2),
                                        },
                                        activeca::ItemInner::ScyllaWrite => ConnectedItem {
                                            ts_create: item.ts_create,
                                            inner: ItemInner::ScyllaWrite,
                                        },
                                    };
                                    break Ready(Some(Ok(item)));
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
                }
                State::Done => {
                    error!("State::Done  {}  {}", hpp.have_progress(), hpp.have_pending());
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
