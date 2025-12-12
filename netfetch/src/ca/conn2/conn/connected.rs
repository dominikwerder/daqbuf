use super::handshake::Handshake;
use crate::ca::conn2::asynchan;
use crate::ca::conn2::conn::activeca;
use crate::ca::conn2::conn::activeca::ActiveCa;
use crate::ca::conn2::protowrap;
use crate::ca::progpend::HaveProgressPending;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
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
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
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
    Init(asynchan::Receiver<CaMsg>),
    Handshake(Handshake),
    ActiveCa(ActiveCa),
    Done,
}

#[derive(Debug)]
pub enum ItemInner {
    ScyllaWrite,
}

#[derive(Debug)]
pub struct ConnectedItem {
    // Only for performance measurement:
    ts_create: Instant,
    inner: ItemInner,
}

#[derive(Debug)]
pub struct Connected {
    tsbeg: Instant,
    addr: SocketAddrV4,
    protowrap: protowrap::ProtoPusher,
    state: State,
    out_tx: asynchan::Sender<CaMsg>,
    inp_buf: VecDeque<CaMsg>,
    inp_tx_main: asynchan::Sender<CaMsg>,
}

impl Connected {
    pub fn new(tcp: TcpStream, addr: SocketAddrV4, tsnow: Instant) -> Self {
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
        //

        Self {
            tsbeg: tsnow,
            addr,
            protowrap,
            state: State::Init(inp_rx),
            out_tx,
            inp_buf: VecDeque::with_capacity(32),
            inp_tx_main: inp_tx,
        }
    }
}

impl Stream for Connected {
    type Item = Result<ConnectedItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        trace4!("Connected:poll_next");
        loop {
            let tsnow = Instant::now();
            let mut self2 = self.as_mut();
            let mut hpp = HaveProgressPending::new();
            if self2.inp_buf.len() < self2.inp_buf.capacity() {
                match Pin::new(&mut self2).protowrap.poll_next_unpin(cx) {
                    Ready(Some(Ok(x))) => {
                        hpp.mark_progress();
                        match x {
                            CaItem::Msg(x) => {
                                self2.inp_buf.push_back(x);
                            }
                            CaItem::Empty => {}
                        }
                    }
                    Ready(Some(Err(e))) => {
                        hpp.mark_progress();
                        self2.state = State::Done;
                        break Ready(Some(Err(e.into())));
                    }
                    Ready(None) => {}
                    Pending => {
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
                            self2.inp_buf.push_front(item);
                            hpp.mark_pending();
                        }
                        SendPollError::Closed(item) => {
                            self2.inp_buf.push_front(item);
                            hpp.mark_progress();
                            self.state = State::Done;
                            break Ready(Some(Err(Error::ProtoOutputClosed)));
                        }
                    },
                }
            }
            match &mut self2.state {
                State::Init(st1) => {
                    let inp_rx = std::mem::replace(st1, asynchan::bounded(1, "Connected-dummy").1);
                    let stn = Handshake::new(inp_rx, self.out_tx.clone(), tsnow, self.addr.clone());
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
                        let st1 = std::mem::replace(st1, st1.to_dummy());
                        let tx = self2.out_tx.clone();
                        let (rx,) = st1.dismantle();
                        let stn = ActiveCa::new(rx, tx, tsnow, self2.addr, cx);
                        self.state = State::ActiveCa(stn);
                        hpp.mark_progress();
                    }
                    Ready(Err(e)) => {
                        trace!("Handshake:Error");
                        self.state = State::Done;
                        hpp.mark_progress();
                        break Ready(Some(Err(e.into())));
                    }
                    Pending => {
                        trace_pending!("Handshake");
                        hpp.mark_pending();
                    }
                },
                State::ActiveCa(st1) => match st1.poll_next_unpin(cx) {
                    Ready(Some(x)) => match x {
                        Ok(item) => {
                            trace!("ActiveCa:Ready");
                            error!("ActiveCa:Ready  TODO do something with item");
                            let item = match item.inner {
                                activeca::ItemInner::ScyllaWrite => ConnectedItem {
                                    ts_create: item.ts_create,
                                    inner: ItemInner::ScyllaWrite,
                                },
                            };
                            hpp.mark_progress();
                            break Ready(Some(Ok(item)));
                        }
                        Err(e) => {
                            trace!("ActiveCa:Error");
                            self.state = State::Done;
                            hpp.mark_progress();
                            break Ready(Some(Err(e.into())));
                        }
                    },
                    Ready(None) => {
                        trace!("ActiveCa:Done");
                        self.state = State::Done;
                        hpp.mark_progress();
                    }
                    Pending => {
                        trace_pending!("ActiveCa");
                        hpp.mark_pending();
                    }
                },
                State::Done => {}
            }
            break if hpp.have_progress() {
                trace!("HPP:Progress");
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
