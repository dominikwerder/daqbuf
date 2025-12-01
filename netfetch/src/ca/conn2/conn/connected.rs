use super::super::synchan;
use super::handshake::Handshake;
use crate::ca::conn2::progpend::HaveProgressPending;
use crate::ca::conn2::protowrap;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
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
use tokio::time::error::Elapsed;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Connected"),
    enum variants {
        Timeout,
        IO(#[from] std::io::Error),
        Protowrap(#[from] protowrap::Error),
        Handshake(#[from] super::handshake::Error),
    },
);

#[derive(Debug)]
enum State {
    Handshake(Handshake),
    Done,
}

#[derive(Debug)]
pub struct Connected {
    tsbeg: Instant,
    addr: SocketAddrV4,
    protowrap: protowrap::ProtoPusher,
    state: State,
    inp_buf: VecDeque<CaItem>,
    inp_tx_main: synchan::Sender<CaItem>,
    inp_tx_main_sending: Option<synchan::Sending<'static, CaItem>>,
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
        let (inp_tx, inp_rx) = synchan::bounded(16);
        let (out_tx, out_rx) = async_channel::bounded(16);
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
            state: State::Handshake(Handshake::new(inp_rx, out_tx, tsnow)),
            inp_buf: VecDeque::with_capacity(32),
            inp_tx_main: inp_tx,
            inp_tx_main_sending: None,
        }
    }
}

impl Future for Connected {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        trace!("poll_next");
        if false {
            // check whether this can be useful or not
            self.inp_tx_main.set_waker(cx.waker());
        }
        loop {
            let mut self2 = self.as_mut().get_mut();
            let mut hpp = HaveProgressPending::new();
            if self2.inp_buf.len() < self2.inp_buf.capacity() {
                match Pin::new(&mut self2).protowrap.poll_next_unpin(cx) {
                    Ready(Some(Ok(x))) => {
                        hpp.have_progress();
                        self2.inp_buf.push_back(x);
                    }
                    Ready(Some(Err(e))) => {
                        hpp.have_progress();
                        self2.state = State::Done;
                        break Ready(Err(e.into()));
                    }
                    Ready(None) => {}
                    Pending => {
                        hpp.have_pending();
                    }
                }
            }

            if let Some(send) = &mut self2.inp_tx_main_sending {
                match send.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        trace!("Connected:InputMain:Send:Poll:Ok");
                        self2.inp_tx_main_sending = None;
                        hpp.have_progress();
                    }
                    Ready(Err(_item)) => {
                        trace!("Connected:InputMain:Send:Poll:Err");
                        self2.inp_tx_main_sending = None;
                        trace!("Connected:Input:SendError");
                        hpp.have_progress();
                    }
                    Pending => {
                        trace!("Connected:InputMain:Send:Poll:Pending");
                        hpp.have_pending();
                    }
                }
            } else {
                if let Some(item) = self2.inp_buf.pop_front() {
                    trace!("Connected:InputMain:Send:Make");
                    let tx: *mut _ = if false {
                        todo!("choose correct tx")
                    } else {
                        &mut self2.inp_tx_main
                    };
                    // SAFETY self-referential
                    let tx = unsafe { &mut *tx };
                    self2.inp_tx_main_sending = Some(tx.send(item));
                    hpp.have_progress();
                }
            }

            todo!("combine handshake with hpp");

            break match &mut self2.state {
                State::Handshake(st1) => match st1.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        trace!("Handshake:Done");
                        self.state = State::Done;
                        continue;
                    }
                    Ready(Err(e)) => {
                        trace!("Handshake:Error");
                        Ready(Err(e.into()))
                    }
                    Pending => {
                        trace!("Handshake:Pending");
                        Pending
                    }
                },
                State::Done => Ready(Ok(())),
            };
        }
    }
}
