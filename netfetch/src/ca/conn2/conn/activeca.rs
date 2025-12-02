use crate::ca::conn2::synchan;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures_util::FutureExt;
use futures_util::Stream;
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

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Connected"),
    enum variants {
        IO(#[from] std::io::Error),
        ProtoTxClosed,
    },
);

#[derive(Debug)]
enum State {
    Running,
    Done,
}

impl State {
    fn new() -> Self {
        Self::Running
    }
}

#[derive(Debug)]
pub struct ActiveCa {
    tsbeg: Instant,
    addr: SocketAddrV4,
    state: State,
    tx: async_channel::Sender<CaMsg>,
    rx: synchan::Receiver<CaMsg>,
}

impl ActiveCa {
    pub fn new(
        rx: synchan::Receiver<CaMsg>,
        tx: async_channel::Sender<CaMsg>,
        tsnow: Instant,
        addr: SocketAddrV4,
    ) -> Self {
        Self {
            tsbeg: tsnow,
            addr,
            state: State::new(),
            tx,
            rx,
        }
    }

    pub fn dismantle(self) -> (synchan::Receiver<CaMsg>,) {
        (self.rx,)
    }
}

impl Future for ActiveCa {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        trace!("poll_next");
        loop {
            let self2 = self.as_mut().get_mut();
            break match &mut self2.state {
                State::Running => {
                    break match self.rx.poll_unpin(cx) {
                        Ready(Ok(item)) => {
                            trace!("Rx:Ready:Item:{item:?}");
                            match &item.ty {
                                _ => {
                                    warn!("got some other unhandled message: {item:?}");
                                    Ready(Ok(()))
                                }
                            }
                        }
                        Ready(Err(_)) => {
                            trace!("Rx:Done");
                            Ready(Ok(()))
                        }
                        Pending => {
                            trace!("Rx:Pending");
                            Pending
                        }
                    };
                }
                State::Done => {
                    trace!("Done");
                    Ready(Ok(()))
                }
            };
        }
    }
}
