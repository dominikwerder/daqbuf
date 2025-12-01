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

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Connected"),
    enum variants {
        IO(#[from] std::io::Error),
    },
);

#[derive(Debug)]
struct HelloSend {
    msgs: VecDeque<CaMsg>,
}

impl HelloSend {
    fn new() -> Self {
        let tsnow = Instant::now();
        let mut msgs = VecDeque::new();
        let hostname = "data-api.psi.ch".into();
        let msg = CaMsg::from_ty_ts(CaMsgTy::Version, tsnow);
        msgs.push_back(msg);
        let msg = CaMsg::from_ty_ts(CaMsgTy::ClientName, tsnow);
        msgs.push_back(msg);
        let msg = CaMsg::from_ty_ts(CaMsgTy::HostName(hostname), tsnow);
        msgs.push_back(msg);
        Self { msgs }
    }
}

#[derive(Debug)]
enum State {
    HelloSend(HelloSend),
    HelloRecv,
    Done,
}

impl State {
    fn new() -> Self {
        Self::HelloSend(HelloSend::new())
    }
}

#[derive(Debug)]
pub struct Handshake {
    tsbeg: Instant,
    state: State,
    tx: async_channel::Sender<CaMsg>,
    rx: synchan::Receiver<CaItem>,
}

impl Handshake {
    pub fn new(rx: synchan::Receiver<CaItem>, tx: async_channel::Sender<CaMsg>, tsnow: Instant) -> Self {
        Self {
            tsbeg: tsnow,
            state: State::new(),
            tx,
            rx,
        }
    }

    pub fn dismantle(self) -> (synchan::Receiver<CaItem>,) {
        (self.rx,)
    }
}

impl Future for Handshake {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        trace!("poll_next");
        loop {
            let self2 = self.as_mut().get_mut();
            break match &mut self2.state {
                State::HelloSend(st1) => {
                    if let Some(msg) = st1.msgs.pop_front() {
                        match self2.tx.try_send(msg) {
                            Ok(()) => {
                                trace!("Tx:Sent");
                                continue;
                            }
                            Err(e) => match e {
                                async_channel::TrySendError::Full(e) => {
                                    trace!("Tx:Pending");
                                    st1.msgs.push_front(e);
                                    Pending
                                }
                                async_channel::TrySendError::Closed(_) => {
                                    trace!("Tx:Closed");
                                    panic!("todo")
                                }
                            },
                        }
                    } else {
                        trace!("Tx:AllSent");
                        self.state = State::HelloRecv;
                        continue;
                    }
                }
                State::HelloRecv => {
                    break match self.rx.poll_unpin(cx) {
                        Ready(Ok(item)) => {
                            trace!("Rx:Ready:Item:{item:?}");
                            continue;
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
