use crate::ca::conn2::asynchan;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Handshake"),
    enum variants {
        IO(#[from] std::io::Error),
        ProtoTxClosed,
        EpicsVersion(u16),
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
    addr: SocketAddrV4,
    state: State,
    tx: asynchan::Sender<CaMsg>,
    rx: asynchan::Receiver<CaMsg>,
}

impl Handshake {
    pub fn new(rx: asynchan::Receiver<CaMsg>, tx: asynchan::Sender<CaMsg>, tsnow: Instant, addr: SocketAddrV4) -> Self {
        Self {
            tsbeg: tsnow,
            addr,
            state: State::new(),
            tx,
            rx,
        }
    }

    pub fn dismantle(self) -> (asynchan::Receiver<CaMsg>,) {
        (self.rx,)
    }

    pub fn to_dummy(&self) -> Self {
        Self {
            tsbeg: self.tsbeg.clone(),
            addr: self.addr.clone(),
            state: State::Done,
            tx: asynchan::bounded(1, "handshake-dummy-A").0,
            rx: asynchan::bounded(1, "handshake-dummy-B").1,
        }
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
                        use asynchan::SendPoll;
                        use asynchan::SendPollError;
                        match self2.tx.poll_send_unpin(msg, cx) {
                            Ok(()) => {
                                trace!("Tx:Sent");
                                continue;
                            }
                            Err(e) => match e {
                                SendPollError::Full(e) => {
                                    trace!("Tx:Pending");
                                    st1.msgs.push_front(e);
                                    Pending
                                }
                                SendPollError::Closed(_) => {
                                    trace!("Tx:Closed");
                                    self.state = State::Done;
                                    Ready(Err(Error::ProtoTxClosed))
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
                    break match self.rx.poll_next_unpin(cx) {
                        Ready(x) => match x {
                            Some(item) => {
                                trace!("Rx:Ready:Item:{item:?}");
                                match &item.ty {
                                    CaMsgTy::VersionRes(n) => {
                                        let n = *n;
                                        if n < 12 || n > 13 {
                                            error!("unexpected channel access version {} from {}", n, self.addr);
                                            self.state = State::Done;
                                            Ready(Err(Error::EpicsVersion(n)))
                                        } else {
                                            if n != 13 {
                                                warn!("received peer channel access version {} from {}", n, self.addr);
                                            }
                                            self.state = State::Done;
                                            Ready(Ok(()))
                                        }
                                    }
                                    CaMsgTy::CreateChanRes(k) => {
                                        warn!("got unexpected {:?}", k);
                                        Ready(Ok(()))
                                    }
                                    CaMsgTy::AccessRightsRes(k) => {
                                        warn!("got unexpected {:?}", k);
                                        Ready(Ok(()))
                                    }
                                    _ => {
                                        warn!("got some other unhandled message: {item:?}");
                                        Ready(Ok(()))
                                    }
                                }
                            }
                            None => {
                                trace!("Rx:Done");
                                Ready(Ok(()))
                            }
                        },
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
