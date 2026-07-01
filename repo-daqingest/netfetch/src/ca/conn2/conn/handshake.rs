use crate::asynbuf;
use crate::asynchan;
use crate::ca::conn2::conn::activeca::CaCommand;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use futures::Stream;
use futures::StreamExt;
use netpod::hpp::HaveProgressPending;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Handshake"),
    enum variants {
        IO(#[from] std::io::Error),
        ProtoTxClosed,
        EpicsVersion(u16),
    },
);

#[derive(Debug)]
pub enum Item {
    CaMsg(CaMsg),
    HandshakeDone,
}

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
    inp_buf: asynbuf::AsynBuf<CaMsg>,
}

impl Handshake {
    pub fn new(tsnow: Instant, addr: SocketAddrV4) -> Self {
        Self {
            tsbeg: tsnow,
            addr,
            state: State::new(),
            inp_buf: asynbuf::AsynBuf::new(16),
        }
    }

    pub fn dismantle(self) -> (asynbuf::AsynBuf<CaMsg>,) {
        (self.inp_buf,)
    }

    pub fn to_dummy(&self) -> Self {
        Self {
            tsbeg: self.tsbeg.clone(),
            addr: self.addr.clone(),
            state: State::Done,
            inp_buf: asynbuf::AsynBuf::new(16),
        }
    }

    pub fn inp_push_try(&mut self, item: CaMsg, cx: &mut Context<'_>) -> asynbuf::PushRes<CaMsg> {
        let self2 = self;
        let v = &mut self2.inp_buf;
        v.push_back(item)
    }
}

impl Stream for Handshake {
    type Item = Result<Item, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        trace!("poll_next");
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::HelloSend(st1) => {
                    if let Some(msg) = st1.msgs.pop_front() {
                        break Ready(Some(Ok(Item::CaMsg(msg))));
                    } else {
                        trace!("Tx:AllSent");
                        hpp.mark_progress();
                        self.state = State::HelloRecv;
                    }
                }
                State::HelloRecv => {
                    if let Some(item) = self.inp_buf.pop_front() {
                        trace!("Rx:Ready:Item:{item:?}");
                        match &item.ty {
                            CaMsgTy::VersionRes(n) => {
                                let n = *n;
                                if n < 12 || n > 13 {
                                    error!("unexpected channel access version {} from {}", n, self.addr);
                                    hpp.have_progress();
                                    self.state = State::Done;
                                    break Ready(Some(Err(Error::EpicsVersion(n))));
                                } else {
                                    hpp.have_progress();
                                    if n != 13 {
                                        warn!("received peer channel access version {} from {}", n, self.addr);
                                    }
                                    self.state = State::Done;
                                    break Ready(Some(Ok(Item::HandshakeDone)));
                                }
                            }
                            CaMsgTy::CreateChanRes(k) => {
                                hpp.have_progress();
                                warn!("got unexpected {:?}", k);
                            }
                            CaMsgTy::AccessRightsRes(k) => {
                                hpp.have_progress();
                                warn!("got unexpected {:?}", k);
                            }
                            _ => {
                                hpp.have_progress();
                                warn!("got some other unhandled message: {item:?}");
                            }
                        }
                    } else {
                    }
                }
                State::Done => {}
            };
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}
