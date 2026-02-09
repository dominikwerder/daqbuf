use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::timeoutable;
use crate::ca::progpend::HaveProgressPending;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use futures::FutureExt;
use futures::Stream;
use netpod::ScalarType;
use netpod::Shape;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHandler"),
    enum variants {
        ProtoTxClosed,
        ProtoRxClosed,
        ChannelHandlerRxClosed,
        CreateMonitorUnexpectedMessage,
        Recv,
        Timeout,
        Logic,
    },
);

impl From<asynchan::RecvError> for Error {
    fn from(_value: asynchan::RecvError) -> Self {
        Self::Recv
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(_value: async_channel::RecvError) -> Self {
        Self::Recv
    }
}

#[derive(Debug)]
pub enum CreatingItem {
    CaMsgOut(CaMsg, Cid),
    Done((Sid, ScalarType, Shape, CaDbrTy)),
}

#[derive(Debug)]
enum State {
    CreateChanSend(VecDeque<CaMsg>, FutDbg<()>),
    CreateChanRecv,
    Done,
}

#[derive(Debug)]
pub struct Creating {
    cid: Cid,
    state: State,
    removing: bool,
    inp_buf: VecDeque<CaMsg>,
    inp_done: bool,
    fut: Option<FutDbg<Result<(Sid, ScalarType, Shape, CaDbrTy, asynchan::Receiver<CaMsg>), Error>>>,
}

impl Creating {
    pub fn new(cid: Cid, name: String) -> Self {
        let tsnow = Instant::now();
        let msg = CaMsg::from_ty_ts(
            proto::CaMsgTy::CreateChan(proto::CreateChan {
                cid: cid.to_u32(),
                channel: name.clone(),
            }),
            tsnow,
        );
        let msgs = VecDeque::from([msg]);
        let to = tokio::time::sleep(Duration::from_millis(3000));
        Self {
            cid,
            state: State::CreateChanSend(msgs, to.box2()),
            removing: false,
            inp_buf: VecDeque::with_capacity(16),
            inp_done: false,
            fut: None,
        }
    }

    pub fn name_short(&self) -> &str {
        if self.removing {
            "Creating { removing: true }"
        } else {
            "Creating { }"
        }
    }

    pub fn set_removing(&mut self) {
        self.removing = true;
    }

    pub fn poll_inp_push(&mut self, item: CaMsg) -> Option<CaMsg> {
        let v = &mut self.inp_buf;
        if v.len() < v.capacity() {
            v.push_back(item);
            None
        } else {
            Some(item)
        }
    }

    pub fn inp_done(&mut self) {
        self.inp_done = true;
    }
}

impl Stream for Creating {
    // type Output = Result<(Sid, ScalarType, Shape, CaDbrTy, asynchan::Receiver<CaMsg>), Error>;
    type Item = Result<CreatingItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match &mut self.state {
                State::CreateChanSend(msgs, to) => {
                    //
                    match to.poll_unpin(cx) {
                        Ready(()) => {
                            self.state = State::Done;
                            break Ready(Some(Err(Error::Timeout)));
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    if let Some(item) = msgs.pop_front() {
                        break Ready(Some(Ok(CreatingItem::CaMsgOut(item, self.cid.clone()))));
                    }
                }
                State::CreateChanRecv => todo!(),
                State::Done => break Ready(Some(Err(Error::Logic))),
            };
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
