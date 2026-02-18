use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::timeoutable;
use crate::ca::progpend::HaveProgressPending;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use futures::FutureExt;
use futures::Stream;
use futures::TryFutureExt;
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
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHandlerCreating"),
    enum variants {
        ProtoTxClosed,
        ProtoRxClosed,
        ChannelHandlerRxClosed,
        CreateMonitorUnexpectedMessage,
        Recv,
        Timeout,
        Logic,
        Register(#[from] dbpg::seriesbychannel::Error),
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
    CaMsgOut(CaMsg),
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    Done(
        (
            Sid,
            ScalarType,
            Shape,
            CaDbrTy,
            dbpg::seriesbychannel::ChannelInfoResult,
        ),
    ),
}

#[derive(Debug)]
enum State {
    CreateChanSend(VecDeque<CaMsg>, FutDbg<()>),
    CreateChanRecv(FutDbg<()>),
    SeriesIdRecv(
        FutDbg<(
            Result<Result<dbpg::seriesbychannel::ChannelInfoResult, dbpg::seriesbychannel::Error>, Error>,
            Sid,
            ScalarType,
            Shape,
            CaDbrTy,
        )>,
        FutDbg<()>,
    ),
    Done,
}

#[derive(Debug)]
pub struct Creating {
    cid: Cid,
    name: String,
    backend: String,
    state: State,
    removing: bool,
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
}

impl Creating {
    pub fn new(cid: Cid, name: String, backend: String) -> Self {
        let tsnow = Instant::now();
        let msg = CaMsg::from_ty_ts(
            proto::CaMsgTy::CreateChan(proto::CreateChan {
                cid: cid.to_u32(),
                channel: name.clone(),
            }),
            tsnow,
        );
        let msgs = VecDeque::from([msg]);
        let to = tokio::time::sleep(Duration::from_millis(8000));
        Self {
            cid,
            name,
            backend,
            state: State::CreateChanSend(msgs, to.box2()),
            removing: false,
            inp_buf: VecDeque::with_capacity(16),
            inp_done: false,
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

    pub fn poll_inp_push(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
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
        let selfname = "Creating::poll_next";
        trace4!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::CreateChanSend(msgs, to) => {
                    match to.poll_unpin(cx) {
                        Ready(()) => {
                            self2.state = State::Done;
                            break Ready(Some(Err(Error::Timeout)));
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    if let Some(item) = msgs.pop_front() {
                        let item = CreatingItem::CaMsgOut(item);
                        break Ready(Some(Ok(item)));
                    } else {
                        hpp.mark_progress();
                        let to = std::mem::replace(to, async {}.box2());
                        self2.state = State::CreateChanRecv(to);
                    }
                }
                State::CreateChanRecv(to) => {
                    match to.poll_unpin(cx) {
                        Ready(()) => {
                            self2.state = State::Done;
                            break Ready(Some(Err(Error::Timeout)));
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    if let Some(item) = self2.inp_buf.pop_front() {
                        hpp.mark_progress();
                        trace3!("CreateChanRecv  have item  {item:?}");
                        use proto::CaMsgTy;
                        match &item.msg.ty {
                            CaMsgTy::CreateChanRes(k) => {
                                trace!("CreateMonitor:CreateChanRes {k:?}");
                                if k.data_type > 6 {
                                    error!(
                                        "CreateChanRes with unexpected data_type {} count {}",
                                        k.data_type, k.data_count
                                    );
                                    let e = Error::CreateMonitorUnexpectedMessage;
                                    break Ready(Some(Err(e)));
                                }
                                // Ask for DBR_TIME_...
                                let ca_dbr_type = CaDbrTy::new(k.data_type + 14);
                                let scalar_type = match ScalarType::from_ca_id(k.data_type) {
                                    Ok(x) => x,
                                    Err(e) => {
                                        error!(
                                            "CreateChanRes with unexpected data_type {} count {}",
                                            k.data_type, k.data_count
                                        );
                                        let e = Error::CreateMonitorUnexpectedMessage;
                                        break Ready(Some(Err(e)));
                                    }
                                };
                                let shape = match Shape::from_ca_count(k.data_count) {
                                    Ok(x) => x,
                                    Err(e) => {
                                        error!(
                                            "CreateChanRes with unexpected data_type {} count {}",
                                            k.data_type, k.data_count
                                        );
                                        let e = Error::CreateMonitorUnexpectedMessage;
                                        break Ready(Some(Err(e)));
                                    }
                                };
                                let to = std::mem::replace(to, async {}.box2());
                                let sid = Sid::new(k.sid);
                                let (tx, rx) = async_channel::bounded(1);
                                let item = dbpg::seriesbychannel::ChannelInfoQuery {
                                    backend: self2.backend.clone(),
                                    channel: self2.name.clone(),
                                    kind: netpod::SeriesKind::ChannelData,
                                    scalar_type: scalar_type.clone(),
                                    shape: shape.clone(),
                                    tx: Box::pin(tx),
                                };
                                hpp.mark_progress();
                                let fut = async move {
                                    let chi = rx.recv().map_err(|e| Error::Recv).await;
                                    (chi, sid, scalar_type, shape, ca_dbr_type)
                                };
                                self.state = State::SeriesIdRecv(fut.box2(), to);
                                let item = CreatingItem::ChannelInfoQuery(item);
                                trace3!("--- EMIT --- {item:?}");
                                break Ready(Some(Ok(item)));
                            }
                            CaMsgTy::CreateChanFail(k) => {
                                trace!("CreateMonitor:CreateChanFail {k:?}");
                                // TODO
                                // Must cause a re-search of the channel
                                let e = Error::CreateMonitorUnexpectedMessage;
                                break Ready(Some(Err(e)));
                            }
                            CaMsgTy::AccessRightsRes(k) => {
                                trace!("CreateMonitor:AccessRightsRes {k:?}");
                            }
                            _ => {
                                trace!("channel_create: unexpected message {item:?}");
                                let e = Error::CreateMonitorUnexpectedMessage;
                                break Ready(Some(Err(e)));
                            }
                        }
                    } else if self.inp_done {
                        // TODO status event
                        hpp.mark_progress();
                        self.state = State::Done;
                    } else {
                        hpp.mark_pending();
                    }
                }
                State::SeriesIdRecv(rx, to) => {
                    trace3!("State::SeriesIdRecv  polling");
                    match to.poll_unpin(cx) {
                        Ready(()) => {
                            hpp.mark_progress();
                            self2.state = State::Done;
                            break Ready(Some(Err(Error::Timeout)));
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match rx.poll_unpin(cx) {
                        Ready((x, sid, scalar_type, shape, ca_dbr_type)) => {
                            hpp.mark_progress();
                            trace3!("received channel info {x:?}");
                            self2.state = State::Done;
                            match x {
                                Ok(x) => match x {
                                    Ok(x) => {
                                        let item = CreatingItem::Done((sid, scalar_type, shape, ca_dbr_type, x));
                                        break Ready(Some(Ok(item)));
                                    }
                                    Err(e) => {
                                        self2.state = State::Done;
                                        break Ready(Some(Err(e.into())));
                                    }
                                },
                                Err(e) => {
                                    self2.state = State::Done;
                                    break Ready(Some(Err(e)));
                                }
                            }
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Done => break Ready(Some(Err(Error::Logic))),
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
