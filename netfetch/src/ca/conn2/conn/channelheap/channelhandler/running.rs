use super::super::super::channelheap;
use super::fetchmpx::Fetchmpx;
use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Ioid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::SubidOwned;
use crate::ca::conn2::conn::channelheap::channelhandler::fetchmpx;
use crate::ca::conn2::timeoutable;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use channelheap::ChHeapCmd;
use channelheap::channelhandler::ChannelHandlerItem;
use channelheap::channelhandler::ItemInner;
use dbpg::seriesbychannel::ChannelInfoResult;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryFutureExt;
use netpod::ScalarType;
use netpod::Shape;
use stats::mett::ChannelHandlerMetrics;
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
    name(Error, "ChannelHandlerRunning"),
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
pub enum RunningItem {
    CaMsgOut(CaMsg),
    CaMsgOutIoid(CaMsg, Sid, Instant),
    ScyllaWrite,
}

#[derive(Debug)]
enum State {
    Normal(Fetchmpx),
    Done,
}

#[derive(Debug)]
pub struct Running {
    state: State,
    sid: Sid,
    chi: ChannelInfoResult,
    removing: bool,
    chan_close_ack: bool,
    outbuf: VecDeque<CaMsg>,
    remove_done_tx: Option<asynchan::Sender<u32>>,
    inp_buf: VecDeque<CaMsg>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
}

impl Running {
    pub fn new(sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy, chi: ChannelInfoResult) -> Self {
        Self {
            state: State::Normal(Fetchmpx::new(
                sid.clone(),
                scalar_type.clone(),
                shape.clone(),
                ca_dbr_ty.clone(),
            )),
            sid,
            chi,
            removing: false,
            chan_close_ack: false,
            outbuf: VecDeque::new(),
            remove_done_tx: None,
            inp_buf: VecDeque::with_capacity(16),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
        }
    }

    pub fn sid(&self) -> Sid {
        self.sid.clone()
    }

    pub fn trigger_remove(&mut self, done_tx: asynchan::Sender<u32>) {
        error!("TODO set up teardown, signal via done_tx");
        return;
        // TODO
        // Tear down, but ChannelHandler must do the channel close when we are Done.
        // add necessary commands to outbuf.
        // in poll loop, check for outbuf and poll emit.
        // handle:
        // CA_PROTO_EVENT_CANCEL leads to 0-size CA_PROTO_EVENT_ADD response
        // CA_PROTO_CLEAR_CHANNEL leads to CA_PROTO_CLEAR_CHANNEL response
        // and flag when those messages come in "removing" mode.
        // Otherwise, the IOC may also shut down of course.
        // TODO make sure the IOC disconnect triggers correct logic in ingest. (log!)
        // When we are in removing mode, and received all cleanup confirmations, then trigger state change.
        let sid = Sid::new(todo!());
        let cid = Cid::new(todo!());
        let tsnow = Instant::now();
        let item = CaMsg::from_ty_ts(
            proto::CaMsgTy::ChannelClose(proto::ChannelClose {
                sid: sid.to_u32(),
                cid: cid.to_u32(),
            }),
            tsnow,
        );
    }

    pub fn inp_push_try(&mut self, item: CaMsg) -> Option<CaMsg> {
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
        match &mut self.state {
            State::Normal(st) => st.inp_done(),
            State::Done => {}
        }
    }

    fn poll_inp_dispatch(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_inp_dispatch";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal(st2) => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        trace3!("{selfname}  have item  {item:?}");
                        let to_mpx = match &item.ty {
                            proto::CaMsgTy::EventAddRes(_) => true,
                            proto::CaMsgTy::EventAddResEmpty(_) => true,
                            proto::CaMsgTy::ReadNotifyRes(_) => true,
                            _ => false,
                        };
                        if to_mpx {
                            match st2.inp_push_try(item) {
                                Some(item) => {
                                    hpp.mark_pending();
                                    self2.inp_buf.push_front(item);
                                }
                                None => {
                                    hpp.mark_progress();
                                }
                            }
                        } else {
                            match &item.ty {
                                proto::CaMsgTy::ChannelCloseRes(item2) => {
                                    error!("{selfname}  TODO revisit the ChannelClose procedure");
                                    if self.removing {
                                        debug!(
                                            "{selfname}  NOTE ----  while removing  ---- {} {item2:?}",
                                            "ChannelCloseRes"
                                        );
                                        self.chan_close_ack = true;
                                    } else {
                                        error!("{selfname}  TODO not removing but got {} {item2:?}", "ChannelCloseRes");
                                        // TODO abort?
                                    }
                                    hpp.mark_progress();
                                }
                                _ => {
                                    hpp.mark_progress();
                                    error!("{selfname}  TODO unexpected message {item:?}");
                                    let e = Error::CreateMonitorUnexpectedMessage;
                                    break Ready(Some(Err(e)));
                                }
                            }
                        }
                    } else if self.inp_done {
                    } else {
                        hpp.mark_pending();
                    }
                }
                State::Done => {}
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

impl Stream for Running {
    type Item = Result<RunningItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let selfname = "Creating::poll_next";
        trace4!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match self.as_mut().poll_inp_dispatch(cx) {
                Ready(Some(x)) => match x {
                    Ok(()) => {}
                    Err(e) => {
                        error!("TODO handle error {e}");
                        self.state = State::Done;
                        hpp.mark_progress();
                    }
                },
                Ready(None) => {
                    error!("TODO even on input abort, continue with clean shutdown");
                    hpp.mark_progress();
                    self.state = State::Done;
                }
                Pending => {
                    hpp.mark_pending();
                }
            }
            match &mut self.state {
                State::Normal(fetchmpx) => {
                    //
                    match fetchmpx.poll_next_unpin(cx) {
                        Ready(Some(x)) => match x {
                            Ok(x) => {
                                hpp.mark_progress();
                                match x {
                                    fetchmpx::FetchmpxItem::CaMsgOut(msg) => {
                                        let g = RunningItem::CaMsgOut(msg);
                                        break Ready(Some(Ok(g)));
                                    }
                                    fetchmpx::FetchmpxItem::CaMsgOutIoid(msg, sid, tscmd) => {
                                        let g = RunningItem::CaMsgOutIoid(msg, sid, tscmd);
                                        break Ready(Some(Ok(g)));
                                    }
                                    fetchmpx::FetchmpxItem::ScyllaWrite => {
                                        error!("TODO ScyllaWrite");
                                    }
                                }
                            }
                            Err(e) => {
                                error!("TODO handle error {e}");
                                self.state = State::Done;
                                hpp.mark_progress();
                            }
                        },
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Done => {}
            }
            // {
            //     let pres = FetchMethodPollRes {
            //         fetch_data: &mut self.fetch_data,
            //         conf: &self.conf,
            //         cid: self.cid.to_cid(),
            //         sid: self.sid.clone(),
            //         mett: &mut self.mett,
            //     };
            //     match self.fetch_method.poll_next_unpin(pres, cx) {
            //         Ready(Some(x)) => {
            //             hpp.mark_progress();
            //             match x {
            //                 Ok(x) => match x {
            //                     FetchMethodPollOutput::None => {}
            //                     FetchMethodPollOutput::CallbackOnRunning(cb, mut items) => {
            //                         cb(st2);
            //                         if items.len() > 1 {
            //                             self2.outbuf.extend(items);
            //                         } else if let Some(item) = items.pop() {
            //                             break Ready(Some(Ok(item)));
            //                         } else {
            //                         }
            //                     }
            //                 },
            //                 Err(e) => {
            //                     hpp.mark_progress();
            //                     self2.state = State::Done;
            //                     break Ready(Some(Err(e)));
            //                 }
            //             }
            //         }
            //         Ready(None) => {}
            //         Pending => {
            //             hpp.mark_pending();
            //         }
            //     }
            // }
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
