pub(super) const INP_BUF_CAP: usize = 64;

use super::fetchmpx::Fetchmpx;
use crate::asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler::fetchmpx;
use crate::ca::conn2::locallog;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use dbpg::seriesbychannel::ChannelInfoResult;
use futures::Stream;
use futures::StreamExt;
use netpod::ScalarType;
use netpod::Shape;
use netpod::channelstatus::ChannelStatus;
use series::SeriesId;
use stats::mett::ChannelHandlerMetrics;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

macro_rules! todo_shutdown { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! debug_shutdown { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

fn _keep() {
    info!("");
    trace2!("");
}

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
    CaMsgOutSubid(CaMsg, Instant),
    SubidRemove(Cid),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelStatus(ChannelStatus),
    ChannelEventValue(ChannelEventValue),
}

#[derive(Debug)]
enum State {
    Normal(Fetchmpx),
    Done,
}

impl State {
    fn str(&self) -> &str {
        match self {
            State::Normal(..) => "Normal",
            State::Done => "Done",
        }
    }
}

#[derive(Debug)]
pub struct Running {
    state: State,
    cid: Cid,
    sid: Sid,
    chi: ChannelInfoResult,
    removing: bool,
    chan_close_ack: bool,
    outbuf: VecDeque<CaMsg>,
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
}

impl Running {
    pub fn new(
        cid: Cid,
        sid: Sid,
        scalar_type: ScalarType,
        shape: Shape,
        ca_dbr_ty: CaDbrTy,
        chi: ChannelInfoResult,
        chconf: ChannelConfig,
    ) -> Self {
        Self {
            state: State::Normal(Fetchmpx::new(
                chi.series.to_series(),
                cid.clone(),
                sid.clone(),
                scalar_type.clone(),
                shape.clone(),
                ca_dbr_ty.clone(),
                chconf,
            )),
            cid,
            sid,
            chi,
            removing: false,
            chan_close_ack: false,
            outbuf: VecDeque::new(),
            inp_buf: VecDeque::with_capacity(INP_BUF_CAP),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
        }
    }

    pub fn sid(&self) -> Sid {
        self.sid.clone()
    }

    pub fn trigger_remove(&mut self) {
        todo_shutdown!("TODO set up teardown");
        self.removing = true;
        match &mut self.state {
            State::Normal(x) => {
                x.trigger_remove();
            }
            State::Done => {}
        }
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
    }

    pub fn handle_channel_handler_cmd(&mut self, cmd: serde_json::Value) -> serde_json::Value {
        use serde_json::json;
        match &mut self.state {
            State::Normal(st) => st.handle_channel_handler_cmd(cmd),
            State::Done => json!({
                "error": format!("Running  {}", self.state.str()),
            }),
        }
    }

    pub fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
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

    fn poll_inp_dispatch(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_inp_dispatch";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal(st2) => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        trace3!("{selfname}  have item  {item:?}");
                        let to_mpx = match &item.msg.ty {
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
                            match &item.msg.ty {
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
            match &self.state {
                State::Done => {}
                _ => match self.as_mut().poll_inp_dispatch(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(()) => {}
                            Err(e) => {
                                error!("TODO handle error {e}");
                                self.state = State::Done;
                            }
                        }
                    }
                    Ready(None) => {
                        error!("TODO even on input abort, continue with clean shutdown");
                        hpp.mark_progress();
                        self.state = State::Done;
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
            }
            match &mut self.state {
                State::Normal(fetchmpx) => match fetchmpx.poll_next_unpin(cx) {
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
                                fetchmpx::FetchmpxItem::CaMsgOutSubid(msg, tscmd) => {
                                    let g = RunningItem::CaMsgOutSubid(msg, tscmd);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::SubidRemove(cid) => {
                                    let g = RunningItem::SubidRemove(cid);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::TestValue(x) => {
                                    let g = RunningItem::TestValue(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::LocalLog(x) => {
                                    let g = RunningItem::LocalLog(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::ChannelStatus(x) => {
                                    let g = RunningItem::ChannelStatus(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::ChannelEventValue(x) => {
                                    let g = RunningItem::ChannelEventValue(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::InputDone => {
                                    info!("got FetchmpxItem::InputDone  but that's just a notice");
                                }
                            }
                        }
                        Err(e) => {
                            error!("TODO handle error {e}");
                            self.state = State::Done;
                            hpp.mark_progress();
                        }
                    },
                    Ready(None) => {
                        hpp.mark_progress();
                        debug_shutdown!("Done");
                        self.state = State::Done;
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
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
