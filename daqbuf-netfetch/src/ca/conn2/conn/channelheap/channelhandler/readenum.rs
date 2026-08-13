pub(super) const INP_BUF_CAP: usize = 64;

use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::locallog;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use ca_proto::ca::proto::ReadNotify;
use dbpg::seriesbychannel::ChannelInfoResult;
use futures::Stream;
use netpod::ScalarType;
use netpod::Shape;
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
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

macro_rules! todo_shutdown { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! debug_shutdown { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

fn _keep() {
    info!("");
}

autoerr::create_error_v1!(
    name(Error, "ChannelHandlerReadEnum"),
    enum variants {
        // ProtoTxClosed,
        ProtoRxClosed,
        ChannelHandlerRxClosed,
        Timeout,
        CreateMonitorUnexpectedMessage,
    },
);

#[derive(Debug)]
pub enum ReadEnumItem {
    CaMsgOutIoid(CaMsg, Sid, Instant),
    LocalLog(locallog::Entry),
}

#[derive(Debug)]
enum State {
    SendMsg(),
    WaitMsg(),
    Done,
}

impl State {
    fn str(&self) -> &str {
        match self {
            State::SendMsg(..) => "SendMsg",
            State::WaitMsg(..) => "WaitMsg",
            State::Done => "Done",
        }
    }
}

#[derive(Debug)]
pub struct ReadEnum {
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

impl ReadEnum {
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
            state: State::SendMsg(),
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
        todo_shutdown!("trigger_remove");
        self.removing = true;
        match &mut self.state {
            State::SendMsg(..) => {
                //
            }
            State::WaitMsg(..) => {
                //
            }
            State::Done => {}
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
    }

    fn poll_inp_dispatch(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_inp_dispatch";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Done => {}
                _ => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        match &item.msg.ty {
                            proto::CaMsgTy::ReadNotifyRes(x) => {
                                trace!("{selfname}  inp proto msg {item:?}");
                            }
                            _ => {
                                trace!("{selfname}  IGNORED inp proto msg {item:?}");
                            }
                        }
                    } else if self.inp_done {
                    } else {
                        hpp.mark_pending();
                    }
                }
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

impl Stream for ReadEnum {
    type Item = Result<ReadEnumItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let selfname = "ReadEnum::poll_next";
        trace4!("{selfname}");
        use Poll::*;
        loop {
            let tsnow = Instant::now();
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
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::SendMsg(..) => {
                    let dbr_gr_enum = 24;
                    let dbr_ctrl_enum = 31;
                    let msg = CaMsg::from_ty_ts(
                        CaMsgTy::ReadNotify(ReadNotify {
                            data_type: dbr_gr_enum,
                            data_count: 0,
                            sid: self2.sid.to_u32(),
                            ioid: 0,
                        }),
                        tsnow,
                    );
                    self2.state = State::WaitMsg();
                    break Ready(Some(Ok(ReadEnumItem::CaMsgOutIoid(msg, self2.sid.clone(), tsnow))));
                }
                State::WaitMsg(..) => {
                    trace!("..............   in WaitMsg");
                    hpp.mark_pending();
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
