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
use serde_helper::ToSerde;
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
        ProtoRxUnexpected,
    },
);

#[derive(Debug)]
pub enum ReadEnumItem {
    CaMsgOutIoid(CaMsg, Sid, Instant),
    LocalLog(locallog::Entry),
    EnumStringSet(Sid, ScalarType, Shape, CaDbrTy, ChannelInfoResult, Vec<String>),
}

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub", serde(tag = "ty", content = "co"))]
enum State {
    SendMsg(),
    WaitMsg(#[to_serde(elapsed)] Instant),
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

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub")]
pub struct ReadEnum {
    #[to_serde(nest)]
    state: State,
    /// Set by `transition_state`, reported as time-in-state.
    #[to_serde(elapsed)]
    state_dt: Instant,
    cid: Cid,
    sid: Sid,
    scalar_type: ScalarType,
    shape: Shape,
    ca_dbr_ty: CaDbrTy,
    chi: ChannelInfoResult,
    removing: bool,
    chan_close_ack: bool,
    #[to_serde(len)]
    outbuf: VecDeque<CaMsg>,
    #[to_serde(len)]
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    #[to_serde(skip)]
    mett: ChannelHandlerMetrics,
}

impl ReadEnum {
    /// Single choke point for state changes so the time-in-state stamp can not drift.
    fn transition_state(&mut self, new: State) {
        self.state = new;
        self.state_dt = Instant::now();
    }

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
            state_dt: Instant::now(),
            cid,
            sid,
            scalar_type,
            shape,
            ca_dbr_ty,
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

    fn poll_inp_dispatch(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Option<ReadEnumItem>, Error>>> {
        let selfname = "poll_inp_dispatch";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Done => {}
                _ => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        match &mut self2.state {
                            State::WaitMsg(ts1) => {
                                trace!("{selfname}  inp proto msg {item:?}");
                                let dt = ts1.elapsed();
                                let dtms = 1e3 * dt.as_secs_f32();
                                trace!("WaitMsg Recv dt {dtms:0} ms");
                                let (_, m2) = item.msg.into_parts();
                                match m2 {
                                    CaMsgTy::ReadNotifyRes(m3) => match m3.value.meta {
                                        proto::CaMetaValue::CaMetaVariants(vars) => {
                                            trace!("variants {vars:?}");
                                            let item = ReadEnumItem::EnumStringSet(
                                                self2.sid.clone(),
                                                self2.scalar_type.clone(),
                                                self2.shape.clone(),
                                                self2.ca_dbr_ty.clone(),
                                                self2.chi.clone(),
                                                vars.variants,
                                            );
                                            break Ready(Some(Ok(Some(item))));
                                        }
                                        _ => break Ready(Some(Err(Error::ProtoRxUnexpected))),
                                    },
                                    _ => break Ready(Some(Err(Error::ProtoRxUnexpected))),
                                }
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
                            Ok(None) => {}
                            Ok(Some(x)) => break Ready(Some(Ok(x))),
                            Err(e) => {
                                error!("TODO handle error {e}");
                                self.transition_state(State::Done);
                            }
                        }
                    }
                    Ready(None) => {
                        error!("TODO even on input abort, continue with clean shutdown");
                        hpp.mark_progress();
                        self.transition_state(State::Done);
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
            }
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::SendMsg(..) => {
                    let _dbr_gr_enum = 24;
                    let dbr_ctrl_enum = 31;
                    let msg = CaMsg::from_ty_ts(
                        CaMsgTy::ReadNotify(ReadNotify {
                            data_type: dbr_ctrl_enum,
                            data_count: 0,
                            sid: self2.sid.to_u32(),
                            ioid: 0,
                        }),
                        tsnow,
                    );
                    self2.transition_state(State::WaitMsg(tsnow));
                    break Ready(Some(Ok(ReadEnumItem::CaMsgOutIoid(msg, self2.sid.clone(), tsnow))));
                }
                State::WaitMsg(..) => {
                    trace!("..............   in WaitMsg");
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
