//

const SILENCE_MONITOR_WAKEUP: Duration = Duration::from_millis(60000);
const CREATE_SEND_TIMEOUT: Duration = Duration::from_millis(20000);

//

use crate::ca::conn2;
use crate::ca::conn2::locallog;
use crate::ca::progpend::HaveProgressPending;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use conn2::caids::CaDbrTy;
use conn2::caids::Sid;
use conn2::conn::channelheap::ProtoRxItem;
use futures::FutureExt;
use netpod::ScalarType;
use netpod::Shape;
use stats::mett::ChannelHandlerMetrics;
use std::collections::VecDeque;
use std::fmt;
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
    name(Error, "ChannelHandlerRunning"),
    enum variants {
        Logic,
    },
);

#[derive(Debug, Clone)]
enum StateDirection {
    None,
    RemoveMonitorSend,
    Enable,
}

#[derive(Debug)]
pub enum MonitoringItem {
    ProtoOutSubid(CaMsg, Instant),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
}

fn transition_state(old: &mut State, new: State, llog: &mut locallog::LocalLog) {
    llog.push(format!("transition  {} -> {}", old, new));
    *old = new;
}

#[derive(Debug)]
enum State {
    DoNothing(),
    CreateMonitorSend(StateDirection),
    CreateMonitorRecv(FutDbg<()>, StateDirection),
    Monitoring(FutDbg<()>, StateDirection),
    RemoveMonitorSend(StateDirection),
    RemoveMonitorRecv(FutDbg<()>, StateDirection),
}

impl fmt::Display for State {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::DoNothing(..) => write!(fmt, "DoNothing"),
            State::CreateMonitorSend(..) => write!(fmt, "CreateMonitorSend"),
            State::CreateMonitorRecv(..) => write!(fmt, "CreateMonitorRecv"),
            State::Monitoring(..) => write!(fmt, "Monitoring"),
            State::RemoveMonitorSend(..) => write!(fmt, "RemoveMonitorSend"),
            State::RemoveMonitorRecv(..) => write!(fmt, "RemoveMonitorRecv"),
        }
    }
}

#[derive(Debug)]
pub struct FetchMonitoring {
    state: State,
    sid: Sid,
    scalar_type: ScalarType,
    shape: Shape,
    ca_dbr_ty: CaDbrTy,
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
    rng: stats::rand_xoshiro::Xoshiro128PlusPlus,
    llog: locallog::LocalLog,
}

impl FetchMonitoring {
    pub fn new(sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy) -> Self {
        Self {
            state: State::DoNothing(),
            sid,
            scalar_type,
            shape,
            ca_dbr_ty,
            inp_buf: VecDeque::with_capacity(16),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
            rng: stats::xoshiro_from_os_rng(),
            llog: locallog::LocalLog::new(),
        }
    }

    fn rng_next(&mut self) -> u32 {
        stats::rand_xoshiro::rand_core::RngCore::next_u32(&mut self.rng)
    }

    fn duration_jitter(&mut self, dur: Duration) -> Duration {
        let r = self.rng_next() & 0xff;
        let a = 1024;
        let b = a - (a / 8) + r;
        // TODO avoid div
        (b * dur) / a
    }

    pub fn transition_to_enable(&mut self) -> () {
        self.llog.push(format!("transition_to_enable  {}", self.state));
        match &mut self.state {
            State::DoNothing(..) => {
                transition_state(
                    &mut self.state,
                    State::CreateMonitorSend(StateDirection::None),
                    &mut self.llog,
                );
            }
            State::CreateMonitorSend(..) => {}
            State::CreateMonitorRecv(..) => {}
            State::Monitoring(..) => {}
            State::RemoveMonitorSend(stdir) => {
                *stdir = StateDirection::Enable;
            }
            State::RemoveMonitorRecv(_, stdir) => {
                *stdir = StateDirection::Enable;
            }
        }
    }

    pub fn transition_to_disable(&mut self) -> () {
        self.llog.push(format!("transition_to_disable  {}", self.state));
        match &mut self.state {
            State::DoNothing() => {}
            State::CreateMonitorSend(stdir) => {
                *stdir = StateDirection::RemoveMonitorSend;
            }
            State::CreateMonitorRecv(_, stdir) => {
                *stdir = StateDirection::RemoveMonitorSend;
            }
            State::Monitoring(_, stdir) => {
                *stdir = StateDirection::RemoveMonitorSend;
            }
            State::RemoveMonitorSend(..) => {}
            State::RemoveMonitorRecv(..) => {}
        }
    }

    pub fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
        trace3!("FetchPolling  inp_push_try");
        let v = &mut self.inp_buf;
        if v.len() < v.capacity() {
            v.push_back(item);
            None
        } else {
            Some(item)
        }
    }

    pub fn inp_done(&mut self) {
        trace3!("FetchPolling  inp_done");
        self.inp_done = true;
    }

    fn handle_received_item(&mut self, item: ProtoRxItem) -> Poll<Option<Option<Result<MonitoringItem, Error>>>> {
        let selfname = "handle_received_item";
        use Poll::*;
        match item.msg.ty {
            CaMsgTy::EventAddRes(v) => {
                let tsnow = Instant::now();
                let dttrig = Duration::ZERO;
                let dtcmd = tsnow.saturating_duration_since(item.tscmd);
                let dtreg = tsnow.saturating_duration_since(item.tsreg);
                let dtdisp = tsnow.saturating_duration_since(item.tsdisp);
                let valf32 = v.value.f32_for_binning();
                let item = MonitoringItem::TestValue(crate::ca::connset2::connset::TestValue {
                    val: valf32,
                    dttrig: 1e3 * dttrig.as_secs_f32(),
                    dtcmd: 1e3 * dtcmd.as_secs_f32(),
                    dtreg: 1e3 * dtreg.as_secs_f32(),
                    dtdisp: 1e3 * dtdisp.as_secs_f32(),
                });
                Ready(Some(Some(Ok(item))))
            }
            _ => {
                warn!("{selfname}  TODO item while in WaitRes  {item:?}");
                Ready(Some(None))
            }
        }
    }

    fn poll_inp(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Option<Result<MonitoringItem, Error>>>> {
        let selfname = "poll_inp";
        trace4!("{selfname}");
        use Poll::*;
        if let Some(item) = self.inp_buf.pop_front() {
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::DoNothing(..) => {
                    info!("{selfname}  TODO item while in DoNothing  {item:?}");
                    Ready(Some(None))
                }
                State::CreateMonitorSend(..) => {
                    info!("{selfname}  TODO item while in CreateMonitorSend  {item:?}");
                    Ready(Some(None))
                }
                State::CreateMonitorRecv(_, stdir) => {
                    let stdir = stdir.clone();
                    let ret = self2.handle_received_item(item);
                    let dur = self2.duration_jitter(SILENCE_MONITOR_WAKEUP);
                    let fut = tokio::time::sleep(dur).box2();
                    transition_state(&mut self2.state, State::Monitoring(fut, stdir), &mut self2.llog);
                    ret
                }
                State::Monitoring(..) => {
                    let ret = self2.handle_received_item(item);
                    ret
                }
                State::RemoveMonitorSend(..) => {
                    let ret = self2.handle_received_item(item);
                    ret
                }
                State::RemoveMonitorRecv(_, stdir) => {
                    match item.msg.ty {
                        CaMsgTy::EventAddResEmpty(..) | CaMsgTy::EventCancelRes(..) => {
                            if let StateDirection::Enable = stdir {
                                transition_state(
                                    &mut self2.state,
                                    State::CreateMonitorSend(StateDirection::None),
                                    &mut self2.llog,
                                );
                            } else {
                                transition_state(&mut self2.state, State::DoNothing(), &mut self2.llog);
                            }
                        }
                        CaMsgTy::EventAddRes(..) => {}
                        _ => {
                            info!("{selfname}  item while in RemoveMonitorRecv  {item:?}");
                        }
                    }
                    Ready(Some(None))
                }
            }
        } else if self.inp_done {
            Ready(None)
        } else {
            Pending
        }
    }

    pub fn poll_next_unpin(&mut self, cx: &mut Context) -> Poll<Option<Result<MonitoringItem, Error>>> {
        let selfname = "poll_next_unpin";
        trace4!("{selfname}");
        use Poll::*;
        loop {
            if let Some(x) = self.llog.pop() {
                break Ready(Some(Ok(MonitoringItem::LocalLog(x))));
            }
            let mut hpp = HaveProgressPending::new();
            let self2 = &mut *self;
            match Pin::new(&mut *self2).poll_inp(cx) {
                Ready(Some(Some(x))) => {
                    hpp.mark_progress();
                    match x {
                        Ok(x) => break Ready(Some(Ok(x))),
                        Err(e) => {
                            error!("{selfname}  {e}")
                        }
                    }
                }
                Ready(Some(None)) => {
                    hpp.mark_progress();
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            match &mut self2.state {
                State::DoNothing(..) => {}
                State::CreateMonitorSend(stdir) => {
                    hpp.mark_progress();
                    trace2!("{selfname}  CreateMonitorSend  Ready");
                    let tsnow = Instant::now();
                    let msg = CaMsg::from_ty_ts(
                        proto::CaMsgTy::EventAdd(proto::EventAdd::new(
                            self2.ca_dbr_ty.to_u16(),
                            self2.shape.to_ca_count().unwrap(),
                            self2.sid.to_u32(),
                            0,
                        )),
                        tsnow,
                    );
                    // self.mett.read_notify_send().inc();
                    // TODO add some jitter
                    let fut = tokio::time::sleep(CREATE_SEND_TIMEOUT).box2();
                    let stdir = stdir.clone();
                    transition_state(&mut self2.state, State::CreateMonitorRecv(fut, stdir), &mut self2.llog);
                    let ret = MonitoringItem::ProtoOutSubid(msg, tsnow);
                    return Ready(Some(Ok(ret)));
                }
                State::CreateMonitorRecv(to, stdir) => match to.poll_unpin(cx) {
                    Ready(()) => {
                        hpp.mark_progress();
                        error!("{selfname}  CreateMonitorRecv  Timeout  7d956c9");
                        // let dur = self2.duration_jitter(CREA);
                        // let fut = tokio::time::sleep(dur).box2();
                        // let stdir = stdir.clone();
                        transition_state(&mut self2.state, State::DoNothing(), &mut self2.llog);
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::Monitoring(to, stdir) => {
                    if let StateDirection::RemoveMonitorSend = stdir {
                        hpp.mark_progress();
                        transition_state(
                            &mut self2.state,
                            State::RemoveMonitorSend(StateDirection::None),
                            &mut self2.llog,
                        );
                    } else {
                        match to.poll_unpin(cx) {
                            Ready(()) => {
                                hpp.mark_progress();
                                self2.llog.push(format!("{selfname}  Monitoring  Timeout  7d956c9"));
                                let stdir = stdir.clone();
                                let dur = self2.duration_jitter(SILENCE_MONITOR_WAKEUP);
                                let fut = tokio::time::sleep(dur).box2();
                                transition_state(&mut self2.state, State::Monitoring(fut, stdir), &mut self2.llog);
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    }
                }
                State::RemoveMonitorSend(stdir) => {
                    hpp.mark_progress();
                    let tsnow = Instant::now();
                    let msg = CaMsg::from_ty_ts(
                        proto::CaMsgTy::EventCancel(proto::EventCancel::new(
                            self2.ca_dbr_ty.to_u16(),
                            self2.shape.to_ca_count().unwrap(),
                            self2.sid.to_u32(),
                            0,
                        )),
                        tsnow,
                    );
                    // self.mett.read_notify_send().inc();
                    // TODO add some jitter
                    let fut = tokio::time::sleep(CREATE_SEND_TIMEOUT).box2();
                    let stdir = stdir.clone();
                    transition_state(&mut self2.state, State::RemoveMonitorRecv(fut, stdir), &mut self2.llog);
                    let ret = MonitoringItem::ProtoOutSubid(msg, tsnow);
                    return Ready(Some(Ok(ret)));
                }
                State::RemoveMonitorRecv(to, stdir) => {
                    info!("{selfname}  {}", "RemoveMonitorRecv");
                    match to.poll_unpin(cx) {
                        Ready(()) => {
                            hpp.mark_progress();
                            info!("{selfname}  RemoveMonitorRecv  Timeout  7d956c9");
                            // TODO return some error
                            // TODO check state direction
                            transition_state(&mut self2.state, State::DoNothing(), &mut self2.llog);
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
            }
            break if hpp.have_progress() {
                trace!("{selfname}  HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("{selfname}  HPP");
                Pending
            } else {
                trace!("{selfname}  HPP:Done");
                Ready(None)
            };
        }
    }
}
