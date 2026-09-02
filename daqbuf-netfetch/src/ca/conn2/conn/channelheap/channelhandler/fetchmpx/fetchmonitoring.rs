//

const SILENCE_MONITOR_WAKEUP: Duration = Duration::from_millis(60000);
const CREATE_SEND_TIMEOUT: Duration = Duration::from_millis(20000);

//

use crate::ca::conn2;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
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
use netpod::TsNano;
use serde::Serialize;
use serde_helper::ToSerde;
use series::SeriesId;
use stats::mett::ChannelHandlerMetrics;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

macro_rules! debug_transition_state { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! debug_shutdown { ($($arg:tt)*) => { if false { log::debug!($($arg)*); } }; }
macro_rules! todo_shutdown { ($($arg:tt)*) => { if false { log::debug!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "FetchMonitoring"),
    enum variants {
        Logic,
    },
);

#[derive(Debug, Clone, Serialize)]
enum StateDirection {
    None,
    Disable,
    Enable,
    Closing,
}

#[derive(Debug)]
pub enum MonitoringItem {
    ProtoOutSubid(CaMsg, Instant),
    SubidRemove(Cid),
    ChannelEventValue(ChannelEventValue),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
}

fn transition_state(old: &mut State, new: State, ts: &mut Instant, llog: &mut locallog::LocalLog) {
    debug_transition_state!("{}", format!("transition  {} -> {}", old, new));
    llog.push(format!("transition  {} -> {}", old, new));
    *old = new;
    *ts = Instant::now();
}

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub", serde(tag = "ty", content = "co"))]
enum State {
    DoNothing(),
    CreateMonitorSend(StateDirection),
    CreateMonitorRecv(#[to_serde(skip)] FutDbg<()>, StateDirection),
    Monitoring(#[to_serde(skip)] FutDbg<()>, StateDirection),
    RemoveMonitorSend(StateDirection),
    RemoveMonitorRecv(#[to_serde(skip)] FutDbg<()>, StateDirection),
    RemoveSubidSend(StateDirection),
    Closing1,
    Done,
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
            State::RemoveSubidSend(..) => write!(fmt, "RemoveSubidSend"),
            State::Closing1 => write!(fmt, "Closing1"),
            State::Done => write!(fmt, "Done"),
        }
    }
}

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub")]
pub struct FetchMonitoring {
    #[to_serde(nest)]
    state: State,
    /// Set by `transition_state`, reported as time-in-state.
    #[to_serde(elapsed)]
    state_dt: Instant,
    series: SeriesId,
    cid: Cid,
    sid: Sid,
    scalar_type: ScalarType,
    shape: Shape,
    ca_dbr_ty: CaDbrTy,
    #[to_serde(len)]
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    #[to_serde(skip)]
    mett: ChannelHandlerMetrics,
    #[to_serde(skip)]
    rng: stats::rand_xoshiro::Xoshiro128PlusPlus,
    #[to_serde(skip)]
    llog: locallog::LocalLog,
}

impl FetchMonitoring {
    pub fn new(
        series: SeriesId,
        cid: Cid,
        sid: Sid,
        scalar_type: ScalarType,
        shape: Shape,
        ca_dbr_ty: CaDbrTy,
    ) -> Self {
        Self {
            state: State::DoNothing(),
            state_dt: Instant::now(),
            series,
            cid,
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
        stats::rand_xoshiro::rand_core::Rng::next_u32(&mut self.rng)
    }

    fn duration_jitter(&mut self, dur: Duration) -> Duration {
        let r = self.rng_next() & 0xff;
        let a = 1024;
        let b = a - (a / 8) + r;
        // TODO avoid div
        (b * dur) / a
    }

    pub fn transition_to_enable(&mut self) -> () {
        let selfname = "transition_to_enable";
        self.llog.push(format!("{selfname}  {}", self.state));
        match &mut self.state {
            State::DoNothing(..) => {
                transition_state(
                    &mut self.state,
                    State::CreateMonitorSend(StateDirection::Enable),
                    &mut self.state_dt,
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
            State::RemoveSubidSend(stdir) => {
                *stdir = StateDirection::Enable;
            }
            State::Closing1 => {
                debug_shutdown!("{selfname}  in Closing1");
            }
            State::Done => {
                debug_shutdown!("{selfname}  in Done");
            }
        }
    }

    #[allow(unused)]
    pub fn transition_to_disable(&mut self) -> () {
        let selfname = "transition_to_disable";
        debug_shutdown!("{selfname}");
        self.llog.push(format!("{selfname}  {}", self.state));
        match &mut self.state {
            State::DoNothing() => {}
            State::CreateMonitorSend(stdir) => {
                *stdir = StateDirection::Disable;
            }
            State::CreateMonitorRecv(_, stdir) => {
                *stdir = StateDirection::Disable;
            }
            State::Monitoring(_, stdir) => {
                *stdir = StateDirection::Disable;
            }
            State::RemoveMonitorSend(..) => {}
            State::RemoveMonitorRecv(..) => {}
            State::RemoveSubidSend(..) => {}
            State::Closing1 => {
                debug_shutdown!("{selfname}  in Closing1");
            }
            State::Done => {
                debug_shutdown!("{selfname}  in Done");
            }
        }
    }

    fn transition_to_closing(&mut self) -> () {
        let selfname = "transition_to_closing";
        debug_shutdown!("{selfname}");
        self.llog.push(format!("{selfname}  {}", self.state));
        match &mut self.state {
            State::DoNothing() => {
                self.llog.push(format!("{selfname}  State::DoNothing  goto Closing1"));
                transition_state(&mut self.state, State::Closing1, &mut self.state_dt, &mut self.llog);
            }
            State::CreateMonitorSend(stdir) => {
                *stdir = StateDirection::Closing;
            }
            State::CreateMonitorRecv(_, stdir) => {
                *stdir = StateDirection::Closing;
            }
            State::Monitoring(_, stdir) => {
                *stdir = StateDirection::Closing;
            }
            State::RemoveMonitorSend(..) => {}
            State::RemoveMonitorRecv(..) => {}
            State::RemoveSubidSend(..) => {}
            State::Closing1 => {
                debug_shutdown!("{selfname}  in Closing1");
            }
            State::Done => {
                debug_shutdown!("{selfname}  in Done");
            }
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
        let selfname = "inp_done";
        debug_shutdown!("{selfname}");
        self.inp_done = true;
    }

    pub fn trigger_closing(&mut self, reason: conn2::conn::channelheap::channelhandler::ClosingReason) {
        let selfname = "trigger_closing";
        todo_shutdown!("{selfname}  {}  {:?}", self.sid, reason);
        self.transition_to_closing();
    }

    fn handle_received_item(&mut self, item: ProtoRxItem) -> Poll<Option<Option<Result<MonitoringItem, Error>>>> {
        let selfname = "handle_received_item";
        use Poll::*;
        match item.msg.ty {
            CaMsgTy::EventAddRes(v) => {
                trace!("received {v:?}");
                let tsnow = Instant::now();
                let dttrig = Duration::ZERO;
                let dtcmd = tsnow.saturating_duration_since(item.tscmd);
                let dtreg = tsnow.saturating_duration_since(item.tsreg);
                let dtdisp = tsnow.saturating_duration_since(item.tsdisp);
                let val_f32 = v.value.f32_for_binning();
                if false {
                    let item = MonitoringItem::TestValue(crate::ca::connset2::connset::TestValue {
                        val: val_f32,
                        dttrig: 1e3 * dttrig.as_secs_f32(),
                        dtcmd: 1e3 * dtcmd.as_secs_f32(),
                        dtreg: 1e3 * dtreg.as_secs_f32(),
                        dtdisp: 1e3 * dtdisp.as_secs_f32(),
                    });
                }
                let stnow = SystemTime::now();
                let ts = TsNano::from_system_time(stnow);
                let item = MonitoringItem::ChannelEventValue(ChannelEventValue::new(self.series.clone(), ts, val_f32));
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
                    transition_state(
                        &mut self2.state,
                        State::Monitoring(fut, stdir),
                        &mut self2.state_dt,
                        &mut self2.llog,
                    );
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
                    debug_shutdown!("{selfname}  State::RemoveMonitorRecv  {}", item.msg.ty.cmd_title());
                    match item.msg.ty {
                        CaMsgTy::EventAddResEmpty(..) | CaMsgTy::EventCancelRes(..) => {
                            let stdir = stdir.clone();
                            let stn = State::RemoveSubidSend(stdir);
                            transition_state(&mut self2.state, stn, &mut self2.state_dt, &mut self2.llog);
                        }
                        CaMsgTy::EventAddRes(..) => {}
                        _ => {
                            info!("{selfname}  item while in RemoveMonitorRecv  {item:?}");
                        }
                    }
                    Ready(Some(None))
                }
                State::RemoveSubidSend(..) => {
                    self2.inp_buf.clear();
                    Ready(Some(None))
                }
                State::Closing1 => {
                    debug_shutdown!("{selfname}  item in Closing1");
                    todo_shutdown!("{selfname}  TODO  ignore input to avoid wait?");
                    Ready(Some(None))
                }
                State::Done => {
                    debug_shutdown!("{selfname}  item in Done");
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
                    transition_state(
                        &mut self2.state,
                        State::CreateMonitorRecv(fut, stdir),
                        &mut self2.state_dt,
                        &mut self2.llog,
                    );
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
                        transition_state(
                            &mut self2.state,
                            State::DoNothing(),
                            &mut self2.state_dt,
                            &mut self2.llog,
                        );
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::Monitoring(to, stdir) => match stdir {
                    StateDirection::Disable | StateDirection::Closing => {
                        hpp.mark_progress();
                        let stdir = stdir.clone();
                        let stn = State::RemoveMonitorSend(stdir);
                        debug_shutdown!("State::Monitoring  go to {stn:?}");
                        transition_state(&mut self2.state, stn, &mut self2.state_dt, &mut self2.llog);
                    }
                    StateDirection::Enable => match to.poll_unpin(cx) {
                        Ready(()) => {
                            hpp.mark_progress();
                            self2.llog.push(format!("{selfname}  Monitoring  Timeout  7d956c9"));
                            let stdir = stdir.clone();
                            let dur = self2.duration_jitter(SILENCE_MONITOR_WAKEUP);
                            let fut = tokio::time::sleep(dur).box2();
                            transition_state(
                                &mut self2.state,
                                State::Monitoring(fut, stdir),
                                &mut self2.state_dt,
                                &mut self2.llog,
                            );
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    },
                    StateDirection::None => {}
                },
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
                    transition_state(
                        &mut self2.state,
                        State::RemoveMonitorRecv(fut, stdir),
                        &mut self2.state_dt,
                        &mut self2.llog,
                    );
                    let ret = MonitoringItem::ProtoOutSubid(msg, tsnow);
                    break Ready(Some(Ok(ret)));
                }
                State::RemoveMonitorRecv(to, stdir) => {
                    match to.poll_unpin(cx) {
                        Ready(()) => {
                            hpp.mark_progress();
                            debug_shutdown!("{selfname}  RemoveMonitorRecv  Timeout  7d956c9");
                            // TODO return some error
                            // TODO check state direction
                            transition_state(
                                &mut self2.state,
                                State::DoNothing(),
                                &mut self2.state_dt,
                                &mut self2.llog,
                            );
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::RemoveSubidSend(stdir) => {
                    hpp.mark_progress();
                    match stdir {
                        StateDirection::None => {
                            // TODO RemoveMonitorRecv should actually not accommodate StateDirection::None
                            debug_shutdown!("{selfname}  State::RemoveMonitorRecv  dir None");
                            let stn = State::DoNothing();
                            transition_state(&mut self2.state, stn, &mut self2.state_dt, &mut self2.llog);
                        }
                        StateDirection::Disable => {
                            debug_shutdown!("{selfname}  State::RemoveMonitorRecv  dir Disable");
                            let stn = State::DoNothing();
                            transition_state(&mut self2.state, stn, &mut self2.state_dt, &mut self2.llog);
                        }
                        StateDirection::Enable => {
                            debug_shutdown!("{selfname}  State::RemoveMonitorRecv  dir Enable");
                            let stdir = stdir.clone();
                            let stn = State::CreateMonitorSend(stdir);
                            transition_state(&mut self2.state, stn, &mut self2.state_dt, &mut self2.llog);
                        }
                        StateDirection::Closing => {
                            let stn = State::Closing1;
                            transition_state(&mut self2.state, stn, &mut self2.state_dt, &mut self2.llog);
                        }
                    }
                    let ret = MonitoringItem::SubidRemove(self2.cid.clone());
                    break Ready(Some(Ok(ret)));
                }
                State::Closing1 => {
                    todo_shutdown!("TODO  emit all writes for shutdown");
                    hpp.mark_progress();
                    self.inp_done();
                    transition_state(&mut self.state, State::Done, &mut self.state_dt, &mut self.llog);
                }
                State::Done => {}
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
