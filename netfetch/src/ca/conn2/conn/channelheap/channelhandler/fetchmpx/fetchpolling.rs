use crate::ca::conn2;
use crate::ca::conn2::ChannelEventValue;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler::fetchmpx::fetchmonitoring::MonitoringItem;
use crate::ca::conn2::locallog;
use crate::ca::progpend::HaveProgressPending;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use futures::FutureExt;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
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
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

macro_rules! debug_transition_state { ($($arg:tt)*) => { if false { log::debug!($($arg)*); } }; }
macro_rules! todo_shutdown { ($($arg:tt)*) => { if false { log::debug!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "FetchPolling"),
    enum variants {
        Logic,
    },
);

#[derive(Debug, Clone)]
enum StateDirection {
    None,
    DoNothing,
}

#[derive(Debug)]
pub enum Item {
    None,
    ProtoOut(proto::CaMsg),
    ProtoOutIoid(proto::CaMsg, Sid, Instant),
    // CallbackOnRunning(Box<dyn FnOnce(&mut SomeData)>, Vec<ChannelHandlerItem>),
    ChannelEventValue(ChannelEventValue),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
}

fn transition_state(old: &mut State, new: State, llog: &mut locallog::LocalLog) {
    debug_transition_state!("{}", format!("transition  {} -> {}", old, new));
    llog.push(format!("transition  {} -> {}", old, new));
    *old = new;
}

#[derive(Debug)]
enum State {
    DoNothing,
    Idle(FutDbg<()>),
    SendReq(StateDirection),
    WaitRes(FutDbg<()>, StateDirection),
    Closing1,
    Done,
}

impl fmt::Display for State {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::DoNothing => write!(fmt, "DoNothing"),
            State::Idle(..) => write!(fmt, "Idle"),
            State::SendReq(..) => write!(fmt, "SendReq"),
            State::WaitRes(..) => write!(fmt, "WaitRes"),
            State::Closing1 => write!(fmt, "Closing1"),
            State::Done => write!(fmt, "Done"),
        }
    }
}

#[derive(Debug)]
pub struct FetchPolling {
    state: State,
    series: SeriesId,
    sid: Sid,
    scalar_type: ScalarType,
    shape: Shape,
    ca_dbr_ty: CaDbrTy,
    interval: Duration,
    poll_next_ts_exact: Instant,
    poll_next_ts_jitter: Instant,
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
    rng: stats::rand_xoshiro::Xoshiro128PlusPlus,
    llog: locallog::LocalLog,
}

impl FetchPolling {
    pub fn new(series: SeriesId, sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy) -> Self {
        let tsnow = Instant::now();
        Self {
            state: State::DoNothing,
            series,
            sid,
            scalar_type,
            shape,
            ca_dbr_ty,
            interval: Duration::from_millis(3000),
            poll_next_ts_exact: tsnow,
            poll_next_ts_jitter: tsnow,
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

    fn next_poll_instant_jitter(&mut self) -> Instant {
        let tsnow = Instant::now();
        self.poll_next_ts_exact = self.poll_next_ts_exact + self.interval;
        if self.poll_next_ts_exact < tsnow {
            let r = self.rng_next() & 0xff;
            let a = 1024;
            let b = a - (a / 8) + r;
            // TODO avoid div
            let dd = (b * self.interval) / a;
            self.poll_next_ts_exact = tsnow + dd;
        }
        let r = self.rng_next() & 0xff;
        let a = 1024;
        let b = a - 128 + r;
        // TODO avoid div
        let dd = (b * self.interval) / a;
        info!("jittered poll interval: {:.2} sec", (self.interval + dd).as_secs_f32());
        self.poll_next_ts_jitter = self.poll_next_ts_exact + dd;
        self.poll_next_ts_jitter
    }

    pub fn transition_to_enable(&mut self) {
        let selfname = "transition_to_enable";
        match &mut self.state {
            State::DoNothing => {
                self.llog
                    .push(format!("{selfname}  State::DoNothing  enable immediately"));
                let ts = self.next_poll_instant_jitter();
                let fut = async move {
                    tokio::time::sleep_until(ts.into()).await;
                };
                transition_state(&mut self.state, State::Idle(fut.box2()), &mut self.llog);
            }
            State::Idle(..) => {
                self.llog.push(format!("{selfname}  State::Idle  no change"));
            }
            State::SendReq(..) => {
                self.llog.push(format!("{selfname}  State::SendReq  no change"));
            }
            State::WaitRes(..) => {
                self.llog.push(format!("{selfname}  State::WaitRes  no change"));
            }
            State::Closing1 => {
                self.llog.push(format!("{selfname}  State::Closing1  no change"));
            }
            State::Done => {
                self.llog.push(format!("{selfname}  State::Done  no change"));
            }
        }
    }

    pub fn transition_to_disable(&mut self) {
        let selfname = "transition_to_disable";
        match &mut self.state {
            State::DoNothing => {
                self.llog.push(format!("{selfname}  State::DoNothing  no change"));
            }
            State::Idle(..) => {
                self.llog.push(format!("{selfname}  State::Idle  goto immediate"));
                transition_state(&mut self.state, State::DoNothing, &mut self.llog);
            }
            State::SendReq(stdir) => {
                self.llog.push(format!("{selfname}  State::Idle  set stdir"));
                *stdir = StateDirection::DoNothing;
            }
            State::WaitRes(_, stdir) => {
                self.llog.push(format!("{selfname}  State::Idle  set stdir"));
                *stdir = StateDirection::DoNothing;
            }
            State::Closing1 => {
                self.llog.push(format!("{selfname}  State::Closing1  no change"));
            }
            State::Done => {
                self.llog.push(format!("{selfname}  State::Done  no change"));
            }
        }
    }

    fn transition_to_closing(&mut self) {
        let selfname = "transition_to_closing";
        match &mut self.state {
            State::DoNothing => {
                self.llog.push(format!("{selfname}  State::DoNothing  goto Closing1"));
                transition_state(&mut self.state, State::Closing1, &mut self.llog);
            }
            State::Idle(..) => {
                self.llog.push(format!("{selfname}  State::Idle  goto Closing1"));
                transition_state(&mut self.state, State::Closing1, &mut self.llog);
            }
            State::SendReq(stdir) => {
                self.llog.push(format!("{selfname}  State::SendReq  goto Closing1"));
                transition_state(&mut self.state, State::Closing1, &mut self.llog);
            }
            State::WaitRes(_, stdir) => {
                self.llog.push(format!("{selfname}  State::Idle  goto Closing1"));
                transition_state(&mut self.state, State::Closing1, &mut self.llog);
            }
            State::Closing1 => {
                self.llog.push(format!("{selfname}  State::Idle  no change"));
            }
            State::Done => {
                self.llog.push(format!("{selfname}  State::Done  no change"));
            }
        }
    }

    pub fn trigger_closing(&mut self, reason: conn2::conn::channelheap::channelhandler::ClosingReason) {
        let selfname = "trigger_closing";
        todo_shutdown!("{selfname}  {}  {:?}", self.sid, reason);
        self.transition_to_closing();
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

    pub fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<Item, Error>>> {
        let selfname = "poll_next";
        trace3!("{selfname}");
        use Poll::*;
        // TODO allow inner loop
        if let Some(x) = self.llog.pop() {
            return Ready(Some(Ok(Item::LocalLog(x))));
        }
        let mut hpp = HaveProgressPending::new();
        let self2 = self.as_mut().get_mut();
        if let Some(item) = self2.inp_buf.pop_front() {
            match &mut self2.state {
                State::DoNothing => {
                    hpp.mark_progress();
                    warn!("TODO item while in DoNothing  {item:?}");
                }
                State::Idle(_) => {
                    hpp.mark_progress();
                    warn!("TODO item while in Idle  {item:?}");
                }
                State::SendReq(..) => {
                    hpp.mark_progress();
                    warn!("TODO item while in SendReq  {item:?}");
                }
                State::WaitRes(_, stdir) => {
                    hpp.mark_progress();
                    match item.msg.ty {
                        proto::CaMsgTy::ReadNotifyRes(v) => {
                            let tsnow = Instant::now();
                            let dttrig = item.tscmd.saturating_duration_since(self2.poll_next_ts_jitter);
                            let dtcmd = tsnow.saturating_duration_since(item.tscmd);
                            let dtreg = tsnow.saturating_duration_since(item.tsreg);
                            let dtdisp = tsnow.saturating_duration_since(item.tsdisp);
                            let val_f32 = v.value.f32_for_binning();
                            match stdir {
                                StateDirection::DoNothing => {
                                    transition_state(&mut self2.state, State::DoNothing, &mut self2.llog);
                                }
                                StateDirection::None => {
                                    let fut = {
                                        let ts = self2.next_poll_instant_jitter();
                                        async move {
                                            tokio::time::sleep_until(ts.into()).await;
                                        }
                                    };
                                    transition_state(&mut self2.state, State::Idle(fut.box2()), &mut self2.llog);
                                }
                            }
                            if false {
                                let item = Item::TestValue(crate::ca::connset2::connset::TestValue {
                                    val: val_f32,
                                    dttrig: 1e3 * dttrig.as_secs_f32(),
                                    dtcmd: 1e3 * dtcmd.as_secs_f32(),
                                    dtreg: 1e3 * dtreg.as_secs_f32(),
                                    dtdisp: 1e3 * dtdisp.as_secs_f32(),
                                });
                            }
                            let stnow = SystemTime::now();
                            let ts = TsNano::from_system_time(stnow);
                            let item =
                                Item::ChannelEventValue(ChannelEventValue::new(self.series.clone(), ts, val_f32));
                            return Ready(Some(Ok(item)));
                        }
                        _ => {
                            warn!("TODO item while in WaitRes  {item:?}");
                        }
                    }
                }
                State::Closing1 => {
                    hpp.mark_progress();
                }
                State::Done => {
                    hpp.mark_progress();
                }
            }
        } else if self2.inp_done {
        } else {
            hpp.mark_pending();
        }
        match &mut self2.state {
            State::DoNothing => {}
            State::Idle(fut) => match fut.poll_unpin(cx) {
                Ready(()) => {
                    hpp.mark_progress();
                    trace2!("{selfname}  Idle  Ready");
                    transition_state(&mut self2.state, State::SendReq(StateDirection::None), &mut self2.llog);
                }
                Pending => {
                    hpp.mark_pending();
                }
            },
            State::SendReq(stdir) => {
                hpp.mark_progress();
                trace2!("{selfname}  SendReq  Ready");
                let tsnow = Instant::now();
                let msg = proto::CaMsg::from_ty_ts(
                    proto::CaMsgTy::ReadNotify(proto::ReadNotify {
                        data_type: self2.ca_dbr_ty.to_u16(),
                        data_count: self2.shape.to_ca_count().unwrap(),
                        sid: self2.sid.to_u32(),
                        ioid: 0,
                    }),
                    tsnow,
                );
                self2.mett.read_notify_send().inc();
                let fut = async { tokio::time::sleep(Duration::from_millis(3000)).await };
                let stdir = stdir.clone();
                transition_state(&mut self2.state, State::WaitRes(fut.box2(), stdir), &mut self2.llog);
                let ret = Item::ProtoOutIoid(msg, self2.sid.clone(), tsnow);
                return Ready(Some(Ok(ret)));
            }
            State::WaitRes(to, stdir) => match to.poll_unpin(cx) {
                Ready(()) => {
                    info!("{selfname}  WaitRes  Timeout");
                    hpp.mark_progress();
                    error!("\n\n\n  TODO  FetchPollingState::WaitRes  Timeout  fad6ffb3b  \n\n\n");
                    match stdir {
                        StateDirection::DoNothing => {
                            transition_state(&mut self2.state, State::DoNothing, &mut self2.llog);
                        }
                        StateDirection::None => {
                            // TODO emit status event on the first timeout only.
                            // TODO choose random increasing backoff.
                            let fut = async { tokio::time::sleep(Duration::from_millis(27427)).await };
                            let stdir = stdir.clone();
                            transition_state(&mut self2.state, State::WaitRes(fut.box2(), stdir), &mut self2.llog);
                        }
                    }
                }
                Pending => {
                    hpp.mark_pending();
                }
            },
            State::Closing1 => {
                hpp.mark_progress();
                todo_shutdown!("TODO  State::Closing1  emit all writes");
                self2.inp_done();
                transition_state(&mut self2.state, State::Done, &mut self2.llog);
            }
            State::Done => {}
        }
        if hpp.have_progress() {
            trace!("{selfname}  HPP:Progress");
            Ready(Some(Ok(Item::None)))
        } else if hpp.have_pending() {
            trace_pending!("{selfname}  HPP");
            Pending
        } else {
            trace!("{selfname}  HPP:Done");
            Ready(None)
        }
    }
}
