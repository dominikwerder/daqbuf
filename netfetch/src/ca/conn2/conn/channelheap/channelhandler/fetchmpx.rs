mod fetchmonitoring;

use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Ioid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::SubidOwned;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler;
use crate::ca::conn2::conn::channelheap::channelhandler::ChannelHandlerItem;
use crate::ca::conn2::conn::channelheap::channelhandler::ItemInner;
use crate::ca::conn2::conn::channelheap::channelhandler::fetchmpx::fetchmonitoring::FetchMonitoring;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use futures::FutureExt;
use futures::Stream;
use futures::TryFutureExt;
use netpod::ScalarType;
use netpod::Shape;
use stats::mett::ChannelHandlerMetrics;
use stats::rand_xoshiro::rand_core::RngCore;
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
        // Register(#[from] dbpg::seriesbychannel::Error),
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
struct CreatePolling {
    out_tx: asynchan::Sender<CaMsg>,
    inp_tx: asynchan::Sender<CaMsg>,
    fut: FutDbg<Result<(), Error>>,
}

impl CreatePolling {
    fn new(
        cid: Cid,
        sid: Sid,
        out_tx: asynchan::Sender<CaMsg>,
        inp_tx: asynchan::Sender<CaMsg>,
        mut inp_rx: asynchan::Receiver<CaMsg>,
        chconf: ChannelConfig,
    ) -> Self {
        let fut = { async move { Ok(()) } };
        Self {
            out_tx,
            inp_tx,
            fut: fut.box2(),
        }
    }
}

#[derive(Debug)]
struct Monitor {
    subid: SubidOwned,
}

#[derive(Debug)]
struct FetchData {
    ioid: Ioid,
    scalar_type: ScalarType,
    shape: Shape,
    ca_dbr_ty: CaDbrTy,
    // evwriter: crate::ca::conn2::ca_writer_value::CaRtWriter,
    // binwriter: BinWriter,
}

#[derive(Debug)]
enum FetchPollingState {
    DoNothing,
    Idle(FutDbg<()>),
    SendReq,
    WaitRes(FutDbg<()>),
}

#[derive(Debug)]
struct FetchPolling {
    state: FetchPollingState,
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
}

impl FetchPolling {
    fn new(sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy) -> Self {
        let poll_next_ts_exact = Instant::now() + Duration::from_millis(200);
        let poll_next_ts_jitter = poll_next_ts_exact;
        Self {
            // TODO the first poll should be random-soon
            state: FetchPollingState::Idle(
                {
                    let ts = poll_next_ts_jitter;
                    async move {
                        // TODO add jitter
                        tokio::time::sleep_until(ts.into()).await
                    }
                }
                .box2(),
            ),
            sid,
            scalar_type,
            shape,
            ca_dbr_ty,
            interval: Duration::from_millis(3000),
            poll_next_ts_exact,
            poll_next_ts_jitter,
            inp_buf: VecDeque::with_capacity(16),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
            rng: stats::xoshiro_from_os_rng(),
        }
    }

    fn rng_next(&mut self) -> u32 {
        self.rng.next_u32()
    }

    fn next_poll_instant_jitter(&mut self) -> Instant {
        let tsnow = Instant::now();
        self.poll_next_ts_exact = self.poll_next_ts_exact + self.interval;
        if self.poll_next_ts_exact < tsnow {
            // TODO add more jitter in this case
            self.poll_next_ts_exact = tsnow;
        }
        // TODO actually add random jitter
        let r = self.rng_next() & 0xff;
        let a = 1000;
        let b = a - 128 + r;
        // TODO avoid div
        let dd = (a * self.interval) / b;
        info!("jittered poll interval: {:?}", self.interval + dd);
        self.poll_next_ts_jitter = self.poll_next_ts_exact + dd;
        self.poll_next_ts_jitter
    }

    pub fn transition_to_enable(&mut self) {
        match &mut self.state {
            FetchPollingState::DoNothing => {
                let ts = self.next_poll_instant_jitter();
                let fut = async move {
                    tokio::time::sleep_until(ts.into()).await;
                };
                self.state = FetchPollingState::Idle(fut.box2());
            }
            FetchPollingState::Idle(..) => {}
            FetchPollingState::SendReq => {}
            FetchPollingState::WaitRes(..) => {}
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<FetchMethodPollOutput, Error>>> {
        let selfname = "FetchPolling::poll_next_unpin";
        trace3!("{selfname}");
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        {
            if let Some(item) = self.inp_buf.pop_front() {
                match &mut self.state {
                    FetchPollingState::DoNothing => {
                        hpp.mark_progress();
                        warn!("TODO item while in DoNothing  {item:?}");
                    }
                    FetchPollingState::Idle(_) => {
                        hpp.mark_progress();
                        warn!("TODO item while in Idle  {item:?}");
                    }
                    FetchPollingState::SendReq => {
                        hpp.mark_progress();
                        warn!("TODO item while in SendReq  {item:?}");
                    }
                    FetchPollingState::WaitRes(_) => {
                        hpp.mark_progress();
                        match item.msg.ty {
                            CaMsgTy::ReadNotifyRes(v) => {
                                let tsnow = Instant::now();
                                let dttrig = item.tscmd.saturating_duration_since(self.poll_next_ts_jitter);
                                let dtcmd = tsnow.saturating_duration_since(item.tscmd);
                                let dtreg = tsnow.saturating_duration_since(item.tsreg);
                                let dtdisp = tsnow.saturating_duration_since(item.tsdisp);
                                let valf32 = v.value.f32_for_binning();
                                let fut = {
                                    let ts = self.next_poll_instant_jitter();
                                    async move {
                                        tokio::time::sleep_until(ts.into()).await;
                                    }
                                };
                                self.state = FetchPollingState::Idle(fut.box2());
                                let item = FetchMethodPollOutput::TestValue(crate::ca::connset2::connset::TestValue {
                                    val: valf32,
                                    dttrig: 1e3 * dttrig.as_secs_f32(),
                                    dtcmd: 1e3 * dtcmd.as_secs_f32(),
                                    dtreg: 1e3 * dtreg.as_secs_f32(),
                                    dtdisp: 1e3 * dtdisp.as_secs_f32(),
                                });
                                return Ready(Some(Ok(item)));
                            }
                            _ => {
                                warn!("TODO item while in WaitRes  {item:?}");
                            }
                        }
                    }
                }
            } else if self.inp_done {
            } else {
                hpp.mark_pending();
            }
        }
        match &mut self.state {
            FetchPollingState::DoNothing => {}
            FetchPollingState::Idle(fut) => match fut.poll_unpin(cx) {
                Ready(()) => {
                    hpp.mark_progress();
                    trace2!("{selfname}  Idle  Ready");
                    self.state = FetchPollingState::SendReq;
                }
                Pending => {
                    hpp.mark_pending();
                }
            },
            FetchPollingState::SendReq => {
                hpp.mark_progress();
                trace2!("{selfname}  SendReq  Ready");
                let tsnow = Instant::now();
                let msg = CaMsg::from_ty_ts(
                    CaMsgTy::ReadNotify(proto::ReadNotify {
                        data_type: self.ca_dbr_ty.to_u16(),
                        data_count: self.shape.to_ca_count().unwrap(),
                        sid: self.sid.to_u32(),
                        ioid: 0,
                    }),
                    tsnow,
                );
                self.mett.read_notify_send().inc();
                let fut = async { tokio::time::sleep(Duration::from_millis(3000)).await };
                self.state = FetchPollingState::WaitRes(fut.box2());
                let ret = FetchMethodPollOutput::ProtoOutIoid(msg, self.sid.clone(), tsnow);
                return Ready(Some(Ok(ret)));
            }
            FetchPollingState::WaitRes(to) => match to.poll_unpin(cx) {
                Ready(()) => {
                    info!("{selfname}  WaitRes  Timeout");
                    hpp.mark_progress();
                    error!("\n\n\n  TODO  FetchPollingState::WaitRes  Timeout  fad6ffb3b  \n\n\n");
                    // TODO choose random backoff
                    let fut = async { tokio::time::sleep(Duration::from_millis(20000)).await };
                    self.state = FetchPollingState::WaitRes(fut.box2());
                }
                Pending => {
                    hpp.mark_pending();
                }
            },
        }
        if hpp.have_progress() {
            trace!("{selfname}  HPP:Progress");
            Ready(Some(Ok(FetchMethodPollOutput::None)))
        } else if hpp.have_pending() {
            trace_pending!("{selfname}  HPP");
            Pending
        } else {
            trace!("{selfname}  HPP:Done");
            Ready(None)
        }
    }
}

struct SomeData;

enum FetchMethodPollOutput {
    None,
    ProtoOut(CaMsg),
    ProtoOutIoid(CaMsg, Sid, Instant),
    // ScyllaWrite,
    // CallbackOnRunning(Box<dyn FnOnce(&mut SomeData)>, Vec<ChannelHandlerItem>),
    TestValue(crate::ca::connset2::connset::TestValue),
}

fn make_cb<F>(f: F) -> Box<dyn FnOnce(&mut SomeData)>
where
    F: FnOnce(&mut SomeData) + 'static,
{
    Box::new(f)
}

fn make_cb2<F>(f: F) -> Box<dyn FnOnce(&mut SomeData)>
where
    F: FnOnce(&mut SomeData) + 'static,
{
    Box::new(f)
}

struct FetchMethodPollRes<'a> {
    fetch_data: &'a mut FetchData,
    conf: &'a ChannelConfig,
    cid: Cid,
    sid: Sid,
    mett: &'a mut ChannelHandlerMetrics,
}

#[derive(Debug)]
pub enum FetchmpxItem {
    CaMsgOut(CaMsg),
    CaMsgOutIoid(CaMsg, Sid, Instant),
    CaMsgOutSubid(CaMsg, Instant),
    ScyllaWrite,
    TestValue(crate::ca::connset2::connset::TestValue),
}

#[derive(Debug)]
enum State {
    Normal,
    Closing1,
    Done,
    Done2,
}

#[derive(Debug)]
pub struct Fetchmpx {
    state: State,
    sid: Sid,
    polling: FetchPolling,
    monitoring: FetchMonitoring,
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
}

impl Fetchmpx {
    pub fn new(sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy) -> Self {
        let mut monitoring = FetchMonitoring::new(sid.clone(), scalar_type.clone(), shape.clone(), ca_dbr_ty.clone());
        monitoring.transition_to_enable();
        Self {
            state: State::Normal,
            sid,
            polling: FetchPolling::new(sid.clone(), scalar_type.clone(), shape.clone(), ca_dbr_ty.clone()),
            monitoring,
            inp_buf: VecDeque::with_capacity(8),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
        }
    }

    fn trigger_closing(&mut self, reason: channelhandler::ClosingReason) {
        match &mut self.state {
            State::Normal => {
                warn!("TODO collect all information that we want to store or log and move into future");
                self.state = State::Closing1;
            }
            State::Closing1 => {}
            State::Done => {}
            State::Done2 => {}
        }
    }

    pub fn trigger_remove(&mut self) {
        self.trigger_closing(channelhandler::ClosingReason::Command);
    }

    pub fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
        trace3!("inp_push_try");
        let v = &mut self.inp_buf;
        if v.len() < v.capacity() {
            v.push_back(item);
            None
        } else {
            Some(item)
        }
    }

    pub fn inp_done(&mut self) {
        trace3!("inp_done");
        self.inp_done = true;
        self.polling.inp_done();
        self.monitoring.inp_done();
    }

    fn poll_inp_dispatch(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_inp_dispatch";
        trace3!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        trace2!("{selfname}  ITEM  {item:?}");
                        match &item.msg.ty {
                            proto::CaMsgTy::EventAddRes(_) | proto::CaMsgTy::EventAddResEmpty(_) => {
                                match self2.monitoring.inp_push_try(item) {
                                    Some(x) => {
                                        hpp.mark_pending();
                                        self2.inp_buf.push_front(x);
                                    }
                                    None => {
                                        hpp.mark_progress();
                                        // TODO count
                                        // self2.mett.read_notify_recv().inc();
                                    }
                                }
                            }
                            proto::CaMsgTy::ReadNotifyRes(_) => match self2.polling.inp_push_try(item) {
                                Some(x) => {
                                    hpp.mark_pending();
                                    self2.inp_buf.push_front(x);
                                }
                                None => {
                                    hpp.mark_progress();
                                    self2.mett.read_notify_recv().inc();
                                }
                            },
                            _ => {
                                trace!("channel_create: unexpected message {item:?}");
                                let e = Error::CreateMonitorUnexpectedMessage;
                                return Ready(Some(Err(e)));
                            }
                        }
                    } else if self2.inp_done {
                        hpp.mark_progress();
                        // TODO status event about this specific case
                        self2.trigger_closing(channelhandler::ClosingReason::InputDone);
                    } else {
                        hpp.mark_pending();
                    }
                }
                State::Closing1 => {
                    debug!("{selfname}  TODO  no input to parse in Closing1");
                }
                State::Done => {}
                State::Done2 => {}
            }
            break if hpp.have_progress() {
                trace4!("{selfname}  HPP:Progress");
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

impl Stream for Fetchmpx {
    type Item = Result<FetchmpxItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let selfname = "Fetchmpx::poll_next";
        trace4!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal => {
                    match self.as_mut().poll_inp_dispatch(cx) {
                        Ready(Some(x)) => match x {
                            Ok(()) => {}
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
                    match Pin::new(&mut self.polling).poll_next(cx) {
                        Ready(Some(x)) => match x {
                            Ok(x) => match x {
                                FetchMethodPollOutput::None => {
                                    hpp.mark_progress();
                                }
                                FetchMethodPollOutput::ProtoOut(msg) => {
                                    hpp.mark_progress();
                                    let g = FetchmpxItem::CaMsgOut(msg);
                                    break Ready(Some(Ok(g)));
                                }
                                FetchMethodPollOutput::ProtoOutIoid(msg, sid, ts) => {
                                    hpp.mark_progress();
                                    let g = FetchmpxItem::CaMsgOutIoid(msg, sid, ts);
                                    break Ready(Some(Ok(g)));
                                }
                                FetchMethodPollOutput::TestValue(x) => {
                                    hpp.mark_progress();
                                    let g = FetchmpxItem::TestValue(x);
                                    break Ready(Some(Ok(g)));
                                }
                            },
                            Err(e) => {
                                error!("polling error {e}");
                                self.state = State::Done;
                                hpp.mark_progress();
                            }
                        },
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match self.monitoring.poll_next_unpin(cx) {
                        Ready(Some(x)) => match x {
                            Ok(x) => {
                                use fetchmonitoring::MonitoringItem;
                                match x {
                                    MonitoringItem::ProtoOutSubid(msg, ts) => {
                                        hpp.mark_progress();
                                        let g = FetchmpxItem::CaMsgOutSubid(msg, ts);
                                        break Ready(Some(Ok(g)));
                                    }
                                    MonitoringItem::TestValue(x) => {
                                        hpp.mark_progress();
                                        let g = FetchmpxItem::TestValue(x);
                                        break Ready(Some(Ok(g)));
                                    }
                                }
                            }
                            Err(e) => {
                                error!("polling error {e}");
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
                State::Closing1 => {
                    debug!("{selfname}  TODO  nothing to do yet, go directly to Done");
                    hpp.mark_progress();
                    self.state = State::Done;
                }
                State::Done => {
                    self.state = State::Done2;
                }
                State::Done2 => {
                    error!("{lf}{selfname}  polled after done{lf}", lf = "\n\n");
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
