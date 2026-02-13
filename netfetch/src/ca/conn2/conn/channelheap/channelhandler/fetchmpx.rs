use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Ioid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::SubidOwned;
use crate::ca::conn2::conn::channelheap::ChHeapCmd;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler;
use crate::ca::conn2::conn::channelheap::channelhandler::ChannelHandlerItem;
use crate::ca::conn2::conn::channelheap::channelhandler::ItemInner;
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
struct CreateMonitor {
    fut: FutDbg<Result<(SubidOwned,), Error>>,
    inp_buf: VecDeque<CaMsg>,
}

impl CreateMonitor {
    fn new(
        cid: Cid,
        sid: Sid,
        ca_dbr_ty: CaDbrTy,
        shape: Shape,
        out_tx: asynchan::Sender<CaMsg>,
        mut ch_hp_tx: asynchan::Sender<ChHeapCmd>,
        chconf: ChannelConfig,
    ) -> Self {
        let mut proto_tx = out_tx;
        let fut = async move {
            let subid = SubidOwned::new();
            {
                let (reg_tx, mut reg_rx) = asynchan::bounded(4, "RegisterSubidResp");
                ch_hp_tx
                    .send(ChHeapCmd::RegisterSubid(cid.clone(), subid.to_subid(), reg_tx))
                    .await
                    .map_err(|_| Error::ProtoTxClosed)?;
                let _reg_res = reg_rx.recv().await?;
                trace!("CreateMonitor: registered subid {subid:?}");
            }
            let msg = CaMsg::from_ty_ts(
                proto::CaMsgTy::EventAdd(proto::EventAdd::new(
                    ca_dbr_ty.to_u16(),
                    shape.to_ca_count().unwrap(),
                    sid.to_u32(),
                    subid.to_u32(),
                )),
                Instant::now(),
            );
            proto_tx.send(msg).await.map_err(|_| Error::ProtoTxClosed)?;
            Ok((subid,))
        };
        Self {
            fut: fut.box2(),
            inp_buf: VecDeque::with_capacity(1),
        }
    }

    fn poll_msg_inp(mut self: Pin<&mut Self>, msg: CaMsg, cx: &mut Context) -> Option<CaMsg> {
        if self.inp_buf.len() < self.inp_buf.capacity() {
            self.inp_buf.push_back(msg);
            None
        } else {
            Some(msg)
        }
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
        mut ch_hp_tx: asynchan::Sender<ChHeapCmd>,
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
                    FetchPollingState::Idle(_) => {
                        warn!("TODO item while in Idle  {item:?}");
                        hpp.mark_progress();
                    }
                    FetchPollingState::SendReq => {
                        warn!("TODO item while in SendReq  {item:?}");
                        hpp.mark_progress();
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
                                self.poll_next_ts_exact = self.poll_next_ts_exact + self.interval;
                                if self.poll_next_ts_exact < tsnow {
                                    // TODO add more jitter in this case
                                    self.poll_next_ts_exact = tsnow;
                                }
                                // TODO actually add random jitter
                                self.poll_next_ts_jitter = self.poll_next_ts_exact;
                                let ts = self.poll_next_ts_jitter;
                                let fut = async move {
                                    tokio::time::sleep_until(ts.into()).await;
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

#[derive(Debug)]
enum FetchMethod {
    None,
    CreateMonitor(CreateMonitor),
    Monitor(Monitor),
    CreatePolling(CreatePolling),
    Polling(FetchPolling),
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

impl FetchMethod {
    fn poll_next_unpin(
        mut self: Pin<&mut Self>,
        pres: FetchMethodPollRes,
        cx: &mut Context,
    ) -> Poll<Option<Result<FetchMethodPollOutput, Error>>> {
        let selfname = "FetchMethod::poll_next_unpin";
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        match self.as_mut().get_mut() {
            FetchMethod::None => {
                trace!("{selfname}  None\n\n\n====================== {}", pres.conf.is_polled());
                hpp.mark_progress();
                error!("\n\n\n  TODO  fetch method  f76846df4  \n\n\n");
                // if pres.conf.is_polled() {
                //     let (inp_tx, inp_rx) = asynchan::bounded(4, "CreatePolling");
                //     let create =
                //         CreatePolling::new(pres.cid.clone(), pres.sid.clone(), inp_tx, inp_rx, pres.conf.clone());
                //     let ret = make_cb(move |st2| {
                //         st2.fetch_method = FetchMethod::CreatePolling(create);
                //     });
                //     return Ready(Some(Ok(ret)));
                // } else {
                //     let create = CreateMonitor::new(
                //         pres.cid.clone(),
                //         pres.sid.clone(),
                //         pres.fetch_data.ca_dbr_ty.clone(),
                //         pres.fetch_data.shape.clone(),
                //         pres.proto_tx.clone(),
                //         pres.ch_hp_tx.clone(),
                //         pres.conf.clone(),
                //     );
                //     let ret = make_cb(move |st2| {
                //         st2.fetch_method = FetchMethod::CreateMonitor(create);
                //     });
                //     return Ready(Some(Ok(ret)));
                // }
            }
            FetchMethod::CreateMonitor(st3) => {
                trace!("{selfname}  CreateMonitor");
                match st3.fut.poll_unpin(cx) {
                    Ready(x) => match x {
                        Ok((subid,)) => {
                            trace!("{selfname}  CreateMonitor:Ready:Ok");
                            trace!("{selfname}  :Ready:Ok  TODO implement monitor handling");
                            hpp.mark_progress();
                            error!("\n\n\n  TODO  create monitor  0208dc0cd  \n\n\n");
                            // let ret = make_cb(move |st2| {
                            //     st2.fetch_method = FetchMethod::Monitor(Monitor { subid });
                            // });
                            // return Ready(Some(Ok(ret)));
                        }
                        Err(e) => {
                            error!("{selfname}  CreateMonitor:Ready:Err  TODO  handle  {e}");
                            hpp.mark_progress();
                            return Ready(Some(Err(e)));
                        }
                    },
                    Pending => {
                        trace_pending!("{selfname}  CreateMonitor");
                        hpp.mark_pending();
                    }
                }
            }
            FetchMethod::Monitor(st3) => {
                // At the moment, nothing to do here.
                // TODO here, probably good to do some housekeeping on timeout:
                // like promote last written value to next longer retention time.
                // TODO when we leave monitoring, must remove monitoring by the subid.
                let _ = &st3.subid;
            }
            FetchMethod::CreatePolling(st3) => match st3.fut.poll_unpin(cx) {
                Ready(x) => {
                    hpp.mark_progress();
                    match x {
                        Ok(()) => {
                            error!("\n\n\n  TODO  create monitor  776062b8e  \n\n\n");
                            // let ret = make_cb(move |st2| {
                            //     st2.fetch_method = FetchMethod::Polling(FetchPolling {
                            //         req: FetchPollingReq::Idle(ErasedFuture::new(tokio::time::sleep(
                            //             Duration::from_millis(3000),
                            //         ))),
                            //     });
                            // });
                            // return Ready(Some(Ok(ret)));
                        }
                        Err(e) => {
                            return Ready(Some(Err(e)));
                        }
                    }
                }
                Pending => {
                    hpp.mark_pending();
                }
            },
            FetchMethod::Polling(st3) => match &mut st3.state {
                FetchPollingState::Idle(fut) => match fut.poll_unpin(cx) {
                    Ready(()) => {
                        trace4!("{selfname}  Polling Idle Done");
                        // let ioid = pres.fetch_data.ioid.inc();
                        let tsnow = Instant::now();
                        let msg = CaMsg::from_ty_ts(
                            CaMsgTy::ReadNotify(proto::ReadNotify {
                                data_type: pres.fetch_data.ca_dbr_ty.to_u16(),
                                data_count: pres.fetch_data.shape.to_ca_count().unwrap(),
                                sid: pres.sid.to_u32(),
                                // ioid: ioid.to_u32(),
                                ioid: 0,
                            }),
                            tsnow,
                        );
                        // {
                        //     // TODO emit channel status, but not on each poll
                        //     let item = ChannelStatusItem {
                        //         ts: self.tmp_ts_poll,
                        //         cssid: st2.channel.cssid.clone(),
                        //         status: ChannelStatus::MonitoringSilenceReadStart,
                        //     };
                        //     conf.wrst.emit_channel_status_item(
                        //         item,
                        //         Self::channel_status_qu(&mut self.iqdqs),
                        //         &mut self.mett,
                        //     )?;
                        // }

                        //

                        hpp.mark_progress();
                        pres.mett.read_notify_send().inc();

                        error!("\n\n\n  TODO  create monitor  0c976c2cb  \n\n\n");
                        // let cb = make_cb2(move |st2| {
                        //     st2.fetch_method = FetchMethod::Polling(FetchPolling {
                        //         req: FetchPollingReq::SendReq(ErasedFuture::new(async move {
                        //             // tokio::time::sleep(Duration::from_millis(3000)).await;
                        //             // TODO handle timeout, change state, metrics.
                        //             // warn!("poll timeout");
                        //         })),
                        //     })
                        // });
                        // let ret = FetchMethodPollOutput::CallbackOnRunning(cb, items);
                        // return Ready(Some(Ok(ret)));
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                FetchPollingState::WaitRes(fut) => match fut.poll_unpin(cx) {
                    Ready(()) => {
                        info!("{selfname}  Polling WaitRes Done");
                        hpp.mark_progress();
                        error!("\n\n\n  TODO  create monitor  fad6ffb3b  \n\n\n");
                        // let ret = make_cb(move |st2| {
                        //     st2.fetch_method = FetchMethod::Polling(FetchPolling {
                        //         req: FetchPollingReq::Idle(ErasedFuture::new(tokio::time::sleep(
                        //             Duration::from_millis(3000),
                        //         ))),
                        //     })
                        // });
                        // return Ready(Some(Ok(ret)));
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                _ => {
                    error!("TODO case not implemented");
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

#[derive(Debug)]
pub enum FetchmpxItem {
    CaMsgOut(CaMsg),
    CaMsgOutIoid(CaMsg, Sid, Instant),
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
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
}

impl Fetchmpx {
    pub fn new(sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy) -> Self {
        Self {
            state: State::Normal,
            sid,
            polling: FetchPolling::new(sid, scalar_type, shape, ca_dbr_ty),
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
                            proto::CaMsgTy::EventAddRes(item2) => {
                                if item2.payload_len == 0 {
                                    debug!("{selfname}  empty  EventAddRes");
                                }
                                self.mett.monitor_read_expected().inc();
                                error!("TODO forward EventAddRes to Monitoring");
                                hpp.mark_progress();
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
