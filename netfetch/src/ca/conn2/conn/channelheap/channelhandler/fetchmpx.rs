use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Ioid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::SubidOwned;
use crate::ca::conn2::conn::channelheap::ChHeapCmd;
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

const EFP1: usize = 300;

#[derive(Debug)]
enum FetchPollingReq {
    Idle(ErasedFuture<(), EFP1>),
    SendReq(ErasedFuture<(), EFP1>),
    WaitRes(ErasedFuture<(), EFP1>),
}

#[derive(Debug)]
struct FetchPolling {
    req: FetchPollingReq,
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
    CallbackOnRunning(Box<dyn FnOnce(&mut SomeData)>, Vec<ChannelHandlerItem>),
}

fn make_cb<F>(f: F) -> FetchMethodPollOutput
where
    F: FnOnce(&mut SomeData) + 'static,
{
    FetchMethodPollOutput::CallbackOnRunning(Box::new(f), Vec::new())
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
            FetchMethod::Polling(st3) => match &mut st3.req {
                FetchPollingReq::Idle(fut) => match fut.poll_unpin(cx) {
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

                        let items = vec![ChannelHandlerItem {
                            ts_create: tsnow,
                            inner: ItemInner::ProtoOutIoid(msg, pres.sid.clone()),
                        }];

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
                FetchPollingReq::SendReq(fut) => match fut.poll_unpin(cx) {
                    Ready(()) => {
                        debug!("{selfname}  Polling SendReq Done");
                        hpp.mark_progress();
                        error!("\n\n\n  TODO  create monitor  22e511ee2  \n\n\n");
                        // let items = Vec::new();
                        // let cb = make_cb2(move |st2| {
                        //     st2.fetch_method = FetchMethod::Polling(FetchPolling {
                        //         req: FetchPollingReq::WaitRes(ErasedFuture::new(async move {
                        //             tokio::time::sleep(Duration::from_millis(3000)).await;
                        //             // TODO handle timeout, change state, metrics.
                        //             warn!("poll timeout");
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
                FetchPollingReq::WaitRes(fut) => match fut.poll_unpin(cx) {
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
    ScyllaWrite,
}

#[derive(Debug)]
enum State {
    Normal,
    Done,
}

#[derive(Debug)]
pub struct Fetchmpx {
    state: State,
    sid: Sid,
    inp_buf: VecDeque<CaMsg>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
}

impl Fetchmpx {
    pub fn new(sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy) -> Self {
        Self {
            state: State::Normal,
            sid,
            inp_buf: VecDeque::with_capacity(8),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
        }
    }

    pub fn sid(&self) -> Sid {
        self.sid.clone()
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
    }

    fn poll_inp_dispatch(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_inp_dispatch";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        trace2!("{selfname}  ITEM  {item:?}");
                        match &item.ty {
                            proto::CaMsgTy::EventAddRes(item2) => {
                                if item2.payload_len == 0 {
                                    debug!("{selfname}  empty  EventAddRes");
                                }
                                // match &mut self2.fetch_method {
                                //     FetchMethod::CreateMonitor(st3) => {
                                //         // TODO metrics count here the first monitor event?
                                //         trace!(
                                //             "ChannelHandler:Running:ProtoDispatch:Ready:Some:CreateMonitor  try_send"
                                //         );
                                //         match Pin::new(st3).poll_msg_inp(item, cx) {
                                //             Some(x) => {
                                //                 hpp.mark_pending();
                                //                 self2.inp_buf.push_front(x);
                                //             }
                                //             None => {
                                //                 hpp.mark_progress();
                                //             }
                                //         }
                                //     }
                                //     FetchMethod::Monitor(st3) => {
                                //         self.mett.monitor_read_expected().inc();
                                //         trace!(
                                //             "ChannelHandler:Running:ProtoDispatch:Ready:Some:Monitor    TODO handle monitor update  {item:?}"
                                //         );
                                //     }
                                //     _ => {
                                //         error!("ChannelHandler:Running:Ready:Ok  TODO handle {item:?}");
                                //     }
                                hpp.mark_progress();
                            }
                            proto::CaMsgTy::EventAddResEmpty(item2) => {
                                hpp.mark_progress();
                            }
                            proto::CaMsgTy::ChannelCloseRes(item2) => {
                                hpp.mark_progress();
                            }
                            proto::CaMsgTy::ReadNotifyRes(msg) => {
                                // trace3!("TODO  handle {item:?}");
                                // match &mut self2.fetch_method {
                                //     FetchMethod::Polling(st3) => {
                                //         self2.mett.read_notify_recv().inc();
                                //         debug!("FetchMethod::Polling  recvd  go back to idle");
                                //         warn!("TODO use the correct idle time");
                                //         st3.req = FetchPollingReq::Idle(ErasedFuture::new(tokio::time::sleep(
                                //             Duration::from_millis(3000),
                                //         )))
                                //     }
                                //     _ => {
                                //         error!("unexpected ReadNotifyRes");
                                //     }
                                // }
                                hpp.mark_progress();
                            }
                            _ => {
                                trace!("channel_create: unexpected message {item:?}");
                                let e = Error::CreateMonitorUnexpectedMessage;
                                return Ready(Some(Err(e)));
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

impl Stream for Fetchmpx {
    type Item = Result<FetchmpxItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let selfname = "Fetchmpx::poll_next";
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
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            {
                // TODO poll internals of the fetch method logic.
                // That poll can return also a future that we must drive.
                // TODO monitoring and polling are mutually exclusive because we can anyway not distinguish
                // replies from the IOC on the same Cid.
                // Maybe Running state can for diagnostics also issue read-notify and remember the Ioid for itself.

                // TODO start here by polling the FetchMethod, which should initiate the timed states.

                // match self.fetch_method.poll_next_unpin(pres, cx) {
                //     Ready(Some(x)) => {
                //         hpp.mark_progress();
                //         match x {
                //             Ok(x) => match x {
                //                 FetchMethodPollOutput::None => {}
                //                 FetchMethodPollOutput::CallbackOnRunning(cb, mut items) => {
                //                     cb(st2);
                //                     if items.len() > 1 {
                //                         self2.outbuf.extend(items);
                //                     } else if let Some(item) = items.pop() {
                //                         break Ready(Some(Ok(item)));
                //                     } else {
                //                     }
                //                 }
                //             },
                //             Err(e) => {
                //                 hpp.mark_progress();
                //                 self2.state = State::Done;
                //                 break Ready(Some(Err(e)));
                //             }
                //         }
                //     }
                //     Ready(None) => {}
                //     Pending => {
                //         hpp.mark_pending();
                //     }
                // }
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
