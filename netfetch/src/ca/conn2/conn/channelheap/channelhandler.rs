use crate::ca::conn2::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::CidOwned;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::Subid;
use crate::ca::conn2::caids::SubidOwned;
use crate::ca::conn2::conn::channelheap::ChHeapCmd;
use crate::ca::conn2::timeoutable;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use asynchan::SendPoll;
use asynchan::SendPollError;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use timeoutable::Timeoutable;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHandler"),
    enum variants {
        ProtoTxClosed,
        ProtoRxClosed,
        ChannelHandlerRxClosed,
        CreateMonitorUnexpectedMessage,
        Recv(#[from] asynchan::RecvError),
        TimeoutError(#[from] timeoutable::TimeoutError),
        Logic,
    },
);

#[derive(Debug)]
struct Init {
    proto_rx: asynchan::Receiver<CaMsg>,
}

#[derive(Debug)]
struct Creating {
    fut: FutDbg<Result<(Sid, asynchan::Receiver<CaMsg>), Error>>,
    removing: bool,
}

#[derive(Debug)]
struct Closing1 {
    fut: FutDbg<Result<(), Error>>,
    done_tx: Option<asynchan::Sender<u32>>,
}

#[derive(Debug)]
struct Closing2 {
    done_tx: Option<asynchan::Sender<u32>>,
}

#[derive(Debug)]
struct CreateMonitor {
    fut: FutDbg<Result<(), Error>>,
    inp_buf: VecDeque<CaMsg>,
}

impl CreateMonitor {
    fn new(
        cid: Cid,
        sid: Sid,
        out_tx: asynchan::Sender<CaMsg>,
        mut ch_hp_tx: asynchan::Sender<ChHeapCmd>,
        chconf: ChannelConfig,
    ) -> Self {
        let fut = {
            let mut proto_tx = out_tx;
            async move {
                let subid = SubidOwned::new();
                {
                    let (reg_tx, mut reg_rx) = asynchan::bounded(4, "RegisterSubidResp");
                    ch_hp_tx
                        .send(ChHeapCmd::RegisterSubid(cid.clone(), subid.to_subid(), reg_tx))
                        .await
                        .map_err(|_| Error::ProtoTxClosed)?;
                    let _reg_res = reg_rx.recv().await?;
                    trace!("CreateMonitor: registered subid {subid:?}");
                    // TODO also store the chosen Subid somewhere for later.
                }
                error!("TODO do not hard code the CA data types");
                // hard code f32
                let data_type = 16;
                let data_count = 0;
                let msg = CaMsg::from_ty_ts(
                    proto::CaMsgTy::EventAdd(proto::EventAdd::new(
                        data_type,
                        data_count,
                        sid.to_u32(),
                        subid.to_u32(),
                    )),
                    Instant::now(),
                );
                proto_tx.send(msg).await.map_err(|_| Error::ProtoTxClosed)?;
                Ok(())
            }
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

    fn poll_unpin(mut self: Pin<&mut Self>, msg: CaMsg, cx: &mut Context) -> Option<CaMsg> {
        use Poll::*;
        todo!("TODO CreateMonitor poll_unpin")
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
    Monitor,
    CreatePolling(CreatePolling),
    Polling(FetchPolling),
}

enum FetchMethodPollOutput {
    None,
    CallbackOnRunning(Box<dyn FnOnce(&mut Running)>),
}

struct FetchMethodPollRes<'a> {
    conf: &'a ChannelConfig,
    cid: Cid,
    sid: Sid,
    proto_tx: &'a asynchan::Sender<CaMsg>,
    ch_hp_tx: &'a asynchan::Sender<ChHeapCmd>,
}

impl FetchMethod {
    fn poll_next_unpin(
        mut self: Pin<&mut Self>,
        // st2: &mut Running,
        pres: FetchMethodPollRes,
        cx: &mut Context,
    ) -> Poll<Option<Result<FetchMethodPollOutput, Error>>> {
        let selfname = "FetchMethod::poll_next_unpin";
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        fn make_cb<F>(f: F) -> FetchMethodPollOutput
        where
            F: FnOnce(&mut Running) + 'static,
        {
            FetchMethodPollOutput::CallbackOnRunning(Box::new(f))
        }
        match self.as_mut().get_mut() {
            FetchMethod::None => {
                trace!("{selfname}  None\n\n\n====================== {}", pres.conf.is_polled());
                hpp.mark_progress();
                if pres.conf.is_polled() {
                    let (inp_tx, inp_rx) = asynchan::bounded(4, "CreatePolling");
                    let create = CreatePolling::new(
                        pres.cid.clone(),
                        pres.sid.clone(),
                        pres.proto_tx.clone(),
                        inp_tx,
                        inp_rx,
                        pres.ch_hp_tx.clone(),
                        pres.conf.clone(),
                    );
                    let ret = make_cb(move |st2| {
                        st2.fetch_method = FetchMethod::CreatePolling(create);
                    });
                    return Ready(Some(Ok(ret)));
                } else {
                    let create = CreateMonitor::new(
                        pres.cid.clone(),
                        pres.sid.clone(),
                        pres.proto_tx.clone(),
                        pres.ch_hp_tx.clone(),
                        pres.conf.clone(),
                    );
                    let ret = make_cb(move |st2| {
                        st2.fetch_method = FetchMethod::CreateMonitor(create);
                    });
                    return Ready(Some(Ok(ret)));
                }
            }
            FetchMethod::CreateMonitor(st3) => {
                trace!("{selfname}  CreateMonitor");
                match st3.fut.poll_unpin(cx) {
                    Ready(x) => match x {
                        Ok(()) => {
                            trace!("{selfname}  CreateMonitor:Ready:Ok");
                            trace!("{selfname}  :Ready:Ok  TODO implement monitor handling");
                            hpp.mark_progress();
                            let ret = make_cb(move |st2| {
                                st2.fetch_method = FetchMethod::Monitor;
                            });
                            return Ready(Some(Ok(ret)));
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
            FetchMethod::Monitor => {
                // At the moment, nothing to do here.
                // TODO here, probably good to do some housekeeping on timeout:
                // like promote last written value to next longer retention time.
            }
            FetchMethod::CreatePolling(st3) => match st3.fut.poll_unpin(cx) {
                Ready(x) => {
                    hpp.mark_progress();
                    match x {
                        Ok(()) => {
                            let ret = make_cb(move |st2| {
                                st2.fetch_method = FetchMethod::Polling(FetchPolling {
                                    req: FetchPollingReq::Idle(ErasedFuture::new(tokio::time::sleep(
                                        Duration::from_millis(3000),
                                    ))),
                                });
                            });
                            return Ready(Some(Ok(ret)));
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
                        info!("{selfname}  Polling Idle Done");
                        hpp.mark_progress();
                        let ret = make_cb(move |st2| {
                            st2.fetch_method = FetchMethod::Polling(FetchPolling {
                                req: FetchPollingReq::SendReq(ErasedFuture::new(tokio::time::sleep(
                                    Duration::from_millis(3000),
                                ))),
                            })
                        });
                        return Ready(Some(Ok(ret)));
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                FetchPollingReq::SendReq(fut) => match fut.poll_unpin(cx) {
                    Ready(()) => {
                        info!("{selfname}  Polling SendReq Done");
                        hpp.mark_progress();
                        let ret = make_cb(move |st2| {
                            st2.fetch_method = FetchMethod::Polling(FetchPolling {
                                req: FetchPollingReq::WaitRes(ErasedFuture::new(tokio::time::sleep(
                                    Duration::from_millis(3000),
                                ))),
                            })
                        });
                        return Ready(Some(Ok(ret)));
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                FetchPollingReq::WaitRes(fut) => match fut.poll_unpin(cx) {
                    Ready(()) => {
                        info!("{selfname}  Polling WaitRes Done");
                        hpp.mark_progress();
                        let ret = make_cb(move |st2| {
                            st2.fetch_method = FetchMethod::Polling(FetchPolling {
                                req: FetchPollingReq::Idle(ErasedFuture::new(tokio::time::sleep(
                                    Duration::from_millis(3000),
                                ))),
                            })
                        });
                        return Ready(Some(Ok(ret)));
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
struct Running {
    fetch_method: FetchMethod,
    sid: Sid,
    proto_rx: asynchan::Receiver<CaMsg>,
    removing: bool,
    chan_close_ack: bool,
    outbuf: VecDeque<CaMsg>,
    remove_done_tx: Option<asynchan::Sender<u32>>,
}

impl Running {
    fn new(sid: Sid, proto_rx: asynchan::Receiver<CaMsg>) -> Self {
        Self {
            fetch_method: FetchMethod::None,
            sid,
            proto_rx,
            removing: false,
            chan_close_ack: false,
            outbuf: VecDeque::new(),
            remove_done_tx: None,
        }
    }
}

#[derive(Debug)]
enum State {
    Init(Init),
    Creating(Creating),
    Running(Running),
    Closing1(Closing1),
    Closing2(Closing2),
    Done1,
    Done,
    Dummy,
}

async fn channel_create(
    cid: u32,
    name: String,
    mut tx: asynchan::Sender<CaMsg>,
    mut proto_rx: asynchan::Receiver<CaMsg>,
    tsnow: Instant,
) -> Result<(Sid, asynchan::Receiver<CaMsg>), Error> {
    let msg = CaMsg::from_ty_ts(
        proto::CaMsgTy::CreateChan(proto::CreateChan {
            cid,
            channel: name.into(),
        }),
        tsnow,
    );
    let to = Duration::from_millis(5000);
    tx.send(msg).timeout(to).await?.map_err(|_| Error::ProtoTxClosed)?;
    // TODO make this more resilient to other messages
    loop {
        let x = proto_rx.recv().await;
        let item = x?;
        use proto::CaMsgTy;
        match &item.ty {
            CaMsgTy::CreateChanRes(k) => {
                trace!("CreateMonitor:CreateChanRes {k:?}");
                return Ok((Sid::new(k.sid), proto_rx));
            }
            CaMsgTy::CreateChanFail(k) => {
                trace!("CreateMonitor:CreateChanFail {k:?}");
                // TODO
                // Must cause a re-search of the channel
                return Err(Error::CreateMonitorUnexpectedMessage);
            }
            CaMsgTy::AccessRightsRes(k) => {
                trace!("CreateMonitor:AccessRightsRes {k:?}");
            }
            _ => {
                trace!("channel_create: unexpected message {item:?}");
                return Err(Error::CreateMonitorUnexpectedMessage);
            }
        }
    }
}

#[derive(Debug)]
pub enum ItemInner {
    ScyllaWrite,
}

#[derive(Debug)]
pub struct ChannelHandlerItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

#[derive(Debug)]
pub struct StatusInfo {
    pub counters: Counters,
}

#[derive(Debug, Clone)]
pub struct Counters {
    pub event_add_res_cnt: u64,
}

impl Counters {
    fn new() -> Self {
        Self { event_add_res_cnt: 0 }
    }
}

#[derive(Debug)]
pub enum Cmd {
    Remove(asynchan::Sender<u32>),
}

#[derive(Debug)]
pub struct ChannelHandler {
    state: State,
    cid: CidOwned,
    conf: ChannelConfig,
    proto_tx: asynchan::Sender<CaMsg>,
    ch_hp_tx: asynchan::Sender<ChHeapCmd>,
    proto_rx_dispatch: Option<CaMsg>,
    counters: Counters,
    cmd_tx: asynchan::Sender<Cmd>,
    cmd_rx: asynchan::Receiver<Cmd>,
}

impl ChannelHandler {
    pub fn new(
        conf: ChannelConfig,
        proto_tx: asynchan::Sender<CaMsg>,
        proto_rx: asynchan::Receiver<CaMsg>,
        ch_hp_tx: asynchan::Sender<ChHeapCmd>,
    ) -> Self {
        let cid = CidOwned::new();
        trace!("ChannelHandler::new  {cid:?}  {conf:?}");

        let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelHandler-cmd");

        // TODO to send the channel create message, I need to be in some async function.
        // Issue:
        // Even if this handler attempts a send, but hits Pending, then ChannelHeap will get
        // woken up at some point, but how does ChannelHeap know to poll this ChannelHandler again?
        // When polling, ChannelHeap must pass a specific Waker.

        Self {
            state: State::Init(Init { proto_rx }),
            cid,
            conf,
            proto_tx,
            ch_hp_tx,
            proto_rx_dispatch: None,
            counters: Counters::new(),
            cmd_tx,
            cmd_rx,
        }
    }

    pub fn status_info(&self) -> StatusInfo {
        StatusInfo {
            counters: self.counters.clone(),
        }
    }

    pub fn cid(&self) -> Cid {
        self.cid.to_cid()
    }

    pub fn cmd_tx(&self) -> &asynchan::Sender<Cmd> {
        &self.cmd_tx
    }

    pub fn poll_housekeeping_1(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        // TODO do periodic tasks here.
        // TODO poll the bin writer.
        // TODO poll channel data flush?
        // TODO we want to achieve rather low latency... therefore, better to return scylla writes via regular Stream.
        todo!()
    }

    pub fn poll_read_1(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        // TODO check and kick-off polling read.
        // TODO count how often we cause Pending.
        todo!()
    }

    fn handle_cmd(&mut self, cmd: Cmd) {
        let selfname = "handle_cmd";
        match cmd {
            Cmd::Remove(done_tx) => {
                match &mut self.state {
                    State::Init(_st2) => {
                        self.state = State::Done;
                    }
                    State::Creating(_) => {
                        let Creating { fut: fut_sid, removing } =
                            if let State::Creating(st2) = std::mem::replace(&mut self.state, State::Dummy) {
                                st2
                            } else {
                                panic!()
                            };
                        // TODO add flags to Creating so that we now what proto messages we still expect
                        // TODO add timeout to Creating (anyways!)
                        // TODO keep done_tx and signal when channel remove done

                        /*
                        let cid = self.cid();
                        let mut proto_tx = self.proto_tx.clone();
                        let fut = async move {
                            let (sid, rx) = fut_sid.timeout(Duration::from_millis(1200)).await??;
                            let tsnow = Instant::now();
                            let item = CaMsg::from_ty_ts(
                                proto::CaMsgTy::ChannelClose(proto::ChannelClose {
                                    sid: sid.to_u32(),
                                    cid: cid.to_u32(),
                                }),
                                tsnow,
                            );
                            if proto_tx.send(item).await.is_err() {
                                error!("{selfname} proto_tx send fail");
                            }
                            Ok(())
                        };
                        self.state = State::Closing1(Closing1 {
                            fut: fut.box2(),
                            done_tx,
                        });
                        */
                    }
                    State::Running(st2) => {
                        /*
                        let Running {
                            fetch_method,
                            sid,
                            proto_rx,
                            removing,
                            outbuf,
                        } = if let State::Running(st2) = std::mem::replace(&mut self.state, State::Dummy) {
                            st2
                        } else {
                            panic!()
                        };
                        */
                        let sid = st2.sid.clone();
                        let cid = self.cid.to_cid();
                        // TODO
                        // add necessary commands to outbuf.
                        // in poll loop, check for outbuf and poll emit.
                        // handle:
                        // CA_PROTO_EVENT_CANCEL leads to 0-size CA_PROTO_EVENT_ADD response
                        // CA_PROTO_CLEAR_CHANNEL leads to CA_PROTO_CLEAR_CHANNEL response
                        // and flag when those messages come in "removing" mode.
                        // Otherwise, the IOC may also shut down of course.
                        // TODO make sure the IOC disconnect triggers correct logic in ingest. (log!)
                        // When we are in removing mode, and received all cleanup confirmations, then trigger state change.

                        st2.removing = true;
                        st2.remove_done_tx = Some(done_tx);

                        let tsnow = Instant::now();
                        let item = CaMsg::from_ty_ts(
                            proto::CaMsgTy::ChannelClose(proto::ChannelClose {
                                sid: sid.to_u32(),
                                cid: cid.to_u32(),
                            }),
                            tsnow,
                        );
                        st2.outbuf.push_back(item);

                        /*
                        let sid = sid.clone();
                        let cid = self.cid();
                        match fetch_method {
                            FetchMethod::None => {}
                            FetchMethod::CreateMonitor(create_monitor) => {
                                todo!("TODO should wait for creation, then remove again")
                            }
                            FetchMethod::Monitor => todo!("TODO use subid to cancel it"),
                        }
                        let mut proto_tx = self.proto_tx.clone();
                        let mut proto_rx = proto_rx;
                        let fut = async move {
                            // TODO at the same time, must continue to poll protocol.
                            // Must expect within a timeout the following server messages:
                            // If we had a subscription ongoing:
                            // CA_PROTO_EVENT_CANCEL leads to 0-size CA_PROTO_EVENT_ADD response
                            // CA_PROTO_CLEAR_CHANNEL leads to CA_PROTO_CLEAR_CHANNEL response

                            // TODO poll the running state as if it was still in Running.
                            //   except, do not poll commands like config change etc.
                            //   The goal is to just finish up and close shop.
                            let tsnow = Instant::now();
                            let item = CaMsg::from_ty_ts(
                                proto::CaMsgTy::ChannelClose(proto::ChannelClose {
                                    sid: sid.to_u32(),
                                    cid: cid.to_u32(),
                                }),
                                tsnow,
                            );
                            let mut f1 = proto_tx.send(item);
                            let mut f1e = true;
                            // if proto_tx.send(item).await.is_err() {
                            //     error!("{selfname} proto_tx send fail");
                            // }
                            let mut f2 = proto_rx.recv();
                            let mut f2e = true;
                            loop {
                                tokio::select! {
                                    x = &mut f1, if f1e => {
                                        f1e = false;
                                        if x.is_err() {
                                            error!("{selfname} can not emit to proto");
                                            break;
                                        }
                                    }
                                    e = &mut f2, if f2e => {
                                        f2 = proto_rx.recv();
                                        f2e = true;
                                        match e {
                                            Ok(x) => {
                                                match x.ty {
                                                }
                                            }
                                            Err(e) => {
                                                // TODO handle the error case
                                                // If connection got dropped, that's not nice, but ok.
                                            }
                                        }
                                    }
                                }
                            }
                            todo!("continue to poll proto_rx, process channel close confirm");
                            Ok(())
                        };
                        self.state = State::Closing1(Closing1 {
                            fut: fut.box2(),
                            done_tx,
                        });
                        */
                    }
                    State::Closing1(st2) => {
                        error!("{selfname} received Remove in State::Closing1");
                    }
                    State::Closing2(st2) => {
                        error!("{selfname} received Remove in State::Closing2");
                    }
                    State::Done1 => {
                        error!("{selfname} received Remove in State::Done1");
                        panic!()
                    }
                    State::Done => {
                        error!("{selfname} received Remove in State::Done");
                        panic!()
                    }
                    State::Dummy => panic!(),
                }
                // TODO send proto msg to cancel monitors.
                // TODO check if we have some open IO, and wait for some timeout.
                // There is already IO in the "normal" code path.
                // Must not duplicate code there.
                // So, maybe this means simply waiting and periodically checking?
                // Or: register a optional callback on-io-done. In that callback, we can signal progress?
                // TODO async send to proto to close the channel.
                // TODO wait for channel close confirm, under timeout.
                // TODO async write status event and final stats.
                // TODO done tx send in response to this command.
                // TODO transition to Done.
            }
        }
    }
}

struct PollProtoRx<'a> {
    cid: Cid,
    proto_rx_dispatch: &'a mut Option<CaMsg>,
    counters: &'a mut Counters,
}

impl<'a> PollProtoRx<'a> {
    fn proto_rx_handle_dispatch(
        st2: &mut Running,
        item: CaMsg,
        cx: &mut Context<'_>,
        counters: &mut Counters,
    ) -> Result<Option<CaMsg>, Error> {
        match &item.ty {
            proto::CaMsgTy::EventAddRes(item2) => {
                if item2.payload_len == 0 {
                    info!("\n\nempty EventAddRes\n\n");
                }
                match &mut st2.fetch_method {
                    FetchMethod::CreateMonitor(st3) => {
                        trace!("ChannelHandler:Running:ProtoDispatch:Ready:Some:CreateMonitor  try_send");
                        match Pin::new(st3).poll_msg_inp(item, cx) {
                            Some(x) => Ok(Some(x)),
                            None => Ok(None),
                        }
                    }
                    FetchMethod::Monitor => {
                        trace!(
                            "ChannelHandler:Running:ProtoDispatch:Ready:Some:Monitor    TODO handle monitor update  {item:?}"
                        );
                        counters.event_add_res_cnt += 1;
                        Ok(None)
                    }
                    _ => {
                        error!("ChannelHandler:Running:Ready:Ok  TODO handle {item:?}");
                        Ok(None)
                    }
                }
            }
            proto::CaMsgTy::EventAddResEmpty(item2) => {
                warn!("TODO ------- {}", "EventAddResEmpty");
                Ok(None)
            }
            proto::CaMsgTy::ChannelCloseRes(item2) => {
                if st2.removing {
                    trace!("NOTE -------- {}", "ChannelCloseRes");
                    st2.chan_close_ack = true;
                } else {
                    error!("TODO not removing but got {}", "ChannelCloseRes");
                    // TODO abort?
                }
                Ok(None)
            }
            _ => {
                warn!("---------->> ChannelHandler:Running:ProtoDispatch:Ready:Some  TODO handle {item:?}");
                Ok(None)
            }
        }
    }

    fn poll_proto_rx(
        mut self: Pin<&mut Self>,
        st2: &mut Running,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_proto_rx";
        use Poll::*;
        trace4!("{selfname}  {}", self.cid);
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(item) = self.as_mut().proto_rx_dispatch.take() {
                match Self::proto_rx_handle_dispatch(st2, item, cx, &mut self.counters) {
                    Ok(x) => match x {
                        Some(item) => {
                            trace2!("{selfname}  item came back");
                            *self.proto_rx_dispatch = Some(item);
                            hpp.mark_pending();
                        }
                        None => {
                            trace2!("{selfname}  item delivered");
                            hpp.mark_progress();
                        }
                    },
                    Err(e) => {
                        error!("{selfname}  {e}");
                        break Ready(Some(Err(e)));
                    }
                }
            } else {
                match st2.proto_rx.poll_next_unpin(cx) {
                    Ready(x) => match x {
                        Some(item) => {
                            *self.proto_rx_dispatch = Some(item);
                            hpp.mark_progress();
                        }
                        None => {
                            error!("ChannelHandler:Running:ProtoRx:Ready:None  TODO handle closed");
                            hpp.mark_progress();
                            break Ready(Some(Err(Error::ProtoRxClosed)));
                        }
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if hpp.have_progress() {
                trace!("{selfname}  HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("{selfname}  HPP");
                break Pending;
            } else {
                trace!("{selfname}  HPP:Done");
                break Ready(None);
            }
        }
    }
}

impl Stream for ChannelHandler {
    type Item = Result<ChannelHandlerItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfname = "ChannelHandler:poll_next";
        trace4!("{selfname}  {}", self.cid);
        loop {
            let tsnow = Instant::now();
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Done => match self2.cmd_rx.poll_next_unpin(cx) {
                    Ready(Some(_)) => {
                        hpp.mark_progress();
                        warn!("received command while Done");
                        // TODO count metrics
                        // ignore commands when Done
                    }
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                _ => match self2.cmd_rx.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        self2.handle_cmd(x);
                    }
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
                },
            }
            match &mut self2.state {
                State::Init(st2) => {
                    trace!("ChannelHandler:Init");
                    let st_old = std::mem::replace(&mut self2.state, State::Dummy);
                    let proto_rx = if let State::Init(st1) = st_old {
                        st1.proto_rx
                    } else {
                        panic!("logic")
                    };
                    let fut = channel_create(
                        self2.cid.to_u32(),
                        self2.conf.name().into(),
                        self2.proto_tx.clone(),
                        proto_rx,
                        tsnow,
                    )
                    .box2();
                    self2.state = State::Creating(Creating { fut, removing: false });
                    hpp.mark_progress();
                }
                State::Creating(st1) => {
                    trace!("ChannelHandler:Creating");
                    match st1.fut.poll_unpin(cx) {
                        Ready(Ok((sid, proto_rx))) => {
                            trace!("ChannelHandler:Creating:Ready:Ok  {sid}");
                            self2.state = State::Running(Running::new(sid, proto_rx));
                            hpp.mark_progress();
                        }
                        Ready(Err(e)) => {
                            trace!("ChannelHandler:Creating:Ready:Err {e}");
                            self2.state = State::Done;
                            hpp.mark_progress();
                            break Ready(Some(Err(e)));
                        }
                        Pending => {
                            trace_pending!("ChannelHandler:Creating");
                            hpp.mark_pending();
                        }
                    }
                }
                State::Running(st2) => {
                    if st2.removing {
                        if let Some(item) = st2.outbuf.pop_front() {
                            match self2.proto_tx.poll_send_unpin(item, cx) {
                                Ok(()) => {
                                    hpp.mark_progress();
                                }
                                Err(e) => match e {
                                    SendPollError::Full(x) => {
                                        hpp.mark_pending();
                                        st2.outbuf.push_front(x);
                                    }
                                    SendPollError::Closed(_) => {
                                        hpp.mark_progress();
                                        error!("{selfname}  can not close channel, no proto");
                                        // TODO handle better? metrics.
                                        st2.chan_close_ack = true;
                                        st2.outbuf.clear();
                                    }
                                },
                            }
                        }
                    }
                    {
                        let mut pr = PollProtoRx {
                            cid: self2.cid.to_cid(),
                            proto_rx_dispatch: &mut self2.proto_rx_dispatch,
                            counters: &mut self2.counters,
                        };
                        match Pin::new(&mut pr).poll_proto_rx(st2, cx) {
                            Ready(Some(x)) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(()) => {}
                                    Err(e) => {
                                        break Ready(Some(Err(e)));
                                    }
                                }
                            }
                            Ready(None) => {}
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    }
                    let pres = FetchMethodPollRes {
                        conf: &self2.conf,
                        cid: self2.cid.to_cid(),
                        sid: st2.sid.clone(),
                        proto_tx: &self2.proto_tx,
                        ch_hp_tx: &self2.ch_hp_tx,
                    };
                    match Pin::new(&mut st2.fetch_method).poll_next_unpin(pres, cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => match x {
                                    FetchMethodPollOutput::None => {}
                                    FetchMethodPollOutput::CallbackOnRunning(cb) => {
                                        cb(st2);
                                    }
                                },
                                Err(e) => {
                                    hpp.mark_progress();
                                    self2.state = State::Done;
                                    break Ready(Some(Err(e)));
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    if st2.outbuf.len() == 0 && st2.chan_close_ack {
                        info!("{selfname}  -----------------------------------------------------------");
                        info!("{selfname}  State::Running  chan_close_ack");
                        hpp.mark_progress();
                        let fut = async move { Ok(()) };
                        self.state = State::Closing1(Closing1 {
                            fut: fut.box2(),
                            done_tx: st2.remove_done_tx.take(),
                        });
                    }
                }
                State::Closing1(st2) => match st2.fut.poll_unpin(cx) {
                    Ready(x) => {
                        hpp.mark_progress();
                        match x {
                            Ok(()) => {
                                trace2!("Closing1 done");
                                self.state = State::Closing2(Closing2 {
                                    done_tx: st2.done_tx.take(),
                                });
                            }
                            Err(e) => {
                                info!("Closing1  {e}");
                                break Ready(Some(Err(e)));
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::Closing2(st2) => {
                    if let Some(tx) = &mut st2.done_tx {
                        match tx.poll_send_unpin(0, cx) {
                            Ok(()) => {
                                hpp.mark_progress();
                                self.state = State::Done1;
                            }
                            Err(e) => match e {
                                SendPollError::Full(_) => {
                                    hpp.mark_pending();
                                }
                                SendPollError::Closed(_) => {
                                    hpp.mark_progress();
                                    error!("{selfname}  can not signal Remove Done");
                                    // TODO metrics
                                    self.state = State::Done1;
                                }
                            },
                        }
                    } else {
                        hpp.mark_progress();
                        self.state = State::Done1;
                    }
                }
                State::Done1 => {
                    trace!("ChannelHandler:Done1");
                    self.state = State::Done;
                    hpp.mark_progress();
                }
                State::Done => {}
                State::Dummy => break Ready(Some(Err(Error::Logic))),
            }
            break if hpp.have_progress() {
                trace!("HPP:Progress");
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
