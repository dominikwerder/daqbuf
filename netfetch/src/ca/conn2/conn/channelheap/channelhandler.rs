use crate::ca::conn2::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::CidOwned;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::Subid;
use crate::ca::conn2::caids::SubidOwned;
use crate::ca::conn2::conn::channelheap::ChHeapCmd;
use crate::ca::conn2::futwrap::FutDbg;
use crate::ca::conn2::futwrap::FutDbgBox;
use crate::ca::conn2::progpend::HaveProgressPending;
use crate::ca::conn2::synchan;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if true { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHandler"),
    enum variants {
        ProtoTxClosed,
        ProtoRxClosed,
        ChannelHandlerRxClosed,
        CreateMonitorUnexpectedMessage,
        Recv(#[from] asynchan::RecvError),
    },
);

#[derive(Debug)]
struct Creating {
    fut: FutDbg<Result<(Sid,), Error>>,
}

#[derive(Debug)]
struct CreateMonitor {
    out_tx: asynchan::Sender<CaMsg>,
    inp_tx: asynchan::Sender<CaMsg>,
    fut: FutDbg<Result<(), Error>>,
}

impl CreateMonitor {
    fn new(
        cid: Cid,
        sid: Sid,
        out_tx: asynchan::Sender<CaMsg>,
        inp_tx: asynchan::Sender<CaMsg>,
        mut inp_rx: asynchan::Receiver<CaMsg>,
        mut ch_hp_tx: asynchan::Sender<ChHeapCmd>,
    ) -> Self {
        let fut = {
            let mut proto_tx = out_tx.clone();
            async move {
                let subid = SubidOwned::new();
                {
                    let (mut reg_tx, mut reg_rx) = asynchan::bounded(4, "RegisterSubidResp");
                    ch_hp_tx
                        .send(ChHeapCmd::RegisterSubid(cid.clone(), subid.to_subid(), reg_tx))
                        .await
                        .map_err(|_| Error::ProtoTxClosed)?;
                    let _reg_res = reg_rx.recv().await?;
                    trace!("CreateMonitor: registered subid {subid:?}");
                    // TODO also store the chosen Subid somewhere for later.
                }
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
                loop {
                    let x = inp_rx.recv().await;
                    let item = x?;
                    use proto::CaMsgTy;
                    match &item.ty {
                        CaMsgTy::EventAddRes(k) => {
                            trace!("CreateMonitor:EventAddRes {k:?}");
                            break;
                        }
                        CaMsgTy::EventAddResEmpty(k) => {
                            trace!("CreateMonitor:EventAddResEmpty {k:?}");
                            break;
                        }
                        _ => {
                            error!("CreateMonitor  Error::CreateMonitorUnexpectedMessage");
                            return Err(Error::CreateMonitorUnexpectedMessage);
                        }
                    }
                }
                Ok(())
            }
        };
        Self {
            out_tx,
            inp_tx,
            fut: fut.box2(),
        }
    }
}

#[derive(Debug)]
enum FetchMethod {
    None,
    CreateMonitor(CreateMonitor),
    Monitor,
}

#[derive(Debug)]
struct Running {
    fetch_method: FetchMethod,
    sid: Sid,
}

impl Running {
    fn new(sid: Sid) -> Self {
        Self {
            fetch_method: FetchMethod::None,
            sid,
        }
    }
}

#[derive(Debug)]
enum State {
    Init,
    Creating(Creating),
    Running(Running),
    Done,
}

async fn channel_create(
    cid: u32,
    name: String,
    mut tx: asynchan::Sender<CaMsg>,
    mut inp_rx: asynchan::Receiver<CaMsg>,
    tsnow: Instant,
) -> Result<(Sid,), Error> {
    let msg = CaMsg::from_ty_ts(
        proto::CaMsgTy::CreateChan(proto::CreateChan {
            cid,
            channel: name.into(),
        }),
        tsnow,
    );
    tx.send(msg).await.map_err(|_| Error::ProtoTxClosed)?;
    // TODO make this more resilient to other messages
    loop {
        let x = inp_rx.recv().await;
        let item = x?;
        use proto::CaMsgTy;
        match &item.ty {
            CaMsgTy::CreateChanRes(k) => {
                trace!("CreateMonitor:CreateChanRes {k:?}");
                return Ok((Sid::new(k.sid),));
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
pub struct ChannelHandler {
    state: State,
    cid: CidOwned,
    conf: ChannelConfig,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_rx: asynchan::Receiver<CaMsg>,
    ch_hp_tx: asynchan::Sender<ChHeapCmd>,
    proto_rx_dispatch: Option<CaMsg>,
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

        // TODO to send the channel create message, I need to be in some async function.
        // Issue:
        // Even if this handler attempts a send, but hits Pending, then ChannelHeap will get
        // woken up at some point, but how does ChannelHeap know to poll this ChannelHandler again?
        // When polling, ChannelHeap must pass a specific Waker.

        Self {
            state: State::Init,
            cid,
            conf,
            proto_tx,
            proto_rx,
            ch_hp_tx,
            proto_rx_dispatch: None,
        }
    }

    pub fn cid(&self) -> Cid {
        self.cid.to_cid()
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

    fn proto_rx_poll(
        proto_rx: &mut asynchan::Receiver<CaMsg>,
        proto_rx_dispatch: &mut Option<CaMsg>,
        cx: &mut Context<'_>,
    ) -> Result<Poll<()>, Error> {
        use Poll::*;
        match proto_rx.poll_next_unpin(cx) {
            Ready(x) => match x {
                Some(item) => {
                    *proto_rx_dispatch = Some(item);
                    Ok(Ready(()))
                }
                None => {
                    trace!("ChannelHandler:Running:ProtoRx:Ready:None");
                    error!("ChannelHandler:Running:ProtoRx:Ready:None  TODO handle closed");
                    // TODO handle closed channel
                    Err(Error::ProtoRxClosed)
                }
            },
            Pending => {
                trace_pending!("ChannelHandler:Running:ProtoRx");
                Ok(Pending)
            }
        }
    }

    fn proto_rx_handle_dispatch(st1: &mut Running, item: CaMsg, cx: &mut Context<'_>) -> Result<Option<CaMsg>, Error> {
        match &item.ty {
            proto::CaMsgTy::EventAddRes(_) | proto::CaMsgTy::EventAddResEmpty(_) => {
                match &mut st1.fetch_method {
                    FetchMethod::CreateMonitor(st2) => {
                        trace!("ChannelHandler:Running:ProtoRx:Ready:Some:CreateMonitor  try_send");
                        // TODO send down channel must be async poll!
                        use asynchan::SendPoll;
                        use asynchan::SendPollError;
                        match st2.inp_tx.poll_send_unpin(item, cx) {
                            Ok(()) => {
                                trace!(
                                    "ChannelHandler:Running:ProtoRx:Ready:Some:CreateMonitor  sent to CreateMonitor"
                                );
                                Ok(None)
                            }
                            Err(e) => match e {
                                SendPollError::Full(item) => {
                                    error!(
                                        "ChannelHandler:Running:ProtoRx:Ready:Some:CreateMonitor  TODO must handle SendError::Full"
                                    );
                                    // TODO keep item in another buffer?
                                    Ok(Some(item))
                                }
                                SendPollError::Closed(item) => {
                                    error!(
                                        "ChannelHandler:Running:ProtoRx:Ready:Some:CreateMonitor  TODO must handle SendError::Closed"
                                    );
                                    Err(Error::ChannelHandlerRxClosed)
                                }
                            },
                        }
                    }
                    FetchMethod::Monitor => {
                        trace!(
                            "ChannelHandler:Running:ProtoRx:Ready:Some:Monitor    TODO handle monitor update{item:?}"
                        );
                        Ok(None)
                    }
                    _ => {
                        trace!("ChannelHandler:Running:Ready:Ok  TODO handle {item:?}");
                        Ok(None)
                    }
                }
            }
            _ => {
                trace!("ChannelHandler:Running:ProtoRx:Ready:Some  TODO handle {item:?}");
                Ok(None)
            }
        }
    }
}

impl Stream for ChannelHandler {
    type Item = Result<ChannelHandlerItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        trace4!("ChannelHandler:poll_next  {}", self.cid);
        'main: loop {
            let tsnow = Instant::now();
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Init => {
                    trace!("ChannelHandler:Init");
                    let fut = channel_create(
                        self2.cid.to_u32(),
                        self2.conf.name().into(),
                        self2.proto_tx.clone(),
                        self2.proto_rx.clone(),
                        tsnow,
                    )
                    .box2();
                    self2.state = State::Creating(Creating { fut });
                    hpp.mark_progress();
                }
                State::Creating(st1) => {
                    trace!("ChannelHandler:Creating");
                    match st1.fut.poll_unpin(cx) {
                        Ready(Ok((sid,))) => {
                            trace!("ChannelHandler:Creating:Ready:Ok  {sid}");
                            self2.state = State::Running(Running::new(sid));
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
                State::Running(st1) => {
                    match &mut st1.fetch_method {
                        FetchMethod::None => {
                            trace!("ChannelHandler:Running:FetchMethod:None");
                            let (inp_tx, inp_rx) = asynchan::bounded(4, "MonitorCreate");
                            let create_monitor = CreateMonitor::new(
                                self2.cid.to_cid(),
                                st1.sid.clone(),
                                self2.proto_tx.clone(),
                                inp_tx,
                                inp_rx,
                                self2.ch_hp_tx.clone(),
                            );
                            st1.fetch_method = FetchMethod::CreateMonitor(create_monitor);
                            hpp.mark_progress();
                        }
                        FetchMethod::CreateMonitor(st2) => {
                            trace!("ChannelHandler:Running:FetchMethod:CreateMonitor");
                            match st2.fut.poll_unpin(cx) {
                                Ready(Ok(())) => {
                                    trace!("ChannelHandler:Running:FetchMethod:CreateMonitor:Ready:Ok");
                                    trace!(
                                        "ChannelHandler:Running:FetchMethod:CreateMonitor:Ready:Ok  TODO implement monitor handling"
                                    );
                                    st1.fetch_method = FetchMethod::Monitor;
                                    hpp.mark_progress();
                                }
                                Ready(Err(e)) => {
                                    error!(
                                        "ChannelHandler:Running:FetchMethod:CreateMonitor:Ready:Err  TODO  handle  {e}"
                                    );
                                    self2.state = State::Done;
                                    hpp.mark_progress();
                                    break Ready(Some(Err(e)));
                                }
                                Pending => {
                                    trace_pending!("ChannelHandler:Running:FetchMethod:CreateMonitor");
                                    hpp.mark_pending();
                                }
                            }
                        }
                        FetchMethod::Monitor => {
                            trace!("ChannelHandler:Running:FetchMethod:Monitor  TODO");
                        }
                    }
                    loop {
                        let hpp2 = &mut hpp;
                        let mut hpp = HaveProgressPending::new();
                        if let Some(item) = self2.proto_rx_dispatch.take() {
                            match Self::proto_rx_handle_dispatch(st1, item, cx) {
                                Ok(x) => match x {
                                    Some(item) => {
                                        self2.proto_rx_dispatch = Some(item);
                                        hpp.mark_pending();
                                    }
                                    None => {
                                        hpp.mark_progress();
                                    }
                                },
                                Err(e) => {
                                    self2.state = State::Done;
                                    hpp.mark_progress();
                                    break 'main Ready(Some(Err(e)));
                                }
                            }
                        } else {
                            match Self::proto_rx_poll(&mut self2.proto_rx, &mut self2.proto_rx_dispatch, cx) {
                                Ok(Ready(())) => {}
                                Ok(Pending) => {
                                    hpp.mark_pending();
                                    break;
                                }
                                Err(e) => {
                                    self2.state = State::Done;
                                    hpp.mark_progress();
                                    break 'main Ready(Some(Err(e)));
                                }
                            }
                        }
                        if hpp.have_progress() {
                            trace!("ChannelHandler:Running:ProtoRx:loop:HPP:Progress");
                            continue;
                        } else if hpp.have_pending() {
                            trace_pending!("ChannelHandler:Running:ProtoRx:loop:HPP");
                            hpp2.mark_pending();
                            break;
                        } else {
                            trace!("ChannelHandler:Running:ProtoRx:loop:HPP:Done");
                            break;
                        }
                    }
                }
                State::Done => {
                    trace!("ChannelHandler:Done");
                }
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
