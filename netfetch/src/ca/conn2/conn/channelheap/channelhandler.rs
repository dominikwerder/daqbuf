mod create;
mod fetchmpx;
mod running;

use crate::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::CidOwned;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler::create::Creating;
use crate::ca::conn2::conn::channelheap::channelhandler::running::Running;
use crate::ca::conn2::locallog;
use crate::ca::conn2::timeoutable;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use hashbrown::HashMap;
use netpod::channelstatus::ChannelStatus;
use serde::Serialize;
use serieswriter::binwriter::BinWriter;
use stats::mett::ChannelHandlerMetrics;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use timeoutable::Timeoutable;

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
    name(Error, "ChannelHandler"),
    enum variants {
        ProtoTxClosed,
        ProtoRxClosed,
        ChannelHandlerRxClosed,
        CreateMonitorUnexpectedMessage,
        Recv,
        TimeoutError(#[from] timeoutable::TimeoutError),
        Creating(#[from] create::Error),
        Running(#[from] running::Error),
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
pub enum ClosingReason {
    ErrorMsg(String),
    InputDone,
    Command,
}

#[derive(Debug)]
struct Init {}

#[derive(Debug)]
struct Closing1 {
    chan_close_ack: bool,
    fut: Option<FutDbg<Result<(), Error>>>,
    to: FutDbg<()>,
}

#[derive(Debug)]
struct Closing2 {}

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

impl State {
    fn str(&self) -> &str {
        self.name_short()
    }
}

impl State {
    fn name_short(&self) -> &str {
        match self {
            State::Init(..) => "Init",
            State::Creating(st) => st.name_short(),
            State::Running(..) => "Running",
            State::Closing1(..) => "Closing1",
            State::Closing2(..) => "Closing2",
            State::Done1 => "Done1",
            State::Done => "Done",
            State::Dummy => "Dummy",
        }
    }
}

#[derive(Debug)]
pub enum ItemInner {
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    ProtoOut(CaMsg),
    ProtoOutIoid(CaMsg, Sid, Instant),
    ProtoOutSubid(CaMsg, Instant),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelStatus(ChannelStatus),
    ChannelEventValue(ChannelEventValue),
}

#[derive(Debug)]
pub struct ChannelHandlerItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

#[derive(Debug, Serialize)]
pub struct StatusInfo {
    pub counters: Counters,
}

#[derive(Debug, Clone, Serialize)]
pub struct Counters {
    pub event_add_res_cnt: u64,
}

impl Counters {
    fn new() -> Self {
        Self { event_add_res_cnt: 0 }
    }
}

#[derive(Debug)]
pub struct ChannelHandlerStatusResponse {}

#[derive(Debug)]
pub struct ChannelHandlerStatusRequest {
    tx: asynchan::Sender<ChannelHandlerStatusResponse>,
}

#[derive(Debug)]
pub enum Cmd {
    Remove(asynchan::Sender<u32>),
    ChannelHandlerStatus(ChannelHandlerStatusRequest),
}

#[derive(Debug)]
pub struct ChannelHandler {
    state: State,
    removing: Option<asynchan::Sender<u32>>,
    cid: CidOwned,
    backend: String,
    conf: ChannelConfig,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_inp_buf: VecDeque<ProtoRxItem>,
    proto_inp_done: bool,
    counters: Counters,
    cmd_tx: asynchan::Sender<Cmd>,
    cmd_rx: asynchan::Receiver<Cmd>,
    outbuf: VecDeque<ChannelHandlerItem>,
    mett: ChannelHandlerMetrics,
    waker_1: Option<Waker>,
    waker_2: Option<Waker>,
}

impl ChannelHandler {
    pub fn new(
        backend: String,
        conf: ChannelConfig,
        // TODO remove the asyc proto channels.
        proto_tx: asynchan::Sender<CaMsg>,
    ) -> Self {
        let cid = CidOwned::new();
        trace!("ChannelHandler::new  {cid:?}  {conf:?}");
        let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelHandler-cmd");
        Self {
            state: State::Init(Init {}),
            removing: None,
            cid,
            backend,
            conf,
            proto_tx,
            proto_inp_buf: VecDeque::with_capacity(16),
            proto_inp_done: false,
            counters: Counters::new(),
            cmd_tx,
            cmd_rx,
            outbuf: VecDeque::new(),
            mett: ChannelHandlerMetrics::new(),
            waker_1: None,
            waker_2: None,
        }
    }

    pub fn status_info(&self) -> StatusInfo {
        StatusInfo {
            counters: self.counters.clone(),
        }
    }

    pub fn handle_dyn_cmd_v03(&mut self, cmd: serde_json::Value) -> impl Future<Output = serde_json::Value> + use<> {
        use futures::future::ready;
        use serde_json::json;
        // let s = format!("{:?}", self.conf);
        let x = json!({
            // "DUMMY": format!("response from handle {s}"),
            "config": serde_json::to_value(&self.conf).unwrap(),
        });
        ready(x)
        // match &mut self.state {
        //     State::Running(st) => st.handle_channel_handler_cmd(cmd),
        //     _ => json!({
        //         "error": format!("ChannelHandler  {:?}", self.state),
        //     }),
        // }
    }

    pub fn channel_info_v1(&mut self) -> crate::metrics::ChannelInfoV1 {
        let name = self.conf.name().into();
        crate::metrics::ChannelInfoV1 {
            name,
            state_short: self.state.name_short().into(),
        }
    }

    pub fn channel_info_v2(&mut self) -> crate::metrics::ChannelInfoV2 {
        let name = self.conf.name().into();
        let config = serde_json::json!({
            "userconfig": &self.conf,
        });
        crate::metrics::ChannelInfoV2 {
            name,
            state: self.state.name_short().into(),
            config: serde_json::to_value(&config).unwrap(),
        }
    }

    pub fn mett_take(&mut self) -> ChannelHandlerMetrics {
        std::mem::replace(&mut self.mett, ChannelHandlerMetrics::new())
    }

    pub fn cid(&self) -> Cid {
        self.cid.to_cid()
    }

    pub fn sid(&self) -> Option<Sid> {
        match &self.state {
            State::Init(_) => None,
            State::Creating(_) => None,
            State::Running(st) => Some(st.sid()),
            State::Closing1(_) => None,
            State::Closing2(_) => None,
            State::Done1 => None,
            State::Done => None,
            State::Dummy => None,
        }
    }

    pub fn cmd_tx(&self) -> &asynchan::Sender<Cmd> {
        &self.cmd_tx
    }

    fn handle_cmd_remove(&mut self, done_tx: asynchan::Sender<u32>) {
        let selfname = "handle_cmd_remove";
        debug!("{selfname}");
        self.removing = Some(done_tx);
        match &mut self.state {
            State::Init(_st2) => {
                self.state = State::Done1;
            }
            State::Creating(_) => {
                let Creating { .. } = if let State::Creating(st2) = std::mem::replace(&mut self.state, State::Dummy) {
                    st2
                } else {
                    panic!()
                };
                error!("TODO impl Cmd::Remove for State::Creating");
                panic!("TODO impl Cmd::Remove for State::Creating");
                // TODO add flags to Creating so that we now what proto messages we still expect
                // TODO add timeout to Creating (anyways!)
            }
            State::Running(st2) => {
                st2.trigger_remove();
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

    fn handle_cmd(&mut self, cmd: Cmd) {
        let selfname = "handle_cmd";
        match cmd {
            Cmd::Remove(done_tx) => {
                if self.removing.is_some() {
                    warn!("already removing")
                } else {
                    self.handle_cmd_remove(done_tx);
                }
            }
            Cmd::ChannelHandlerStatus(cmd) => {
                self.channel_info_v1();
                self.channel_info_v2();
                self.status_info();
            }
        }
    }

    fn poll_proto_rx_creating(
        mut st1: Pin<&mut Creating>,
        buf: &mut VecDeque<ProtoRxItem>,
        done: &mut bool,
        waker_2: &mut Option<Waker>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<u32, Error>>> {
        let selfname = "poll_proto_rx_creating";
        use Poll::*;
        trace4!("{selfname}");
        let mut idp = 0;
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(item) = buf.pop_front() {
                match st1.as_mut().poll_inp_push(item, cx) {
                    Some(item) => {
                        trace2!("{selfname}  item came back");
                        buf.push_front(item);
                        hpp.mark_pending();
                    }
                    None => {
                        trace2!("{selfname}  item delivered");
                        if 1 + buf.len() >= buf.capacity() {
                            if let Some(w) = waker_2.take() {
                                w.wake();
                            }
                        }
                        idp += 1;
                        hpp.mark_progress();
                    }
                }
            } else if *done {
                break Ready(Some(Err(Error::ProtoRxClosed)));
            } else {
                *waker_2 = Some(cx.waker().clone());
                hpp.mark_pending();
            }
            break if hpp.have_progress() {
                trace!("{selfname}  HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("{selfname}  HPP");
                if idp != 0 { Ready(Some(Ok(idp))) } else { Pending }
            } else {
                trace!("{selfname}  HPP:Done");
                Ready(None)
            };
        }
    }

    pub fn inp_push_try(self: Pin<&mut Self>, item: ProtoRxItem, cx: &mut Context<'_>) -> Option<CaMsg> {
        let self2 = self.get_mut();
        let v = &mut self2.proto_inp_buf;
        let w1 = &mut self2.waker_1;
        let w2 = &mut self2.waker_2;
        if v.len() < v.capacity() {
            if v.len() == 0 {
                if let Some(w) = w1.take() {
                    w.wake();
                }
            }
            v.push_back(item);
            None
        } else {
            *w2 = Some(cx.waker().clone());
            Some(item.msg)
        }
    }

    pub fn inp_done(&mut self) {
        self.proto_inp_done = true;
        match &mut self.state {
            State::Init(st) => {}
            State::Creating(st) => st.inp_done(),
            State::Running(st) => st.inp_done(),
            State::Closing1(st) => todo!(),
            State::Closing2(st) => todo!(),
            State::Done1 => {}
            State::Done => {}
            State::Dummy => {}
        }
    }
}

impl Stream for ChannelHandler {
    type Item = Result<ChannelHandlerItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfname = "poll_next";
        trace3!("{selfname}  {}", self.cid);
        loop {
            let tsloop = Instant::now();
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            if let Some(item) = self2.outbuf.pop_front() {
                break Ready(Some(Ok(item)));
            }
            match self2.cmd_rx.poll_next_unpin(cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match &mut self2.state {
                        State::Done => {
                            warn!("ignore command in Done");
                        }
                        _ => {
                            self2.handle_cmd(x);
                        }
                    }
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            match &mut self2.state {
                State::Init(st2) => {
                    trace!("ChannelHandler:Init");
                    self2.state = State::Creating(Creating::new(
                        self2.cid.to_cid(),
                        self2.conf.name().into(),
                        self2.backend.clone(),
                    ));
                    hpp.mark_progress();
                }
                State::Creating(st1) => {
                    trace!("ChannelHandler:Creating");
                    match Self::poll_proto_rx_creating(
                        Pin::new(st1),
                        &mut self2.proto_inp_buf,
                        &mut self2.proto_inp_done,
                        &mut self2.waker_2,
                        cx,
                    ) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => {
                                    // metrics?
                                }
                                Err(e) => {
                                    warn!("ChannelHandler:Creating:Proto:Ready:Err {e}");
                                    self2.state = State::Done1;
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match st1.poll_next_unpin(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => match x {
                                    create::CreatingItem::CaMsgOut(item) => {
                                        let item = ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ProtoOut(item),
                                        };
                                        break Ready(Some(Ok(item)));
                                    }
                                    create::CreatingItem::ChannelInfoQuery(item) => {
                                        let item = ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ChannelInfoQuery(item),
                                        };
                                        break Ready(Some(Ok(item)));
                                    }
                                    create::CreatingItem::Done((sid, scalar_type, shape, ca_dbr_ty, chi)) => {
                                        self2.state = State::Running(Running::new(
                                            sid,
                                            scalar_type,
                                            shape,
                                            ca_dbr_ty,
                                            chi,
                                            self2.conf.clone(),
                                        ));
                                    }
                                },
                                Err(e) => {
                                    info!("ChannelHandler:Creating:state:Ready:Err {e}");
                                    if true {
                                        let ptr = crate::ca::conn2::conn::CONN_DBG_PTR.load(atomic::Ordering::Acquire);
                                        let ptr = ptr as *const crate::ca::conn2::conn::CaConn;
                                        let x = unsafe { &*ptr };
                                        x.dump_state_poll();
                                        std::process::exit(88);
                                    }
                                    // self2.state = State::Done1;
                                    // break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            trace_pending!("ChannelHandler:Creating");
                            hpp.mark_pending();
                        }
                    }
                }
                State::Running(st2) => {
                    let vi = &mut self2.proto_inp_buf;
                    if let Some(item) = vi.pop_front() {
                        match st2.inp_push_try(item) {
                            Some(x) => {
                                hpp.mark_pending();
                                vi.push_front(x);
                            }
                            None => {
                                if 1 + vi.len() >= vi.capacity() {
                                    if let Some(w) = self2.waker_2.take() {
                                        w.wake();
                                    }
                                }
                                hpp.mark_progress();
                            }
                        }
                    } else if self2.proto_inp_done {
                    } else {
                        hpp.mark_pending();
                    }
                    match st2.poll_next_unpin(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => match x {
                                    running::RunningItem::CaMsgOut(msg) => {
                                        let item = msg;
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ProtoOut(item),
                                        })));
                                    }
                                    running::RunningItem::CaMsgOutIoid(msg, sid, tscmd) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ProtoOutIoid(msg, sid, tscmd),
                                        })));
                                    }
                                    running::RunningItem::CaMsgOutSubid(msg, tscmd) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ProtoOutSubid(msg, tscmd),
                                        })));
                                    }
                                    running::RunningItem::TestValue(x) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::TestValue(x),
                                        })));
                                    }
                                    running::RunningItem::LocalLog(x) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::LocalLog(x),
                                        })));
                                    }
                                    running::RunningItem::ChannelStatus(x) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ChannelStatus(x),
                                        })));
                                    }
                                    running::RunningItem::ChannelEventValue(x) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ChannelEventValue(x),
                                        })));
                                    }
                                },
                                Err(e) => {
                                    info!("ChannelHandler:Running:Ready:Err {e}");
                                    self2.state = State::Done1;
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            debug!(" =-= =-= =-= =-= =-= =-= =-= =-= =-=  move to Closing1");
                            debug!(" =-= =-= =-= =-= =-= =-= =-= =-= =-=  move to Closing1");
                            if self2.removing.is_none() {
                                warn!("closing, but apparently not on user command");
                            }
                            let item = if let Some(sid) = self2.sid() {
                                let msg = CaMsg::from_ty_ts(
                                    CaMsgTy::ChannelClose(proto::ChannelClose {
                                        sid: sid.to_u32(),
                                        cid: self2.cid.to_u32(),
                                    }),
                                    tsloop,
                                );
                                Some(ChannelHandlerItem {
                                    ts_create: tsloop,
                                    inner: ItemInner::ProtoOut(msg),
                                })
                            } else {
                                warn!("can not close channel, maybe never fully created");
                                // seems like the channel got never created.
                                // TODO except in the case when we send create but did not receive response.
                                None
                            };
                            let fut = async move { Ok(()) };
                            self2.state = State::Closing1(Closing1 {
                                // TODO channel close may be also already received from server in Running state.
                                // TODO handle the remove done tx in better way.
                                chan_close_ack: false,
                                fut: Some(fut.box2()),
                                to: tokio::time::sleep(Duration::from_millis(2000)).box2(),
                            });
                            if let Some(item) = item {
                                break Ready(Some(Ok(item)));
                            }
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Closing1(st2) => {
                    if let Some(item) = self2.proto_inp_buf.pop_front() {
                        hpp.mark_progress();
                        match item.msg.ty {
                            CaMsgTy::ChannelClose(x) => {
                                warn!("TODO  maybe  ChannelClose");
                            }
                            CaMsgTy::ChannelCloseRes(x) => {
                                debug!("{lf}GREAT SUCCESS ChannelCloseRes{lf}", lf = "\n\n");
                                st2.chan_close_ack = true;
                            }
                            CaMsgTy::ChannelDisconnect(x) => {
                                warn!("TODO  maybe  ChannelDisconnect");
                            }
                            _ => {}
                        }
                    } else if self2.proto_inp_done {
                    } else {
                        hpp.mark_pending();
                    }
                    {
                        let futopt = &mut st2.fut;
                        if let Some(fut) = futopt {
                            match fut.poll_unpin(cx) {
                                Ready(x) => {
                                    *futopt = None;
                                    hpp.mark_progress();
                                    match x {
                                        Ok(()) => {}
                                        Err(e) => {
                                            info!("Closing1  {e}");
                                            break Ready(Some(Err(e.into())));
                                        }
                                    }
                                }
                                Pending => {
                                    hpp.mark_pending();
                                }
                            }
                        }
                    }
                    if st2.chan_close_ack && st2.fut.is_none() {
                        hpp.mark_progress();
                        trace2!("Closing1 done");
                        self2.state = State::Closing2(Closing2 {});
                    } else {
                        match st2.to.poll_unpin(cx) {
                            Ready(()) => {
                                hpp.mark_progress();
                                warn!("channel close timeout");
                                self2.state = State::Closing2(Closing2 {});
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    }
                }
                State::Closing2(st2) => {
                    hpp.mark_progress();
                    self.state = State::Done1;
                }
                State::Done1 => {
                    trace!("ChannelHandler:Done1");
                    if let Some(tx) = self.removing.as_mut() {
                        let _ = tx.try_send(0);
                    }
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
                self.waker_1 = Some(cx.waker().clone());
                Pending
            } else {
                trace!("HPP:Done");
                Ready(None)
            };
        }
    }
}
