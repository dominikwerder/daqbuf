mod channelhandler;

use crate::ca::conn2::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::Subid;
use crate::ca::conn2::conn::channelheap::channelhandler::ChannelHandler;
use crate::ca::conn2::conn::ctchan::CtChan;
use crate::ca::conn2::timeoutable::TimeoutError;
use crate::ca::conn2::timeoutable::Timeoutable;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto::CaMsg;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::future::ready;
use hashbrown::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHeap"),
    enum variants {
        ChannelHandler(#[from] channelhandler::Error),
        Recv(#[from] asynchan::RecvError),
        Msg(String),
        ProtoRxClosed,
        Timeout(#[from] TimeoutError),
    },
);

#[derive(Debug)]
struct ChHandlerActive {
    handler: ChannelHandler,
    proto_tx: asynchan::Sender<CaMsg>,
    waker: task::Waker,
}

#[derive(Debug)]
enum ChHandler {
    ChHandlerActive(ChHandlerActive),
    Done,
}

#[derive(Debug)]
struct ChannelEntry {
    name: String,
    ch_handler: ChHandler,
}

#[derive(Debug)]
enum State {
    Running,
    Done,
}

mod waker1 {
    use crate::ca::conn2::caids::Cid;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering::AcqRel;
    use std::sync::atomic::Ordering::Acquire;
    use std::task;

    struct WakeData {
        cid: Cid,
        cnt: AtomicU32,
        wk1: task::Waker,
        wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>,
    }

    impl WakeData {
        fn cid(&self) -> Cid {
            self.cid.clone()
        }
    }

    fn clone(d: *const ()) -> task::RawWaker {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace4!(
            "waker1:clone  {}  {}  {}  {}",
            data.cid(),
            data.cnt.load(Acquire),
            Arc::strong_count(&data),
            Arc::weak_count(&data)
        );
        let data2 = data.clone();
        let _ = Arc::into_raw(data);
        data2.cnt.fetch_add(1, AcqRel);
        let data2 = Arc::into_raw(data2) as *const ();
        task::RawWaker::new(data2, &VTABLE)
    }

    fn wake(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace4!(
            "waker1:wake  {}  {}  {}  {}",
            data.cid(),
            data.cnt.load(Acquire),
            Arc::strong_count(&data),
            Arc::weak_count(&data)
        );
        {
            data.wakeup_cids.insert(data.cid(), ());
        }
        data.wk1.wake_by_ref();
        let _ = Arc::into_raw(data);
    }

    fn wake_by_ref(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace4!(
            "waker1:wake_by_ref  {}  {}  {}  {}",
            data.cid(),
            data.cnt.load(Acquire),
            Arc::strong_count(&data),
            Arc::weak_count(&data)
        );
        {
            data.wakeup_cids.insert(data.cid(), ());
        }
        data.wk1.wake_by_ref();
        let _ = Arc::into_raw(data);
    }

    fn drop(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace4!(
            "waker1:drop  {}  {}  {}  {}",
            data.cid(),
            data.cnt.load(Acquire),
            Arc::strong_count(&data),
            Arc::weak_count(&data)
        );
        data.cnt.fetch_sub(1, AcqRel);
        std::mem::drop(data);
    }

    pub fn waker(cid: Cid, wk1: task::Waker, wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>) -> task::Waker {
        let data = WakeData {
            cid,
            cnt: AtomicU32::new(1),
            wk1,
            wakeup_cids,
        };
        let data = Arc::new(data);
        let data = Arc::into_raw(data) as *const ();
        let rw = task::RawWaker::new(data, &VTABLE);
        unsafe { task::Waker::from_raw(rw) }
    }

    const VTABLE: task::RawWakerVTable = task::RawWakerVTable::new(clone, wake, wake_by_ref, drop);
}

#[derive(Debug)]
pub enum ItemInner {
    ScyllaWrite,
}

#[derive(Debug)]
pub struct ChannelHeapItem {
    // Only for performance measurement:
    ts_create: Instant,
    inner: ItemInner,
}

#[derive(Debug)]
pub enum ChHeapCmd {
    RegisterSubid(Cid, Subid, asynchan::Sender<u32>),
}

enum PollHandlerItem {
    ChannelHeapItem(ChannelHeapItem),
    ChHandlerMod,
}

#[derive(Debug)]
pub enum StatusChannelHandlerState {
    Active(channelhandler::StatusInfo),
    Done,
}

#[derive(Debug)]
pub struct StatusChannelHandler {
    pub name: String,
    pub cid: Cid,
    pub status: StatusChannelHandlerState,
}

#[derive(Debug)]
pub struct StatusInfo {
    pub handlers: Vec<StatusChannelHandler>,
}

#[derive(Debug)]
pub enum Cmd {
    RemoveChannel(String, asynchan::Sender<u32>),
}

enum Poll2<T> {
    Item(T),
    Progress,
    Pending,
    Done,
}

type StreamItem = Result<ChannelHeapItem, Error>;

type Cb1Box = Box<dyn FnOnce(&mut ChannelHeap) -> () + Send>;

#[derive(Debug)]
pub struct ChannelHeap {
    state: State,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_rx: asynchan::Receiver<CaMsg>,
    by_cid: HashMap<Cid, ChannelEntry>,
    by_subid: HashMap<Subid, Cid>,
    inp_buf: VecDeque<CaMsg>,
    wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>,
    wakeup_cids_tmp: Vec<Cid>,
    ch_hp_tx: asynchan::Sender<ChHeapCmd>,
    ch_hp_rx: asynchan::Receiver<ChHeapCmd>,
    cmd_exec_fut: Option<FutDbg<Result<(Cb1Box,), Error>>>,
    disconnect_on_idle: bool,
}

impl ChannelHeap {
    pub fn new(proto_tx: asynchan::Sender<CaMsg>, proto_rx: asynchan::Receiver<CaMsg>) -> Self {
        let (ch_hp_tx, ch_hp_rx) = asynchan::bounded(12, "ChannelHandler-to-ChannelHeap");
        Self {
            state: State::Running,
            proto_tx,
            proto_rx,
            by_cid: HashMap::new(),
            by_subid: HashMap::new(),
            inp_buf: VecDeque::with_capacity(8),
            wakeup_cids: Arc::new(dashmap::DashMap::new()),
            wakeup_cids_tmp: Vec::new(),
            ch_hp_tx,
            ch_hp_rx,
            cmd_exec_fut: None,
            disconnect_on_idle: false,
        }
    }

    pub fn status_info(&self) -> StatusInfo {
        let handlers = self
            .by_cid
            .iter()
            .map(|(cid, e)| match &e.ch_handler {
                ChHandler::ChHandlerActive(ha) => {
                    ha.handler.status_info();
                    StatusChannelHandler {
                        name: e.name.clone(),
                        cid: cid.clone(),
                        status: StatusChannelHandlerState::Active(ha.handler.status_info()),
                    }
                }
                ChHandler::Done => StatusChannelHandler {
                    name: e.name.clone(),
                    cid: cid.clone(),
                    status: StatusChannelHandlerState::Done,
                },
            })
            .collect();
        StatusInfo { handlers }
    }

    pub fn channel_add(&mut self, conf: ChannelConfig, cx: &mut Context) {
        trace!("channel_add {conf:?}");
        let name = conf.name().into();
        let (tx, rx) = asynchan::bounded(12, "ChannelHeap-channeladd");
        let handler = ChannelHandler::new(conf, self.proto_tx.clone(), rx, self.ch_hp_tx.clone());
        let cid = handler.cid();
        if self.by_cid.contains_key(&cid) {
            error!("ChannelHeap::channel_add: channel with cid {cid:?} already in map");
            return;
        }
        let waker = waker1::waker(cid.clone(), cx.waker().clone(), self.wakeup_cids.clone());
        let e = ChannelEntry {
            name,
            ch_handler: ChHandler::ChHandlerActive(ChHandlerActive {
                handler,
                proto_tx: tx,
                waker,
            }),
        };
        self.by_cid.insert(cid.clone(), e);
        self.wakeup_cids.insert(cid, ());
        cx.waker().wake_by_ref();
    }

    pub fn disconnect_on_idle(&mut self) {
        let selfname = "disconnect_on_idle";
        info!("{selfname}");
        self.disconnect_on_idle = true;
    }

    // low-level cleanup, call only when channel behind this Cid is actually done.
    fn remove_cid(&mut self, cid: Cid) {
        let selfname = "remove_cid";
        self.by_cid.remove(&cid);
        let subids: Vec<_> = self
            .by_subid
            .iter()
            .filter(|(_, v)| **v == cid)
            .map(|x| x.0.clone())
            .collect();
        if subids.len() != 0 {
            error!("{selfname} subids discovered");
            for subid in subids {
                self.by_subid.remove(&subid);
            }
        }
        self.wakeup_cids.remove(&cid);
    }

    fn poll_handler(
        mut handler: Pin<&mut ChannelHandler>,
        cx: &mut Context,
        cid: Cid,
        // st1: &mut ChannelEntry,
    ) -> Poll<Option<Result<PollHandlerItem, Error>>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match handler.poll_next_unpin(cx) {
                Ready(Some(x)) => match x {
                    Ok(item) => {
                        trace!("ChannelHeap:Handler:Some {cid}");
                        let item = match item.inner {
                            channelhandler::ItemInner::ScyllaWrite => ChannelHeapItem {
                                ts_create: item.ts_create,
                                inner: ItemInner::ScyllaWrite,
                            },
                        };
                        break Ready(Some(Ok(PollHandlerItem::ChannelHeapItem(item))));
                    }
                    Err(e) => {
                        hpp.mark_progress();
                        // TODO must not go into error state.
                        // TODO clean up this channel handler.
                        // TODO report upstream about failed channel.
                        // TODO upstream should try to remove and re-add the channel a few times.
                        // TODO if that does not work, remove the connection, invalidate
                        // all those channels, and try again all channels.
                        // self2.state = State::Done;
                        warn!("ChannelHeap:Handler:Done  TODO handle closed channel handler gracefully {cid}");
                        let msg = format!("ChannelHeap:Handler error {cid} {e}");
                        break Ready(Some(Err(Error::Msg(msg))));
                    }
                },
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                    trace_pending!("ChannelHeap:Handler  {cid}");
                }
            }
            break if hpp.have_progress() {
                trace!("poll_handler:HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("poll_handler:HPP");
                Pending
            } else {
                trace!("poll_handler:HPP:Done");
                Ready(None)
            };
        }
    }

    fn dispatch_input_to_channels(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll2<Error> {
        let selfname = "dispatch_input_to_channels";
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        let self2 = self.get_mut();
        if self2.inp_buf.len() != 0 {
            while let Some(item) = self2.inp_buf.pop_front() {
                // Some message can be dispatched by Cid.
                // Others need translation from Subid.
                let disp_cid = if let Some(cid) = item.cid() {
                    Some(Cid::new(cid))
                } else if let Some(subid) = item.subid() {
                    if let Some(cid) = self2.by_subid.get(&Subid::new(subid)) {
                        Some(cid.clone())
                    } else {
                        trace!("{selfname}  TODO  msg has subid, but can not map to a cid");
                        None
                    }
                } else if let Some(ioid) = item.ioid() {
                    trace!("{selfname}  TODO  msg has ioid, map to cid not yet implemented");
                    None
                } else {
                    None
                };
                if let Some(cid) = disp_cid {
                    if let Some(e) = self2.by_cid.get_mut(&cid) {
                        match &mut e.ch_handler {
                            ChHandler::ChHandlerActive(st2) => {
                                use asynchan::SendPoll;
                                use asynchan::SendPollError;
                                match st2.proto_tx.poll_send_unpin(item, cx) {
                                    Ok(()) => {
                                        trace!("{selfname}  ChannelHeap:Dispatch:Sent {cid}");
                                        self2.wakeup_cids.insert(cid, ());
                                        hpp.mark_progress();
                                    }
                                    Err(e) => match e {
                                        SendPollError::Full(item) => {
                                            trace_pending!("{selfname}  ChannelHeap:Dispatch  {cid}");
                                            self2.inp_buf.push_front(item);
                                            hpp.mark_pending();
                                        }
                                        SendPollError::Closed(item) => {
                                            trace!("{selfname}  ChannelHeap:Dispatch:Closed {cid}");
                                            self2.inp_buf.push_front(item);
                                            hpp.mark_progress();
                                            self2.state = State::Done;
                                            warn!("{selfname}  TODO handle closed channel handler gracefully");
                                            let e = Error::Msg(format!(
                                                "{selfname}  ChannelHeap: channel handler for {cid} closed"
                                            ));
                                            return Poll2::Item(e);
                                        }
                                    },
                                }
                            }
                            ChHandler::Done => {
                                // Can not handle this msg.
                                // TODO count for metrics.
                                hpp.mark_progress();
                            }
                        }
                    } else {
                        hpp.mark_progress();
                        warn!("{selfname}  ChannelHeap: no channel handler  {cid}  {item:?}");
                    }
                } else {
                    hpp.mark_progress();
                    warn!("{selfname}  TODO not idea how to handle this  {item:?}");
                }
            }
        }
        if hpp.have_progress() {
            Poll2::Progress
        } else if hpp.have_pending() {
            Poll2::Pending
        } else {
            Poll2::Done
        }
    }

    fn poll_all_handler(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll2<()> {
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        let self2 = self.get_mut();
        self2.wakeup_cids_tmp.clear();
        self2
            .wakeup_cids_tmp
            .extend(self2.wakeup_cids.iter().map(|x| x.key().clone()));
        for cid in self2.wakeup_cids_tmp.iter() {
            trace4!("ChannelHeap  waking  {cid}");
            if let Some(st1) = self2.by_cid.get_mut(cid) {
                match &mut st1.ch_handler {
                    ChHandler::ChHandlerActive(st2) => {
                        let cx2 = &mut Context::from_waker(&st2.waker);
                        let handler = Pin::new(&mut st2.handler);
                        match Self::poll_handler(handler, cx2, cid.clone()) {
                            Ready(Some(x)) => match x {
                                Ok(x) => match x {
                                    PollHandlerItem::ChannelHeapItem(item) => {
                                        trace!("TODO handle PollHandlerItem::ChannelHeapItem(item)  {item:?}");
                                        todo!()
                                    }
                                    PollHandlerItem::ChHandlerMod => {
                                        trace!("TODO handle PollHandlerItem::ChHandlerMod");
                                        todo!()
                                    }
                                },
                                Err(e) => {
                                    trace!("TODO handle Self::poll_handler  Err  {e}");
                                    todo!()
                                }
                            },
                            Ready(None) => {
                                trace!("ChannelHeap:Handler:Finished {cid}");
                                error!("TODO handle finished channel handler gracefully {cid}");
                                // TODO remove it? No, then it would be less observable.
                                // Instead, move it to Done, otherwise we continue polling all the time.
                                // TODO after Ready(None), anything else to clean up or reuse?
                                st1.ch_handler = ChHandler::Done;
                                hpp.mark_progress();
                                todo!()
                            }
                            Pending => {
                                trace_pending!("ChannelHeap:Handler  {cid}");
                                hpp.mark_pending();
                            }
                        }
                        // TODO handle result
                        // TODO;
                    }
                    ChHandler::Done => {
                        // Wakeup marker for no longer served cid.
                        // TODO count for metrics
                    }
                }
            } else {
                warn!("ChannelHeap: no channel handler for wakeup cid {cid}");
                hpp.mark_progress();
            }
        }
        if hpp.have_progress() {
            Poll2::Progress
        } else if hpp.have_pending() {
            Poll2::Pending
        } else {
            Poll2::Done
        }
    }

    fn handle_command(&mut self, cmd: Cmd) {
        let selfname = "handle_command";
        match cmd {
            Cmd::RemoveChannel(name, mut done_tx) => {
                let cids: Vec<_> = self
                    .by_cid
                    .iter()
                    .filter(|x| x.1.name == name)
                    .map(|x| x.0.clone())
                    .collect();
                let handler_txs: Vec<_> = cids
                    .iter()
                    .filter_map(|cid| {
                        self.by_cid.get(cid).map(|h1| {
                            match &h1.ch_handler {
                                ChHandler::ChHandlerActive(h2) => Some(h2.handler.cmd_tx().clone()),
                                ChHandler::Done => {
                                    // TODO count?
                                    None
                                }
                            }
                        })
                    })
                    .filter_map(|x| x)
                    .collect();
                let fut = async move {
                    let selfname = "handle_command:RemoveChannel:fut";
                    for mut tx in handler_txs {
                        let (inner_done_tx, mut inner_done_rx) =
                            asynchan::bounded(4, "ChannelHeap-ChannelHandler-done-tx");
                        let item = channelhandler::Cmd::Remove(inner_done_tx);
                        if tx.send(item).await.is_err() {
                            error!("{selfname}  tx  send  fail");
                            // TODO handle
                        }
                        if inner_done_rx.recv().await.is_err() {
                            error!("{selfname}  inner_done_rx  recv  fail");
                            // TODO handle
                        }
                    }
                    if done_tx.send(0).await.is_err() {
                        error!("{selfname}  done_tx  send  fail");
                        // TODO handle
                    }
                    let donecb = |cheap: &mut ChannelHeap| {
                        let selfname = "handle_command:RemoveChannel:fut:donecb";
                        error!("{selfname}  TODO impl donecb");
                        for cid in cids {
                            cheap.remove_cid(cid);
                        }
                    };
                    Ok((Box::new(donecb) as Cb1Box,))
                };
                let fut = fut.timeout(Duration::from_millis(2000)).then(|x| match x {
                    Ok(x) => ready(x),
                    Err(e) => ready(Err(e.into())),
                });
                self.cmd_exec_fut = Some(fut.box2());
            }
        }
    }

    fn poll_outer_cmd(
        mut self: Pin<&mut Self>,
        cmd_rx: &mut asynchan::Receiver<Cmd>,
        cx: &mut Context,
    ) -> Poll<Option<()>> {
        let selfname = "poll_outer_cmd";
        use Poll::*;
        if let Some(fut) = &mut self.cmd_exec_fut {
            match fut.poll_unpin(cx) {
                Ready(x) => {
                    self.cmd_exec_fut = None;
                    match x {
                        Ok((donecb,)) => {
                            donecb(&mut self);
                            Ready(Some(()))
                        }
                        Err(e) => {
                            // TODO could be a remote timeout!
                            error!("{selfname}  {e}");
                            Ready(Some(()))
                        }
                    }
                }
                Pending => Pending,
            }
        } else {
            match cmd_rx.poll_next_unpin(cx) {
                Ready(Some(x)) => {
                    self.handle_command(x);
                    Ready(Some(()))
                }
                Ready(None) => {
                    // TODO make sure polling on closed is cheap enough.
                    Ready(None)
                }
                Pending => Pending,
            }
        }
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        cmd_rx: &mut asynchan::Receiver<Cmd>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<StreamItem>> {
        use Poll::*;
        trace4!("ChannelHeap  poll_next");
        'main: loop {
            let mut hpp = HaveProgressPending::new();
            match &self.state {
                State::Running => {
                    match self.as_mut().poll_outer_cmd(cmd_rx, cx) {
                        Ready(Some(())) => {
                            hpp.mark_progress();
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match self.ch_hp_rx.poll_next_unpin(cx) {
                        Ready(Some(cmd)) => {
                            trace!("ChannelHeap:ChHpCmd:Got");
                            hpp.mark_progress();
                            match cmd {
                                ChHeapCmd::RegisterSubid(cid, subid, mut resp_tx) => {
                                    trace!("ChannelHeap:ChHpCmd:RegisterSubid  {cid}  {subid}");
                                    let mut existed = false;
                                    self.by_subid
                                        .entry(subid.clone())
                                        .and_modify(|_| {
                                            existed = true;
                                        })
                                        .or_insert_with(|| cid.clone());
                                    if existed {
                                        warn!(
                                            "ChannelHeap:ChHpCmd:RegisterSubid:SubidExists  {subid}  TODO handle error"
                                        );
                                    } else {
                                        trace!("ChannelHeap:ChHpCmd:RegisterSubid:Ok  {subid}");
                                        use asynchan::SendPoll;
                                        use asynchan::SendPollError;
                                        match resp_tx.poll_send_unpin(1, cx) {
                                            Ok(()) => {}
                                            Err(_) => {
                                                // TODO should never happen
                                                error!(
                                                    "ChannelHeap:ChHpCmd:RegisterSubid  TODO  response channel unavailable"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ready(None) => {
                            trace!("ChannelHeap:ChHpCmd:Done");
                            hpp.mark_progress();
                            // TODO handle closed command channel
                        }
                        Pending => {
                            trace_pending!("ChannelHeap:ChHpCmd");
                            hpp.mark_pending();
                        }
                    }
                    if self.inp_buf.len() < self.inp_buf.capacity() {
                        match self.proto_rx.poll_next_unpin(cx) {
                            Ready(x) => match x {
                                Some(item) => {
                                    trace!("ChannelHeap:CaInp:Rx:Ok");
                                    hpp.mark_progress();
                                    self.inp_buf.push_back(item);
                                }
                                None => {
                                    trace!("ChannelHeap:CaInp:Rx:Err");
                                    hpp.mark_progress();
                                    self.state = State::Done;
                                    break Ready(Some(Err(Error::ProtoRxClosed)));
                                }
                            },
                            Pending => {
                                trace_pending!("ChannelHeap:CaInp:Rx");
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        // Maybe nothing to do here?
                    }
                    loop {
                        break match self.as_mut().dispatch_input_to_channels(cx) {
                            Poll2::Item(e) => {
                                hpp.mark_progress();
                                break 'main Ready(Some(Err(e)));
                            }
                            Poll2::Progress => {
                                continue;
                            }
                            Poll2::Pending => {
                                hpp.mark_pending();
                            }
                            Poll2::Done => {}
                        };
                    }
                    loop {
                        break match self.as_mut().poll_all_handler(cx) {
                            Poll2::Item(item) => {
                                hpp.mark_progress();
                                error!("TODO  ChannelHeap:poll_all_handler:Item {item:?}");
                            }
                            Poll2::Progress => {
                                hpp.mark_progress();
                            }
                            Poll2::Pending => {
                                hpp.mark_pending();
                            }
                            Poll2::Done => {}
                        };
                    }
                    if self.disconnect_on_idle && hpp.have_progress() == false {
                        if self.by_cid.len() == 0 {
                            info!("disconnect_on_idle goto Done");
                            hpp.mark_progress();
                            self.state = State::Done;
                        }
                    }
                }
                State::Done => {}
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

    pub fn poll_next_unpin(
        &mut self,
        cmd_rx: &mut asynchan::Receiver<Cmd>,
        cx: &mut Context,
    ) -> Poll<Option<StreamItem>> {
        Pin::new(self).poll_next(cmd_rx, cx)
    }
}
