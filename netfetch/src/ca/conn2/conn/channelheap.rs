//
//

const INP_BUF_CAP: usize = 16;

//

mod channelhandler;

use crate::ca::conn2::asynchan;
use crate::ca::conn2::asynchan2::SendPoll;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Ioid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::Subid;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::channelheap::channelhandler::ChannelHandler;
use crate::ca::conn2::locallog;
use crate::ca::conn2::timeoutable::TimeoutError;
use crate::ca::conn2::timeoutable::Timeoutable;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use asynchan::SendPollError;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::ready;
use hashbrown::HashMap;
use serde::Serialize;
use stats::mett::CaConnConnectedMetrics;
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
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }
macro_rules! todo_shutdown { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }

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
pub struct ProtoRxItem {
    pub msg: CaMsg,
    pub tscmd: Instant,
    pub tsreg: Instant,
    pub tsdisp: Instant,
}

#[derive(Debug)]
struct ChHandlerActive {
    handler: ChannelHandler,
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
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelEventValue(ChannelEventValue),
}

#[derive(Debug)]
pub struct ChannelHeapItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

enum PollHandlerItem {
    None,
    ChannelHeapItem(ChannelHeapItem),
    ChHandlerMod,
    ProtoOut(CaMsg),
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelEventValue(ChannelEventValue),
}

#[derive(Debug, Serialize)]
pub enum StatusChannelHandlerState {
    Active(channelhandler::StatusInfo),
    Done,
}

#[derive(Debug, Serialize)]
pub struct StatusChannelHandler {
    pub name: String,
    pub cid: Cid,
    pub status: StatusChannelHandlerState,
}

#[derive(Debug, Serialize)]
pub struct StatusInfo {
    pub handlers: Vec<StatusChannelHandler>,
}

#[derive(Debug)]
pub enum Cmd {
    RemoveChannel(String, asynchan::Sender<u32>),
}

type StreamItem = Result<ChannelHeapItem, Error>;

type Cb1Box = Box<dyn FnOnce(&mut ChannelHeap) -> () + Send>;

#[derive(Debug)]
struct IoidRegistry {
    ioids: HashMap<Ioid, (Cid, Sid, Instant, Instant)>,
    current: Ioid,
}

impl IoidRegistry {
    fn new() -> Self {
        Self {
            ioids: HashMap::new(),
            current: Ioid::new(0),
        }
    }

    fn register(&mut self, sid: Sid, ioid: Ioid, cid: Cid, tscmd: Instant, tsreg: Instant) {
        // TODO count errors for metrics
        self.ioids
            .entry(ioid.clone())
            .and_modify(|e| {
                // mett.ioid_read_error_exists().inc();
                trace2!("IoidRegistry  register  update  {}  {}  {}", sid, ioid, cid);
                e.2 = tscmd;
                e.3 = tsreg;
            })
            .or_insert_with(|| {
                // mett.ioid_read_begin().inc();
                trace2!("IoidRegistry  register  fresh  {}  {}  {}", sid, ioid, cid);
                (cid, sid, tscmd, tsreg)
            });
    }

    fn take(&mut self, ioid: Ioid) -> Option<(Cid, Sid, Instant, Instant)> {
        if let Some(x) = self.ioids.remove(&ioid) {
            Some(x)
        } else {
            None
        }
    }

    fn current(&mut self) -> &mut Ioid {
        &mut self.current
    }
}

#[derive(Debug)]
struct SubidRegistry {
    subids: HashMap<Subid, (Cid, Instant)>,
    rev: HashMap<Cid, Subid>,
    current: Subid,
}

impl SubidRegistry {
    fn new() -> Self {
        Self {
            subids: HashMap::new(),
            rev: HashMap::new(),
            current: Subid::new(0),
        }
    }

    fn register(&mut self, cid: Cid, tsreg: Instant) -> Subid {
        // TODO count hits and misses for metrics
        if let Some(subid) = self.rev.get(&cid) {
            subid.clone()
        } else {
            let subid = self.current.inc();
            self.subids.insert(subid, (cid.clone(), tsreg));
            self.rev.insert(cid, subid);
            subid
        }
    }

    fn lookup(&mut self, subid: Subid) -> Option<(Cid, Instant)> {
        if let Some(x) = self.subids.get(&subid) {
            Some(x.clone())
        } else {
            None
        }
    }

    fn remove(&mut self, cid: Cid) -> usize {
        if let Some(subid) = self.rev.remove(&cid) {
            self.subids.remove(&subid);
            1
        } else {
            0
        }
    }
}

enum PollHandlerItemB {
    None,
    Fut(FutDbg<Result<(), Error>>),
    ProtoOut(CaMsg),
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelEventValue(ChannelEventValue),
}

#[derive(Debug)]
pub struct ChannelHeap {
    backend: String,
    state: State,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_rx: asynchan::Receiver<CaMsg>,
    proto_tx_buf: VecDeque<CaMsg>,
    by_cid: HashMap<Cid, ChannelEntry>,
    by_name: HashMap<String, Cid>,
    inp_buf: VecDeque<CaMsg>,
    inp_done: bool,
    wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>,
    wakeup_cids_tmp: Vec<Cid>,
    cmd_exec_fut: Option<FutDbg<Result<(Cb1Box,), Error>>>,
    disconnect_on_idle: bool,
    ioid_reg: IoidRegistry,
    subid_reg: SubidRegistry,
    poll_handler_fut: Option<FutDbg<Result<(), Error>>>,
    mett: CaConnConnectedMetrics,
}

impl ChannelHeap {
    pub fn new(backend: String, proto_tx: asynchan::Sender<CaMsg>, proto_rx: asynchan::Receiver<CaMsg>) -> Self {
        Self {
            backend,
            state: State::Running,
            proto_tx,
            proto_rx,
            proto_tx_buf: VecDeque::with_capacity(16),
            by_cid: HashMap::new(),
            by_name: HashMap::new(),
            inp_buf: VecDeque::with_capacity(INP_BUF_CAP),
            inp_done: false,
            wakeup_cids: Arc::new(dashmap::DashMap::new()),
            wakeup_cids_tmp: Vec::new(),
            cmd_exec_fut: None,
            disconnect_on_idle: false,
            ioid_reg: IoidRegistry::new(),
            subid_reg: SubidRegistry::new(),
            poll_handler_fut: None,
            mett: CaConnConnectedMetrics::new(),
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

    pub fn handle_dyn_cmd_v03(&mut self, cmd: serde_json::Value) -> impl Future<Output = serde_json::Value> + use<> {
        use futures::future::ready;
        use serde::Deserialize;
        use serde_json::json;
        #[derive(Debug, Deserialize)]
        struct CmdTmp {
            type_channel_heap: String,
        }
        #[derive(Debug, Deserialize)]
        struct CmdTmpNamed {
            chname: String,
        }
        match serde_json::from_value::<CmdTmp>(cmd.clone()) {
            Ok(cmd2) => {
                if cmd2.type_channel_heap == "heaptest01" {
                    ready(json!({
                        "TODO": "ChannelHeap  heaptest01  ok..",
                    }))
                    .box2()
                } else if cmd2.type_channel_heap == "heaptest02"
                    && let Ok(cmd3) = serde_json::from_value::<CmdTmpNamed>(cmd.clone())
                {
                    match &mut self.state {
                        State::Running => {
                            for ch in self.by_cid.iter_mut().filter(|x| x.1.name == cmd3.chname) {
                                return match &mut ch.1.ch_handler {
                                    ChHandler::ChHandlerActive(st2) => {
                                        st2.handler.handle_dyn_cmd_v03(cmd).box2()
                                        // self.wakeup_cids.insert(ch.0.clone(), ());
                                        // st2.handler.handle_channel_handler_cmd(cmd)
                                        // ready(json!({
                                        //     "error": "ChannelHeap  TODO  ChHandler::ChHandlerActive",
                                        // }))
                                    }
                                    ChHandler::Done => ready(json!({
                                        "error": "ChannelHeap  ChHandler::Done",
                                    }))
                                    .box2(),
                                };
                            }
                            ready(json!({
                                "error": "ChannelHeap  chname not found",
                            }))
                            .box2()
                        }
                        State::Done => ready(json!({
                            "error": "ChannelHeap  State::Done",
                        }))
                        .box2(),
                    }
                } else {
                    ready(json!({
                        "error": format!("unexpected command {cmd:?}"),
                    }))
                    .box2()
                }
            }
            Err(_) => ready(json!({
                "error": "unexpected command {cmd:?}",
            }))
            .box2(),
        }
    }

    pub fn channel_info_v1(&mut self) -> crate::metrics::ChannelsForAddrInfoV1 {
        let mut ret = crate::metrics::ChannelsForAddrInfoV1::new();
        match &mut self.state {
            State::Running => {
                self.by_cid
                    .iter_mut()
                    .map(|(_, che)| match &mut che.ch_handler {
                        ChHandler::ChHandlerActive(hh) => {
                            let x = hh.handler.channel_info_v1();
                            ret.channels.push(x);
                        }
                        ChHandler::Done => {}
                    })
                    .for_each(|_| {});
            }
            State::Done => {}
        }
        ret
    }

    pub fn channel_info_v2(&mut self, name: String) -> crate::metrics::ChannelsForAddrInfoV2 {
        let mut ret = crate::metrics::ChannelsForAddrInfoV2::new();
        let re1 = match regex::Regex::new(&name) {
            Ok(x) => x,
            Err(_) => return ret,
        };
        match &mut self.state {
            State::Running => {
                self.by_cid
                    .iter_mut()
                    .map(|(_, che)| che)
                    .filter(move |che| re1.is_match(&che.name))
                    .map(|che| match &mut che.ch_handler {
                        ChHandler::ChHandlerActive(hh) => {
                            let x = hh.handler.channel_info_v2();
                            ret.channels.push(x);
                        }
                        ChHandler::Done => {}
                    })
                    .for_each(|_| {});
            }
            State::Done => {}
        }
        ret
    }

    pub fn channels_by_regex_v1(&mut self, kind: String, reg: String) -> Vec<serde_json::Value> {
        let mut ret = Vec::new();
        let re1 = match regex::Regex::new(&reg) {
            Ok(x) => x,
            Err(_) => return ret,
        };
        match &mut self.state {
            State::Running => {
                self.by_cid
                    .iter_mut()
                    .map(|(_, che)| che)
                    .filter(move |che| re1.is_match(&che.name))
                    .map(|che| match &mut che.ch_handler {
                        ChHandler::ChHandlerActive(hh) => {
                            let x = hh.handler.channel_info_v2();
                            let x = serde_json::to_value(&x).unwrap();
                            ret.push(x);
                        }
                        ChHandler::Done => {}
                    })
                    .for_each(|_| {});
            }
            State::Done => {}
        }
        ret
    }

    pub fn mett_take(&mut self) -> CaConnConnectedMetrics {
        for (cid, ee) in self.by_cid.iter_mut() {
            match &mut ee.ch_handler {
                ChHandler::ChHandlerActive(ha) => {
                    let m = ha.handler.mett_take();
                    self.mett.channel_handler().ingest(m);
                }
                ChHandler::Done => {
                    // TODO count in metrics
                }
            }
        }
        std::mem::replace(&mut self.mett, CaConnConnectedMetrics::new())
    }

    pub fn channel_add(&mut self, conf: ChannelConfig, cx: &mut Context) {
        trace!("channel_add {conf:?}");
        let name = conf.name().to_string();
        if self.by_name.contains_key(&name) {
            warn!("TODO channel already present, return error");
        } else {
            self.mett.channel_handler_new().inc();
            let handler = ChannelHandler::new(self.backend.clone(), conf, self.proto_tx.clone());
            let cid = handler.cid();
            if self.by_cid.contains_key(&cid) {
                error!("ChannelHeap::channel_add: channel with cid {cid:?} already in map");
                return;
            }
            let waker = waker1::waker(cid.clone(), cx.waker().clone(), self.wakeup_cids.clone());
            let e = ChannelEntry {
                name: name.clone(),
                ch_handler: ChHandler::ChHandlerActive(ChHandlerActive { handler, waker }),
            };
            self.by_cid.insert(cid.clone(), e);
            self.by_name.insert(name, cid.clone());
            self.wakeup_cids.insert(cid, ());
        }
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
        if let Some(c) = self.by_cid.remove(&cid) {
            self.by_name.remove(&c.name);
        }
        let nrem = self.subid_reg.remove(cid.clone());
        if nrem != 0 {
            error!("{selfname}  {nrem} subids still removed");
        }
        self.wakeup_cids.remove(&cid);
    }

    fn poll_handler(
        mut handler: Pin<&mut ChannelHandler>,
        cx: &mut Context,
        cid: Cid,
        ioid_reg: &mut IoidRegistry,
        subid_reg: &mut SubidRegistry,
        tsnow: Instant,
    ) -> Poll<Option<Result<PollHandlerItem, Error>>> {
        let selfname = "poll_handler";
        trace2!("{selfname}  {cid}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match handler.poll_next_unpin(cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    trace2!("{selfname}  {cid}  Some  {x:?}");
                    match x {
                        Ok(item) => {
                            let item = match item.inner {
                                channelhandler::ItemInner::ProtoOut(item) => {
                                    trace!("{selfname}  received channelhandler::ItemInner::ProtoOut {item:?}");
                                    PollHandlerItem::ProtoOut(item)
                                }
                                channelhandler::ItemInner::ProtoOutIoid(mut ca_msg, sid, tscmd) => {
                                    if let Some(sid2) = handler.sid() {
                                        if sid2 != sid {
                                            warn!("{selfname}  ProtoOutIoid but handler sid differs");
                                            PollHandlerItem::None
                                        } else {
                                            let ioid = ioid_reg.current().inc();
                                            ioid_reg.register(sid, ioid.clone(), cid, tscmd, tsnow);
                                            ca_msg.overwrite_ioid(ioid.to_u32());
                                            PollHandlerItem::ProtoOut(ca_msg)
                                        }
                                    } else {
                                        warn!("{selfname}  ProtoOutIoid but handler missing sid");
                                        PollHandlerItem::None
                                    }
                                }
                                channelhandler::ItemInner::ProtoOutSubid(mut ca_msg, tscmd) => {
                                    let subid = subid_reg.register(cid, tsnow);
                                    ca_msg.overwrite_subid(subid.to_u32());
                                    PollHandlerItem::ProtoOut(ca_msg)
                                }
                                channelhandler::ItemInner::ChannelInfoQuery(item) => {
                                    //
                                    PollHandlerItem::ChannelInfoQuery(item)
                                }
                                channelhandler::ItemInner::TestValue(x) => PollHandlerItem::TestValue(x),
                                channelhandler::ItemInner::LocalLog(x) => PollHandlerItem::LocalLog(x),
                                channelhandler::ItemInner::ChannelStatus(x) => {
                                    warn!("{selfname}  TODO  do something with received {x:?}");
                                    PollHandlerItem::None
                                }
                                channelhandler::ItemInner::ChannelEventValue(x) => {
                                    PollHandlerItem::ChannelEventValue(x)
                                }
                            };
                            break Ready(Some(Ok(item)));
                        }
                        Err(e) => {
                            // TODO must not go into error state.
                            // TODO clean up this channel handler.
                            // TODO report upstream about failed channel.
                            // TODO upstream should try to remove and re-add the channel a few times.
                            // TODO if that does not work, remove the connection, invalidate
                            // all those channels, and try again all channels.
                            // self2.state = State::Done;
                            warn!(
                                "{selfname}  ChannelHeap:Handler:Done  TODO handle closed channel handler gracefully {cid}"
                            );
                            let msg = format!("ChannelHeap:Handler error {cid} {e}");
                            break Ready(Some(Err(Error::Msg(msg))));
                        }
                    }
                }
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

    fn dispatch_input_to_channels(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "dispatch_input_to_channels";
        use Poll::*;
        loop {
            let tsnow = Instant::now();
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            let mut idp = 0;
            if let Some(item) = self2.inp_buf.pop_front() {
                let disp = if let Some(cid) = item.cid() {
                    trace!("{selfname}  resolved via cid");
                    Some((Cid::new(cid), tsnow, tsnow))
                } else if let Some(subid) = item.subid() {
                    if let Some((cid, tsreg)) = self2.subid_reg.lookup(Subid::new(subid)) {
                        trace!("{selfname}  resolved via subid");
                        Some((cid.clone(), tsnow, tsnow))
                    } else {
                        trace!("{selfname}  TODO  msg has unknown subid");
                        None
                    }
                } else if let Some(ioid) = item.ioid() {
                    if let Some((cid, _sid, tscmd, tsreg)) = self2.ioid_reg.take(Ioid::new(ioid)) {
                        // TODO metrics
                        {
                            let dt = Instant::now().duration_since(tscmd);
                            let dtcmd = 1e3 * dt.as_secs_f32();
                            let dt = Instant::now().duration_since(tsreg);
                            let dtreg = 1e3 * dt.as_secs_f32();
                            debug!("resolve incoming Ioid  dtcmd {:.3} ms  dtreg {:.3} ms", dtcmd, dtreg);
                        }
                        Some((cid, tscmd, tsreg))
                    } else {
                        debug!("{selfname}  ioid  {ioid}  unknown");
                        None
                    }
                } else if let Some(sid) = item.sid() {
                    debug!("{selfname}  TODO  msg has sid, map to cid not yet implemented");
                    None
                } else {
                    debug!("{selfname}  TODO  msg has no routing id");
                    None
                };
                if let Some((cid, tscmd, tsreg)) = disp {
                    if let Some(e) = self2.by_cid.get_mut(&cid) {
                        match &mut e.ch_handler {
                            ChHandler::ChHandlerActive(st2) => {
                                let sdbg = format!("{item:?}");
                                let u = ProtoRxItem {
                                    msg: item,
                                    tscmd,
                                    tsreg,
                                    tsdisp: tsnow,
                                };
                                match Pin::new(&mut st2.handler).inp_push_try(u, cx) {
                                    Some(item) => {
                                        hpp.mark_pending();
                                        trace2!("{selfname}  ChannelHeap:Dispatch:Pending  {cid}  {sdbg}");
                                        self2.inp_buf.push_front(item);
                                        self2.wakeup_cids.insert(cid, ());
                                    }
                                    None => {
                                        hpp.mark_progress();
                                        idp += 1;
                                        trace2!("{selfname}  ChannelHeap:Dispatch:Done  {cid}  {sdbg}");
                                        self2.wakeup_cids.insert(cid, ());
                                    }
                                }
                            }
                            ChHandler::Done => {
                                // Can not handle this msg.
                                // TODO count for metrics.
                                hpp.mark_progress();
                                idp += 1;
                                warn!("{selfname}  ChannelHeap: channel handler Done  {cid}  {item:?}");
                            }
                        }
                    } else {
                        hpp.mark_progress();
                        idp += 1;
                        warn!("{selfname}  ChannelHeap: no channel handler  {cid}  {item:?}");
                    }
                } else {
                    hpp.mark_progress();
                    idp += 1;
                    match &item.ty {
                        proto::CaMsgTy::EventAddRes(..) => {
                            // TODO keep removed subids separately for a while to sort out monitoring
                            // events for stale subscriptions.
                        }
                        _ => {
                            warn!("{selfname}  TODO no idea how to handle this  {item:?}");
                        }
                    }
                }
            } else {
                if self2.inp_done {
                } else {
                    hpp.mark_pending();
                }
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                if idp != 0 { Ready(Some(Ok(()))) } else { Pending }
            } else {
                Ready(None)
            };
        }
    }

    fn poll_all_handler_sub(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<PollHandlerItemB, Error>>> {
        let selfname = "poll_all_handler_sub";
        trace4!("{selfname}");
        use Poll::*;
        let tsnow = Instant::now();
        let mut hpp = HaveProgressPending::new();
        let self2 = self.get_mut();
        self2.wakeup_cids_tmp.clear();
        self2
            .wakeup_cids_tmp
            .extend(self2.wakeup_cids.iter().map(|x| x.key().clone()));
        let mut add_wakeup = Vec::new();
        let loopres: Poll<Option<Result<PollHandlerItemB, Error>>> = loop {
            // TODO monitor the capacity of the wakeup lists
            trace4!("{selfname}  loop");
            if let Some(cid) = self2.wakeup_cids_tmp.pop() {
                self2.wakeup_cids.remove(&cid);
                hpp.mark_progress();
                trace2!("{selfname}  loop  ChannelHeap  waking  {cid}");
                if let Some(st1) = self2.by_cid.get_mut(&cid) {
                    match &mut st1.ch_handler {
                        ChHandler::ChHandlerActive(st2) => {
                            if st2.waker.will_wake(cx.waker()) {
                            } else {
                                st2.waker = waker1::waker(cid.clone(), cx.waker().clone(), self2.wakeup_cids.clone());
                            }
                            let cx2 = &mut Context::from_waker(&st2.waker);
                            let handler = Pin::new(&mut st2.handler);
                            match Self::poll_handler(
                                handler,
                                cx2,
                                cid.clone(),
                                &mut self2.ioid_reg,
                                &mut self2.subid_reg,
                                tsnow,
                            ) {
                                Ready(Some(x)) => {
                                    add_wakeup.push(cid);
                                    match x {
                                        Ok(x) => match x {
                                            PollHandlerItem::None => {}
                                            PollHandlerItem::ChannelHeapItem(item) => {
                                                error!("TODO handle PollHandlerItem::ChannelHeapItem(item)  {item:?}");
                                            }
                                            PollHandlerItem::ChHandlerMod => {
                                                error!("TODO handle PollHandlerItem::ChHandlerMod");
                                            }
                                            PollHandlerItem::ProtoOut(ca_msg) => {
                                                break Ready(Some(Ok(PollHandlerItemB::ProtoOut(ca_msg))));
                                            }
                                            PollHandlerItem::ChannelInfoQuery(item) => {
                                                break Ready(Some(Ok(PollHandlerItemB::ChannelInfoQuery(item))));
                                            }
                                            PollHandlerItem::TestValue(x) => {
                                                break Ready(Some(Ok(PollHandlerItemB::TestValue(x))));
                                            }
                                            PollHandlerItem::LocalLog(x) => {
                                                break Ready(Some(Ok(PollHandlerItemB::LocalLog(x))));
                                            }
                                            PollHandlerItem::ChannelEventValue(x) => {
                                                break Ready(Some(Ok(PollHandlerItemB::ChannelEventValue(x))));
                                            }
                                        },
                                        Err(e) => {
                                            error!("TODO handle Self::poll_handler  error  {e}");
                                            break Ready(Some(Err(e)));
                                        }
                                    }
                                }
                                Ready(None) => {
                                    // TODO remove it? No, then it would be less observable.
                                    // Instead, move it to Done, otherwise we continue polling all the time.
                                    // TODO after Ready(None), anything else to clean up or reuse?
                                    st1.ch_handler = ChHandler::Done;
                                    todo_shutdown!(
                                        "ChannelHeap:Handler:Finished  TODO handle finished channel handler gracefully {cid}"
                                    );
                                }
                                Pending => {
                                    trace_pending!("ChannelHeap:Handler  {cid}");
                                    hpp.mark_pending();
                                }
                            }
                        }
                        ChHandler::Done => {
                            // Wakeup marker for no longer served cid.
                            // TODO count for metrics
                        }
                    }
                } else {
                    warn!("ChannelHeap: no channel handler for wakeup cid {cid}");
                }
            } else {
                break Ready(None);
            }
        };
        for cid in add_wakeup {
            trace!("add for wakeup  {cid}");
            self2.wakeup_cids.insert(cid, ());
        }
        match &loopres {
            Ready(Some(_)) => loopres,
            _ => {
                if hpp.have_progress() {
                    Ready(Some(Ok(PollHandlerItemB::None)))
                } else if hpp.have_pending() {
                    Pending
                } else {
                    Ready(None)
                }
            }
        }
    }

    fn poll_all_handler(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<PollHandlerItem, Error>>> {
        use Poll::*;
        let selfname = "poll_all_handler";
        trace4!("{selfname}");
        loop {
            trace4!("{selfname}  loop");
            let mut hpp = HaveProgressPending::new();
            if let Some(mut fut) = self.poll_handler_fut.as_mut().map(Pin::new) {
                match fut.poll_unpin(cx) {
                    Ready(x) => {
                        trace4!("poll_handler_fut  Ready");
                        self.poll_handler_fut = None;
                        hpp.mark_progress();
                        match x {
                            Ok(()) => {}
                            Err(e) => {
                                break Ready(Some(Err(e)));
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else {
                match self.as_mut().poll_all_handler_sub(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => match x {
                                PollHandlerItemB::None => {
                                    trace4!("poll_all_handler_sub  None");
                                }
                                PollHandlerItemB::Fut(fut) => {
                                    trace4!("poll_all_handler_sub  Fut");
                                    self.poll_handler_fut = Some(fut);
                                }
                                PollHandlerItemB::ProtoOut(x) => {
                                    trace4!("poll_all_handler_sub  ProtoOut");
                                    break Ready(Some(Ok(PollHandlerItem::ProtoOut(x))));
                                }
                                PollHandlerItemB::ChannelInfoQuery(x) => {
                                    break Ready(Some(Ok(PollHandlerItem::ChannelInfoQuery(x))));
                                }
                                PollHandlerItemB::TestValue(x) => {
                                    break Ready(Some(Ok(PollHandlerItem::TestValue(x))));
                                }
                                PollHandlerItemB::LocalLog(x) => {
                                    break Ready(Some(Ok(PollHandlerItem::LocalLog(x))));
                                }
                                PollHandlerItemB::ChannelEventValue(x) => {
                                    break Ready(Some(Ok(PollHandlerItem::ChannelEventValue(x))));
                                }
                            },
                            Err(e) => {
                                error!("{selfname}  {e}");
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
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
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
                    let selfname = "{selfname}  RemoveChannel  fut";
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
                        let selfname = "{selfname}  RemoveChannel  fut  donecb";
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
        let selfname = "poll_next";
        trace4!("ChannelHeap  poll_next");
        let mut i1 = 0;
        loop {
            i1 += 1;
            if i1 > 2000 {
                panic!("i1 max");
            }
            let tsloop = Instant::now();
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
                    if self.inp_buf.len() < self.inp_buf.capacity() {
                        match self.proto_rx.poll_next_unpin(cx) {
                            Ready(Some(item)) => {
                                hpp.mark_progress();
                                trace2!("ChannelHeap:CaInp:Rx:Ok  {item:?}  ({n})", n = 1 + self.inp_buf.len());
                                self.inp_buf.push_back(item);
                            }
                            Ready(None) => {
                                hpp.mark_progress();
                                debug!("ChannelHeap:CaInp:Rx:Err");
                                self.inp_done = true;
                                self.by_cid.iter_mut().for_each(|(_, ce)| match &mut ce.ch_handler {
                                    ChHandler::ChHandlerActive(h1) => {
                                        h1.handler.inp_done();
                                    }
                                    ChHandler::Done => {}
                                });
                                self.state = State::Done;
                                break Ready(Some(Err(Error::ProtoRxClosed)));
                            }
                            Pending => {
                                hpp.mark_pending();
                                trace_pending!("ChannelHeap:CaInp:Rx");
                            }
                        }
                    } else {
                        warn!("{selfname}  SKIP proto_rx.poll_next_unpin  BLOCKED BY inp_buf");
                    }
                    match self.as_mut().dispatch_input_to_channels(cx) {
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
                    if self.proto_tx_buf.len() < self.proto_tx_buf.capacity() {
                        match self.as_mut().poll_all_handler(cx) {
                            Ready(Some(x)) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(x) => match x {
                                        PollHandlerItem::None => {}
                                        PollHandlerItem::ChannelHeapItem(item) => {
                                            error!("TODO handle ChannelHeapItem  {item:?}");
                                        }
                                        PollHandlerItem::ChHandlerMod => {}
                                        PollHandlerItem::ProtoOut(ca_msg) => {
                                            self.proto_tx_buf.push_back(ca_msg);
                                        }
                                        PollHandlerItem::ChannelInfoQuery(item) => {
                                            let inner = ItemInner::ChannelInfoQuery(item);
                                            let item = ChannelHeapItem {
                                                ts_create: tsloop,
                                                inner,
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                        PollHandlerItem::TestValue(x) => {
                                            let inner = ItemInner::TestValue(x);
                                            let item = ChannelHeapItem {
                                                ts_create: tsloop,
                                                inner,
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                        PollHandlerItem::LocalLog(x) => {
                                            let inner = ItemInner::LocalLog(x);
                                            let item = ChannelHeapItem {
                                                ts_create: tsloop,
                                                inner,
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                        PollHandlerItem::ChannelEventValue(x) => {
                                            let inner = ItemInner::ChannelEventValue(x);
                                            let item = ChannelHeapItem {
                                                ts_create: tsloop,
                                                inner,
                                            };
                                            break Ready(Some(Ok(item)));
                                        }
                                    },
                                    Err(e) => {
                                        self.state = State::Done;
                                        break Ready(Some(Err(e)));
                                    }
                                }
                            }
                            Ready(None) => {}
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        error!("{selfname}  SKIP poll_all_handler  BLOCKED BY proto_tx_buf");
                    }
                    if let Some(item) = self.proto_tx_buf.pop_front() {
                        trace!("self.proto_tx_buf.pop_front()");
                        match self.proto_tx.poll_send_unpin(item, cx) {
                            Ok(()) => {
                                hpp.mark_progress();
                                // TODO metrics
                            }
                            Err(x) => match x {
                                SendPollError::Full(x) => {
                                    hpp.mark_pending();
                                    self.proto_tx_buf.push_front(x);
                                }
                                SendPollError::Closed(_) => {
                                    error!("TODO connection closed, go into shutdown");
                                    hpp.mark_progress();
                                    // TODO check proper shutdown procedure
                                    self.state = State::Done;
                                }
                            },
                        }
                    }
                    if self.disconnect_on_idle && hpp.have_progress() == false {
                        if self.by_cid.len() == 0 {
                            debug!("disconnect_on_idle goto Done");
                            hpp.mark_progress();
                            self.state = State::Done;
                        }
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

    pub fn poll_next_unpin(
        &mut self,
        cmd_rx: &mut asynchan::Receiver<Cmd>,
        cx: &mut Context,
    ) -> Poll<Option<StreamItem>> {
        Pin::new(self).poll_next(cmd_rx, cx)
    }
}
