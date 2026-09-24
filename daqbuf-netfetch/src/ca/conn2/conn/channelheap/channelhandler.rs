const INP_BUF_CAP: usize = 64;
const CLOSE_RES_TIMEOUT_MS: u64 = 2000;

mod create;
mod fetchmpx;
mod readenum;
mod running;

use crate::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::CidOwned;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::Subid;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::StatusDetail;
use crate::ca::conn2::conn::StatusSel;
use crate::ca::conn2::conn::channelheap::IoidRegistry;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler::create::Creating;
use crate::ca::conn2::conn::channelheap::channelhandler::create::CreatingSerde;
use crate::ca::conn2::conn::channelheap::channelhandler::running::Running;
use crate::ca::conn2::conn::channelheap::channelhandler::running::RunningStateSerde;
use crate::ca::conn2::locallog;
use crate::ca::conn2::timeoutable;
use crate::ca::connset2::connset::channeltrace::CaProto;
use crate::ca::connset2::connset::channeltrace::ChannelTraceItem;
use crate::ca::connset2::connset::channeltrace::ChannelTraceItemInner;
use crate::ca::connset2::connset::channeltrace::ChannelTraceL1Item;
use crate::ca::connset2::connset::channeltrace::Created;
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
use netpod::channelstatus::ChannelStatusClosedReason;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use serde_helper::ToSerde;
use serde_helper::to_serde::HasLenCap;
use serde_helper::to_serde::LenCap;
use serieswriter::binwriter::BinWriter;
use stats::mett::ChannelHandlerMetrics;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use timeoutable::Timeoutable;
use utoipa::ToSchema;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
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
        ReadEnum(#[from] readenum::Error),
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

#[derive(Debug, Clone)]
pub enum ClosingReason {
    ErrorMsg(String),
    InputDone,
    Command,
    PeerClosed,
}

impl ClosingReason {
    fn to_channel_status_closed_reason(&self) -> ChannelStatusClosedReason {
        match self {
            ClosingReason::ErrorMsg(_) => ChannelStatusClosedReason::ProtocolError,
            ClosingReason::InputDone => ChannelStatusClosedReason::NoProtocol,
            ClosingReason::Command => ChannelStatusClosedReason::ChannelRemove,
            ClosingReason::PeerClosed => ChannelStatusClosedReason::ProtocolDone,
        }
    }

    pub fn no_more_protocol(&self) -> bool {
        match self {
            ClosingReason::ErrorMsg(_) => false,
            ClosingReason::InputDone => true,
            ClosingReason::Command => false,
            ClosingReason::PeerClosed => true,
        }
    }
}

#[derive(Debug)]
struct Init {}

#[derive(Debug)]
struct CloseWait {
    to: FutDbg<()>,
}

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub", serde(tag = "ty"), derive(utoipa::ToSchema))]
enum State {
    Init {
        #[to_serde(elapsed, dwell_ms = 4000, schema(value_type = String))]
        ts: Instant,
        #[to_serde(skip)]
        init: Init,
    },
    Creating {
        #[to_serde(elapsed, dwell_ms = 8000, schema(value_type = String))]
        ts: Instant,
        #[to_serde(nest, schema(value_type = CreatingSerde))]
        state: Creating,
    },
    ReadEnum {
        #[to_serde(elapsed, dwell_ms = 4000, schema(value_type = String))]
        ts: Instant,
        #[to_serde(nest, schema(value_type = readenum::ReadEnumSerde))]
        state: readenum::ReadEnum,
    },
    Running {
        #[to_serde(elapsed, schema(value_type = String))]
        ts: Instant,
        #[to_serde(nest, schema(value_type = RunningStateSerde))]
        state: Running,
    },
    CloseSend {
        #[to_serde(elapsed, dwell_ms = 1000, schema(value_type = String))]
        ts: Instant,
    },
    CloseWait {
        #[to_serde(elapsed, dwell_ms = 2000, schema(value_type = String))]
        ts: Instant,
        #[to_serde(skip)]
        state: CloseWait,
    },
    Done1 {
        #[to_serde(elapsed, dwell_ms = 4000, schema(value_type = String))]
        ts: Instant,
    },
    Done {
        #[to_serde(elapsed, schema(value_type = String))]
        ts: Instant,
    },
}

impl State {
    fn str(&self) -> &str {
        self.name_short()
    }

    fn state_since(&self) -> Instant {
        match self {
            State::Init { ts, .. } => *ts,
            State::Creating { ts, .. } => *ts,
            State::ReadEnum { ts, .. } => *ts,
            State::Running { ts, .. } => *ts,
            State::CloseSend { ts, .. } => *ts,
            State::CloseWait { ts, .. } => *ts,
            State::Done1 { ts } => *ts,
            State::Done { ts } => *ts,
        }
    }
}

impl State {
    fn name_short(&self) -> &str {
        match self {
            State::Init { .. } => "Init",
            State::Creating { .. } => "Creating",
            State::ReadEnum { .. } => "ReadEnum",
            State::Running { .. } => "Running",
            State::CloseSend { .. } => "CloseSend",
            State::CloseWait { .. } => "CloseWait",
            State::Done1 { .. } => "Done1",
            State::Done { .. } => "Done",
        }
    }
}

#[derive(Debug)]
pub enum ItemInner {
    ChannelInfoQuery(dbpg::seriesbychannel::ChannelInfoQuery),
    ProtoOut(CaMsg),
    ProtoOutIoid(CaMsg, Sid, Instant),
    ProtoOutSubid(CaMsg, Instant),
    SubidRemove(Cid),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelStatus(ChannelStatus),
    ChannelEventValue(ChannelEventValue),
    ChannelWriteItems(Vec<QueryItem>),
    ChannelTrace(ChannelTraceItem),
}

#[derive(Debug)]
pub struct ChannelHandlerItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

pub struct FullSnap(pub <ChannelHandler as ToSerde>::Serde);

impl fmt::Debug for FullSnap {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("FullSnap {{ TODO }}")
    }
}

impl Serialize for FullSnap {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(ser)
    }
}

#[derive(Debug, Serialize)]
pub struct StatusInfo {
    pub state_short: String,
    pub state_elapsed_ms: u64,
    pub proto_inp_buf: LenCap,
    pub outbuf: LenCap,
    pub enum_variants_len: Option<u32>,
    pub counters: Counters,
    /// Only present when the request asked for `StatusDetail::Full`.
    pub full: Option<Box<FullSnap>>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Counters {
    pub event_add_res_cnt: u64,
}

impl Counters {
    fn new() -> Self {
        Self { event_add_res_cnt: 0 }
    }

    pub fn zero() -> Self {
        Self::new()
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

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub", derive(utoipa::ToSchema))]
#[to_serde(extra(cid: Cid = self.cid.to_cid()))]
pub struct ChannelHandler {
    #[to_serde(nest, schema(value_type = StateSerde))]
    state: State,
    #[to_serde(skip)]
    removing: Option<asynchan::Sender<u32>>,
    #[to_serde(skip)]
    cid: CidOwned,
    #[to_serde(skip)]
    sid: Option<Sid>,
    #[to_serde(skip)]
    closing: Option<ClosingReason>,
    peer_closed: bool,
    chan_close_ack: bool,
    backend: String,
    #[to_serde(schema(value_type = Object))]
    conf: ChannelConfig,
    enum_variants: Option<Vec<String>>,
    #[to_serde(len)]
    proto_inp_buf: VecDeque<ProtoRxItem>,
    proto_inp_done: bool,
    counters: Counters,
    #[to_serde(skip)]
    cmd_tx: asynchan::Sender<Cmd>,
    #[to_serde(skip)]
    cmd_rx: asynchan::Receiver<Cmd>,
    #[to_serde(len)]
    outbuf: VecDeque<ChannelHandlerItem>,
    #[to_serde(skip)]
    mett: ChannelHandlerMetrics,
    #[to_serde(skip)]
    waker_1: Option<Waker>,
    #[to_serde(skip)]
    waker_2: Option<Waker>,
    #[to_serde(skip)]
    ioid_reg: Arc<IoidRegistry>,
}

impl ChannelHandler {
    pub(super) fn new(backend: String, conf: ChannelConfig, ioid_reg: Arc<IoidRegistry>) -> Self {
        let cid = CidOwned::new();
        trace!("ChannelHandler::new  {}", conf.name());
        trace2!("ChannelHandler::new  {cid:?}  {conf:?}");
        let (cmd_tx, cmd_rx) = asynchan::bounded(16, "ChannelHandler-cmd");
        Self {
            state: State::Init {
                ts: Instant::now(),
                init: Init {},
            },
            removing: None,
            cid,
            sid: None,
            closing: None,
            peer_closed: false,
            chan_close_ack: false,
            backend,
            conf,
            enum_variants: None,
            proto_inp_buf: VecDeque::with_capacity(INP_BUF_CAP),
            proto_inp_done: false,
            counters: Counters::new(),
            cmd_tx,
            cmd_rx,
            outbuf: VecDeque::new(),
            mett: ChannelHandlerMetrics::new(),
            waker_1: None,
            waker_2: None,
            ioid_reg,
        }
    }

    pub fn status_info(&self) -> StatusInfo {
        self.status_info_sel(&StatusSel::periodic())
    }

    pub fn status_info_sel(&self, sel: &StatusSel) -> StatusInfo {
        let full = match sel.detail {
            StatusDetail::Full => Some(Box::new(FullSnap(self.to_serde()))),
            StatusDetail::Light => None,
        };
        StatusInfo {
            state_short: self.state.name_short().into(),
            state_elapsed_ms: self.state.state_since().elapsed().as_millis() as u64,
            proto_inp_buf: self.proto_inp_buf.len_cap(),
            outbuf: self.outbuf.len_cap(),
            enum_variants_len: self.enum_variants.as_ref().map(|x| x.len() as u32),
            counters: self.counters.clone(),
            full,
        }
    }

    pub fn state_serde(&self) -> StateSerde {
        self.state.to_serde()
    }

    pub fn state_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self.state.to_serde())
            .unwrap_or_else(|e| serde_json::json!({"error":format!("serde: {e}")}))
    }

    pub fn handle_dyn_cmd_v03(&mut self, cmd: serde_json::Value) -> impl Future<Output = serde_json::Value> + use<> {
        use futures::future::ready;
        use serde_json::json;
        let v = match serde_json::to_value(self.to_serde()) {
            Ok(x) => json!({
                "state": {
                    "type": "ChannelHandler",
                    "handler": x,
                },
            }),
            Err(e) => json!({
                "state": {
                    "type": "ChannelHandler",
                    "error": e.to_string(),
                },
            }),
        };
        ready(v)
    }

    pub fn cmd_read_notify(&mut self, mut resp_tx: asynchan::Sender<serde_json::Value>) -> serde_json::Value {
        use serde_json::json;
        match &mut self.state {
            State::Running { state, .. } => state.cmd_read_notify(resp_tx),
            _ => {
                let v = json!({"error": format!("ChannelHandler  {}", self.state.str())});
                let _ = resp_tx.try_send(v.clone());
                v
            }
        }
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

    /// Harvest the metrics of this handler and of the current state.
    pub fn mett_take(&mut self) -> ChannelHandlerMetrics {
        let mut ret = std::mem::replace(&mut self.mett, ChannelHandlerMetrics::new());
        match &mut self.state {
            State::ReadEnum { state: st, .. } => {
                ret.ingest(st.mett_take());
            }
            State::Running { state: st, .. } => {
                ret.ingest(st.mett_take());
            }
            _ => {}
        }
        ret
    }

    pub fn channel_config(&self) -> &ChannelConfig {
        &self.conf
    }

    pub fn cid(&self) -> Cid {
        self.cid.to_cid()
    }

    pub fn sid(&self) -> Option<Sid> {
        self.sid.clone()
    }

    pub fn cmd_tx(&self) -> &asynchan::Sender<Cmd> {
        &self.cmd_tx
    }

    /// Every site that pops from `proto_inp_buf` must pass the item through here. The pop
    /// sites are drain loops, so this has to run per item, not as a peek at the front.
    fn intercept_close_msg(
        item: ProtoRxItem,
        closing: &Option<ClosingReason>,
        peer_closed: &mut bool,
        chan_close_ack: &mut bool,
        chn: &str,
    ) -> Option<ProtoRxItem> {
        match &item.msg.ty {
            CaMsgTy::ChannelCloseRes(_) => {
                *chan_close_ack = true;
                if closing.is_none() {
                    warn!("unsolicited ChannelCloseRes  {chn}");
                    *peer_closed = true;
                }
                None
            }
            CaMsgTy::ChannelDisconnect(_) => {
                warn!("ChannelDisconnect  {chn}");
                *peer_closed = true;
                None
            }
            CaMsgTy::ChannelClose(_) => {
                warn!("unexpected ChannelClose from server  {chn}");
                *peer_closed = true;
                None
            }
            _ => Some(item),
        }
    }

    fn drain_inp_closing(
        buf: &mut VecDeque<ProtoRxItem>,
        closing: &Option<ClosingReason>,
        peer_closed: &mut bool,
        chan_close_ack: &mut bool,
        chn: &str,
    ) -> bool {
        let mut any = false;
        while let Some(item) = buf.pop_front() {
            any = true;
            if let Some(item) = Self::intercept_close_msg(item, closing, peer_closed, chan_close_ack, chn) {
                debug!("discard while closing  {chn}  {item:?}");
            }
        }
        any
    }

    fn note_closing(
        closing: &mut Option<ClosingReason>,
        outbuf: &mut VecDeque<ChannelHandlerItem>,
        reason: ClosingReason,
    ) {
        if closing.is_some() {
            return;
        }
        outbuf.push_back(ChannelHandlerItem {
            ts_create: Instant::now(),
            inner: ItemInner::ChannelStatus(ChannelStatus::Closed(reason.to_channel_status_closed_reason())),
        });
        *closing = Some(reason);
    }

    fn initiate_close(&mut self, reason: ClosingReason) {
        if self.closing.is_some() {
            return;
        }
        debug!("initiate_close  {}  {reason:?}", self.conf.name());
        Self::note_closing(&mut self.closing, &mut self.outbuf, reason.clone());
        match &mut self.state {
            State::Init { .. } => {
                self.state = State::Done1 { ts: Instant::now() };
            }
            State::Creating { state: st, .. } => st.trigger_close(reason),
            State::ReadEnum { state: st, .. } => st.trigger_close(reason),
            State::Running { state: st, .. } => st.trigger_close(reason),
            State::CloseSend { .. } => {}
            State::CloseWait { .. } => {}
            State::Done1 { .. } => {}
            State::Done { .. } => {}
        }
    }

    fn enter_close_send(&mut self, ts: Instant) {
        if self.closing.is_none() {
            warn!("sub-handler ended without a close request  {}", self.conf.name());
            Self::note_closing(&mut self.closing, &mut self.outbuf, ClosingReason::InputDone);
        }
        let no_proto = self.closing.as_ref().map_or(false, |x| x.no_more_protocol());
        if self.sid.is_none() || self.peer_closed || self.proto_inp_done || no_proto {
            self.state = State::Done1 { ts };
        } else {
            self.state = State::CloseSend { ts };
        }
    }

    fn handle_cmd_remove(&mut self, done_tx: asynchan::Sender<u32>) {
        let selfname = "handle_cmd_remove";
        debug!("{selfname}  {}", self.conf.name());
        self.removing = Some(done_tx);
        self.initiate_close(ClosingReason::Command);
    }

    fn handle_cmd(&mut self, cmd: Cmd) {
        let selfname = "handle_cmd";
        match cmd {
            Cmd::Remove(done_tx) => {
                if self.removing.is_some() {
                    warn!("{selfname}  already removing")
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
        closing: &Option<ClosingReason>,
        peer_closed: &mut bool,
        chan_close_ack: &mut bool,
        chn: &str,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<u32, Error>>> {
        let selfname = "poll_proto_rx_creating";
        use Poll::*;
        trace4!("{selfname}");
        let mut idp = 0;
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(item) = buf.pop_front() {
                let Some(item) = Self::intercept_close_msg(item, closing, peer_closed, chan_close_ack, chn) else {
                    continue;
                };
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
                trace4!("{selfname}  HPP:Progress");
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
        let abort_close = match &mut self.state {
            State::Init { .. } => false,
            State::Creating { state: st, .. } => {
                st.inp_done();
                false
            }
            State::ReadEnum { state: st, .. } => {
                st.inp_done();
                false
            }
            State::Running { state: st, .. } => {
                st.inp_done();
                false
            }
            State::CloseSend { .. } => true,
            State::CloseWait { .. } => true,
            State::Done1 { .. } => false,
            State::Done { .. } => false,
        };
        self.initiate_close(ClosingReason::InputDone);
        if abort_close {
            self.state = State::Done1 { ts: Instant::now() };
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
            let mut close_request: Option<ClosingReason> = None;
            let self2 = self.as_mut().get_mut();
            if let Some(item) = self2.outbuf.pop_front() {
                break Ready(Some(Ok(item)));
            }
            match self2.cmd_rx.poll_next_unpin(cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match &mut self2.state {
                        State::Done { .. } => {
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
                State::Init { .. } => {
                    self2.state = State::Creating {
                        ts: tsloop,
                        state: Creating::new(self2.cid.to_cid(), self2.conf.name().into(), self2.backend.clone()),
                    };
                    hpp.mark_progress();
                }
                State::Creating {
                    ts: ts_creating,
                    state: st1,
                } => {
                    match Self::poll_proto_rx_creating(
                        Pin::new(st1),
                        &mut self2.proto_inp_buf,
                        &mut self2.proto_inp_done,
                        &mut self2.waker_2,
                        &self2.closing,
                        &mut self2.peer_closed,
                        &mut self2.chan_close_ack,
                        self2.conf.name(),
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
                                    self2.state = State::Done1 { ts: tsloop };
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    if self2.peer_closed && self2.closing.is_none() {
                        hpp.mark_progress();
                        Self::note_closing(&mut self2.closing, &mut self2.outbuf, ClosingReason::PeerClosed);
                        st1.notify_peer_closed();
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
                                        trace2!("got create::CreatingItem::Done  {scalar_type}  {shape}");
                                        self2.sid = Some(sid.clone());
                                        // TODO guard on outbuf len?
                                        self2.outbuf.push_back(ChannelHandlerItem {
                                            ts_create: Instant::now(),
                                            inner: ItemInner::ChannelTrace(ChannelTraceItem::new(
                                                ChannelTraceItemInner::CaProto(CaProto::Created(Created::new(
                                                    ts_creating.elapsed(),
                                                ))),
                                            )),
                                        });
                                        if let netpod::ScalarType::Enum = scalar_type {
                                            self2.state = State::ReadEnum {
                                                ts: tsloop,
                                                state: readenum::ReadEnum::new(
                                                    self2.cid(),
                                                    sid,
                                                    scalar_type,
                                                    shape,
                                                    ca_dbr_ty,
                                                    chi,
                                                    self2.conf.clone(),
                                                ),
                                            };
                                        } else {
                                            match Running::new(
                                                self2.cid(),
                                                sid,
                                                scalar_type,
                                                shape,
                                                ca_dbr_ty,
                                                chi,
                                                self2.conf.clone(),
                                                self2.ioid_reg.clone(),
                                            ) {
                                                Ok(running) => {
                                                    self2.state = State::Running {
                                                        ts: tsloop,
                                                        state: running,
                                                    };
                                                }
                                                Err(e) => {
                                                    error!("Running::new failed  {e}");
                                                    self2.state = State::Done1 { ts: tsloop };
                                                    break Ready(Some(Err(e.into())));
                                                }
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    info!("ChannelHandler:Creating:state:Ready:Err {e}");
                                    if false {
                                        let ptr = crate::ca::conn2::conn::CONN_DBG_PTR.load(atomic::Ordering::Acquire);
                                        let ptr = ptr as *const crate::ca::conn2::conn::CaConn;
                                        let x = unsafe { &*ptr };
                                        x.dump_state_poll();
                                        std::process::exit(88);
                                    }
                                    self2.state = State::Done1 { ts: Instant::now() };
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            self2.enter_close_send(tsloop);
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::ReadEnum { state: st2, .. } => {
                    let vi = &mut self2.proto_inp_buf;
                    while let Some(item) = vi.pop_front() {
                        let Some(item) = Self::intercept_close_msg(
                            item,
                            &self2.closing,
                            &mut self2.peer_closed,
                            &mut self2.chan_close_ack,
                            self2.conf.name(),
                        ) else {
                            hpp.mark_progress();
                            continue;
                        };
                        match st2.inp_push_try(item) {
                            Some(x) => {
                                hpp.mark_pending();
                                vi.push_front(x);
                                break;
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
                    }
                    if self2.peer_closed && self2.closing.is_none() {
                        hpp.mark_progress();
                        Self::note_closing(&mut self2.closing, &mut self2.outbuf, ClosingReason::PeerClosed);
                        st2.notify_peer_closed();
                    }
                    match st2.poll_next_unpin(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => match x {
                                    readenum::ReadEnumItem::CaMsgOutIoid(msg, sid, ts) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: Instant::now(),
                                            inner: ItemInner::ProtoOutIoid(msg, sid, ts),
                                        })));
                                    }
                                    readenum::ReadEnumItem::LocalLog(x) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: Instant::now(),
                                            inner: ItemInner::LocalLog(x),
                                        })));
                                    }
                                    readenum::ReadEnumItem::EnumStringSet(
                                        sid,
                                        scalar_type,
                                        shape,
                                        ca_dbr_ty,
                                        chi,
                                        vars,
                                    ) => {
                                        trace!("EnumStringSet  {vars:?}");
                                        match Running::new(
                                            self2.cid(),
                                            sid,
                                            scalar_type,
                                            shape,
                                            ca_dbr_ty,
                                            chi,
                                            self2.conf.clone(),
                                            self2.ioid_reg.clone(),
                                        ) {
                                            Ok(running) => {
                                                self2.state = State::Running {
                                                    ts: tsloop,
                                                    state: running,
                                                };
                                            }
                                            Err(e) => {
                                                error!("Running::new failed  {e}");
                                                self2.state = State::Done1 { ts: tsloop };
                                                break Ready(Some(Err(e.into())));
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    info!("ChannelHandler:Running:Ready:Err {e}");
                                    self2.state = State::Done1 { ts: tsloop };
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            self2.enter_close_send(tsloop);
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Running { state: st2, .. } => {
                    let vi = &mut self2.proto_inp_buf;
                    while let Some(item) = vi.pop_front() {
                        let Some(item) = Self::intercept_close_msg(
                            item,
                            &self2.closing,
                            &mut self2.peer_closed,
                            &mut self2.chan_close_ack,
                            self2.conf.name(),
                        ) else {
                            hpp.mark_progress();
                            continue;
                        };
                        match st2.inp_push_try(item) {
                            Some(x) => {
                                hpp.mark_pending();
                                vi.push_front(x);
                                break;
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
                    }
                    if self2.peer_closed && self2.closing.is_none() {
                        hpp.mark_progress();
                        Self::note_closing(&mut self2.closing, &mut self2.outbuf, ClosingReason::PeerClosed);
                        st2.notify_peer_closed();
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
                                    running::RunningItem::SubidRemove(cid) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::SubidRemove(cid),
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
                                    running::RunningItem::ChannelWriteItems(x) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ChannelWriteItems(x),
                                        })));
                                    }
                                    running::RunningItem::RequestClose(reason) => {
                                        close_request = Some(reason);
                                    }
                                    running::RunningItem::ChannelTrace(x) => {
                                        break Ready(Some(Ok(ChannelHandlerItem {
                                            ts_create: tsloop,
                                            inner: ItemInner::ChannelTrace(x),
                                        })));
                                    }
                                },
                                Err(e) => {
                                    info!("ChannelHandler:Running:Ready:Err {e}");
                                    self2.state = State::Done1 { ts: tsloop };
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {
                            hpp.mark_progress();
                            self2.enter_close_send(tsloop);
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::CloseSend { .. } => {
                    hpp.mark_progress();
                    Self::drain_inp_closing(
                        &mut self2.proto_inp_buf,
                        &self2.closing,
                        &mut self2.peer_closed,
                        &mut self2.chan_close_ack,
                        self2.conf.name(),
                    );
                    let item = match &self2.sid {
                        Some(sid) => {
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
                        }
                        None => {
                            warn!("can not close channel, never fully created  {}", self2.conf.name());
                            None
                        }
                    };
                    self2.state = State::CloseWait {
                        ts: tsloop,
                        state: CloseWait {
                            to: tokio::time::sleep(Duration::from_millis(CLOSE_RES_TIMEOUT_MS)).box2(),
                        },
                    };
                    if let Some(item) = item {
                        break Ready(Some(Ok(item)));
                    }
                }
                State::CloseWait { state: st2, .. } => {
                    if Self::drain_inp_closing(
                        &mut self2.proto_inp_buf,
                        &self2.closing,
                        &mut self2.peer_closed,
                        &mut self2.chan_close_ack,
                        self2.conf.name(),
                    ) {
                        hpp.mark_progress();
                    }
                    if self2.chan_close_ack {
                        hpp.mark_progress();
                        trace2!("CloseWait done");
                        self2.state = State::Done1 { ts: tsloop };
                    } else if self2.proto_inp_done {
                        hpp.mark_progress();
                        debug!("input gone while waiting for close confirm");
                        self2.state = State::Done1 { ts: tsloop };
                    } else {
                        match st2.to.poll_unpin(cx) {
                            Ready(()) => {
                                hpp.mark_progress();
                                warn!("channel close timeout  {}", self2.conf.name());
                                self2.state = State::Done1 { ts: tsloop };
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    }
                }
                State::Done1 { .. } => {
                    let name = self.conf.name();
                    trace!("{selfname}  Done1  {name}");
                    if let Some(tx) = self.removing.as_mut() {
                        let _ = tx.try_send(0);
                    }
                    self.state = State::Done { ts: tsloop };
                    hpp.mark_progress();
                }
                State::Done { .. } => {}
            }
            if let Some(reason) = close_request {
                hpp.mark_progress();
                self.as_mut().get_mut().initiate_close(reason);
            }
            break if hpp.have_progress() {
                trace4!("HPP:Progress");
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

#[cfg(test)]
mod test_close_sequence {
    use super::*;
    use futures::task::noop_waker;

    fn rx_item(ty: CaMsgTy) -> ProtoRxItem {
        let ts = Instant::now();
        ProtoRxItem {
            msg: CaMsg::from_ty_ts(ty, ts),
            tscmd: ts,
            tsreg: ts,
            tsdisp: ts,
        }
    }

    fn event_add_res() -> CaMsgTy {
        CaMsgTy::EventAddResEmpty(proto::EventAddResEmpty {
            data_type: 0,
            sid: 7,
            subid: 1,
        })
    }

    fn close_res() -> CaMsgTy {
        CaMsgTy::ChannelCloseRes(proto::ChannelCloseRes { sid: 7, cid: 3 })
    }

    fn handler() -> ChannelHandler {
        let conf = ChannelConfig::st_monitor("SOME:CHANNEL", "test.hcl");
        let ioid_reg = IoidRegistry::testing();
        ChannelHandler::new("testbackend".into(), conf, ioid_reg)
    }

    #[test]
    fn intercept_consumes_only_close_protocol() {
        let mut peer_closed = false;
        let mut ack = false;
        let pass =
            ChannelHandler::intercept_close_msg(rx_item(event_add_res()), &None, &mut peer_closed, &mut ack, "CH");
        assert!(pass.is_some(), "a normal message must be forwarded");
        assert!(!peer_closed);
        assert!(!ack);

        let taken = ChannelHandler::intercept_close_msg(rx_item(close_res()), &None, &mut peer_closed, &mut ack, "CH");
        assert!(taken.is_none(), "ChannelCloseRes must be consumed");
        assert!(ack);
        assert!(peer_closed, "an ack we did not ask for means the peer closed");
    }

    #[test]
    fn solicited_ack_is_not_treated_as_peer_close() {
        let mut peer_closed = false;
        let mut ack = false;
        let closing = Some(ClosingReason::Command);
        ChannelHandler::intercept_close_msg(rx_item(close_res()), &closing, &mut peer_closed, &mut ack, "CH");
        assert!(ack);
        assert!(!peer_closed);
    }

    #[test]
    fn ack_behind_a_batch_is_intercepted() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _g = rt.enter();
        let mut ch = handler();
        for _ in 0..5 {
            ch.proto_inp_buf.push_back(rx_item(event_add_res()));
        }
        ch.proto_inp_buf.push_back(rx_item(close_res()));
        let w = noop_waker();
        let mut cx = Context::from_waker(&w);
        let _ = Pin::new(&mut ch).poll_next(&mut cx);
        assert!(ch.chan_close_ack, "ack behind the batch was missed");
        assert!(ch.peer_closed);
        assert!(ch.closing.is_some(), "peer close must start the close sequence");
    }

    #[test]
    fn peer_close_skips_the_close_command() {
        let mut ch = handler();
        ch.sid = Some(Sid::new(7));
        ch.peer_closed = true;
        ch.closing = Some(ClosingReason::PeerClosed);
        ch.enter_close_send(Instant::now());
        assert_eq!(ch.state.name_short(), "Done1");
    }

    #[test]
    fn never_created_skips_the_close_command() {
        let mut ch = handler();
        ch.closing = Some(ClosingReason::Command);
        ch.enter_close_send(Instant::now());
        assert_eq!(ch.state.name_short(), "Done1");
    }

    #[test]
    fn healthy_close_sends_exactly_one_close_command() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _g = rt.enter();
        let mut ch = handler();
        ch.sid = Some(Sid::new(7));
        ch.closing = Some(ClosingReason::Command);
        ch.enter_close_send(Instant::now());
        assert_eq!(ch.state.name_short(), "CloseSend");

        let w = noop_waker();
        let mut cx = Context::from_waker(&w);
        let mut close_cnt = 0;
        for _ in 0..8 {
            if let Poll::Ready(Some(Ok(item))) = Pin::new(&mut ch).poll_next(&mut cx) {
                if let ItemInner::ProtoOut(msg) = &item.inner {
                    if let CaMsgTy::ChannelClose(x) = &msg.ty {
                        close_cnt += 1;
                        assert_eq!(x.sid, 7);
                    }
                }
            }
        }
        assert_eq!(close_cnt, 1, "ChannelClose must be emitted exactly once");
        assert_eq!(ch.state.name_short(), "CloseWait");

        ch.proto_inp_buf.push_back(rx_item(close_res()));
        let _ = Pin::new(&mut ch).poll_next(&mut cx);
        assert!(ch.chan_close_ack);
        assert!(
            matches!(ch.state.name_short(), "Done1" | "Done"),
            "the ack must end the wait, state is {}",
            ch.state.name_short()
        );
    }

    #[test]
    fn input_gone_while_closing_does_not_panic() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _g = rt.enter();
        let mut ch = handler();
        ch.sid = Some(Sid::new(7));
        ch.closing = Some(ClosingReason::Command);
        ch.enter_close_send(Instant::now());
        let w = noop_waker();
        let mut cx = Context::from_waker(&w);
        let _ = Pin::new(&mut ch).poll_next(&mut cx);
        assert_eq!(ch.state.name_short(), "CloseWait");
        ch.inp_done();
        assert_eq!(ch.state.name_short(), "Done1");
    }
}

#[cfg(test)]
mod test_state_serde {
    use super::*;

    #[test]
    fn channel_handler_snapshot_shape() {
        let conf = ChannelConfig::st_monitor("SOME:CHANNEL", "test.hcl");
        let ioid_reg = IoidRegistry::testing();
        let mut ch = ChannelHandler::new("testbackend".into(), conf, ioid_reg);
        let v = serde_json::to_value(ch.to_serde()).unwrap();
        assert_eq!(v["state"]["ty"], "Init");
        assert_eq!(v["backend"], "testbackend");
        assert_eq!(v["conf"]["name"], "SOME:CHANNEL");
        assert_eq!(v["proto_inp_buf"]["cap"], INP_BUF_CAP);
        assert_eq!(v["proto_inp_buf"]["len"], 0);
        assert_eq!(v["counters"]["event_add_res_cnt"], 0);
        assert!(v["cid"].is_number(), "cid missing in {v}");
        for k in ["cmd_tx", "cmd_rx", "mett", "waker_1", "waker_2", "removing"] {
            assert!(v.get(k).is_none(), "{k} leaked into {v}");
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _g = rt.enter();
        ch.state = State::Creating {
            ts: Instant::now(),
            state: Creating::new(ch.cid.to_cid(), "SOME:CHANNEL".into(), "testbackend".into()),
        };
        let v = serde_json::to_value(ch.state_serde()).unwrap();
        assert_eq!(v["ty"], "Creating");
        assert!(v["ts"].is_string(), "no time-in-state in {v}");
        assert!(v["dwell_score"].is_number(), "no dwell_score in {v}");
        assert_eq!(v["state"]["state"]["ty"], "CreateChanSend");
        assert!(v["state"]["state"]["ts"].is_string());
        assert!(v["state"]["state"]["dwell_score"].is_number());
        assert_eq!(v["state"]["state"]["outbuf"]["len"], 1);
    }
}
