pub(super) const INP_BUF_CAP: usize = 64;
const READ_NOTIFY_FUTS_CAP: usize = 16;
const READ_NOTIFY_CMD_TIMEOUT_MS: u64 = 3000;

mod consume_event_data;

use super::fetchmpx::Fetchmpx;
use crate::asynchan;
use crate::ca::conn2::ca_writer_value::CaRtWriter;
use crate::ca::conn2::ca_writer_value::CaWriterValueState;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Ioid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::channelheap::IoidRegistry;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler::ClosingReason;
use crate::ca::conn2::conn::channelheap::channelhandler::fetchmpx;
use crate::ca::conn2::locallog;
use crate::ca::connset2::connset::channeltrace::ChannelTraceItemInner;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use dbpg::seriesbychannel::ChannelInfoResult;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::channel::oneshot;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
use netpod::channelstatus::ChannelStatus;
use netpod::ttl::RetentionTime;
use scywr::iteminsertqueue::QueryItem;
use serde_helper::ToSerde;
use series::ChannelStatusSeriesId;
use series::SeriesId;
use serieswriter::binwriter::BinWriter;
use serieswriter::binwriter::DiscardFirstOutput;
use serieswriter::binwriter::WriteCntZero;
use stats::mett::ChannelHandlerMetrics;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
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

macro_rules! todo_shutdown { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! debug_shutdown { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace_shutdown { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

fn _keep() {
    info!("");
    trace2!("");
}

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
        MissingTimestamp,
        Register(#[from] dbpg::seriesbychannel::Error),
        RtWriter(#[from] serieswriter::rtwriter::Error),
        BinWriter(#[from] serieswriter::binwriter::Error),
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
pub enum RunningItem {
    CaMsgOut(CaMsg),
    CaMsgOutIoid(CaMsg, Sid, Instant),
    CaMsgOutSubid(CaMsg, Instant),
    SubidRemove(Cid),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelStatus(ChannelStatus),
    ChannelEventValue(ChannelEventValue),
    ChannelWriteItems(Vec<QueryItem>),
    RequestClose(ClosingReason),
    ChannelTrace(ChannelTraceItemInner),
}

#[derive(Debug, ToSerde)]
#[to_serde(
    vis = "pub",
    name = RunningStateSerde,
    serde(tag = "ty"),
    derive(utoipa::ToSchema)
)]
enum State {
    #[to_serde(schema(value_type = NormalSerde))]
    Normal(#[to_serde(nest)] Normal),
    Done,
}

impl State {
    fn str(&self) -> &str {
        match self {
            State::Normal(..) => "Normal",
            State::Done => "Done",
        }
    }
}

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub", derive(utoipa::ToSchema))]
struct Normal {
    #[to_serde(nest, schema(value_type = fetchmpx::FetchmpxSerde))]
    fetchmpx: Fetchmpx,
    /// In-flight `cmd_read_notify` command futures, capped at `READ_NOTIFY_FUTS_CAP`.
    #[to_serde(len)]
    futs: VecDeque<Option<(Ioid, FutDbg<()>)>>,
    /// Completed together with the matching `futs` slot (see `Running::poll_futs`),
    /// so a timed-out command can't leak a stale entry here.
    #[to_serde(skip)]
    read_notify_pending: HashMap<Ioid, oneshot::Sender<proto::ReadNotifyRes>>,
}

#[derive(Debug, ToSerde)]
#[to_serde(vis = "pub", derive(utoipa::ToSchema))]
pub struct Running {
    #[to_serde(nest, schema(value_type = RunningStateSerde))]
    state: State,
    /// Set by `transition_state`, reported as time-in-state.
    #[to_serde(elapsed, schema(value_type = String))]
    state_dt: Instant,
    cid: Cid,
    sid: Sid,
    #[to_serde(schema(value_type = Object))]
    chi: ChannelInfoResult,
    removing: bool,
    #[to_serde(len)]
    outbuf: VecDeque<CaMsg>,
    #[to_serde(len)]
    trace_outbuf: VecDeque<ChannelTraceItemInner>,
    #[to_serde(len)]
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    #[to_serde(skip)]
    mett: ChannelHandlerMetrics,
    #[to_serde(skip)]
    rtwriter: CaRtWriter,
    #[to_serde(skip)]
    binwriter: Option<BinWriter>,
    #[to_serde(skip)]
    crst: consume_event_data::ChannelConsumeState,
    use_ioc_time: bool,
    #[to_serde(skip)]
    chname: String,
    #[to_serde(skip)]
    ioid_reg: Arc<IoidRegistry>,
}

impl Running {
    /// Single choke point for state changes so the time-in-state stamp can not drift.
    fn transition_state(&mut self, new: State) {
        self.state = new;
        self.state_dt = Instant::now();
    }

    pub fn new(
        cid: Cid,
        sid: Sid,
        scalar_type: ScalarType,
        shape: Shape,
        ca_dbr_ty: CaDbrTy,
        chi: ChannelInfoResult,
        chconf: ChannelConfig,
        ioid_reg: Arc<IoidRegistry>,
    ) -> Result<Self, Error> {
        let chname = chconf.name().into();
        let series = chi.series.to_series();
        let use_ioc_time = chconf.use_ioc_time();
        let min_quiets = chconf.min_quiets();
        let is_polled = chconf.is_polled();
        let cssid = ChannelStatusSeriesId::new(series.id());
        let rtwriter = CaRtWriter::new(
            series,
            scalar_type.clone(),
            shape.clone(),
            min_quiets.clone(),
            is_polled,
            chconf.replication(),
            &|| CaWriterValueState::new(series, series, RetentionTime::Short),
        )?;
        let binwriter = BinWriter::new(
            TsNano::from_system_time(SystemTime::now()),
            min_quiets,
            is_polled,
            WriteCntZero::default_for_on_the_fly(),
            DiscardFirstOutput::default_for_on_the_fly(),
            cssid,
            series,
            scalar_type.clone(),
            shape.clone(),
            chconf.name().into(),
        )?;
        Ok(Self {
            state: State::Normal(Normal {
                fetchmpx: Fetchmpx::new(series, cid.clone(), sid.clone(), scalar_type, shape, ca_dbr_ty, chconf),
                futs: VecDeque::new(),
                read_notify_pending: HashMap::new(),
            }),
            state_dt: Instant::now(),
            cid,
            sid,
            chi,
            removing: false,
            outbuf: VecDeque::new(),
            trace_outbuf: VecDeque::new(),
            inp_buf: VecDeque::with_capacity(INP_BUF_CAP),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
            rtwriter,
            binwriter: Some(binwriter),
            crst: consume_event_data::ChannelConsumeState::new(),
            use_ioc_time,
            chname,
            ioid_reg,
        })
    }

    /// Harvest the metrics of this state and of the contained Fetchmpx.
    pub fn mett_take(&mut self) -> ChannelHandlerMetrics {
        let mut ret = std::mem::replace(&mut self.mett, ChannelHandlerMetrics::new());
        match &mut self.state {
            State::Normal(st) => {
                ret.ingest(st.fetchmpx.mett_take());
            }
            State::Done => {}
        }
        ret
    }

    pub fn sid(&self) -> Sid {
        self.sid.clone()
    }

    pub fn trigger_close(&mut self, reason: ClosingReason) {
        self.removing = true;
        match &mut self.state {
            State::Normal(x) => {
                x.fetchmpx.trigger_closing(reason);
            }
            State::Done => {}
        }
    }

    pub fn notify_peer_closed(&mut self) {
        self.removing = true;
        self.inp_done = true;
        match &mut self.state {
            State::Normal(x) => {
                x.fetchmpx.notify_peer_closed();
            }
            State::Done => {}
        }
    }

    pub fn handle_channel_handler_cmd(&mut self, cmd: serde_json::Value) -> serde_json::Value {
        use serde_json::json;
        match &mut self.state {
            State::Normal(st) => st.fetchmpx.handle_channel_handler_cmd(cmd),
            State::Done => json!({
                "error": format!("Running  {}", self.state.str()),
            }),
        }
    }

    /// Issue an on-demand `ReadNotify` for this channel. The immediate return value is
    /// only a synchronous ack/error; the actual result is delivered later through `resp_tx`,
    /// from the future queued onto `Normal.futs` and driven by `poll_futs`.
    pub fn cmd_read_notify(&mut self, mut resp_tx: asynchan::Sender<serde_json::Value>) -> serde_json::Value {
        use serde_json::json;
        let selfname = "cmd_read_notify";
        match &mut self.state {
            State::Normal(normal) => {
                if normal.futs.len() >= READ_NOTIFY_FUTS_CAP {
                    return json!({"error": format!("{selfname}  too many in-flight read-notify commands")});
                }
                let tsnow = Instant::now();
                let ioid = self
                    .ioid_reg
                    .register_new(self.sid.clone(), self.cid.clone(), tsnow, tsnow);
                let msg = proto::CaMsg::from_ty_ts(
                    proto::CaMsgTy::ReadNotify(proto::ReadNotify {
                        data_type: normal.fetchmpx.ca_dbr_ty().to_u16(),
                        data_count: normal.fetchmpx.shape().to_ca_count().unwrap(),
                        sid: self.sid.to_u32(),
                        ioid: ioid.to_u32(),
                    }),
                    tsnow,
                );
                let (oneshot_tx, oneshot_rx) = oneshot::channel();
                normal.read_notify_pending.insert(ioid, oneshot_tx);
                let fut = async move {
                    let res = match tokio::time::timeout(Duration::from_millis(READ_NOTIFY_CMD_TIMEOUT_MS), oneshot_rx)
                        .await
                    {
                        Ok(Ok(v)) => json!({
                            "ok": true,
                            "value": serde_json::to_value(&v.value).unwrap_or_else(|e| json!({"error": e.to_string()})),
                            "data_type": v.data_type,
                            "data_count": v.data_count,
                        }),
                        Ok(Err(_canceled)) => json!({"error": "canceled"}),
                        Err(_timeout) => json!({"error": "timeout"}),
                    };
                    let _ = resp_tx.try_send(res);
                };
                normal.futs.push_back(Some((ioid, fut.box2())));
                self.outbuf.push_back(msg);
                self.trace_outbuf.push_back(ChannelTraceItemInner::ReadNotify);
                json!({"queued": true})
            }
            State::Done => json!({
                "error": format!("{selfname}  Running  {}", self.state.str()),
            }),
        }
    }

    fn poll_futs(&mut self, cx: &mut Context<'_>) {
        use Poll::*;
        if let State::Normal(normal) = &mut self.state {
            for slot in normal.futs.iter_mut() {
                if let Some((ioid, fut)) = slot {
                    if let Ready(()) = fut.poll_unpin(cx) {
                        normal.read_notify_pending.remove(ioid);
                        *slot = None;
                    }
                }
            }
            normal.futs.retain(Option::is_some);
        }
    }

    pub fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
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
        match &mut self.state {
            State::Normal(st) => st.fetchmpx.inp_done(),
            State::Done => {}
        }
    }

    fn poll_inp_dispatch(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<(), Error>>> {
        let selfname = "poll_inp_dispatch";
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal(st2) => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        trace3!("{selfname}  have item  {item:?}");
                        let read_notify_ioid = match &item.msg.ty {
                            proto::CaMsgTy::ReadNotifyRes(v) => Some(Ioid::new(v.ioid)),
                            _ => None,
                        };
                        let read_notify_waiter =
                            read_notify_ioid.and_then(|ioid| st2.read_notify_pending.remove(&ioid));
                        if let Some(tx) = read_notify_waiter {
                            hpp.mark_progress();
                            self2.trace_outbuf.push_back(ChannelTraceItemInner::ReadNotifyRes);
                            match item.msg.ty {
                                proto::CaMsgTy::ReadNotifyRes(v) => {
                                    let _ = tx.send(v);
                                }
                                _ => unreachable!(),
                            }
                        } else {
                            let to_mpx = match &item.msg.ty {
                                proto::CaMsgTy::EventAddRes(_) => true,
                                proto::CaMsgTy::EventAddResEmpty(_) => true,
                                proto::CaMsgTy::ReadNotifyRes(_) => true,
                                _ => false,
                            };
                            if to_mpx {
                                match st2.fetchmpx.inp_push_try(item) {
                                    Some(item) => {
                                        hpp.mark_pending();
                                        self2.inp_buf.push_front(item);
                                    }
                                    None => {
                                        hpp.mark_progress();
                                    }
                                }
                            } else {
                                hpp.mark_progress();
                                error!("{selfname}  unexpected message {item:?}");
                                let e = Error::CreateMonitorUnexpectedMessage;
                                break Ready(Some(Err(e)));
                            }
                        }
                    } else if self.inp_done {
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

impl Stream for Running {
    type Item = Result<RunningItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let selfname = "Creating::poll_next";
        trace4!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match &self.state {
                State::Done => {}
                _ => match self.as_mut().poll_inp_dispatch(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(()) => {}
                            Err(e) => {
                                error!("TODO handle error {e}");
                                self.transition_state(State::Done);
                            }
                        }
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self.trigger_close(ClosingReason::InputDone);
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
            }
            self.poll_futs(cx);
            if let Some(msg) = self.outbuf.pop_front() {
                break Ready(Some(Ok(RunningItem::CaMsgOut(msg))));
            }
            if let Some(x) = self.trace_outbuf.pop_front() {
                break Ready(Some(Ok(RunningItem::ChannelTrace(x))));
            }
            match &mut self.state {
                State::Normal(normal) => match normal.fetchmpx.poll_next_unpin(cx) {
                    Ready(Some(x)) => match x {
                        Ok(x) => {
                            hpp.mark_progress();
                            match x {
                                fetchmpx::FetchmpxItem::CaMsgOut(msg) => {
                                    let g = RunningItem::CaMsgOut(msg);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::CaMsgOutIoid(msg, sid, tscmd) => {
                                    let g = RunningItem::CaMsgOutIoid(msg, sid, tscmd);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::CaMsgOutSubid(msg, tscmd) => {
                                    let g = RunningItem::CaMsgOutSubid(msg, tscmd);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::SubidRemove(cid) => {
                                    let g = RunningItem::SubidRemove(cid);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::TestValue(x) => {
                                    let g = RunningItem::TestValue(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::LocalLog(x) => {
                                    let g = RunningItem::LocalLog(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::ChannelStatus(x) => {
                                    let g = RunningItem::ChannelStatus(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::ChannelEventValue(x) => {
                                    let g = RunningItem::ChannelEventValue(x);
                                    break Ready(Some(Ok(g)));
                                }
                                fetchmpx::FetchmpxItem::RawEventForWrite(x) => {
                                    match self.ingest_event(x.value, x.payload_len, x.tsnow, x.stnow, x.tscaproto) {
                                        Ok(items) => {
                                            if !items.is_empty() {
                                                break Ready(Some(Ok(RunningItem::ChannelWriteItems(items))));
                                            }
                                        }
                                        Err(e) => {
                                            error!("TODO handle error {e}");
                                            self.transition_state(State::Done);
                                            break Ready(Some(Err(e)));
                                        }
                                    }
                                }
                                fetchmpx::FetchmpxItem::RequestClose(reason) => {
                                    break Ready(Some(Ok(RunningItem::RequestClose(reason))));
                                }
                            }
                        }
                        Err(e) => {
                            error!("TODO handle error {e}");
                            self.transition_state(State::Done);
                            hpp.mark_progress();
                        }
                    },
                    Ready(None) => {
                        hpp.mark_progress();
                        trace_shutdown!("Done  {}", self.chname);
                        self.transition_state(State::Done);
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
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
