pub const TRACE_BLOCK: bool = false;

pub const LOOP_MAX_PASS_CMD: usize = 1;
const INP_BUF_CAP: usize = 128;

//

pub mod activeca;
pub mod channelheap;
pub mod connected;
pub mod ctchan;
pub mod handshake;

use crate::asynbuf::AsynBuf;
use crate::asynchan;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::locallog;
use crate::ca::connset2::connset::TestValue;
use crate::ca::connset2::connset::channeltrace::ChannelTraceItem;
use crate::ca::connset2::connset::channeltrace::ChannelTraceL2Item;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use asynchan::SendPoll;
use connected::Connected;
use dbpg::seriesbychannel::ChannelInfoQuery;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryFutureExt;
use regex::Regex;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::io::Write;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

pub static CONN_DBG_PTR: AtomicUsize = AtomicUsize::new(0);

const OUT_QUEUE_LEN_MAX: usize = 64;
const WRITE_BATCH_LEN_MAX: usize = 256;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }
macro_rules! trace_blocked { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Conn"),
    enum variants {
        TickerPoll,
        IO(#[from] std::io::Error),
        Handshake(#[from] handshake::Error),
        Connected(#[from] connected::Error),
        ChanSend,
        ChanRecv,
    },
);

impl<T> From<asynchan::SendError<T>> for Error {
    fn from(value: asynchan::SendError<T>) -> Self {
        Self::ChanSend
    }
}

impl From<asynchan::RecvError> for Error {
    fn from(value: asynchan::RecvError) -> Self {
        Self::ChanRecv
    }
}

#[derive(Debug)]
struct JitterTicker {
    ivl: Duration,
    ticker: Pin<Box<tokio::time::Sleep>>,
    rng: Xoshiro128PlusPlus,
}

impl JitterTicker {
    fn new(ivl: Duration) -> Self {
        let rng = stats::xoshiro_from_os_rng();
        let ticker = tokio::time::sleep(ivl);
        let mut ret = Self {
            ivl,
            ticker: Box::pin(ticker),
            rng,
        };
        let ticker = ret.make_ticker();
        ret.ticker.set(ticker);
        ret
    }

    fn make_ticker(&mut self) -> tokio::time::Sleep {
        use stats::rand_xoshiro::rand_core::Rng;
        let b = self.ivl;
        let t = b + b * (self.rng.next_u32() & 0x1f) / 0xff;
        trace3!("make_ticker  {:.0} ms", 1e3 * t.as_secs_f32());
        tokio::time::sleep(t)
    }
}

impl Stream for JitterTicker {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.ticker.poll_unpin(cx) {
                Ready(()) => {
                    let ticker = self.make_ticker();
                    self.ticker.set(ticker);
                    match self.ticker.poll_unpin(cx) {
                        Ready(()) => {
                            error!("JitterTicker: immediate re-fire  TODO handle");
                        }
                        Pending => {}
                    }
                    Ready(Some(()))
                }
                Pending => Pending,
            };
        }
    }
}

#[derive(Debug)]
struct DurationMeasureSteps {
    ts: Instant,
    durs: smallvec::SmallVec<[Duration; 8]>,
}

impl DurationMeasureSteps {
    fn new() -> Self {
        Self {
            ts: Instant::now(),
            durs: smallvec::SmallVec::new(),
        }
    }

    fn step(&mut self) {
        let ts = Instant::now();
        let d = ts.saturating_duration_since(self.ts);
        self.durs.push(d);
        self.ts = ts;
    }
}

#[derive(Debug)]
enum State {
    Connecting(Connecting),
    Connected(Connected),
    Done,
}

impl State {
    fn new(remote_addr: SocketAddrV4) -> Self {
        let fut = tokio::net::TcpStream::connect(remote_addr).map_err(Error::from);
        let fut = Box::pin(fut);
        let fut = ConnectFut(fut);
        Self::Connecting(Connecting {
            remote_addr,
            // ress_a,
            fut,
        })
    }

    fn display_short(&self) -> StateDisplayShort<'_> {
        StateDisplayShort { inner: self }
    }
}

struct StateDisplayShort<'a> {
    inner: &'a State,
}

impl<'a> fmt::Display for StateDisplayShort<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            State::Connecting(_) => fmt.debug_tuple("Connecting").finish(),
            State::Connected(_) => fmt.debug_tuple("Connected").finish(),
            State::Done => fmt.debug_tuple("Done").finish(),
        }
    }
}

struct ConnectFut(Pin<Box<dyn Future<Output = Result<tokio::net::TcpStream, Error>> + Send>>);

impl fmt::Debug for ConnectFut {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("ConnectFut").finish()
    }
}

#[derive(Debug)]
struct Connecting {
    remote_addr: SocketAddrV4,
    fut: ConnectFut,
}

impl Connecting {
    async fn handle_channel_handler_cmd(&mut self, cmd: ChannelHandlerCmd) -> Result<serde_json::Value, Error> {
        use serde_json::json;
        let ret = json!({
            "TODO": "Connecting",
        });
        Ok(ret)
    }

    fn health_check(&self) -> bool {
        true
    }
}

impl Future for Connecting {
    type Output = Result<tokio::net::TcpStream, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.0.as_mut().poll(cx)
    }
}

#[derive(Debug)]
pub struct ChannelHandlerCmd {
    cmd: serde_json::Value,
    tx: asynchan::Sender<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ScatterGatherV1ResConn {
    pub conn: serde_json::Value,
    pub channels: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ScatterGatherV1 {
    channel_regex: Regex,
    cmd: serde_json::Value,
}

impl ScatterGatherV1 {
    pub fn new(channel_regex: Regex, cmd: serde_json::Value) -> Self {
        Self { channel_regex, cmd }
    }
}

#[derive(Debug)]
enum CaConnCmdKind {
    ChannelAdd(ChannelConfig, asynchan::Sender<u32>),
    ChannelRemove(ChannelConfig, asynchan::Sender<u32>),
    DisconnectOnIdle(asynchan::Sender<u32>),
    ChannelsForAddrInfoV1(asynchan::Sender<crate::metrics::ChannelsForAddrInfoV1>),
    ChannelsForAddrInfoV2(String, asynchan::Sender<crate::metrics::ChannelsForAddrInfoV2>),
    ChannelsByRegexV1(String, String, asynchan::Sender<Vec<serde_json::Value>>),
    DynCmdV03(serde_json::Value, asynchan::Sender<serde_json::Value>),
    ScatterGatherV1(ScatterGatherV1, asynchan::Sender<ScatterGatherV1ResConn>),
}

#[derive(Debug)]
pub struct CaConnCmd {
    kind: CaConnCmdKind,
}

#[derive(Debug, Clone)]
pub struct CaConnComm {
    cmd_tx: asynchan::Sender<CaConnCmd>,
}

impl CaConnComm {
    pub async fn channel_add(&mut self, conf: ChannelConfig) -> Result<(), Error> {
        let (done_tx, mut done_rx) = asynchan::bounded(1, "CaConnComm-ChannelAdd");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ChannelAdd(conf, done_tx),
        };
        self.cmd_tx.send(cmd).await?;
        let _ = done_rx.next().await;
        Ok(())
    }

    pub async fn channel_remove(&mut self, conf: ChannelConfig) -> Result<(), Error> {
        let (done_tx, mut done_rx) = asynchan::bounded(1, "CaConnComm-ChannelRemove");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ChannelRemove(conf, done_tx),
        };
        self.cmd_tx.send(cmd).await?;
        let _ = done_rx.next().await;
        Ok(())
    }

    pub async fn trigger_disconnect_on_idle(&mut self) -> Result<(), Error> {
        // The confirmation will get sent on command receive.
        // User then waits until the future is done.
        let (done_tx, mut done_rx) = asynchan::bounded(1, "CaConnComm-DisconnectOnIdle");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::DisconnectOnIdle(done_tx),
        };
        self.cmd_tx.send(cmd).await?;
        let ret = done_rx.recv().await?;
        Ok(())
    }

    pub async fn channels_info_v1(&mut self) -> Result<crate::metrics::ChannelsForAddrInfoV1, Error> {
        let (tx, mut rx) = asynchan::bounded(1, "CaConnComm-ChannelsForAddrInfoV1");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ChannelsForAddrInfoV1(tx),
        };
        self.cmd_tx.send(cmd).await?;
        let ret = rx.recv().await?;
        Ok(ret)
    }

    pub async fn channels_info_v2(&mut self, name: String) -> Result<crate::metrics::ChannelsForAddrInfoV2, Error> {
        let (tx, mut rx) = asynchan::bounded(1, "CaConnComm-ChannelsForAddrInfoV2");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ChannelsForAddrInfoV2(name, tx),
        };
        self.cmd_tx.send(cmd).await?;
        let ret = rx.recv().await?;
        Ok(ret)
    }

    pub async fn channels_by_regex_v1(&mut self, kind: String, regex: String) -> Result<Vec<serde_json::Value>, Error> {
        let (tx, mut rx) = asynchan::bounded(1, "CaConnComm-ChannelsByRegexV1");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ChannelsByRegexV1(kind, regex, tx),
        };
        self.cmd_tx.send(cmd).await?;
        let ret = rx.recv().await?;
        Ok(ret)
    }

    pub async fn dyn_cmd_v03(&mut self, cmd: serde_json::Value) -> serde_json::Value {
        let (tx, mut rx) = asynchan::bounded(1, "CaConnComm-dyn_cmd_v03");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::DynCmdV03(cmd, tx),
        };
        if self.cmd_tx.send(cmd).await.is_err() {
            serde_json::json!({
                "error": "can not send command",
            })
        } else {
            match rx.recv().await {
                Ok(x) => x,
                Err(_) => {
                    serde_json::json!({
                        "error": "can not receive result",
                    })
                }
            }
        }
    }

    pub async fn scatter_gather_v1(&mut self, cmd: ScatterGatherV1) -> ScatterGatherV1ResConn {
        let (tx, mut rx) = asynchan::bounded(1, "CaConnComm-scatter_gather_v1");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ScatterGatherV1(cmd, tx),
        };
        if self.cmd_tx.send(cmd).await.is_err() {
            ScatterGatherV1ResConn {
                conn: serde_json::json!({
                    "error": "can not send command",
                }),
                channels: BTreeMap::new(),
            }
        } else {
            match rx.recv().await {
                Ok(x) => x,
                Err(_) => ScatterGatherV1ResConn {
                    conn: serde_json::json!({
                        "error": "can not recv response",
                    }),
                    channels: BTreeMap::new(),
                },
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub enum StatusState {
    Connecting,
    Connected(connected::StatusInfo),
    Done,
}

#[derive(Debug, Serialize)]
pub struct StatusInfo {
    pub ts: time::UtcDateTime,
    pub addr: SocketAddrV4,
    pub state: StatusState,
}

#[derive(Debug)]
pub enum CaConnItem {
    StatusInfo(StatusInfo),
    ChannelInfoQuery(ChannelInfoQuery),
    TestValue(TestValue),
    LocalLog(locallog::Entry),
    ChannelEventValue(ChannelEventValue),
    ChannelWriteItems(VecDeque<QueryItem>),
    ChannelTrace(ChannelTraceL2Item),
    Metrics(stats::mett::CaConn2Metrics),
}

#[derive(Debug)]
pub struct CaConn {
    backend: String,
    remote_addr: SocketAddrV4,
    local_epics_hostname: String,
    state: State,
    write_batch: VecDeque<QueryItem>,
    ticker: JitterTicker,
    write_flush_ticker: JitterTicker,
    mett: stats::mett::CaConn2Metrics,
    cmd_tx: asynchan::Sender<CaConnCmd>,
    cmd_rx: asynchan::Receiver<CaConnCmd>,
    ca_cmd_tx: asynchan::Sender<activeca::CaCommand>,
    ca_cmd_tx_fut: Option<FutDbg<Result<(), Error>>>,
    ca_cmd_rx: asynchan::Receiver<activeca::CaCommand>,
    out_buf: AsynBuf<CaConnItem>,
}

impl CaConn {
    pub fn new(
        backend: String,
        remote_addr: SocketAddrV4,
        local_epics_hostname: String,
        // iqtxs: InsertQueuesTx,
        // channel_info_query_tx: Sender<ChannelInfoQuery>,
    ) -> Self {
        // let ress_a: StateRessShr1 = todoval();
        let (cmd_tx, cmd_rx) = asynchan::bounded(32, "CaConn-cmd");
        let (ca_cmd_tx, ca_cmd_rx) = asynchan::bounded(16, "ActiveCa-cmd");
        let ret = Self {
            backend,
            remote_addr,
            local_epics_hostname,
            state: State::new(remote_addr),
            write_batch: VecDeque::new(),
            ticker: JitterTicker::new(Duration::from_millis(2000)),
            write_flush_ticker: JitterTicker::new(Duration::from_millis(200)),
            mett: stats::mett::CaConn2Metrics::new(),
            cmd_tx,
            cmd_rx,
            ca_cmd_tx,
            ca_cmd_tx_fut: None,
            ca_cmd_rx,
            out_buf: AsynBuf::new(INP_BUF_CAP),
        };
        ret
    }

    pub fn comm(&self) -> CaConnComm {
        CaConnComm {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    pub fn into_task(self) -> (CaConnTask, asynchan::Receiver<Result<AsynBuf<CaConnItem>, Error>>) {
        CaConnTask::new(self)
    }

    fn status_info(&mut self) -> StatusInfo {
        // We only consider state which is sync available here.
        // For other information, we take the last known values.
        match &mut self.state {
            State::Connecting(st) => StatusInfo {
                ts: time::UtcDateTime::now(),
                addr: self.remote_addr,
                state: StatusState::Connecting,
            },
            State::Connected(st) => StatusInfo {
                ts: time::UtcDateTime::now(),
                addr: self.remote_addr,
                state: StatusState::Connected(st.status_info()),
            },
            State::Done => StatusInfo {
                ts: time::UtcDateTime::now(),
                addr: self.remote_addr,
                state: StatusState::Done,
            },
        }
    }

    fn on_ticker_fired(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<()>> {
        use Poll::*;
        self.mett.ticker_fired().inc();
        if !self.health_check() {
            self.mett.health_check_fail().inc();
        }
        if true {
            match &mut self.state {
                State::Connecting(st) => {
                    // TODO
                }
                State::Connected(st) => {
                    let m = st.mett_take();
                    self.mett.connected().ingest(m);
                }
                State::Done => {
                    // TODO
                }
            }
        }
        self.as_mut().metrics_emit();
        if self.out_buf.len() < OUT_QUEUE_LEN_MAX {
            trace!("TODO  poll_own_ticker  emit status info");
            let v = self.as_mut().status_info();
            let item = CaConnItem::StatusInfo(v);
            self.out_buf.push_back_force(item);
            self.mett.status_info_emit().inc();
            Ready(Some(()))
        } else {
            self.mett.status_info_out_queue_full().inc();
            Ready(None)
        }
    }

    /// Hand the metrics which accumulated since the last call over to the
    /// consumer of our item stream (the ConnSet).
    fn metrics_emit(mut self: Pin<&mut Self>) {
        let n = self.out_buf.len() as u32;
        self.mett.out_buf_len().set(n);
        let n = self.write_batch.len() as u32;
        self.mett.write_batch_len().set(n);
        self.mett.metrics_emit().inc();
        let m = self.mett.take_and_reset();
        self.out_buf.push_back_force(CaConnItem::Metrics(m));
    }

    fn poll_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<HaveProgressPending, Error> {
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        match self.ticker.poll_next_unpin(cx) {
            Ready(Some(())) => {
                hpp.mark_progress();
                match self.as_mut().on_ticker_fired(cx) {
                    Ready(Some(())) => {
                        hpp.mark_progress();
                    }
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        let self2 = self.as_mut().get_mut();
        match self2.write_flush_ticker.poll_next_unpin(cx) {
            Ready(Some(())) => {
                hpp.mark_progress();
                self2.try_flush_write_batch();
            }
            Ready(None) => {}
            Pending => {
                hpp.mark_pending();
            }
        }
        Ok(hpp)
    }

    fn try_flush_write_batch(&mut self) {
        if !self.write_batch.is_empty() {
            if self.out_buf.len() < OUT_QUEUE_LEN_MAX {
                let batch = std::mem::take(&mut self.write_batch);
                self.mett.write_batch_flush().inc();
                self.mett.write_batch_flush_len().push_val(batch.len() as u32);
                self.out_buf.push_back_force(CaConnItem::ChannelWriteItems(batch));
            } else {
                self.mett.write_batch_flush_blocked().inc();
            }
        }
    }

    fn channel_info_v1(&mut self) -> crate::metrics::ChannelsForAddrInfoV1 {
        match &mut self.state {
            State::Connecting(..) => crate::metrics::ChannelsForAddrInfoV1::new(),
            State::Connected(st) => st.channel_info_v1(),
            State::Done => crate::metrics::ChannelsForAddrInfoV1::new(),
        }
    }

    fn channel_info_v2(&mut self, name: String) -> crate::metrics::ChannelsForAddrInfoV2 {
        match &mut self.state {
            State::Connecting(..) => crate::metrics::ChannelsForAddrInfoV2::new(),
            State::Connected(st) => st.channel_info_v2(name),
            State::Done => crate::metrics::ChannelsForAddrInfoV2::new(),
        }
    }

    fn channels_by_regex_v1(&mut self, kind: String, reg: String) -> Vec<serde_json::Value> {
        match &mut self.state {
            State::Connecting(..) => Vec::new(),
            State::Connected(st) => st.channels_by_regex_v1(kind, reg),
            State::Done => Vec::new(),
        }
    }

    fn scatter_gather_v1(&mut self, cmd: ScatterGatherV1) -> ScatterGatherV1ResConn {
        match &mut self.state {
            State::Connecting(..) => ScatterGatherV1ResConn {
                conn: serde_json::json!({
                    "state": self.state.display_short().to_string(),
                }),
                channels: BTreeMap::new(),
            },
            State::Connected(st) => {
                if cmd.cmd.eq(&serde_json::json!("state_full_v1")) {
                    ScatterGatherV1ResConn {
                        conn: serde_json::json!({
                            "state": "Connected",
                            "response": st.scatter_gather_v1(cmd),
                        }),
                        channels: BTreeMap::new(),
                    }
                } else {
                    ScatterGatherV1ResConn {
                        conn: serde_json::json!({
                            "state": self.state.display_short().to_string(),
                        }),
                        channels: BTreeMap::new(),
                    }
                }
            }
            State::Done => ScatterGatherV1ResConn {
                conn: serde_json::json!(null),
                channels: BTreeMap::new(),
            },
        }
    }

    fn handle_dyn_cmd_v03(
        &mut self,
        cmd: serde_json::Value,
        mut tx: asynchan::Sender<serde_json::Value>,
    ) -> impl Future<Output = Result<(), Error>> + use<> {
        use futures::future::ready;
        use serde::Deserialize;
        use serde_json::json;
        #[derive(Debug, Deserialize)]
        struct Cmd {
            caconn_cmd: String,
        }
        if let Ok(cmd2) = serde_json::from_value::<Cmd>(cmd.clone()) {
            info!("{cmd2:?}");
            if cmd2.caconn_cmd == "conn_state_channel_full" {
                let status_info = self.status_info();
                async move {
                    let val = serde_json::to_value(&status_info).unwrap();
                    if tx.send(val).await.is_err() {
                        // TODO metrics
                    }
                    Ok(())
                }
                .box2()
            } else if cmd2.caconn_cmd == "ca_conn_state_proto" {
                let x = match &mut self.state {
                    State::Connecting(st1) => ready(json!({
                        "error": "CaConn  State::Connecting",
                    }))
                    .box2(),
                    State::Connected(st1) => {
                        let ss = st1.status_socket();
                        ready(json!({
                            "proto": {
                                "ss": ss,
                            },
                        }))
                        .box2()
                    }
                    State::Done => ready(json!({
                        "error": "CaConn  State::Done",
                    }))
                    .box2(),
                };
                async move {
                    if tx.send(x.await).await.is_err() {
                        // TODO metrics
                    }
                    Ok(())
                }
                .box2()
            } else {
                let x = match &mut self.state {
                    State::Connecting(st1) => ready(json!({
                        "error": "CaConn  State::Connecting",
                    }))
                    .box2(),
                    State::Connected(st1) => st1.handle_dyn_cmd_v03(cmd).box2(),
                    State::Done => ready(json!({
                        "error": "CaConn  State::Done",
                    }))
                    .box2(),
                };
                async move {
                    if tx.send(x.await).await.is_err() {
                        // TODO metrics
                    }
                    Ok(())
                }
                .box2()
            }
        } else {
            let val = serde_json::json!({
                "type": "error",
                "msg": format!("command bad"),
            });
            let _ = tx.try_send(val);
            ready(Ok(())).box2()
        }
    }

    fn check_flow_state(&self) {
        match &self.state {
            State::Connecting(st) => {}
            State::Connected(st) => {
                st.check_flow_state();
            }
            State::Done => {}
        }
    }

    fn dump_state_poll(&self) {
        use serde_json::json;
        let st = match &self.state {
            State::Connecting(st) => json!({"Connecting": {}}),
            State::Connected(st) => json!({"Connected": st.dump_state_poll()}),
            State::Done => json!({"Done": {}}),
        };
        let js = json!({
            "state": st,
        });
        if let Ok(fout) = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open("dump-conn.txt")
        {
            serde_json::to_writer(fout, &js).unwrap();
        } else {
            error!("can not create dump file");
        }
    }

    fn health_check(&self) -> bool {
        let mut healthy = true;
        if self.out_buf.len() > 5 * INP_BUF_CAP {
            warn!("out_buf len");
            healthy = false;
        }
        healthy |= match &self.state {
            State::Connecting(st) => st.health_check(),
            State::Connected(st) => st.health_check(),
            State::Done => true,
        };
        healthy
    }
}

macro_rules! handle_poll_res {
    ($res:expr, $hpp:expr) => {
        match $res {
            Ready(x) => match x {
                Ok(x) => match x {
                    Some(x) => {
                        $hpp.have_progress();
                    }
                    None => {}
                },
                Err(e) => {
                    // TODO how to handle error:
                    // Transition state, emit item.
                    error!("{}", e);
                }
            },
            Pending => {
                $hpp.have_pending();
            }
        }
    };
}

impl Stream for CaConn {
    type Item = Result<AsynBuf<CaConnItem>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfname = "CaConn::poll_next";
        trace4!("{selfname}");
        CONN_DBG_PTR.store(
            self.as_ref().get_ref() as *const Self as usize,
            atomic::Ordering::Release,
        );
        let mut durs = DurationMeasureSteps::new();
        let ts_poll_begin = Instant::now();
        self.mett.poll_fn_begin().inc();
        let ret = loop {
            let self2 = self.as_mut().get_mut();
            trace4!("{selfname}  loop  state {}", self2.state.display_short());
            let tsloop = Instant::now();
            let hpp = &mut HaveProgressPending::new();
            if self2.out_buf.len() != 0 {
                break Ready(Some(Ok(self2.out_buf.take())));
            } else if let Some(fut) = self2.ca_cmd_tx_fut.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(x) => {
                        self2.ca_cmd_tx_fut = None;
                        hpp.mark_progress();
                        match x {
                            Ok(()) => {}
                            Err(e) => {
                                error!("{selfname}  ca_cmd_tx_fut error: {e}");
                                self2.mett.cmd_send_err().inc();
                                self2.state = State::Done;
                                break Ready(Some(Err(e)));
                            }
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else if let State::Done = &self2.state {
            } else {
                //
                // TODO these commands run in a future.
                // Some want to use a channel to async send the command to ActiveCa.
                //
                match self2.cmd_rx.poll_next_unpin(cx) {
                    Ready(Some(cmd)) => {
                        hpp.mark_progress();
                        self2.mett.cmd_recv().inc();
                        match cmd.kind {
                            CaConnCmdKind::ChannelAdd(conf, done_tx) => {
                                self2.mett.cmd_channel_add().inc();
                                trace!("{selfname}:Received:ChannelAdd  {conf:?}");
                                let cmd = activeca::CaCommand::channel_add(conf, done_tx);
                                let mut tx = self2.ca_cmd_tx.clone();
                                let fut = async move {
                                    tx.send(cmd).await?;
                                    // The is-done-sender is already passed to inner handler.
                                    Ok(())
                                };
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                            CaConnCmdKind::ChannelRemove(conf, done_tx) => {
                                self2.mett.cmd_channel_remove().inc();
                                trace!("{selfname}:Received:ChannelRemove  {conf:?}");
                                let cmd = activeca::CaCommand::channel_remove(conf.name(), done_tx);
                                let mut tx = self2.ca_cmd_tx.clone();
                                let fut = async move {
                                    tx.send(cmd).await?;
                                    // The is-done-sender is already passed to inner handler.
                                    Ok(())
                                };
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                            CaConnCmdKind::DisconnectOnIdle(done_tx) => {
                                self2.mett.cmd_disconnect_on_idle().inc();
                                trace!("{selfname}:Received:DisconnectOnIdle");
                                let cmd = activeca::CaCommand::disconnect_on_idle(done_tx);
                                let mut tx = self2.ca_cmd_tx.clone();
                                let fut = async move {
                                    tx.send(cmd).await?;
                                    // The is-done-sender is already passed to inner handler.
                                    Ok(())
                                };
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                            CaConnCmdKind::DynCmdV03(cmd, tx) => {
                                self2.mett.cmd_dyn_v03().inc();
                                trace!("{selfname}:Received:DynCmd  {cmd:?}");
                                let fut = self2.handle_dyn_cmd_v03(cmd, tx);
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                            CaConnCmdKind::ChannelsForAddrInfoV1(mut tx) => {
                                self2.mett.cmd_channels_for_addr_v1().inc();
                                trace!("{selfname}:Received:ChannelsForAddrInfoV1");
                                let ret = self2.channel_info_v1();
                                let fut = async move {
                                    tx.send(ret).await?;
                                    Ok(())
                                };
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                            CaConnCmdKind::ChannelsForAddrInfoV2(name, mut tx) => {
                                self2.mett.cmd_channels_for_addr_v2().inc();
                                trace!("{selfname}:Received:ChannelsForAddrInfoV2");
                                let ret = self2.channel_info_v2(name);
                                let fut = async move {
                                    tx.send(ret).await?;
                                    Ok(())
                                };
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                            CaConnCmdKind::ChannelsByRegexV1(kind, reg, mut tx) => {
                                self2.mett.cmd_channels_by_regex_v1().inc();
                                trace!("{selfname}:Received:ChannelsByRegexV1");
                                let ret = self2.channels_by_regex_v1(kind, reg);
                                let fut = async move {
                                    tx.send(ret).await?;
                                    Ok(())
                                };
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                            CaConnCmdKind::ScatterGatherV1(cmd, mut tx) => {
                                trace!("{selfname}:Received:ScatterGatherV1");
                                let ret = self2.scatter_gather_v1(cmd);
                                let fut = async move {
                                    if tx.send(ret).await.is_err() {
                                        error!("could not send response");
                                    }
                                    Ok(())
                                };
                                self2.ca_cmd_tx_fut = Some(fut.box2());
                            }
                        }
                    }
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if true {
                match &mut self2.state {
                    State::Connecting(st1) => match st1.poll_unpin(cx) {
                        Ready(Ok(x)) => {
                            trace!("{selfname}:Connecting:Ready");
                            self2.mett.tcp_connected().inc();
                            // ok, we replace the full state
                            let stn = Connected::new(self2.backend.clone(), x, self.remote_addr, tsloop);
                            self.state = State::Connected(stn);
                            hpp.mark_progress();
                        }
                        Ready(Err(e)) => {
                            trace!("{selfname}:Connecting:Err:{e}");
                            self.mett.connect_error().inc();
                            self.state = State::Done;
                            hpp.mark_progress();
                            break Ready(Some(Err(e)));
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    },
                    State::Connected(st1) => {
                        let mut i = 0;
                        loop {
                            i += 1;
                            break if i > LOOP_MAX_PASS_CMD {
                            } else if st1.inp_cmd_buf().is_space() {
                                match self2.ca_cmd_rx.poll_next_unpin(cx) {
                                    Ready(Some(x)) => {
                                        hpp.mark_progress();
                                        // guarded
                                        st1.inp_cmd_buf().push_back_force(x);
                                        continue;
                                    }
                                    Ready(None) => {}
                                    Pending => {
                                        hpp.mark_pending();
                                    }
                                }
                            } else {
                                trace_blocked!(
                                    "{selfname}  SKIP Self::ca_cmd_rx poll  BLOCKED BY Connected inp_cmd_buf has_space"
                                );
                            };
                        }
                        match st1.poll_next_unpin(cx) {
                            Ready(Some(x)) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(items) => {
                                        // Precondition: can only arrive here if self.out_qu not too full.
                                        for item in items {
                                            match item.inner {
                                                connected::ItemInner::ChannelInfoQuery(item) => {
                                                    self2.mett.item_channel_info_query().inc();
                                                    let item = CaConnItem::ChannelInfoQuery(item);
                                                    self2.out_buf.push_back_force(item);
                                                }
                                                connected::ItemInner::TestValue(x) => {
                                                    self2.mett.item_test_value().inc();
                                                    info!("{selfname}  sees  connected::ItemInner::TestValue  {x:?}");
                                                    let item = CaConnItem::TestValue(x);
                                                    self2.out_buf.push_back_force(item);
                                                }
                                                connected::ItemInner::LocalLog(x) => {
                                                    self2.mett.item_local_log().inc();
                                                    let item = CaConnItem::LocalLog(x);
                                                    self2.out_buf.push_back_force(item);
                                                }
                                                connected::ItemInner::ChannelEventValue(x) => {
                                                    self2.mett.item_channel_event_value().inc();
                                                    let item = CaConnItem::ChannelEventValue(x);
                                                    self2.out_buf.push_back_force(item);
                                                }
                                                connected::ItemInner::ChannelWriteItems(x) => {
                                                    self2.mett.item_channel_write_items().add(x.len() as u32);
                                                    self2.write_batch.extend(x);
                                                }
                                                connected::ItemInner::ChannelTrace(x) => {
                                                    self2.mett.item_channel_trace().inc();
                                                    let item = ChannelTraceL2Item::new(st1.addr(), x);
                                                    let item = CaConnItem::ChannelTrace(item);
                                                    self2.out_buf.push_back_force(item);
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("{selfname}:Connected:Err  TODO handle error more elegant?  {e}");
                                        self.mett.connected_error().inc();
                                        self.dump_state_poll();
                                        self.state = State::Done;
                                        break Ready(Some(Err(e.into())));
                                    }
                                }
                            }
                            Ready(None) => {
                                error!("{selfname}:Connected:Done  TODO handle shutdown");
                                self2.mett.connected_end_of_stream().inc();
                                self2.state = State::Done;
                                hpp.mark_progress();
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                        if self2.write_batch.len() >= WRITE_BATCH_LEN_MAX {
                            self2.try_flush_write_batch();
                        }
                    }
                    State::Done => {
                        error!(
                            "{selfname}  State::Done  {}  {}",
                            hpp.have_progress(),
                            hpp.have_pending()
                        );
                        // TODO when in Done, we should no longer be stuck with Pending on something.
                    }
                };
            }

            // TODO get rid of the handling of ca_conn_event_out_queue in here.
            // The future which pushes to the queue must also trigger the async push if needed.

            if let State::Done = &self.state {
            } else {
                // TODO add up duration of this scope
                match self.as_mut().poll_own_ticker(cx) {
                    Ok(hpp2) => {
                        hpp.merge(hpp2);
                    }
                    Err(e) => {
                        hpp.mark_progress();
                        break Ready(Some(Err(e)));
                    }
                }
            }

            // TODO handle channel info queries async batched on demand, like write_batch.

            // if !self.is_shutdown() {
            //     flush_queue!(
            //         self,
            //         channel_info_query_qu,
            //         channel_info_query_tx,
            //         send_individual,
            //         32,
            //         (&mut have_progress, &mut have_pending),
            //         "chinf",
            //         cx,
            //         |_| {}
            //     );
            // }

            // match self.as_mut().handle_writer_establish_result(cx) {
            //     Ok(Ready(Some(()))) => {
            //         have_progress = true;
            //     }
            //     Ok(Ready(None)) => {}
            //     Ok(Pending) => {
            //         have_pending = true;
            //     }
            //     Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
            // }

            // match self.as_mut().handle_conn_command(cx) {
            //     Ok(Ready(Some(()))) => {
            //         have_progress = true;
            //     }
            //     Ok(Ready(None)) => {}
            //     Ok(Pending) => {
            //         have_pending = true;
            //     }
            //     Err(e) => break Ready(Some(CaConnEvent::err_now(e))),
            // }

            // match self.loop_inner(cx) {
            //     Ok(Ready(Some(()))) => {
            //         have_progress = true;
            //     }
            //     Ok(Ready(None)) => {}
            //     Ok(Pending) => {
            //         have_pending = true;
            //     }
            //     Err(e) => {
            //         error!("{e}");
            //         self.state = CaConnState::EndOfStream;
            //         break Ready(Some(CaConnEvent::err_now(e)));
            //     }
            // }

            // let tsnow4 = Instant::now();

            // break if self.is_shutdown() {
            //     if self.queues_out_flushed() {
            //         debug!("is_shutdown  queues_out_flushed  set EOS  {}", self.remote_addr_dbg);
            //         if let CaConnState::Shutdown(x) = std::mem::replace(&mut self.state, CaConnState::EndOfStream) {
            //             Ready(Some(CaConnEvent::new_now(CaConnEventValue::EndOfStream(x))))
            //         } else {
            //             continue;
            //         }
            //     } else {
            //         if have_progress {
            //             debug!("is_shutdown  NOT queues_out_flushed  prog  {}", self.remote_addr_dbg);
            //             self.stats.poll_reloop().inc();
            //             reloops += 1;
            //             continue;
            //         } else if have_pending {
            //             debug!("is_shutdown  NOT queues_out_flushed  pend  {}", self.remote_addr_dbg);
            //             self.log_queues_summary();
            //             self.stats.poll_pending().inc();
            //             Pending
            //         } else {
            //             // TODO error
            //             error!("shutting down, queues not flushed, no progress, no pending");
            //             self.stats.logic_error().inc();
            //             let e = Error::ShutdownWithQueuesNoProgressNoPending;
            //             Ready(Some(CaConnEvent::err_now(e)))
            //         }
            //     }
            // } else {
            //     if have_progress {
            //         if poll_ts1.elapsed() > Duration::from_millis(5) {
            //             self.stats.poll_wake_break().inc();
            //             cx.waker().wake_by_ref();
            //             break Ready(Some(CaConnEvent::new(self.poll_tsnow, CaConnEventValue::None)));
            //         } else {
            //             self.stats.poll_reloop().inc();
            //             reloops += 1;
            //             continue;
            //         }
            //     } else if have_pending {
            //         self.stats.poll_pending().inc();
            //         Pending
            //     } else {
            //         self.stats.poll_no_progress_no_pending().inc();
            //         let e = Error::NoProgressNoPending;
            //         Ready(Some(CaConnEvent::err_now(e)))
            //     }
            // };

            break if hpp.have_progress() {
                trace4!("HPP:Progress");
                self.mett.poll_reloop().inc();
                continue;
            } else if hpp.have_pending() {
                trace_pending!("HPP");
                self.mett.poll_pending().inc();
                Pending
            } else {
                trace3!("HPP:Done");
                self.mett.poll_no_progress_no_pending().inc();
                Ready(None)
            };
        };

        durs.step();
        self.mett.poll_all_dt().push_dur_10us(ts_poll_begin.elapsed());
        // if self.trace_channel_poll {
        //     self.stats.poll_all_dt().ingest_dur_dms(dt);
        //     if dt >= Duration::from_millis(10) {
        //         trace!("long poll {dt:?}");
        //     } else if dt >= Duration::from_micros(400) {
        //         let v = self.stats.poll_all_dt.to_display();
        //         let ip = self.remote_addr_dbg;
        //         trace!("poll_all_dt  {ip}  {v}");
        //     }
        // }
        // self.stats.read_ioids_len().set(self.read_ioids.len() as u64);
        // let n = match &self.proto {
        //     Some(x) => x.proto_out_len() as u64,
        //     None => 0,
        // };
        // self.stats.proto_out_len().set(n);
        // self.stats.poll_reloops().ingest(reloops);
        ret
    }
}

#[derive(Debug)]
pub struct CaConnTask {
    conn: CaConn,
    t1: Option<Result<AsynBuf<CaConnItem>, Error>>,
    out_tx: asynchan::Sender<Result<AsynBuf<CaConnItem>, Error>>,
}

impl CaConnTask {
    fn new(conn: CaConn) -> (Self, asynchan::Receiver<Result<AsynBuf<CaConnItem>, Error>>) {
        let (out_tx, out_rx) = asynchan::bounded(32, "CaConnTask-out");
        let fut = Self { conn, t1: None, out_tx };
        (fut, out_rx)
    }
}

impl Future for CaConnTask {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            break if let Some(x) = self.t1.take() {
                match self.out_tx.poll_send_unpin(x, cx) {
                    Ok(()) => {
                        continue;
                    }
                    Err(e) => match e {
                        asynchan::SendPollError::Full(x) => {
                            self.t1 = Some(x);
                            Pending
                        }
                        asynchan::SendPollError::Closed(x) => {
                            error!("CaConnTask: output channel closed");
                            Ready(Ok(()))
                        }
                    },
                }
            } else {
                match self.conn.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        self.t1 = Some(x);
                        continue;
                    }
                    Ready(None) => Ready(Ok(())),
                    Pending => Pending,
                }
            };
        }
    }
}
