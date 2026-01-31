pub mod activeca;
pub mod channelheap;
pub mod connected;
pub mod ctchan;
pub mod handshake;

use super::conncmd::ConnCommand;
use super::connevent::CaConnEvent;
use super::connevent::EndOfStreamReason;
use crate::ca::conn::CaConnOpts;
use crate::ca::conn2::asynchan;
use crate::ca::conn2::asynchan::SendPoll;
use crate::ca::conn2::statetrans::conn::IocConnStateBase;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::misc::todoval;
use connected::Connected;
use dbpg::seriesbychannel::ChannelInfoQuery;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryFutureExt;
use handshake::Handshake;
use hashbrown::HashMap;
use scywr::insertqueues::InsertDeques;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use serde::Serialize;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

const OUT_QUEUE_LEN_MAX: usize = 64;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! conn_err { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "Conn"),
    enum variants {
        TickerPoll,
        IO(#[from] std::io::Error),
        Handshake(#[from] handshake::Error),
        Connected(#[from] connected::Error),
        ChanSend,
    },
);

impl<T> From<asynchan::SendError<T>> for Error {
    fn from(value: asynchan::SendError<T>) -> Self {
        Self::ChanSend
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
        use stats::rand_xoshiro::rand_core::RngCore;
        let b = self.ivl;
        let t = b + b * (self.rng.next_u32() & 0x1f) / 0xff;
        trace!("TODO  make_ticker  {:.0} ms", 1e3 * t.as_secs_f32());
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
    fn new(remote_addr: SocketAddrV4, ca_cmd_rx: asynchan::Receiver<activeca::CaCommand>) -> Self {
        let fut = tokio::net::TcpStream::connect(remote_addr).map_err(Error::from);
        let fut = Box::pin(fut);
        let fut = ConnectFut(fut);
        Self::Connecting(Connecting {
            remote_addr,
            // ress_a,
            fut,
            ca_cmd_rx,
        })
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
    // ress_a: StateRessShr1,
    fut: ConnectFut,
    ca_cmd_rx: asynchan::Receiver<activeca::CaCommand>,
}

impl Future for Connecting {
    type Output = Result<tokio::net::TcpStream, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.0.as_mut().poll(cx)
    }
}

#[derive(Debug)]
enum CaConnCmdKind {
    ChannelAdd(ChannelConfig, asynchan::Sender<u32>),
    ChannelRemove(ChannelConfig, asynchan::Sender<u32>),
    DisconnectOnIdle(asynchan::Sender<u32>),
}

#[derive(Debug)]
pub struct CaConnCmd {
    kind: CaConnCmdKind,
}

#[derive(Debug, Clone)]
pub struct CaConnComm {
    cmd_tx: asynchan::Sender<CaConnCmd>,
    // rx: asynchan::Receiver<Result<CaConnItem, Error>>,
}

impl CaConnComm {
    pub async fn channel_add(&mut self, conf: ChannelConfig) -> Result<(), Error> {
        let (done_tx, mut done_rx) = asynchan::bounded(1, "CaConnComm-channel_add-done");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ChannelAdd(conf, done_tx),
        };
        self.cmd_tx.send(cmd).await?;
        let _ = done_rx.next().await;
        Ok(())
    }

    pub async fn channel_remove(&mut self, conf: ChannelConfig) -> Result<(), Error> {
        let (done_tx, mut done_rx) = asynchan::bounded(1, "CaConnComm-channel_add-done");
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
        let (done_tx, mut done_rx) = asynchan::bounded(1, "CaConnComm-trigger_disconnect_on_idle-done");
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::DisconnectOnIdle(done_tx),
        };
        self.cmd_tx.send(cmd).await?;
        let _ = done_rx.next().await;
        Ok(())
    }
}

#[derive(Debug)]
pub enum StatusState {
    Connecting,
    Connected(connected::StatusInfo),
    Done,
}

#[derive(Debug)]
pub struct StatusInfo {
    pub ts: time::UtcDateTime,
    pub addr: SocketAddrV4,
    pub state: StatusState,
}

#[derive(Debug)]
pub enum CaConnItem {
    StatusInfo(StatusInfo),
}

const EF4: usize = 0x500;

#[derive(Debug)]
pub struct CaConn {
    backend: String,
    remote_addr: SocketAddrV4,
    local_epics_hostname: String,
    state: State,
    // iqdqs: InsertDeques,
    // ca_conn_event_out_queue: VecDeque<CaConnEvent>,
    // ca_conn_event_out_queue_max: usize,
    ticker: JitterTicker,
    mett: stats::mett::CaConnMetrics,
    cmd_tx: asynchan::Sender<CaConnCmd>,
    cmd_rx: asynchan::Receiver<CaConnCmd>,
    ca_cmd_tx: asynchan::Sender<activeca::CaCommand>,
    ca_cmd_tx_fut: Option<ErasedFuture<Result<(), Error>, EF4>>,
    out_qu: VecDeque<Result<CaConnItem, Error>>,
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
            state: State::new(remote_addr, ca_cmd_rx),
            // iqdqs: InsertDeques::new(),
            // ca_conn_event_out_queue: VecDeque::new(),
            // ca_conn_event_out_queue_max: 2000,
            ticker: JitterTicker::new(Duration::from_millis(2000)),
            mett: stats::mett::CaConnMetrics::new(),
            cmd_tx,
            cmd_rx,
            ca_cmd_tx,
            ca_cmd_tx_fut: None,
            out_qu: VecDeque::new(),
        };
        ret
    }

    pub fn comm(&self) -> CaConnComm {
        CaConnComm {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    pub fn into_task(self) -> (CaConnTask, asynchan::Receiver<std::result::Result<CaConnItem, Error>>) {
        CaConnTask::new(self)
    }

    // call this only from the main fn poll
    fn shutdown_on_error(&mut self, e: Error) {
        todo!()
    }

    fn make_status_info(self: Pin<&mut Self>) -> StatusInfo {
        // We only consider state which is sync available here.
        // For other information, we take the last known values.
        match &self.state {
            State::Connecting(connecting) => StatusInfo {
                ts: time::UtcDateTime::now(),
                addr: self.remote_addr,
                state: StatusState::Connecting,
            },
            State::Connected(connected) => StatusInfo {
                ts: time::UtcDateTime::now(),
                addr: self.remote_addr,
                state: StatusState::Connected(connected.status_info()),
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
        if self.out_qu.len() < OUT_QUEUE_LEN_MAX {
            trace!("TODO  poll_own_ticker  emit status info");
            let v = self.as_mut().make_status_info();
            let item = CaConnItem::StatusInfo(v);
            self.out_qu.push_back(Ok(item));
            Ready(Some(()))
        } else {
            // TODO count in stats
            Ready(None)
        }
    }

    fn poll_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<HaveProgressPending, Error> {
        use Poll::*;
        let mut hpp = HaveProgressPending::new();
        match self.ticker.poll_next_unpin(cx) {
            Ready(Some(())) => {
                trace!("TODO  CaConn:Ticker fired");
                hpp.mark_progress();
                match self.on_ticker_fired(cx) {
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
        Ok(hpp)
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
    type Item = Result<CaConnItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        TODO;
        // TODO
        // We should observe here that our next inner handler is Done.
        // Check that we do that.
        // It seems that Connected does not exit properly. Listening on command input?
        use Poll::*;
        trace3!("CaConn  poll_next");
        let mut durs = DurationMeasureSteps::new();
        self.mett.poll_fn_begin().inc();
        let ret = loop {
            trace3!("CaConn  poll_next  loop");
            let self2 = self.as_mut().get_mut();
            self2.mett.poll_loop_begin().inc();
            let tsloop = Instant::now();
            let hpp = &mut HaveProgressPending::new();
            if let Some(item) = self2.out_qu.pop_front() {
                break Ready(Some(item));
            } else if let Some(fut) = self2.ca_cmd_tx_fut.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        self2.ca_cmd_tx_fut = None;
                        hpp.mark_progress();
                    }
                    Ready(Err(e)) => {
                        self2.ca_cmd_tx_fut = None;
                        error!("CaConn: ca_cmd_tx_fut error: {}", e);
                        self2.state = State::Done;
                        hpp.mark_progress();
                        break Ready(Some(Err(e)));
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else {
                match self2.cmd_rx.poll_next_unpin(cx) {
                    Ready(x) => match x {
                        Some(cmd) => {
                            hpp.mark_progress();
                            match cmd.kind {
                                CaConnCmdKind::ChannelAdd(conf, done_tx) => {
                                    trace!("CaConn:Received:ChannelAdd  {conf:?}");
                                    let cmd = activeca::CaCommand::channel_add(conf, done_tx);
                                    let mut tx = self2.ca_cmd_tx.clone();
                                    let fut = async move {
                                        tx.send(cmd).await?;
                                        // The is-done-sender is already passed to inner handler.
                                        Ok(())
                                    };
                                    self2.ca_cmd_tx_fut = Some(ErasedFuture::new(fut));
                                }
                                CaConnCmdKind::ChannelRemove(conf, done_tx) => {
                                    trace!("CaConn:Received:ChannelRemove  {conf:?}");
                                    let cmd = activeca::CaCommand::channel_remove(conf.name(), done_tx);
                                    let mut tx = self2.ca_cmd_tx.clone();
                                    let fut = async move {
                                        tx.send(cmd).await?;
                                        // The is-done-sender is already passed to inner handler.
                                        Ok(())
                                    };
                                    self2.ca_cmd_tx_fut = Some(ErasedFuture::new(fut));
                                }
                                CaConnCmdKind::DisconnectOnIdle(done_tx) => {
                                    trace!("CaConn:Received:DisconnectOnIdle");
                                    let cmd = activeca::CaCommand::disconnect_on_idle(done_tx);
                                    let mut tx = self2.ca_cmd_tx.clone();
                                    let fut = async move {
                                        tx.send(cmd).await?;
                                        // The is-done-sender is already passed to inner handler.
                                        Ok(())
                                    };
                                    self2.ca_cmd_tx_fut = Some(ErasedFuture::new(fut));
                                }
                            }
                        }
                        None => {}
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if true {
                match &mut self2.state {
                    State::Connecting(st1) => match st1.poll_unpin(cx) {
                        Ready(Ok(x)) => {
                            trace!("CaConn:Connecting:Ready");
                            // ok, we replace the full state
                            let ca_cmd_rx = std::mem::replace(&mut st1.ca_cmd_rx, asynchan::bounded(1, "dummy").1);
                            let stn = Connected::new(x, self.remote_addr, tsloop, ca_cmd_rx);
                            self.state = State::Connected(stn);
                            hpp.mark_progress();
                        }
                        Ready(Err(e)) => {
                            trace!("CaConn:Connecting:Err:{}", e);
                            self.state = State::Done;
                            hpp.mark_progress();
                            break Ready(Some(Err(e)));
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    },
                    State::Connected(st1) => match st1.poll_next_unpin(cx) {
                        Ready(Some(x)) => match x {
                            Ok(x) => {
                                trace!("CaConn:Connected:Ready");
                                error!("CaConn:Connected:Ready  TODO handle the item");
                                hpp.mark_progress();
                            }
                            Err(e) => {
                                trace!("CaConn:Connected:Err:{}", e);
                                error!("CaConn:Connected:Err:  TODO handle error and shutdown");
                                self.state = State::Done;
                                hpp.mark_progress();
                                break Ready(Some(Err(e.into())));
                            }
                        },
                        Ready(None) => {
                            trace!("CaConn:Connected:Done");
                            error!("CaConn:Connected:Done  TODO handle shutdown");
                            self.state = State::Done;
                            hpp.mark_progress();
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    },
                    State::Done => {
                        error!("State::Done  {}  {}", hpp.have_progress(), hpp.have_pending());
                        // TODO when in Done, we should no longer be stuck with Pending on something.
                    }
                };
            }

            // TODO get rid of the handling of ca_conn_event_out_queue in here.
            // The future which pushes to the queue must also trigger the async push if needed.

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

            {
                // TODO handle iqdqs async on demand.
                // Batch all insert requests.
                // Send requests when the queue is full enough.
                // Send requests periodically: need to fine-tune the internal timer tick.
                // Use internal timer tick modulo for various purposes.

                // let stats2 = self.stats.clone();
                // let stats_fn = move |item: &VecDeque<QueryItem>| {
                //     stats2.iiq_batch_len().ingest(item.len() as u32);
                // };
                // flush_queue_dqs!(
                //     self,
                //     st_rf1_qu,
                //     st_rf1_sp_pin,
                //     send_batched::<256, _>,
                //     32,
                //     (&mut have_progress, &mut have_pending),
                //     "st_rf1_rx",
                //     cx,
                //     stats_fn
                // );

                // etc...
            }

            // TODO handle channel info queries async batched on demand, like iqdqs.

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
                trace!("HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("HPP");
                Pending
            } else {
                trace!("HPP:Done");
                Ready(None)
            };
        };

        durs.step();
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
    t1: Option<Result<CaConnItem, Error>>,
    out_tx: asynchan::Sender<Result<CaConnItem, Error>>,
}

impl CaConnTask {
    fn new(conn: CaConn) -> (Self, asynchan::Receiver<Result<CaConnItem, Error>>) {
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
                    Ok(()) => continue,
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
                    Ready(x) => match x {
                        Some(x) => {
                            self.t1 = Some(x);
                            continue;
                        }
                        None => Ready(Ok(())),
                    },
                    Pending => Pending,
                }
            };
        }
    }
}
