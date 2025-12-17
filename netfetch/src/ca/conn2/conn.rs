mod activeca;
mod channelheap;
mod connected;
mod handshake;

use super::conncmd::ConnCommand;
use super::connevent::CaConnEvent;
use super::connevent::EndOfStreamReason;
use crate::ca::conn::CaConnOpts;
use crate::ca::conn2::asynchan;
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
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! conn_err { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
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
}

impl Future for Connecting {
    type Output = Result<tokio::net::TcpStream, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.0.as_mut().poll(cx)
    }
}

#[derive(Debug)]
enum CaConnCmdKind {
    ChannelAdd(ChannelConfig),
    Shutdown,
}

#[derive(Debug)]
pub struct CaConnCmd {
    kind: CaConnCmdKind,
}

impl CaConnCmd {
    /// Does not offer a confirmation. Shutdown done the CaConn Stream has ended.
    fn shutdown() -> Self {
        Self {
            kind: CaConnCmdKind::Shutdown,
        }
    }
}

#[derive(Debug)]
pub struct CaConnComm {
    cmd_tx: asynchan::Sender<CaConnCmd>,
}

impl CaConnComm {
    pub async fn channel_add(&mut self, conf: ChannelConfig) -> Result<(), Error> {
        let cmd = CaConnCmd {
            kind: CaConnCmdKind::ChannelAdd(conf),
        };
        self.cmd_tx.send(cmd).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CaConn {
    backend: String,
    remote_addr: SocketAddrV4,
    local_epics_hostname: String,
    state: State,
    // iqdqs: InsertDeques,
    // ca_conn_event_out_queue: VecDeque<CaConnEvent>,
    // ca_conn_event_out_queue_max: usize,
    mett: stats::mett::CaConnMetrics,
    cmd_tx: asynchan::Sender<CaConnCmd>,
    cmd_rx: asynchan::Receiver<CaConnCmd>,
    ca_cmd_tx: asynchan::Sender<activeca::CaCommand>,
    ca_cmd_rx: asynchan::Receiver<activeca::CaCommand>,
    ca_cmd_tx_fut: Option<ErasedFuture<Result<(), Error>, 0x200>>,
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
        Self {
            backend,
            remote_addr,
            local_epics_hostname,
            state: State::new(remote_addr),
            // iqdqs: InsertDeques::new(),
            // ca_conn_event_out_queue: VecDeque::new(),
            // ca_conn_event_out_queue_max: 2000,
            mett: stats::mett::CaConnMetrics::new(),
            cmd_tx,
            cmd_rx,
            ca_cmd_tx,
            ca_cmd_rx,
            ca_cmd_tx_fut: None,
        }
    }

    pub fn comm(&self) -> CaConnComm {
        CaConnComm {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    fn poll_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<(), Error> {
        // TODO nothing taken yet
        Ok(())
    }

    // call this only from the main fn poll
    fn shutdown_on_error(&mut self, e: Error) {
        todo!()
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
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        trace4!("CaConn  poll_next");
        let mut durs = DurationMeasureSteps::new();
        self.mett.poll_fn_begin().inc();
        let ret = loop {
            let self2 = self.as_mut().get_mut();
            self2.mett.poll_loop_begin().inc();
            let tsloop = Instant::now();
            let hpp = &mut HaveProgressPending::new();
            if let Some(fut) = self2.ca_cmd_tx_fut.as_mut() {
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
                                CaConnCmdKind::ChannelAdd(conf) => {
                                    trace!("CaConn:Received:ChannelAdd  {conf:?}");
                                    let cmd = activeca::CaCommand::channel_add(conf);
                                    let mut tx = self2.ca_cmd_tx.clone();
                                    let fut = async move {
                                        tx.send(cmd).await?;
                                        Ok(())
                                    };
                                    self2.ca_cmd_tx_fut = Some(ErasedFuture::new(fut));
                                    hpp.mark_progress();
                                }
                                CaConnCmdKind::Shutdown => {
                                    trace!("CaConn:Received:Shutdown");
                                    error!("TODO trigger shutdown in inner");
                                    error!("TODO wait until inner is done");
                                    self2.state = State::Done;
                                    hpp.mark_progress();
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
                            let stn = Connected::new(x, self.remote_addr, tsloop, self.ca_cmd_rx.clone());
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
                    State::Done => {}
                };
            }

            // TODO get rid of the handling of ca_conn_event_out_queue in here.
            // The future which pushes to the queue must also trigger the async push if needed.

            // TODO add up duration of this scope
            match self.as_mut().poll_own_ticker(cx) {
                Ok(()) => {}
                Err(e) => {
                    self.shutdown_on_error(e);
                    hpp.mark_progress();
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
