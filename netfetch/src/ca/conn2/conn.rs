mod connected;
mod connecting;

use super::conncmd::ConnCommand;
use super::connevent::CaConnEvent;
use super::connevent::EndOfStreamReason;
use crate::ca::conn::CaConnOpts;
use crate::ca::conn2::channel::ChannelBasic;
use crate::ca::conn2::progpend::HaveProgressPending;
use crate::ca::conn2::statetrans::conn::IocConnStateBase;
use crate::ca::conn2::statetrans::stateress1::StateRessShr1;
use async_channel::Sender;
use ca_proto::ca::proto;
use connected::Connected;
use connecting::Connecting;
use dbpg::seriesbychannel::ChannelInfoQuery;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use proto::CaProto;
use scywr::insertqueues::InsertDeques;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! conn_err { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Conn"),
    enum variants {
        TickerPoll,
    },
);

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
struct CaConnState {
    ioc_conn_state: IocConnStateBase,
}

impl CaConnState {
    fn new(remote_addr: SocketAddrV4, ress_a: StateRessShr1) -> Self {
        Self {
            ioc_conn_state: IocConnStateBase::new(remote_addr, ress_a),
        }
    }
}

impl Stream for CaConnState {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        todo!()
    }
}

pub struct CaConn {
    opts: CaConnOpts,
    backend: String,
    state: CaConnState,
    iqdqs: InsertDeques,
    ca_conn_event_out_queue: VecDeque<CaConnEvent>,
    ca_conn_event_out_queue_max: usize,
    rng: Xoshiro128PlusPlus,
    mett: stats::mett::CaConnMetrics,
}

impl CaConn {
    pub fn new(
        opts: CaConnOpts,
        backend: String,
        remote_addr: SocketAddrV4,
        local_epics_hostname: String,
        iqtx: InsertQueuesTx,
        channel_info_query_tx: Sender<ChannelInfoQuery>,
    ) -> Self {
        let tsnow = Instant::now();
        let (cq_tx, cq_rx) = async_channel::bounded::<ConnCommand>(32);
        let rng = stats::xoshiro_from_time();
        let ress_a = todo!();
        Self {
            opts,
            backend,
            state: CaConnState::new(remote_addr, ress_a),
            iqdqs: InsertDeques::new(),
            ca_conn_event_out_queue: VecDeque::new(),
            ca_conn_event_out_queue_max: 2000,
            rng,
            mett: stats::mett::CaConnMetrics::new(),
        }
    }

    fn __test_channel(mut self: Pin<&mut Self>, cx: &mut Context, ch1: &mut ChannelBasic) {
        // let mut ch1 = ChannelBasic::new();
        let ch1pin = Pin::new(ch1);
        let cx = todo!();
        ch1pin.config_update(cx, todo!());
    }

    fn poll_own_ticker(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        // TODO nothing taken yet
        todo!()
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
    type Item = CaConnEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut durs = DurationMeasureSteps::new();
        self.mett.poll_fn_begin().inc();
        let ret = loop {
            self.mett.poll_loop_begin().inc();

            // TODO
            // Drive the state machine, but in which form.
            // Relationship between overall connection state and channel state?

            match self.state.poll_next_unpin(cx) {
                _ => todo!(),
            }

            let hpp = &mut HaveProgressPending::new();
            let are_we_done = false;
            if are_we_done {
                break Ready(None);
            } else if let Some(item) = self.ca_conn_event_out_queue.pop_front() {
                // TODO get rid of the handling of ca_conn_event_out_queue in here.
                // The future which pushes to the queue must also trigger the async push if needed.
                break Ready(Some(item));
            }

            // TODO add up duration of this scope
            match self.as_mut().poll_own_ticker(cx) {
                Ok(Ready(())) => {
                    hpp.have_progress();
                }
                Ok(Pending) => {
                    hpp.have_pending();
                }
                Err(e) => {
                    self.shutdown_on_error(e);
                    continue;
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

            let tsnow4 = Instant::now();

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

            break if hpp.is_progress() {
                continue;
            } else if hpp.is_pending() {
                Pending
            } else {
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
