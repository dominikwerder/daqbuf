use crate::ca::conn2;
use crate::ca::progpend::HaveProgressPending;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use conn2::caids::CaDbrTy;
use conn2::caids::Sid;
use conn2::conn::channelheap::ProtoRxItem;
use futures::FutureExt;
use netpod::ScalarType;
use netpod::Shape;
use stats::mett::ChannelHandlerMetrics;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHandlerRunning"),
    enum variants {
        // CreateMonitorUnexpectedMessage,
        // Recv,
        // Timeout,
        Logic,
    },
);

#[derive(Debug)]
pub enum MonitoringItem {
    ProtoOutSubid(CaMsg, Instant),
    TestValue(crate::ca::connset2::connset::TestValue),
}

#[derive(Debug)]
enum State {
    DoNothing,
    CreateMonitorSend,
    CreateMonitorRecv(FutDbg<()>),
}

#[derive(Debug)]
pub struct FetchMonitoring {
    state: State,
    sid: Sid,
    scalar_type: ScalarType,
    shape: Shape,
    ca_dbr_ty: CaDbrTy,
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
}

impl FetchMonitoring {
    pub fn new(sid: Sid, scalar_type: ScalarType, shape: Shape, ca_dbr_ty: CaDbrTy) -> Self {
        Self {
            state: State::DoNothing,
            sid,
            scalar_type,
            shape,
            ca_dbr_ty,
            inp_buf: VecDeque::with_capacity(16),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
        }
    }

    pub fn transition_to_enable(&mut self) -> Result<(), Error> {
        match &self.state {
            State::DoNothing => {
                self.state = State::CreateMonitorSend;
                Ok(())
            }
            State::CreateMonitorSend => Ok(()),
            State::CreateMonitorRecv(..) => Ok(()),
        }
    }

    pub fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
        trace3!("FetchPolling  inp_push_try");
        let v = &mut self.inp_buf;
        if v.len() < v.capacity() {
            v.push_back(item);
            None
        } else {
            Some(item)
        }
    }

    pub fn inp_done(&mut self) {
        trace3!("FetchPolling  inp_done");
        self.inp_done = true;
    }

    fn poll_inp(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Option<Result<MonitoringItem, Error>>>> {
        let selfname = "poll_inp";
        trace3!("{selfname}");
        use Poll::*;
        if let Some(item) = self.inp_buf.pop_front() {
            match &mut self.state {
                State::DoNothing => {
                    warn!("{selfname}  TODO item while in DoNothing  {item:?}");
                    Ready(Some(None))
                }
                State::CreateMonitorSend => {
                    warn!("{selfname}  TODO item while in SendReq  {item:?}");
                    Ready(Some(None))
                }
                State::CreateMonitorRecv(_) => {
                    info!("{selfname}  item while in CreateMonitorRecv  {item:?}");
                    match item.msg.ty {
                        CaMsgTy::EventAddRes(v) => {
                            let tsnow = Instant::now();
                            let dttrig = Duration::ZERO;
                            let dtcmd = tsnow.saturating_duration_since(item.tscmd);
                            let dtreg = tsnow.saturating_duration_since(item.tsreg);
                            let dtdisp = tsnow.saturating_duration_since(item.tsdisp);
                            let valf32 = v.value.f32_for_binning();
                            let item = MonitoringItem::TestValue(crate::ca::connset2::connset::TestValue {
                                val: valf32,
                                dttrig: 1e3 * dttrig.as_secs_f32(),
                                dtcmd: 1e3 * dtcmd.as_secs_f32(),
                                dtreg: 1e3 * dtreg.as_secs_f32(),
                                dtdisp: 1e3 * dtdisp.as_secs_f32(),
                            });
                            return Ready(Some(Some(Ok(item))));
                        }
                        _ => {
                            warn!("{selfname}  TODO item while in WaitRes  {item:?}");
                        }
                    }
                    Ready(Some(None))
                }
            }
        } else if self.inp_done {
            Ready(None)
        } else {
            Pending
        }
    }

    pub fn poll_next_unpin(&mut self, cx: &mut Context) -> Poll<Option<Result<MonitoringItem, Error>>> {
        let selfname = "poll_next_unpin";
        trace3!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match Pin::new(&mut *self).poll_inp(cx) {
                Ready(Some(Some(x))) => {
                    hpp.mark_progress();
                    match x {
                        Ok(x) => break Ready(Some(Ok(x))),
                        Err(e) => {
                            error!("{selfname}  {e}")
                        }
                    }
                }
                Ready(Some(None)) => {
                    hpp.mark_progress();
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            match &mut self.state {
                State::DoNothing => {}
                State::CreateMonitorSend => {
                    hpp.mark_progress();
                    trace2!("{selfname}  CreateMonitorSend  Ready");
                    let tsnow = Instant::now();
                    let msg = CaMsg::from_ty_ts(
                        proto::CaMsgTy::EventAdd(proto::EventAdd::new(
                            self.ca_dbr_ty.to_u16(),
                            self.shape.to_ca_count().unwrap(),
                            self.sid.to_u32(),
                            0,
                        )),
                        Instant::now(),
                    );
                    // self.mett.read_notify_send().inc();
                    // TODO add some jitter
                    let fut = async { tokio::time::sleep(Duration::from_millis(10817)).await };
                    self.state = State::CreateMonitorRecv(fut.box2());
                    let ret = MonitoringItem::ProtoOutSubid(msg, tsnow);
                    return Ready(Some(Ok(ret)));
                }
                State::CreateMonitorRecv(to) => match to.poll_unpin(cx) {
                    Ready(()) => {
                        hpp.mark_progress();
                        info!("{selfname}  CreateMonitorRecv  Timeout  7d956c9");
                        let fut = async { tokio::time::sleep(Duration::from_millis(30817)).await };
                        self.state = State::CreateMonitorRecv(fut.box2());
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
            }
            break if hpp.have_progress() {
                trace!("{selfname}  HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("{selfname}  HPP");
                Pending
            } else {
                trace!("{selfname}  HPP:Done");
                Ready(None)
            };
        }
    }
}
