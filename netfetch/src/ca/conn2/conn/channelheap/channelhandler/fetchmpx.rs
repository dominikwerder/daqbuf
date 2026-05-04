mod fetchmonitoring;
mod fetchpolling;

use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::channel_event_value::ChannelEventValue;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::conn2::conn::channelheap::channelhandler;
use crate::ca::conn2::conn::channelheap::channelhandler::fetchmpx::fetchmonitoring::FetchMonitoring;
use crate::ca::conn2::conn::channelheap::channelhandler::fetchmpx::fetchpolling::FetchPolling;
use crate::ca::conn2::locallog;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto;
use futures::Stream;
use netpod::ScalarType;
use netpod::Shape;
use netpod::channelstatus::ChannelStatus;
use serde::Deserialize;
use series::SeriesId;
use stats::mett::ChannelHandlerMetrics;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
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

fn _keep() {
    info!("");
}

autoerr::create_error_v1!(
    name(Error, "ChannelHandlerRunning"),
    enum variants {
        CreateMonitorUnexpectedMessage,
        Recv,
        FetchPolling(#[from] fetchpolling::Error),
        FetchMonitoring(#[from] fetchmonitoring::Error),
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

struct SomeData;

fn make_cb<F>(f: F) -> Box<dyn FnOnce(&mut SomeData)>
where
    F: FnOnce(&mut SomeData) + 'static,
{
    Box::new(f)
}

fn make_cb2<F>(f: F) -> Box<dyn FnOnce(&mut SomeData)>
where
    F: FnOnce(&mut SomeData) + 'static,
{
    Box::new(f)
}

#[allow(unused)]
fn _cb_tmp() {
    let _ = make_cb(|_| ());
    let _ = make_cb2(|_| ());
}

#[derive(Debug)]
pub enum FetchmpxItem {
    CaMsgOut(proto::CaMsg),
    CaMsgOutIoid(proto::CaMsg, Sid, Instant),
    CaMsgOutSubid(proto::CaMsg, Instant),
    TestValue(crate::ca::connset2::connset::TestValue),
    LocalLog(locallog::Entry),
    ChannelStatus(ChannelStatus),
    InputDone,
    ChannelEventValue(ChannelEventValue),
}

#[derive(Debug)]
enum State {
    Normal,
    Closing1,
    Done,
    Done2,
}

impl State {
    fn str(&self) -> &str {
        match self {
            State::Normal => "Normal",
            State::Closing1 => "Closing1",
            State::Done => "Done",
            State::Done2 => "Done2",
        }
    }
}

#[derive(Debug)]
pub struct Fetchmpx {
    state: State,
    series: SeriesId,
    sid: Sid,
    polling: FetchPolling,
    monitoring: FetchMonitoring,
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    mett: ChannelHandlerMetrics,
    llog: locallog::LocalLog,
}

impl Fetchmpx {
    pub fn new(
        series: SeriesId,
        sid: Sid,
        scalar_type: ScalarType,
        shape: Shape,
        ca_dbr_ty: CaDbrTy,
        chconf: ChannelConfig,
    ) -> Self {
        let mut polling = FetchPolling::new(
            series.clone(),
            sid.clone(),
            scalar_type.clone(),
            shape.clone(),
            ca_dbr_ty.clone(),
        );
        let mut monitoring = FetchMonitoring::new(
            series.clone(),
            sid.clone(),
            scalar_type.clone(),
            shape.clone(),
            ca_dbr_ty.clone(),
        );
        if chconf.is_polled() {
            polling.transition_to_enable();
        } else {
            monitoring.transition_to_enable();
        }
        Self {
            state: State::Normal,
            series,
            sid,
            polling,
            monitoring,
            inp_buf: VecDeque::with_capacity(8),
            inp_done: false,
            mett: ChannelHandlerMetrics::new(),
            llog: locallog::LocalLog::new(),
        }
    }

    fn trigger_closing(&mut self, reason: channelhandler::ClosingReason) {
        let selfname = "trigger_closing";
        trace2!("{selfname}  {}  {:?}", self.sid, reason);
        match &mut self.state {
            State::Normal => {
                warn!("{selfname}  TODO collect all information that we want to store or log and move into future");
                self.state = State::Closing1;
            }
            State::Closing1 => {}
            State::Done => {}
            State::Done2 => {}
        }
    }

    pub fn trigger_remove(&mut self) {
        self.trigger_closing(channelhandler::ClosingReason::Command);
    }

    pub fn handle_channel_handler_cmd(&mut self, mut cmd: serde_json::Value) -> serde_json::Value {
        use serde_json::json;
        let selfname = "handle_channel_handler_cmd";
        match &mut self.state {
            State::Normal => {
                #[allow(unused)]
                #[derive(Debug, Deserialize)]
                struct CmdTmp {
                    #[serde(rename = "type")]
                    ty: String,
                    name: String,
                    fetchmpx: String,
                }
                match serde_json::from_value::<CmdTmp>(cmd.clone()) {
                    Ok(x) => {
                        if x.fetchmpx == "polling_disable" {
                            self.polling.transition_to_disable();
                            json!({"done":"polling_disable"})
                        } else if x.fetchmpx == "polling_enable" {
                            self.polling.transition_to_enable();
                            json!({"done":"polling_enable"})
                        } else if x.fetchmpx == "monitoring_disable" {
                            self.monitoring.transition_to_disable();
                            json!({"done":"monitoring_disable"})
                        } else if x.fetchmpx == "monitoring_enable" {
                            self.monitoring.transition_to_enable();
                            json!({"done":"monitoring_enable"})
                        } else {
                            json!({"error":format!("TODO handle while in {} {:?}", self.state.str(), cmd)})
                        }
                    }
                    Err(e) => {
                        json!({"error":format!("TODO can not parse {} {:?}", self.state.str(), cmd)})
                    }
                }
            }
            State::Done => {
                json!({"error":format!("TODO handle while in {} {:?}", self.state.str(), cmd)})
            }
            State::Closing1 => {
                json!({"error":format!("TODO handle while in {} {:?}", self.state.str(), cmd)})
            }
            State::Done2 => {
                json!({"error":format!("TODO handle while in {} {:?}", self.state.str(), cmd)})
            }
        }
    }

    pub fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
        trace3!("inp_push_try");
        let v = &mut self.inp_buf;
        if v.len() < v.capacity() {
            v.push_back(item);
            None
        } else {
            Some(item)
        }
    }

    pub fn inp_done(&mut self) {
        trace3!("inp_done");
        self.inp_done = true;
        self.polling.inp_done();
        self.monitoring.inp_done();
    }

    fn poll_inp_dispatch(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Option<FetchmpxItem>, Error>>> {
        let selfname = "poll_inp_dispatch";
        trace3!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal => {
                    if let Some(item) = self2.inp_buf.pop_front() {
                        trace2!("{selfname}  ITEM  {item:?}");
                        match &item.msg.ty {
                            proto::CaMsgTy::EventAddRes(_)
                            | proto::CaMsgTy::EventAddResEmpty(_)
                            | proto::CaMsgTy::EventCancelRes(_) => match self2.monitoring.inp_push_try(item) {
                                Some(x) => {
                                    hpp.mark_pending();
                                    self2.inp_buf.push_front(x);
                                }
                                None => {
                                    hpp.mark_progress();
                                    self2.mett.event_add_recv().inc();
                                }
                            },
                            proto::CaMsgTy::ReadNotifyRes(_) => match self2.polling.inp_push_try(item) {
                                Some(x) => {
                                    hpp.mark_pending();
                                    self2.inp_buf.push_front(x);
                                }
                                None => {
                                    hpp.mark_progress();
                                    self2.mett.read_notify_recv().inc();
                                }
                            },
                            _ => {
                                warn!("{selfname}  unexpected  {item:?}");
                                let e = Error::CreateMonitorUnexpectedMessage;
                                return Ready(Some(Err(e)));
                            }
                        }
                    } else if self2.inp_done {
                        hpp.mark_progress();
                        self2.trigger_closing(channelhandler::ClosingReason::InputDone);
                        let item = FetchmpxItem::InputDone;
                        break Ready(Some(Ok(Some(item))));
                    } else {
                        hpp.mark_pending();
                    }
                }
                State::Closing1 => {
                    debug!("{selfname}  TODO  no input to parse in Closing1");
                }
                State::Done => {}
                State::Done2 => {}
            }
            break if hpp.have_progress() {
                trace4!("{selfname}  HPP:Progress");
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

impl Stream for Fetchmpx {
    type Item = Result<FetchmpxItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let selfname = "Fetchmpx::poll_next";
        trace3!("{selfname}");
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(x) = self.llog.pop() {
                break Ready(Some(Ok(FetchmpxItem::LocalLog(x))));
            }
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Normal => {
                    match self.as_mut().poll_inp_dispatch(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => match x {
                                    Some(x) => {
                                        break Ready(Some(Ok(x)));
                                    }
                                    None => {}
                                },
                                Err(e) => {
                                    error!("{selfname}  TODO handle error {e}");
                                    self.state = State::Done;
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match Pin::new(&mut self.polling).poll_next(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => match x {
                                    fetchpolling::Item::None => {}
                                    fetchpolling::Item::ProtoOut(msg) => {
                                        let g = FetchmpxItem::CaMsgOut(msg);
                                        break Ready(Some(Ok(g)));
                                    }
                                    fetchpolling::Item::ProtoOutIoid(msg, sid, ts) => {
                                        let g = FetchmpxItem::CaMsgOutIoid(msg, sid, ts);
                                        break Ready(Some(Ok(g)));
                                    }
                                    fetchpolling::Item::ChannelEventValue(x) => {
                                        let g = FetchmpxItem::ChannelEventValue(x);
                                        break Ready(Some(Ok(g)));
                                    }
                                    fetchpolling::Item::TestValue(x) => {
                                        let g = FetchmpxItem::TestValue(x);
                                        break Ready(Some(Ok(g)));
                                    }
                                    fetchpolling::Item::LocalLog(x) => {
                                        let g = FetchmpxItem::LocalLog(x);
                                        break Ready(Some(Ok(g)));
                                    }
                                },
                                Err(e) => {
                                    error!("{selfname}  {e}");
                                    self.state = State::Done;
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match self.monitoring.poll_next_unpin(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => {
                                    use fetchmonitoring::MonitoringItem;
                                    match x {
                                        MonitoringItem::ProtoOutSubid(msg, ts) => {
                                            let g = FetchmpxItem::CaMsgOutSubid(msg, ts);
                                            break Ready(Some(Ok(g)));
                                        }
                                        MonitoringItem::TestValue(x) => {
                                            let g = FetchmpxItem::TestValue(x);
                                            break Ready(Some(Ok(g)));
                                        }
                                        MonitoringItem::LocalLog(x) => {
                                            let g = FetchmpxItem::LocalLog(x);
                                            break Ready(Some(Ok(g)));
                                        }
                                        MonitoringItem::ChannelEventValue(x) => {
                                            let g = FetchmpxItem::ChannelEventValue(x);
                                            break Ready(Some(Ok(g)));
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("{selfname}  {e}");
                                    self.state = State::Done;
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Closing1 => {
                    debug!("{selfname}  TODO  nothing to do yet, go directly to Done");
                    hpp.mark_progress();
                    self.state = State::Done;
                }
                State::Done => {
                    self.state = State::Done2;
                }
                State::Done2 => {
                    error!("{lf}{selfname}  polled after done{lf}", lf = "\n\n");
                }
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
