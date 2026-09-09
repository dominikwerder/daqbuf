mod withcssid;

use crate::asynchan;
use crate::ca::conn2::locallog;
use crate::ca::conn2::locallog::LocalLog;
use crate::ca::connset2::connset::channels;
use crate::ca::connset2::connset::channels::channel::locallog::llog;
use crate::ca::connset2::connset::channels::pollcstm;
use crate::ca::finder::FinderHandleV02;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use channels::pollcstm::Cmd;
use channels::pollcstm::PollCstm;
use channels::pollcstm::PollRess;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
use dbpg::seriesbychannel::ChannelInfoResult;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use std::fmt;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

const ADDR_SEARCH_TIMEOUT: Duration = Duration::from_millis(30000);
const CSSID_SEARCH_TIMEOUT: Duration = Duration::from_millis(10000);

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }

macro_rules! todo_shutdown { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ConnSetChannel"),
    enum variants {
        Finder(#[from] crate::ca::finder::Error),
        AddrNotFound(String),
        LogicSendBlock,
        Lookup(#[from] dbpg::seriesbychannel::Error),
        Logic,
    },
);

async fn addr_search(conf: ChannelConfig, mut fh: FinderHandleV02) -> Result<SocketAddrV4, Error> {
    let selfname = "addr_search";
    let res = fh.find_uncached(conf.name().into()).await?;
    trace!("{selfname}  res {res:?}");
    let ret = res.addr().ok_or_else(|| Error::AddrNotFound(conf.name().into()))?;
    Ok(ret)
}

#[derive(Debug)]
struct Observing {
    cssid: ChannelStatusSeriesId,
    addr: SocketAddrV4,
}

impl Observing {
    fn addr(&self) -> Option<SocketAddrV4> {
        Some(self.addr.clone())
    }
}

#[derive(Debug, Clone)]
pub struct RemovingCommon {
    cssid: Option<ChannelStatusSeriesId>,
    addr: Option<SocketAddrV4>,
}

impl RemovingCommon {
    pub fn addr(&self) -> Option<SocketAddrV4> {
        self.addr.clone()
    }
}

struct Backoff {
    to: FutDbg<()>,
    until: Instant,
    make_state: Box<dyn FnOnce(&mut Channel) -> State + Send>,
}

impl fmt::Debug for Backoff {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Backoff").field("to", &self.to).finish()
    }
}

#[derive(Debug)]
struct CssidReq {
    fut: FutDbg<Result<ChannelInfoResult, Error>>,
    to: FutDbg<()>,
}

#[derive(Debug)]
struct AddrSearch {
    cssid: ChannelStatusSeriesId,
    chi: ChannelInfoResult,
    fut: FutDbg<Result<SocketAddrV4, Error>>,
    to: FutDbg<()>,
}

#[derive(Debug)]
enum State {
    Init,
    Backoff(Backoff),
    CssidReq(CssidReq),
    AddrSearch(AddrSearch),
    Observing(Observing),
    Removing0(RemovingCommon),
    Removing1(RemovingCommon, FutDbg<Result<(), Error>>),
    Removing2(RemovingCommon, FutDbg<Result<(), Error>>),
    Removed,
    Done,
}

impl State {
    pub fn variant_name(&self) -> &str {
        match self {
            State::Init => "Init",
            State::Backoff(..) => "Backoff",
            State::CssidReq(..) => "CssidReq",
            State::AddrSearch(..) => "AddrSearch",
            State::Observing(..) => "Observing",
            State::Removing0(..) => "Removing0",
            State::Removing1(..) => "Removing1",
            State::Removing2(..) => "Removing2",
            State::Removed => "Removed",
            State::Done => "Done",
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}", self.variant_name())
    }
}

#[derive(Debug, Serialize)]
pub struct BackoffInfo {
    #[serde(with = "serde_helper::serde_Duration_human")]
    until: Duration,
}

#[derive(Debug, Serialize)]
pub struct AddrSearchInfo {
    cssid: ChannelStatusSeriesId,
}

#[derive(Debug, Serialize)]
pub struct ObservingInfo {
    cssid: ChannelStatusSeriesId,
    addr: SocketAddrV4,
}

#[derive(Debug, Serialize)]
pub struct RemovingInfo2 {
    cssid: Option<ChannelStatusSeriesId>,
    addr: Option<SocketAddrV4>,
}

#[derive(Debug, Serialize)]
pub enum StateInfo {
    Init,
    Backoff(BackoffInfo),
    CssidReq,
    AddrSearch(AddrSearchInfo),
    Observing(ObservingInfo),
    Removing0(RemovingInfo2),
    Removing1(RemovingInfo2),
    Removing2(RemovingInfo2),
    Removed,
    Done,
}

#[derive(Debug, Serialize)]
pub struct ChannelInfo {
    state: StateInfo,
    backoff_i: u32,
    local_log: Vec<locallog::Entry>,
}

/// A ConnSet-side channel as it appears in the status endpoints: one that is still
/// searching for an address or backing off has no connection to be reported under.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ChannelStatusLight {
    pub name: String,
    pub state: String,
    pub backoff_i: u32,
    pub backoff_remaining_ms: Option<u64>,
    pub addr: Option<String>,
}

#[derive(Debug)]
pub enum ChannelActionItem {
    AddToCaConn(ChannelConfig, SocketAddrV4),
    RemoveFromCaConn(ChannelConfig, RemovingCommon, asynchan::Sender<u32>),
    LocalLog(locallog::Entry),
}

#[derive(Debug)]
pub struct Channel {
    backend: String,
    conf: ChannelConfig,
    state: State,
    cmd_rx: asynchan::Receiver<pollcstm::Cmd>,
    removing: bool,
    addr: Option<SocketAddrV4>,
    backoff_i: u32,
    llog: LocalLog,
    waker: Option<Waker>,
}

impl Channel {
    pub fn new(backend: String, conf: ChannelConfig, cmd_rx: asynchan::Receiver<pollcstm::Cmd>) -> Self {
        let state = State::Init;
        Self {
            backend,
            conf,
            state,
            cmd_rx,
            removing: false,
            addr: None,
            backoff_i: 0,
            llog: LocalLog::new(),
            waker: None,
        }
    }

    pub fn name(&self) -> &str {
        self.conf.name()
    }

    pub fn config(&self) -> &ChannelConfig {
        &self.conf
    }

    pub fn channel_info(&self) -> ChannelInfo {
        ChannelInfo {
            state: match &self.state {
                State::Init => StateInfo::Init,
                State::Backoff(x) => {
                    let until = x.until.saturating_duration_since(Instant::now());
                    StateInfo::Backoff(BackoffInfo { until })
                }
                State::CssidReq(..) => StateInfo::CssidReq,
                State::AddrSearch(st, ..) => StateInfo::AddrSearch(AddrSearchInfo {
                    cssid: st.cssid.clone(),
                }),
                State::Observing(st, ..) => StateInfo::Observing(ObservingInfo {
                    cssid: st.cssid.clone(),
                    addr: st.addr.clone(),
                }),
                State::Removing0(st, ..) => StateInfo::Removing0(RemovingInfo2 {
                    cssid: st.cssid.clone(),
                    addr: st.addr.clone(),
                }),
                State::Removing1(st, ..) => StateInfo::Removing1(RemovingInfo2 {
                    cssid: st.cssid.clone(),
                    addr: st.addr.clone(),
                }),
                State::Removing2(st, ..) => StateInfo::Removing2(RemovingInfo2 {
                    cssid: st.cssid.clone(),
                    addr: st.addr.clone(),
                }),
                State::Removed => StateInfo::Removed,
                State::Done => StateInfo::Done,
            },
            backoff_i: self.backoff_i,
            local_log: self.llog.to_vec_string(),
        }
    }

    pub fn status_light(&self) -> ChannelStatusLight {
        let backoff_remaining_ms = match &self.state {
            State::Backoff(x) => Some(x.until.saturating_duration_since(Instant::now()).as_millis() as u64),
            _ => None,
        };
        ChannelStatusLight {
            name: self.conf.name().into(),
            state: self.state.variant_name().into(),
            backoff_i: self.backoff_i,
            backoff_remaining_ms,
            addr: self.addr.map(|x| x.to_string()),
        }
    }

    pub fn signal_ca_conn_down(&mut self, dbg_addr: SocketAddrV4, dbg_chn: &str) {
        let selfname = "signal_ca_conn_down";
        match &self.state {
            State::Removing0(..) | State::Removing1(..) | State::Removing2(..) | State::Removed | State::Done => {
                todo_shutdown!("{selfname}  already tearing down/done, not resurrecting  {dbg_addr}  {dbg_chn}");
            }
            _ => {
                self.state = State::Init;
            }
        }
        if let Some(waker) = self.waker.take() {
            todo_shutdown!("{selfname}  wake  {dbg_addr}  {dbg_chn}");
            waker.wake();
        } else {
            todo_shutdown!("{selfname}  no waker  {dbg_addr}  {dbg_chn}");
        }
    }

    fn transition_to_removing(&mut self) {
        let selfname = "transition_to_removing";
        todo_shutdown!("{selfname} called  TODO impl");
        // TODO
        // Correct? More to do?
        // Must be safe to be called in any state.
        self.removing = true;
        let addr = match &self.state {
            State::Observing(st) => Some(st.addr),
            _ => None,
        };
        let cssid = match &self.state {
            State::AddrSearch(st) => Some(st.cssid.clone()),
            State::Observing(st) => Some(st.cssid.clone()),
            State::Removing0(st) => st.cssid.clone(),
            State::Removing1(st, _) => st.cssid.clone(),
            State::Removing2(st, _) => st.cssid.clone(),
            _ => None,
        };
        let stn = State::Removing0(RemovingCommon { addr, cssid });
        llog!(self, "transition {} -> {}", self.state, stn);
        self.state = stn;
    }

    fn produce_cssid_req_state(&mut self, mut ch_info: ChannelInfoQuerySender) -> State {
        let backend = self.backend.clone();
        let name = self.conf.name().into();
        let to = tokio::time::sleep(CSSID_SEARCH_TIMEOUT).box2();
        State::CssidReq(CssidReq {
            fut: async move {
                ch_info
                    .query(backend, name, SeriesKind::ChannelStatus, ScalarType::U64, Shape::Scalar)
                    .map_err(Error::from)
                    .await
            }
            .box2(),
            to,
        })
    }

    fn produce_addr_search_state(&mut self, chi: ChannelInfoResult, fh: FinderHandleV02) -> State {
        let cssid = ChannelStatusSeriesId::new(chi.series.to_series().id());
        let conf = self.conf.clone();
        let to = tokio::time::sleep(ADDR_SEARCH_TIMEOUT).box2();
        State::AddrSearch(AddrSearch {
            cssid,
            chi,
            fut: addr_search(conf, fh).box2(),
            to,
        })
    }

    fn backoff_to_until(&mut self) -> (FutDbg<()>, Instant) {
        self.backoff_i = (1 + self.backoff_i).min(999);
        let x = 120e3 * (self.backoff_i as f32 / 20.).tanh();
        let until = Instant::now() + Duration::from_millis(x as u64);
        let to = tokio::time::sleep_until(until.into());
        (to.box2(), until)
    }

    fn handle_command(&mut self, cmd: Cmd) -> Result<(), Error> {
        let selfname = "handle_command";
        match cmd {
            Cmd::Remove(cmd) => {
                todo_shutdown!("{selfname}  Remove  {}", self.name());
                self.transition_to_removing();
                let mut tx = cmd.done_tx;
                if tx.try_send(Ok(())).is_err() {
                    warn!("command issuer seems gone");
                }
                Ok(())
            }
        }
    }

    pub fn addr(&self) -> Option<SocketAddrV4> {
        match &self.state {
            State::Init => None,
            State::Backoff(..) => self.addr.clone(),
            State::CssidReq(..) => None,
            State::AddrSearch(..) => None,
            State::Observing(st) => st.addr(),
            State::Removing0(st) => st.addr(),
            State::Removing1(st, ..) => st.addr(),
            State::Removing2(st, ..) => st.addr(),
            State::Removed => None,
            State::Done => None,
        }
    }

    fn poll_cmd(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(), Error>>> {
        use Poll::*;
        let self2 = self.as_mut().get_mut();
        match &self2.state {
            State::Done => Ready(None),
            _ => match self2.cmd_rx.poll_next_unpin(cx) {
                Ready(Some(cmd)) => match self2.handle_command(cmd) {
                    Ok(()) => Ready(Some(Ok(()))),
                    Err(e) => Ready(Some(Err(e))),
                },
                Ready(None) => Ready(None),
                Pending => Pending,
            },
        }
    }
}

impl PollCstm for Channel {
    type Output = Option<Result<ChannelActionItem, Error>>;

    fn poll<'a>(mut self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        if self.waker.as_ref().map_or(false, |x| x.will_wake(cx.waker())) {
        } else {
            self.waker = Some(cx.waker().clone());
        }
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(x) = self.llog.pop() {
                break Ready(Some(Ok(ChannelActionItem::LocalLog(x))));
            }
            match self.as_mut().poll_cmd(cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match x {
                        Ok(()) => {}
                        Err(e) => break Ready(Some(Err(e))),
                    }
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Init => {
                    let stn = self2.produce_cssid_req_state(ress.ch_info.clone());
                    llog!(self2, "transition {} -> {}", self2.state, stn);
                    self2.state = stn;
                    hpp.mark_progress();
                }
                State::Backoff(st) => match st.to.poll_unpin(cx) {
                    Ready(()) => {
                        hpp.mark_progress();
                        if let State::Backoff(j) = std::mem::replace(&mut self2.state, State::Done) {
                            let stn = (j.make_state)(self2);
                            llog!(self2, "transition {} -> {}", self2.state, stn);
                            self2.state = stn;
                        } else {
                            panic!("logic")
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::CssidReq(st1) => {
                    match st1.fut.poll_unpin(cx) {
                        Ready(x) => {
                            hpp.mark_progress();
                            match x {
                                Ok(chi) => {
                                    self2.backoff_i = 0;
                                    let stn = self2.produce_addr_search_state(chi, ress.finder_handle.clone());
                                    llog!(self2, "transition {} -> {}", self2.state, stn);
                                    self2.state = stn;
                                    continue;
                                }
                                Err(e) => {
                                    warn!("can not get channel status series id  {e}");
                                    // TODO instead, back off and try again. Count metrics.
                                    let stn = State::Removed;
                                    llog!(self2, "transition {} -> {}", self2.state, stn);
                                    self2.state = stn;
                                    continue;
                                }
                            }
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match st1.to.poll_unpin(cx) {
                        Ready(()) => {
                            hpp.mark_progress();
                            // TODO emtrics instead of log
                            warn!("CssidReq timeout");
                            let (to, until) = self2.backoff_to_until();
                            let ch_info = ress.ch_info.clone();
                            let stn = State::Backoff(Backoff {
                                to,
                                until,
                                make_state: Box::new(move |this: &mut Self| this.produce_cssid_req_state(ch_info)),
                            });
                            llog!(self2, "transition {} -> {}", self2.state, stn);
                            self2.state = stn;
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::AddrSearch(st1) => {
                    match st1.fut.poll_unpin(cx) {
                        Ready(x) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => {
                                    trace!("State::AddrSearch  found {x}");
                                    self2.backoff_i = 0;
                                    let stn = State::Observing(Observing {
                                        cssid: st1.cssid.clone(),
                                        addr: x.clone(),
                                    });
                                    llog!(self2, "transition {} -> {}", self2.state, stn);
                                    self2.state = stn;
                                    self2.addr = Some(x);
                                    let item = ChannelActionItem::AddToCaConn(self2.conf.clone(), x);
                                    break Ready(Some(Ok(item)));
                                }
                                Err(e) => {
                                    match e {
                                        Error::AddrNotFound(_) => {
                                            // TODO metrics
                                            let (to, until) = self2.backoff_to_until();
                                            if let State::AddrSearch(st1) =
                                                std::mem::replace(&mut self2.state, State::Done)
                                            {
                                                let chi = st1.chi;
                                                let fh = ress.finder_handle.clone();
                                                let stn = State::Backoff(Backoff {
                                                    to,
                                                    until,
                                                    make_state: Box::new(move |this: &mut Self| {
                                                        this.produce_addr_search_state(chi, fh)
                                                    }),
                                                });
                                                llog!(self2, "transition {} -> {}", self2.state, stn);
                                                self2.state = stn;
                                                continue;
                                            } else {
                                                self2.state = State::Done;
                                                break Ready(Some(Err(Error::Logic)));
                                            }
                                        }
                                        e => {
                                            warn!("State::AddrSearch  finder error {e}");
                                            let stn = State::Done;
                                            llog!(self2, "transition {} -> {}", self2.state, stn);
                                            self2.state = stn;
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                    match st1.to.poll_unpin(cx) {
                        Ready(()) => {
                            hpp.mark_progress();
                            // TODO count for metrics
                            let (to, until) = self2.backoff_to_until();
                            if let State::AddrSearch(st1) = std::mem::replace(&mut self2.state, State::Done) {
                                let chi = st1.chi;
                                let fh = ress.finder_handle.clone();
                                let stn = State::Backoff(Backoff {
                                    to,
                                    until,
                                    make_state: Box::new(move |this: &mut Self| {
                                        this.produce_addr_search_state(chi, fh)
                                    }),
                                });
                                llog!(self2, "transition {} -> {}", self2.state, stn);
                                self2.state = stn;
                            } else {
                                self2.state = State::Done;
                                break Ready(Some(Err(Error::Logic)));
                            }
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Observing(_) => {
                    // TODO listen to status update of this channel from the CaConn output.
                }
                State::Removing0(reminfo) => {
                    let fut = async move {
                        // TODO do I need to care at this point about any sub-state to finish?
                        // If yes, then maybe move that future into some box here with a strict timeout?
                        // TODO metrics to flush?
                        Ok(())
                    };
                    hpp.mark_progress();
                    let stn = State::Removing1(reminfo.clone(), fut.box2());
                    llog!(self2, "transition {} -> {}", self2.state, stn);
                    self2.state = stn;
                }
                State::Removing1(reminfo, fut) => {
                    match fut.poll_unpin(cx) {
                        Ready(x) => match x {
                            Ok(()) => {
                                let (removed_from_conn_tx, mut removed_from_conn_rx) =
                                    asynchan::bounded(1, "ConnSet-remove-from-caconn");
                                let fut = async move {
                                    todo_shutdown!("TODO emit a channel status event write");
                                    // TODO we are letting ConnSet remove this channel from the actual CaConn.
                                    // This is an async operation and we have to wait here for it to finish.
                                    let _ = removed_from_conn_rx.next().await;
                                    // TODO emit another channel status event write.
                                    Ok(())
                                };
                                hpp.mark_progress();
                                // TODO take instead of clone
                                let reminfo = reminfo.clone();
                                let item = ChannelActionItem::RemoveFromCaConn(
                                    self2.conf.clone(),
                                    reminfo.clone(),
                                    removed_from_conn_tx,
                                );
                                let stn = State::Removing2(reminfo, fut.box2());
                                llog!(self2, "transition {} -> {}", self2.state, stn);
                                self2.state = stn;
                                break Ready(Some(Ok(item)));
                            }
                            Err(e) => {
                                let stn = State::Done;
                                llog!(self2, "transition {} -> {}", self2.state, stn);
                                self2.state = stn;
                                break Ready(Some(Err(e)));
                            }
                        },
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Removing2(reminfo, fut) => {
                    match fut.poll_unpin(cx) {
                        Ready(x) => match x {
                            Ok(()) => {
                                todo_shutdown!("channel removed from ca conn  TODO emit metrics, status event");
                                let fut = async move {
                                    // TODO emit another channel status event write?
                                    // Ok(())
                                };
                                hpp.mark_progress();
                                let stn = State::Removed;
                                llog!(self2, "transition {} -> {}", self2.state, stn);
                                self2.state = stn;
                            }
                            Err(e) => {
                                let stn = State::Done;
                                llog!(self2, "transition {} -> {}", self2.state, stn);
                                self2.state = stn;
                                break Ready(Some(Err(e)));
                            }
                        },
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Removed => {
                    hpp.mark_progress();
                    let stn = State::Done;
                    llog!(self2, "transition {} -> {}", self2.state, stn);
                    self2.state = stn;
                }
                State::Done => {}
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                let chn = self.conf.name();
                todo_shutdown!("HPP:Done  {chn}");
                Ready(None)
            };
        }
    }

    fn poll_unpin<'a>(&mut self, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        Pin::new(self).poll(ress, cx)
    }
}
