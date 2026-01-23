mod withcssid;

use crate::ca::conn2::asynchan;
use crate::ca::connset::IocAddrQuery;
use crate::ca::connset2::connset::channels;
use crate::ca::connset2::connset::channels::pollcstm;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use crate::futwrap::FutDbg;
use crate::futwrap::FutDbgBox;
use crate::misc::todoval;
use channels::pollcstm::Cmd;
use channels::pollcstm::PollCstm;
use channels::pollcstm::PollRess;
use dbpg::seriesbychannel::ChannelInfoResult;
use futures::FutureExt;
use futures::StreamExt;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use series::ChannelStatusSeriesId;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { log::info!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ConnSet:Channel"),
    enum variants {
        Finder(#[from] crate::ca::finder::Error),
        AddrNotFound(String),
        LogicSendBlock,
    },
);

async fn addr_search(conf: ChannelConfig, ress: &mut PollRess<'_>) -> Result<SocketAddrV4, Error> {
    let selfname = "addr_search";
    let mut fh = ress.finder_handle.clone();
    let res = fh.find_uncached(conf.name().into()).await?;
    trace!("{selfname}  res {res:?}");
    let ret = res.addr.ok_or_else(|| Error::AddrNotFound(conf.name().into()))?;
    Ok(ret)
}

#[derive(Debug)]
struct Observing {
    addr: SocketAddrV4,
}

#[derive(Debug, Clone)]
pub struct RemovingInfo {
    pub addr: Option<SocketAddrV4>,
}

#[derive(Debug)]
enum State {
    Init,
    CssidReq(ErasedFuture<Result<ChannelInfoResult, Error>, 0x150>),
    AddrSearch(ChannelStatusSeriesId, ErasedFuture<Result<SocketAddrV4, Error>, 0x200>),
    Observing(Observing),
    Removing0(RemovingInfo),
    Removing1(RemovingInfo, FutDbg<Result<(), Error>>),
    Removing2(RemovingInfo, FutDbg<Result<(), Error>>),
    Removed,
    Done,
}

#[derive(Debug)]
pub enum ChannelActionItem {
    AddToCaConn(ChannelConfig, SocketAddrV4),
    RemoveFromCaConn(ChannelConfig, RemovingInfo, asynchan::Sender<u32>),
}

#[derive(Debug)]
pub struct Channel {
    backend: String,
    conf: ChannelConfig,
    state: State,
    cmd_rx: asynchan::Receiver<pollcstm::Cmd>,
}

impl Channel {
    pub fn new(backend: String, conf: ChannelConfig, cmd_rx: asynchan::Receiver<pollcstm::Cmd>) -> Self {
        let state = State::Init;
        Self {
            backend,
            conf,
            state,
            cmd_rx,
        }
    }

    pub fn name(&self) -> &str {
        self.conf.name()
    }

    fn transition_to_removing(&mut self) {
        let selfname = "transition_to_removing";
        debug!("{selfname} called");
        warn!("{selfname} TODO impl");
        // TODO
        // Correct? More to do?
        // Must be safe to be called in any state.
        let addr = match &self.state {
            State::Observing(obs) => Some(obs.addr),
            _ => None,
        };
        self.state = State::Removing0(RemovingInfo { addr });
    }

    fn handle_command(mut self: Pin<&mut Self>, cmd: Cmd, cx: &mut Context) -> Result<(), Error> {
        let selfname = "handle_command";
        debug!("{selfname} called");
        match cmd {
            Cmd::Remove(cmd) => {
                self.transition_to_removing();
                let mut tx = cmd.done_tx;
                if tx.try_send(Ok(())).is_err() {
                    warn!("command issuer seems gone");
                }
                Ok(())
            }
        }
    }
}

impl PollCstm for Channel {
    type Output = Option<Result<ChannelActionItem, Error>>;

    fn poll<'a>(mut self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match self.as_mut().cmd_rx.poll_next_unpin(cx) {
                Ready(Some(cmd)) => {
                    match self.as_mut().handle_command(cmd, cx) {
                        Ok(()) => {}
                        Err(e) => break Ready(Some(Err(e))),
                    }
                    hpp.mark_progress();
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            match &mut self.state {
                State::Init => {
                    let backend = self.backend.clone();
                    let conf = self.conf.clone();
                    let mut ch_info = ress.ch_info.clone();
                    let fut = async move {
                        let x = ch_info
                            .query(
                                backend,
                                conf.name().into(),
                                SeriesKind::ChannelStatus,
                                ScalarType::U64,
                                Shape::Scalar,
                            )
                            .await;
                        // TODO return the value
                        Ok(x.unwrap())
                    };
                    self.state = State::CssidReq(ErasedFuture::new(fut));
                    hpp.mark_progress();
                }
                State::CssidReq(fut) => match fut.poll_unpin(cx) {
                    Ready(x) => match x {
                        Ok(x) => {
                            let cssid = ChannelStatusSeriesId::new(x.series.to_series().id());
                            let conf = self.conf.clone();
                            self.state = State::AddrSearch(cssid, ErasedFuture::new(addr_search(conf, ress)));
                            hpp.mark_progress();
                        }
                        Err(e) => {
                            warn!("can not get channel status series id  {e}");
                            // TODO instead, back off and try again. Count metrics.
                            self.state = State::Removed;
                            hpp.mark_progress();
                        }
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::AddrSearch(cssid, fut) => match fut.poll_unpin(cx) {
                    Ready(x) => match x {
                        Ok(x) => {
                            trace!("State::AddrSearch  found {x}");
                            trace!("State::AddrSearch  TODO  issue channel-add and then monitor for status updates");
                            hpp.mark_progress();
                            self.state = State::Observing(Observing { addr: x });
                            let item = ChannelActionItem::AddToCaConn(self.conf.clone(), x);
                            break Ready(Some(Ok(item)));
                        }
                        Err(e) => {
                            match e {
                                Error::AddrNotFound(_) => {
                                    // TODO instead, back off and try again. Count metrics.
                                    self.state = State::Removed;
                                    hpp.mark_progress();
                                }
                                e => {
                                    warn!("State::AddrSearch  finder error {e}");
                                    self.state = State::Removed;
                                    hpp.mark_progress();
                                }
                            }
                        }
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                },
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
                    self.state = State::Removing1(reminfo.clone(), fut.box2());
                }
                State::Removing1(reminfo, fut) => {
                    match fut.poll_unpin(cx) {
                        Ready(x) => match x {
                            Ok(()) => {
                                let (removed_from_conn_tx, mut removed_from_conn_rx) =
                                    asynchan::bounded(1, "ConnSet-remove-from-caconn");
                                let fut = async move {
                                    warn!("TODO emit a channel status event write");
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
                                    self.conf.clone(),
                                    reminfo.clone(),
                                    removed_from_conn_tx,
                                );
                                self.state = State::Removing2(reminfo, fut.box2());
                                break Ready(Some(Ok(item)));
                            }
                            Err(e) => {
                                self.state = State::Done;
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
                                debug!("channel removed from ca conn");
                                let fut = async move {
                                    // TODO emit another channel status event write?
                                    // Ok(())
                                };
                                hpp.mark_progress();
                                self.state = State::Removed;
                            }
                            Err(e) => {
                                self.state = State::Done;
                                break Ready(Some(Err(e)));
                            }
                        },
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
                State::Removed => {}
                State::Done => {}
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

    fn poll_unpin<'a>(&mut self, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        Pin::new(self).poll(ress, cx)
    }
}
