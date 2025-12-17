mod withcssid;

use crate::ca::conn2::asynchan;
use crate::ca::connset::IocAddrQuery;
use crate::ca::connset2::connset::channels;
use crate::ca::connset2::connset::channels::pollcstm;
use crate::ca::futstack::ErasedFuture;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
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
enum State {
    Init,
    CssidReq(ErasedFuture<Result<ChannelInfoResult, Error>, 0x150>),
    AddrSearch(ChannelStatusSeriesId, ErasedFuture<Result<SocketAddrV4, Error>, 0x200>),
    Removed,
}

#[derive(Debug)]
pub enum ChannelActionItem {
    AddToCaConn(String, SocketAddrV4),
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
}

impl PollCstm for Channel {
    type Output = Option<Result<ChannelActionItem, Error>>;

    fn poll<'a>(mut self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match self.cmd_rx.poll_next_unpin(cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match x {
                        Cmd::Remove => todo!(),
                    }
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
                            self.state = State::Removed;
                            hpp.mark_progress();
                        }
                        Err(e) => {
                            match e {
                                Error::Finder(e) => {
                                    warn!("State::AddrSearch  finder error {e}");
                                    self.state = State::Removed;
                                    hpp.mark_progress();
                                }
                                Error::AddrNotFound(_) => {
                                    // TODO instead, back off and try again. Count metrics.
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
                State::Removed => {}
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
