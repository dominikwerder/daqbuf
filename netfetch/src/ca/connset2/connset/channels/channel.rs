mod withcssid;

use crate::ca::conn2::asynchan;
use crate::ca::connset::IocAddrQuery;
use crate::ca::connset2::connset::channels;
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
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ConnSet:Channel"),
    enum variants {
        Logic,
    },
);

#[derive(Debug)]
enum State {
    Init,
    CssidReq(ErasedFuture<Result<ChannelInfoResult, Error>, 0x150>),
    AddrSearch(ErasedFuture<Result<SocketAddrV4, Error>, 0x70>),
    Removed,
}

#[derive(Debug)]
pub struct Channel {
    backend: String,
    conf: ChannelConfig,
    state: State,
    cmd_rx: asynchan::Receiver<Cmd>,
}

impl Channel {
    pub fn new(backend: String, conf: ChannelConfig, cmd_rx: asynchan::Receiver<Cmd>) -> Self {
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
    type Output = Result<(), Error>;

    fn poll<'a>(mut self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match self.cmd_rx.poll_next_unpin(cx) {
                Ready(x) => {}
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
                            let fut = async move {
                                trace!("TODO start with cached lookup");
                                let query = IocAddrQuery::uncached(conf.name().into());
                                todoval()
                            };
                            self.state = State::AddrSearch(ErasedFuture::new(fut));
                            hpp.mark_progress();
                        }
                        Err(e) => {
                            // TODO instead, back off and try again. Count metrics.
                            self.state = State::Removed;
                            hpp.mark_progress();
                        }
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::AddrSearch(fut) => match fut.poll_unpin(cx) {
                    Ready(x) => match x {
                        Ok(x) => {
                            todo!();
                            hpp.mark_progress();
                        }
                        Err(e) => {
                            // TODO instead, back off and try again. Count metrics.
                            self.state = State::Removed;
                            hpp.mark_progress();
                        }
                    },
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::Removed => break Ready(Ok(())),
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(Ok(()))
            };
        }
    }

    fn poll_unpin<'a>(&mut self, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        Pin::new(self).poll(ress, cx)
    }
}
