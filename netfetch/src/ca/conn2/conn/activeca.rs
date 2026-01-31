use crate::ca::conn2::asynchan;
use crate::ca::conn2::asynchan::Receiver;
use crate::ca::conn2::asynchan::SendPoll;
use crate::ca::conn2::asynchan::Sender;
use crate::ca::conn2::conn::channelheap;
use crate::ca::conn2::conn::channelheap::ChannelHeap;
use crate::ca::conn2::conn::ctchan::CtChan;
use crate::ca::conn2::conn::ctchan::CtChanRc;
use crate::ca::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto::CaMsg;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use taskrun::tokio;
use tokio::time::error::Elapsed;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace4 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
macro_rules! trace_pending { ($($arg:tt)*) => { if false { trace!("{}  Pending", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "ActiveCa"),
    enum variants {
        IO(#[from] std::io::Error),
        ProtoTxClosed,
        ChannelHeap(#[from] channelheap::Error),
    },
);

#[derive(Debug)]
pub struct CaCommand {
    kind: CaCommandKind,
}

impl CaCommand {
    pub fn channel_add(conf: ChannelConfig, done_tx: asynchan::Sender<u32>) -> Self {
        Self {
            kind: CaCommandKind::ChannelAdd(conf, done_tx),
        }
    }

    pub fn channel_remove<S: Into<String>>(name: S, done_tx: asynchan::Sender<u32>) -> Self {
        Self {
            kind: CaCommandKind::ChannelRemove(name.into(), done_tx),
        }
    }

    pub fn disconnect_on_idle(done_tx: asynchan::Sender<u32>) -> Self {
        Self {
            kind: CaCommandKind::DisconnectOnIdle(done_tx),
        }
    }
}

#[derive(Debug)]
enum CaCommandKind {
    ChannelAdd(ChannelConfig, asynchan::Sender<u32>),
    ChannelRemove(String, asynchan::Sender<u32>),
    DisconnectOnIdle(asynchan::Sender<u32>),
}

#[derive(Debug)]
enum State {
    Running,
    Done,
}

impl State {
    fn new() -> Self {
        Self::Running
    }
}

struct CommandFut(Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>);

impl fmt::Debug for CommandFut {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("CommandFut").finish()
    }
}

#[derive(Debug)]
pub enum ItemInner {
    ScyllaWrite,
}

#[derive(Debug)]
pub struct ActiveCaItem {
    // Only for performance measurement:
    pub ts_create: Instant,
    pub inner: ItemInner,
}

#[derive(Debug)]
pub enum StatusInfoState {
    Running(channelheap::StatusInfo),
    Done,
}

#[derive(Debug)]
pub struct StatusInfo {
    pub state: StatusInfoState,
}

type StreamItem = Result<ActiveCaItem, Error>;

#[derive(Debug)]
pub struct ActiveCa {
    tsbeg: Instant,
    addr: SocketAddrV4,
    state: State,
    chanheap: ChannelHeap,
    proto_tx: Sender<CaMsg>,
    proto_rx: asynchan::Receiver<CaMsg>,
    proto_rx_buf: VecDeque<CaMsg>,
    proto_2_tx: asynchan::Sender<CaMsg>,
    // cmd_rx: Receiver<CaCommand>,
    cmd_fut: Option<CommandFut>,
    chanheap_cmd_tx: asynchan::Sender<channelheap::Cmd>,
    chanheap_cmd_rx: asynchan::Receiver<channelheap::Cmd>,
}

impl ActiveCa {
    pub fn new(
        proto_rx: asynchan::Receiver<CaMsg>,
        proto_tx: Sender<CaMsg>,
        // cmd_rx: Receiver<CaCommand>,
        tsnow: Instant,
        addr: SocketAddrV4,
        cx: &mut Context,
    ) -> Self {
        let (proto_2_tx, proto_2_rx) = asynchan::bounded(120, "ActiveCa-proto2");
        let (chanheap_cmd_tx, chanheap_cmd_rx) = asynchan::bounded(16, "ActiveCa-ChannelHeap-cmd");
        Self {
            tsbeg: tsnow,
            addr,
            state: State::new(),
            chanheap: ChannelHeap::new(proto_tx.clone(), proto_2_rx),
            proto_tx,
            proto_rx,
            proto_rx_buf: VecDeque::with_capacity(16),
            proto_2_tx,
            // cmd_rx,
            cmd_fut: None,
            chanheap_cmd_tx,
            chanheap_cmd_rx,
        }
    }

    pub fn dismantle(self) -> (asynchan::Receiver<CaMsg>,) {
        (self.proto_rx,)
    }

    pub fn status_info(&self) -> StatusInfo {
        match &self.state {
            State::Running => StatusInfo {
                state: StatusInfoState::Running(self.chanheap.status_info()),
            },
            State::Done => StatusInfo {
                state: StatusInfoState::Done,
            },
        }
    }

    fn handle_command(&mut self, cmd: CaCommand, cx: &mut Context) -> CommandFut {
        let selfname = "handle_command";
        debug!("{selfname} called");
        match cmd.kind {
            CaCommandKind::ChannelAdd(conf, mut done_tx) => {
                self.chanheap.channel_add(conf, cx);
                let fut = async move {
                    let _ = done_tx.send(0).await;
                    Ok(())
                }
                .boxed();
                CommandFut(Box::pin(fut))
            }
            CaCommandKind::ChannelRemove(name, mut done_tx) => {
                let mut chanheap_cmd_tx = self.chanheap_cmd_tx.clone();
                let fut = async move {
                    let (done_2_tx, mut done_2_rx) = asynchan::bounded(2, "ChannelHeap-Done");
                    let cmd = channelheap::Cmd::RemoveChannel(name, done_2_tx);
                    let ff = chanheap_cmd_tx.send(cmd);
                    match ff.await {
                        Ok(()) => {
                            trace!("{selfname} ChannelRemove Future: sent RemoveChannel command");
                            if done_2_rx.recv().await.is_err() {
                                error!("{selfname}  done_2_rx  recv  fail")
                            }
                            if done_tx.send(0).await.is_err() {
                                error!("{selfname}  done_tx  send  fail")
                            }
                            Ok(())
                        }
                        Err(e) => {
                            error!("{selfname} ChannelRemove Future: failed to send RemoveChannel command");
                            todo!()
                        }
                    }
                }
                .boxed();
                CommandFut(Box::pin(fut))
            }
            CaCommandKind::DisconnectOnIdle(mut done_tx) => {
                self.chanheap.disconnect_on_idle();
                let fut = async move {
                    let _ = done_tx.send(0).await;
                    Ok(())
                }
                .boxed();
                CommandFut(Box::pin(fut))
            }
        }
    }

    fn poll_command_input(
        mut self: Pin<&mut Self>,
        cmd_rx: &mut CtChan<CaCommand>,
        cx: &mut Context,
        hpp: &mut HaveProgressPending,
    ) -> Option<Error> {
        use Poll::*;
        let self2 = self.as_mut().get_mut();
        if let Some(fut) = self2.cmd_fut.as_mut() {
            match fut.0.as_mut().poll(cx) {
                Ready(Ok(())) => {
                    trace!("CmdFut:Ready:Ok");
                    self2.cmd_fut = None;
                    hpp.mark_progress();
                    None
                }
                Ready(Err(e)) => {
                    trace!("CmdFut:Ready:Err {e}");
                    hpp.mark_progress();
                    Some(e)
                }
                Pending => {
                    trace_pending!("CmdFut");
                    hpp.mark_pending();
                    None
                }
            }
        } else {
            match cmd_rx.poll_next_unpin(cx) {
                Ready(Some(cmd)) => {
                    trace!("CmdRx:Some");
                    trace!("---------------------------------------------------     CmdRx:Some");
                    hpp.mark_progress();
                    let fut = self2.handle_command(cmd, cx);
                    self2.cmd_fut = Some(fut);
                    None
                }
                Ready(None) => None,
                Pending => {
                    trace_pending!("CmdRx");
                    hpp.mark_pending();
                    None
                }
            }
        }
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        cmd_rx: &mut CtChan<CaCommand>,
        cx: &mut Context,
    ) -> Poll<Option<StreamItem>> {
        use Poll::*;
        trace4!("ActiveCa:poll_next");
        loop {
            let mut hpp = HaveProgressPending::new();
            // let self2 = self.as_mut().get_mut();
            match &mut self.state {
                State::Running => {
                    match self.as_mut().poll_command_input(cmd_rx, cx, &mut hpp) {
                        Some(e) => {
                            hpp.mark_progress();
                            break Ready(Some(Err(e)));
                        }
                        None => {}
                    }
                    let self2 = self.as_mut().get_mut();
                    if self2.proto_rx_buf.len() < self2.proto_rx_buf.capacity() {
                        match self2.proto_rx.poll_next_unpin(cx) {
                            Ready(x) => match x {
                                Some(item) => {
                                    trace!("ActiveCa:ProtoRx:Some");
                                    self.proto_rx_buf.push_back(item);
                                    hpp.mark_progress();
                                }
                                None => {
                                    trace!("ActiveCa:ProtoRx:Error");
                                    error!("TODO clean shutdown, remote seems gone");
                                    self.state = State::Done;
                                    hpp.mark_progress();
                                }
                            },
                            Pending => {
                                trace_pending!("ActiveCa:ProtoRx");
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        // TODO maybe count for metrics?
                    }
                    loop {
                        if let Some(item) = self.proto_rx_buf.pop_front() {
                            let dispatch = if item.cid().is_some() {
                                true
                            } else if item.subid().is_some() {
                                true
                            } else if item.ioid().is_some() {
                                true
                            } else {
                                false
                            };
                            if dispatch {
                                use asynchan::SendPoll;
                                use asynchan::SendPollError;
                                match self.proto_2_tx.poll_send_unpin(item, cx) {
                                    Ok(()) => {
                                        trace!("Proto2Tx:Sent");
                                        hpp.mark_progress();
                                    }
                                    Err(e) => match e {
                                        SendPollError::Full(item) => {
                                            trace_pending!("Proto2Tx");
                                            self.proto_rx_buf.push_front(item);
                                            hpp.mark_pending();
                                        }
                                        SendPollError::Closed(item) => {
                                            trace!("Proto2Tx:Closed");
                                            self.proto_rx_buf.push_front(item);
                                            error!("TODO handle Proto2Tx:Closed");
                                        }
                                    },
                                }
                            } else {
                                error!("TODO handle incoming item internally: {item:?}");
                                hpp.mark_progress();
                            }
                        } else {
                            break;
                        }
                    }
                    let self2 = self.as_mut().get_mut();
                    match self2.chanheap.poll_next_unpin(&mut self2.chanheap_cmd_rx, cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(item) => {
                                    trace!("ActiveCa:ChannelHeap:Done");
                                    error!("ActiveCa:ChannelHeap:Done  TODO do something with item  {item:?}");
                                    panic!("ActiveCa:ChannelHeap:Done");
                                }
                                Err(e) => {
                                    error!("ActiveCa:ChannelHeap:Error {e}");
                                    error!("ActiveCa:ChannelHeap:Error  TODO clean shutdown");
                                    self2.state = State::Done;
                                    break Ready(Some(Err(e.into())));
                                }
                            }
                        }
                        Ready(None) => {
                            error!("ActiveCa:ChannelHeap:Done  TODO clean shutdown");
                            hpp.mark_progress();
                            self2.state = State::Done;
                        }
                        Pending => {
                            trace_pending!("ActiveCa:ChannelHeap");
                            hpp.mark_pending();
                        }
                    }
                }
                State::Done => {
                    error!("State::Done  {}  {}", hpp.have_progress(), hpp.have_pending());
                    // TODO when in Done, we should no longer be stuck with Pending on something.
                }
            }
            break if hpp.have_progress() {
                trace4!("HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace_pending!("HPP");
                Pending
            } else {
                trace4!("HPP:Done");
                Ready(None)
            };
        }
    }

    pub fn poll_next_unpin(&mut self, cmd_rx: &mut CtChan<CaCommand>, cx: &mut Context) -> Poll<Option<StreamItem>> {
        Pin::new(self).poll_next(cmd_rx, cx)
    }
}
