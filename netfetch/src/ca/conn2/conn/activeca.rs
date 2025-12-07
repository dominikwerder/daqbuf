use crate::ca::conn2::asynchan;
use crate::ca::conn2::asynchan::Receiver;
use crate::ca::conn2::asynchan::SendPoll;
use crate::ca::conn2::asynchan::Sender;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::conn::channelheap;
use crate::ca::conn2::conn::channelheap::ChannelHeap;
use crate::ca::conn2::progpend::HaveProgressPending;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaMsgTy;
use ca_proto::ca::proto::CaProto;
use ca_proto_tokio::tcpasyncwriteread::TcpAsyncWriteRead;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;
use tokio::time::error::Elapsed;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace3 { ($($arg:tt)*) => { if false { log::info!($($arg)*); } }; }
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
    pub fn channel_add(conf: ChannelConfig) -> Self {
        Self {
            kind: CaCommandKind::ChannelAdd(conf),
        }
    }
    pub fn channel_remove<S: Into<String>>(name: S) -> Self {
        Self {
            kind: CaCommandKind::ChannelRemove(name.into()),
        }
    }
}

#[derive(Debug)]
enum CaCommandKind {
    ChannelAdd(ChannelConfig),
    ChannelRemove(String),
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
pub struct ActiveCa {
    tsbeg: Instant,
    addr: SocketAddrV4,
    state: State,
    chanheap: ChannelHeap,
    proto_tx: Sender<CaMsg>,
    proto_rx: asynchan::Receiver<CaMsg>,
    proto_rx_buf: VecDeque<CaMsg>,
    proto_2_tx: asynchan::Sender<CaMsg>,
    cmd_tx: Sender<CaCommand>,
    cmd_rx: Receiver<CaCommand>,
    cmd_fut: Option<CommandFut>,
}

impl ActiveCa {
    pub fn new(
        proto_rx: asynchan::Receiver<CaMsg>,
        proto_tx: Sender<CaMsg>,
        tsnow: Instant,
        addr: SocketAddrV4,
        cx: &mut Context,
    ) -> Self {
        let (mut cmd_tx, cmd_rx) = asynchan::bounded(16, "ActiveCa-cmd");
        {
            let conf = ChannelConfig::st_monitor("TEST:SLOW:SCALAR:F32:000000", "test");
            let cmd = CaCommand::channel_add(conf);
            match cmd_tx.poll_send_unpin(cmd, cx) {
                Ok(()) => {}
                Err(e) => {
                    panic!("ActiveCa: new: initial cmd_tx send failed: {e}");
                }
            }
        }
        let (proto_2_tx, proto_2_rx) = asynchan::bounded(120, "ActiveCa-proto2");
        Self {
            tsbeg: tsnow,
            addr,
            state: State::new(),
            chanheap: ChannelHeap::new(proto_tx.clone(), proto_2_rx),
            proto_tx,
            proto_rx,
            proto_rx_buf: VecDeque::with_capacity(16),
            proto_2_tx,
            cmd_tx,
            cmd_rx,
            cmd_fut: None,
        }
    }

    pub fn dismantle(self) -> (asynchan::Receiver<CaMsg>,) {
        (self.proto_rx,)
    }

    fn handle_command(&mut self, cmd: CaCommand, cx: &mut Context) -> CommandFut {
        match cmd.kind {
            CaCommandKind::ChannelAdd(conf) => {
                self.chanheap.channel_add(conf, cx);
                let fut = async { Ok(()) }.boxed();
                CommandFut(Box::pin(fut))
            }
            CaCommandKind::ChannelRemove(_) => todo!(),
        }
    }

    fn poll_command_input(mut self: Pin<&mut Self>, cx: &mut Context, hpp: &mut HaveProgressPending) -> Option<Error> {
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
            match self2.cmd_rx.poll_next_unpin(cx) {
                Ready(Some(cmd)) => {
                    trace!("CmdRx:Some");
                    let fut = self2.handle_command(cmd, cx);
                    self2.cmd_fut = Some(fut);
                    hpp.mark_progress();
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
}

impl Stream for ActiveCa {
    type Item = Result<ActiveCaItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        trace4!("ActiveCa:poll_next");
        loop {
            let mut hpp = HaveProgressPending::new();
            match self.as_mut().poll_command_input(cx, &mut hpp) {
                Some(e) => {
                    hpp.mark_progress();
                    break Ready(Some(Err(e)));
                }
                None => {}
            }
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Running => {
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
                                    error!("TODO clean shutdown");
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
                            let dispatch = if let Some(_) = item.cid() {
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
                    match self2.chanheap.poll_next_unpin(cx) {
                        Ready(Some(x)) => match x {
                            Ok(item) => {
                                trace!("ActiveCa:ChannelHeap:Done");
                                trace!("ActiveCa:ChannelHeap:Done  TODO do something with item");
                                hpp.mark_progress();
                            }
                            Err(e) => {
                                trace!("ActiveCa:ChannelHeap:Error {e}");
                                error!("ActiveCa:ChannelHeap:Error  TODO clean shutdown");
                                self.state = State::Done;
                                hpp.mark_progress();
                                break Ready(Some(Err(e.into())));
                            }
                        },
                        Ready(None) => {
                            trace!("ActiveCa:ChannelHeap:Done  TODO clean shutdown");
                            self.state = State::Done;
                            hpp.mark_progress();
                        }
                        Pending => {
                            trace_pending!("ActiveCa:ChannelHeap");
                            hpp.mark_pending();
                        }
                    }
                }
                State::Done => {}
            }
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
        }
    }
}
