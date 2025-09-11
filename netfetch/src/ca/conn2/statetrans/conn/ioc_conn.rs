use ca_proto::ca::proto::CaProto;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::time::Instant;
use taskrun::tokio;
use tokio::net::TcpStream;

type ConnectingFutType =
    Pin<Box<dyn Future<Output = Result<Result<TcpStream, std::io::Error>, tokio::time::error::Elapsed>> + Send>>;

struct ConnectingFut(ConnectingFutType);

impl fmt::Debug for ConnectingFut {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("ConnectingFut").finish()
    }
}

#[derive(Debug)]
pub struct Init {
    ts_beg: Instant,
}

#[derive(Debug)]
pub struct ConnectingTcp {
    ts_beg: Instant,
    fut: ConnectingFut,
}

#[derive(Debug)]
pub struct SendingHandshake {
    ts_beg: Instant,
    // TODO smal set of messages that we want to send as handshake.
    // TODO requires access to proto.
    // TODO requires &mut self.
    msgs_buf: VecDeque<String>,
}

#[derive(Debug)]
pub struct IocConnReady {
    ts_beg: Instant,
    shutting_down: bool,
    ca_proto: CaProto,
}

#[derive(Debug)]
pub enum IocConn {
    Init(Init),
    ConnectingTcp,
    SendingHandshake(SendingHandshake),
    IocConnReady(IocConnReady),
    ShutdownDoingRemainingTcpStuff,
    ShutdownTcpClosing,
    Done,
}

impl IocConn {
    pub fn init() -> Self {
        Self::Init(Init { ts_beg: Instant::now() })
    }
}
