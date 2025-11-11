use super::AcceptMessageResult;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::channel::SharedResources;
use crate::ca::conn2::todoval;
use crate::conf::ChannelConfig;
use async_channel::SendError;
use async_channel::Sender;
use async_channel::TrySendError;
use ca_proto::ca::proto;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelError"),
    enum variants {
        ChannelSend,
    },
);

#[derive(Serialize)]
enum State {
    Init,
    #[serde(with = "serde_open_send")]
    OpenSend(
        Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
        VecDeque<proto::CaMsg>,
    ),
    OpenRecv,
    Open,
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            State::Init => write!(fmt, "Init"),
            State::OpenSend(_, v) => write!(fmt, "OpenSend({})", v.len()),
            State::OpenRecv => write!(fmt, "OpenRecv"),
            State::Open => write!(fmt, "Open"),
        }
    }
}

mod serde_open_send {
    use super::Error;
    use super::Pin;
    use super::proto;
    use serde::Serializer;
    use std::collections::VecDeque;

    pub fn serialize<S>(
        _: &Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
        _: &VecDeque<proto::CaMsg>,
        ser: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_str("OpenSend")
    }
}

async fn tx_send(tx: Sender<proto::CaMsg>, item: proto::CaMsg) -> Result<(), Error> {
    tx.send(item).await.map_err(|_| Error::ChannelSend)
}

#[derive(Debug, Serialize)]
pub struct TryOpen {
    state: State,
    conf: ChannelConfig,
    cssid: ChannelStatusSeriesId,
    ress: SharedResources,
    local_epics_hostname: String,
}

impl TryOpen {
    pub fn new(
        conf: ChannelConfig,
        cssid: ChannelStatusSeriesId,
        ress: SharedResources,
        local_epics_hostname: String,
    ) -> Self {
        Self {
            state: State::Init,
            conf,
            cssid,
            ress,
            local_epics_hostname,
        }
    }

    pub fn process_ca_msg(
        mut self: Pin<&mut Self>,
        cx: Context,
        msg: proto::CaMsg,
    ) -> Result<AcceptMessageResult, Error> {
        use Poll::*;
        match msg.ty {
            proto::CaMsgTy::CreateChanRes(k) => {
                todo!();
                // TODO process message
                Ok(AcceptMessageResult::Accepted(()))
            }
            k => {
                warn!("got some other unhandled message during handshake: {:?}", k);
                Ok(AcceptMessageResult::Accepted(()))
            }
        }
    }

    pub fn dismantle(self) -> (ChannelConfig, ChannelStatusSeriesId, SharedResources) {
        (self.conf, self.cssid, self.ress)
    }
}

impl Future for TryOpen {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            let this = &mut *self;
            break match &mut this.state {
                State::Init => {
                    let hostname = self.local_epics_hostname.clone();
                    let mut msgs = VecDeque::new();
                    let tsnow = Instant::now();
                    // TODO generate a cid.
                    let cid = Cid::new(todoval());
                    // let sid = Sid::new(k.sid);
                    // let cssid = cssid.clone();
                    let name = self.conf.name();
                    let msg = proto::CaMsg::from_ty_ts(
                        proto::CaMsgTy::CreateChan(proto::CreateChan {
                            cid: cid.to_u32(),
                            channel: name.into(),
                        }),
                        tsnow,
                    );
                    msgs.push_back(msg);
                    // TODO optional during dev emit to status series that we try to connect.
                    self.state = State::OpenSend(None, msgs);
                    continue;
                }
                State::OpenSend(futopt, msgs) => {
                    if let Some(fut) = futopt {
                        match fut.as_mut().poll(cx) {
                            Ready(x) => {
                                *futopt = None;
                                match x {
                                    Ok(()) => continue,
                                    Err(e) => Ready(Err(e)),
                                }
                            }
                            Pending => Pending,
                        }
                    } else if let Some(item) = msgs.pop_front() {
                        match this.ress.proto_out.tx().try_send(item) {
                            Ok(()) => {
                                continue;
                            }
                            Err(TrySendError::Full(item)) => {
                                let tx = this.ress.proto_out.tx().clone();
                                let fut2 = tx_send(tx, item);
                                let fut2 = Box::pin(fut2);
                                *futopt = Some(fut2);
                                continue;
                            }
                            Err(TrySendError::Closed(_)) => Ready(Err(Error::ChannelSend)),
                        }
                    } else {
                        self.state = State::OpenRecv;
                        continue;
                    }
                }
                State::OpenRecv => {
                    // TODO wait for message.
                    // We do not poll here.
                    Pending
                }
                State::Open => Ready(Ok(())),
            };
        }
    }
}
