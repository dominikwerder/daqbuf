use super::AcceptMessageResult;
use crate::ca::conn2::channel::SharedResources;
use crate::ca::conn2::todoval;
use async_channel::SendError;
use async_channel::Sender;
use async_channel::TrySendError;
use ca_proto::ca::proto;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

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
    ress: SharedResources,
    local_epics_hostname: String,
}

impl TryOpen {
    pub fn new(ress: SharedResources, local_epics_hostname: String) -> Self {
        Self {
            state: State::Init,
            ress,
            local_epics_hostname,
        }
    }

    pub fn process_ca_msg(
        mut self: Pin<&mut Self>,
        cx: Context,
        msg: proto::CaMsg,
    ) -> Result<AcceptMessageResult, Error> {
        todo!()
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
                    let tsnow = Instant::now();
                    let hostname = self.local_epics_hostname.clone();
                    let mut msgs = VecDeque::new();
                    let msg = proto::CaMsg::from_ty_ts(proto::CaMsgTy::Version, tsnow);
                    msgs.push_back(msg);
                    let msg = proto::CaMsg::from_ty_ts(proto::CaMsgTy::ClientName, tsnow);
                    msgs.push_back(msg);
                    let msg = proto::CaMsg::from_ty_ts(proto::CaMsgTy::HostName(hostname), tsnow);
                    msgs.push_back(msg);
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
                State::Open => todo!(),
            };
        }
    }
}
