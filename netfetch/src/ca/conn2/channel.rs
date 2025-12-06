mod init;

use crate::ca::conn2::asynchan;
use crate::ca::conn2::proto_channel::ProtoOutChannel;
use crate::ca::conn2::synchan;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ConnChannelError"),
    enum variants {
        ChannelInit(#[from] init::Error),
    },
);

#[derive(Debug, Serialize)]
pub enum ChannelState {
    TryOpen(init::TryOpen),
    WaitOpened,
    Open,
    TryClose,
    WaitClosed,
}

#[derive(Debug, Serialize)]
pub struct Status {}

#[must_use]
pub enum AcceptMessageResult {
    Declined,
    Accepted(()),
}

#[derive(Debug, Serialize)]
pub struct SharedResources {
    local_epics_hostname: String,
    #[serde(serialize_with = "ser_proto_out")]
    proto_out: Arc<ProtoOutChannel>,
    #[serde(serialize_with = "ser_msg_rx")]
    msg_rx: asynchan::Receiver<proto::CaMsg>,
}

fn ser_proto_out<S>(v: &Arc<ProtoOutChannel>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    ser.serialize_str("Arc<ProtoOutChannel>")
}

fn ser_msg_rx<S>(v: &asynchan::Receiver<proto::CaMsg>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    ser.serialize_str("synchan::Receiver")
}

pub struct ChannelBasic {
    state: ChannelState,
}

impl ChannelBasic {
    pub fn new(conf: ChannelConfig, cssid: ChannelStatusSeriesId, ress: SharedResources) -> Self {
        Self {
            state: ChannelState::TryOpen(init::TryOpen::new(conf, cssid, ress)),
        }
    }

    fn status(&self) -> Status {
        todo!()
    }

    pub fn config_update(mut self: Pin<&mut Self>, cx: Context, conf: ()) -> Result<(), Error> {
        todo!()
    }

    pub fn close(mut self: Pin<&mut Self>, cx: Context, conf: ()) -> Result<(), Error> {
        todo!()
    }
}

impl Future for ChannelBasic {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        match &mut self.as_mut().get_mut().state {
            ChannelState::TryOpen(s) => match Pin::new(s).poll(cx) {
                Ready(Ok(())) => {
                    self.state = ChannelState::WaitOpened;
                    Pending
                }
                Ready(Err(e)) => Ready(Err(Error::ChannelInit(e))),
                Pending => Pending,
            },
            ChannelState::WaitOpened => {
                todo!()
            }
            ChannelState::Open => {
                todo!()
            }
            ChannelState::TryClose => {
                todo!()
            }
            ChannelState::WaitClosed => {
                todo!()
            }
        }
    }
}
