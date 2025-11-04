use ca_proto::ca::proto;
use serde::Serialize;
use std::pin::Pin;
use std::task::Context;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ConnChannelError"),
    enum variants {
        Logic,
    },
);

#[derive(Debug, Clone, Serialize)]
pub enum ChannelState {
    Init,
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub state: ChannelState,
}

#[must_use]
pub enum AcceptMessageResult {
    Full,
    Accept(()),
}

pub struct ChannelBasic {}

impl ChannelBasic {
    pub fn new() -> Self {
        todo!()
    }

    fn status(&self) -> Status {
        todo!()
    }

    pub fn process_ca_msg(
        mut self: Pin<&mut Self>,
        cx: Context,
        msg: proto::CaMsg,
    ) -> Result<AcceptMessageResult, Error> {
        todo!()
    }

    pub fn config_update(mut self: Pin<&mut Self>, cx: Context, conf: ()) -> Result<(), Error> {
        todo!()
    }

    pub fn close(mut self: Pin<&mut Self>, cx: Context, conf: ()) -> Result<(), Error> {
        todo!()
    }
}
