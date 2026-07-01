use super::super::pollcstm::{PollCstm, PollRess};
use crate::conf::ChannelConfig;
use humantime_serde;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::SystemTime;

autoerr::create_error_v1!(
    name(Error, "ConnSet:WithCssid"),
    enum variants {
        Logic,
    },
);

#[derive(Debug, Serialize)]
pub struct HumanSystemTime(#[serde(with = "humantime_serde")] SystemTime);

#[derive(Debug)]
pub enum State {
    AddrSearchPending {
        since: SystemTime,
    },
    WithAddress {
        addr: SocketAddrV4,
        state: crate::ca::statemap::WithAddressState,
    },
    UnknownAddress(crate::ca::statemap::UnknownAddressState),
    NoAddress {
        since: SystemTime,
    },
    MaybeWrongAddress(crate::ca::statemap::MaybeWrongAddressState),
    UnassigningForConfigChange(crate::ca::statemap::UnassigningForConfigChangeState),
    AddrSearchPlanned {
        since: SystemTime,
    },
}

#[derive(Debug)]
pub struct WithCssid {
    conf: ChannelConfig,
    cssid: ChannelStatusSeriesId,
    // addr_find_backoff: u32,
    inner: crate::ca::statemap::WithStatusSeriesIdStateInner,
    // #[serde(serialize_with = "serde_ser_channel_status_writer")]
    // writer_status: Option<ChannelStatusSeriesWriter>,
}

impl PollCstm for WithCssid {
    type Output = Result<(), Error>;

    fn poll<'a>(self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }

    fn poll_unpin<'a>(&mut self, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}
