use crate::ca::connset::IocAddrQuery;
use crate::ca::findioc::FindIocRes;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "PollRess"),
    enum variants {
        Logic,
    },
);

#[derive(Debug)]
pub enum Cmd {
    Remove,
}

pub struct PollRess<'a> {
    pub ch_info: &'a ChannelInfoQuerySender,
}

impl<'a> PollRess<'a> {
    pub fn new(ch_info: &'a ChannelInfoQuerySender) -> Self {
        Self { ch_info }
    }

    pub fn ioc_search(&mut self, query: IocAddrQuery) -> Result<FindIocRes, Error> {
        // TODO
        // Search is currently batched.
        // Rework this: factor out batching logic, use individual single-use channels on outer api layer.
        todo!()
    }
}

pub trait PollCstm {
    type Output;
    fn poll<'a>(self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output>;
    fn poll_unpin<'a>(&mut self, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output>;
}
