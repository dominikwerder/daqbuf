use dbpg::seriesbychannel::ChannelInfoQuerySender;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct PollRess<'a> {
    pub ch_info: &'a ChannelInfoQuerySender,
}

pub trait PollCstm {
    type Output;
    fn poll<'a>(self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output>;
    fn poll_unpin<'a>(&mut self, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output>;
}
