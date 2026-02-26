use crate::ca::conn2::asynchan;
use crate::ca::finder::FinderHandleV02;
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
pub struct Remove {
    pub done_tx: asynchan::Sender<Result<(), Error>>,
}

#[derive(Debug)]
pub enum Cmd {
    Remove(Remove),
}

// #[derive(Clone)]
pub struct PollRess<'a> {
    pub ch_info: &'a ChannelInfoQuerySender,
    pub finder_handle: &'a FinderHandleV02,
}

impl<'a> PollRess<'a> {
    pub fn new(ch_info: &'a ChannelInfoQuerySender, finder_handle: &'a FinderHandleV02) -> Self {
        Self { ch_info, finder_handle }
    }
}

pub trait PollCstm {
    type Output;
    fn poll<'a>(self: Pin<&mut Self>, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output>;
    fn poll_unpin<'a>(&mut self, ress: &'a mut PollRess, cx: &mut Context) -> Poll<Self::Output>;
}
