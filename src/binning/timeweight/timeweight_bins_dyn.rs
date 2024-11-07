use futures_util::Stream;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinningggContainerBinsDyn;
use netpod::BinnedRange;
use netpod::TsNano;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct BinnedBinsTimeweightStream {}

impl BinnedBinsTimeweightStream {
    pub fn new(
        range: BinnedRange<TsNano>,
        inp: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn BinningggContainerBinsDyn>>> + Send>>,
    ) -> Self {
        todo!()
    }
}

impl Stream for BinnedBinsTimeweightStream {
    type Item = Sitemty<Box<dyn BinningggContainerBinsDyn>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
