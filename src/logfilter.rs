use futures_util::Stream;
use items_0::streamitem::AsLogItem;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct LogFilter<S> {
    inp: S,
}

impl<S> fmt::Debug for LogFilter<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("LogFilter").field("name", &"..").finish()
    }
}

impl<S> Stream for LogFilter<S>
where
    S: Stream,
    <S as Stream>::Item: AsLogItem,
{
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        Pending
    }
}
