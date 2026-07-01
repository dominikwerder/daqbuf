use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use netpod::log::Level;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug)]
pub struct LogFilter<S> {
    inp: S,
    level: Level,
}

impl<S> LogFilter<S> {
    pub fn new(inp: S, level: Level) -> Self {
        Self { inp, level }
    }
}

impl<S, T, E> Stream for LogFilter<S>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
{
    type Item = Sitemty2<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.inp.poll_next_unpin(cx) {
                Ready(Some(x)) => match x {
                    Ok(x) => match x {
                        StreamItem::DataItem(x) => Ready(Some(Ok(StreamItem::DataItem(x)))),
                        StreamItem::Log(x) => {
                            if x.level() <= self.level {
                                Ready(Some(Ok(StreamItem::Log(x))))
                            } else {
                                continue;
                            }
                        }
                        StreamItem::Stats(x) => Ready(Some(Ok(StreamItem::Stats(x)))),
                    },
                    Err(e) => Ready(Some(Err(e))),
                },
                Ready(None) => Ready(None),
                Pending => Pending,
            };
        }
    }
}
