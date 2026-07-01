use futures_util::Stream;
use futures_util::StreamExt;
use items_0::WithLen;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct NonEmpty<S> {
    inp: S,
}

impl<S> NonEmpty<S> {
    pub fn new(inp: S) -> Self {
        Self { inp }
    }
}

impl<S, T, E> Stream for NonEmpty<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    T: WithLen,
{
    type Item = <S as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.inp.poll_next_unpin(cx) {
                Ready(Some(Ok(x))) => {
                    if x.len() != 0 {
                        Ready(Some(Ok(x)))
                    } else {
                        continue;
                    }
                }
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => Ready(None),
                Pending => Pending,
            };
        }
    }
}
