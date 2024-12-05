use futures_util::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct SCC<S>
where
    S: Stream,
{
    inp: S,
    errored: bool,
    completed: bool,
}

impl<S> SCC<S>
where
    S: Stream,
{
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            errored: false,
            completed: false,
        }
    }
}

impl<S, I, E> Stream for SCC<S>
where
    S: Stream<Item = Result<I, E>> + Unpin,
{
    type Item = <S as Stream>::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.completed {
            panic!("SCC poll_next on completed");
        } else if self.errored {
            self.completed = true;
            Ready(None)
        } else {
            match self.inp.poll_next_unpin(cx) {
                Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
                Ready(Some(Err(e))) => {
                    self.errored = true;
                    Ready(Some(Err(e)))
                }
                Ready(None) => {
                    self.completed = true;
                    Ready(None)
                }
                Pending => Pending,
            }
        }
    }
}

pub trait IntoSCC<S>
where
    S: Stream,
{
    fn into_scc(self) -> SCC<S>;
}

impl<S> IntoSCC<S> for S
where
    S: Stream,
{
    fn into_scc(self) -> SCC<S> {
        SCC::new(self)
    }
}
