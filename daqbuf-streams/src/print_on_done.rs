use futures_util::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

pub struct PrintOnDone<INP> {
    ts_ctor: Instant,
    inp: INP,
    on_done: Pin<Box<dyn Fn(Instant) -> () + Send>>,
}

impl<INP> PrintOnDone<INP> {
    pub fn new(inp: INP, on_done: Pin<Box<dyn Fn(Instant) -> () + Send>>) -> Self {
        Self {
            ts_ctor: Instant::now(),
            inp,
            on_done,
        }
    }
}

impl<INP> Stream for PrintOnDone<INP>
where
    INP: Stream + Unpin,
{
    type Item = <INP as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(x)) => Ready(Some(x)),
            Ready(None) => {
                (self.on_done)(self.ts_ctor);
                Ready(None)
            }
            Pending => Pending,
        }
    }
}
