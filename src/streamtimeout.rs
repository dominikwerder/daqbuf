use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

pub type BoxedTimeoutFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub trait StreamTimeout2: Send {
    fn timeout_intervals(&self, ivl: Duration) -> BoxedTimeoutFuture;
}

pub struct TimeoutableStream<S> {
    ivl: Duration,
    timeout_provider: Box<dyn StreamTimeout2>,
    inp: Pin<Box<S>>,
    timeout_fut: BoxedTimeoutFuture,
    last_seen: Instant,
}

impl<S> TimeoutableStream<S>
where
    S: Stream,
{
    pub fn new(ivl: Duration, timeout_provider: Box<dyn StreamTimeout2>, inp: S) -> Self {
        let timeout_fut = timeout_provider.timeout_intervals(ivl);
        Self {
            ivl,
            timeout_provider,
            inp: Box::pin(inp),
            timeout_fut,
            last_seen: Instant::now(),
        }
    }

    fn resetup(mut self: Pin<&mut Self>, ivl: Duration) -> () {
        self.timeout_fut = self.timeout_provider.timeout_intervals(ivl)
    }

    fn handle_timeout(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        let tsnow = Instant::now();
        if self.last_seen + self.ivl < tsnow {
            let ivl2 = self.ivl;
            self.resetup(ivl2);
            Ready(Some(None))
        } else {
            let ivl2 = (self.last_seen + self.ivl) - tsnow + Duration::from_millis(1);
            self.resetup(ivl2);
            cx.waker().wake_by_ref();
            Pending
        }
    }

    fn handle_inp_pending(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        match self.timeout_fut.poll_unpin(cx) {
            Ready(()) => self.handle_timeout(cx),
            Pending => Pending,
        }
    }
}

impl<S> Stream for TimeoutableStream<S>
where
    S: Stream,
{
    type Item = Option<<S as Stream>::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(x)) => {
                self.last_seen = Instant::now();
                Ready(Some(Some(x)))
            }
            Ready(None) => Ready(None),
            Pending => self.handle_inp_pending(cx),
        }
    }
}
