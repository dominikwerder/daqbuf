use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct FutDbg<T>(Pin<Box<dyn Future<Output = T> + Send>>);

impl<T> fmt::Debug for FutDbg<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("FutDbg").finish()
    }
}

impl<T> Future for FutDbg<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

pub trait FutDbgBox {
    fn box2(self) -> FutDbg<Self::Output>
    where
        Self: Future + Send + 'static,
        Self::Output: Send + 'static;
}

impl<F> FutDbgBox for F
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn box2(self) -> FutDbg<F::Output> {
        FutDbg(Box::pin(self))
    }
}
