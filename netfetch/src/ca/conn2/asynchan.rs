use futures_util::Stream;
use futures_util::StreamExt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct Sender<T>(Pin<Box<async_channel::Sender<T>>>);

pub struct Receiver<T>(Pin<Box<async_channel::Receiver<T>>>);

pub fn bounded<T, S: Into<String>>(n: usize, tag: S) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = async_channel::bounded(n);
    (Sender(Box::pin(tx)), Receiver(Box::pin(rx)))
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_tuple("Sender").finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_tuple("Receiver").finish()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

pub use async_channel::TryRecvError;
pub use async_channel::TrySendError;

impl<T> Sender<T> {
    pub fn try_send(&mut self, msg: T) -> Result<(), TrySendError<T>> {
        self.0.try_send(msg)
    }

    pub fn send(&self, msg: T) -> async_channel::Send<'_, T> {
        self.0.send(msg)
    }
}

impl<T> Receiver<T> {
    pub fn try_send(&mut self) -> Result<T, TryRecvError> {
        self.0.try_recv()
    }

    pub fn recv(&self) -> async_channel::Recv<'_, T> {
        self.0.recv()
    }
}
