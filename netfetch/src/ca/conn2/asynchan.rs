use futures_util::Stream;
use futures_util::StreamExt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

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

pub use async_channel::RecvError;
pub use async_channel::SendError;
pub use async_channel::TryRecvError;
pub use async_channel::TrySendError;
use std::task::Waker;

impl<T> Sender<T> {
    pub fn try_send(&mut self, msg: T, cx: &mut Context) -> Result<(), TrySendError<T>> {
        trace!("Receiver:try_send  A  n {n}", n = self.0.len());
        let ret = self.0.try_send(msg);
        trace!("Receiver:try_send  B  n {n}", n = self.0.len());
        ret
    }

    pub fn send(&self, msg: T) -> async_channel::Send<'_, T> {
        self.0.send(msg)
    }

    pub fn set_waker(&mut self, waker: &Waker) {}
}

impl<T> Receiver<T> {
    pub fn try_recv(&mut self, cx: &mut Context) -> Result<T, TryRecvError> {
        trace!("Receiver:try_recv  A  n {n}", n = self.0.len());
        let ret = self.0.try_recv();
        trace!("Receiver:try_recv  B  n {n}", n = self.0.len());
        ret
    }

    pub fn recv(&self) -> async_channel::Recv<'_, T> {
        self.0.recv()
    }
}

fn test_some() {
    use crossfire::TrySendError;
    let (tx, rx) = crossfire::mpmc::bounded_async(128);
    match tx.try_send(String::new()) {
        Ok(()) => {}
        Err(e) => match e {
            TrySendError::Full(item) => todo!(),
            TrySendError::Disconnected(item) => todo!(),
        },
    }
}
