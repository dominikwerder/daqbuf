use futures::Sink;
use futures::Stream;
use futures::StreamExt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use taskrun::tokio;
use tokio::sync::mpsc;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

pub struct Sender<T>(mpsc::Sender<T>, String);

pub struct Receiver<T>(mpsc::Receiver<T>, String);

pub fn bounded<T: Unpin + Send + 'static, S: Into<String>>(n: usize, tag: S) -> (Sender<T>, Receiver<T>) {
    let tag = tag.into();
    let (tx, rx) = mpsc::channel(n);
    (Sender(tx.clone(), tag.clone()), Receiver(rx, tag))
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Sender").field("tag", &self.1).finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Receiver").field("tag", &self.1).finish()
    }
}

impl<T> Clone for Sender<T>
where
    T: Unpin + Send + 'static,
{
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<T> Stream for Receiver<T>
where
    T: Unpin + Send + 'static,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.0.poll_recv(cx) {
            Ready(Some(x)) => Ready(Some(x)),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct SendError<T>(T);

pub struct Sending<'a, T> {
    fut: Box<dyn Future<Output = Result<(), mpsc::error::SendError<T>>>>,
    _p1: std::marker::PhantomData<&'a ()>,
}

impl<'a, T> Future for Sending<'a, T>
where
    T: Unpin + Send + 'static,
{
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use Poll::*;
        if let Some(item) = self.item.take() {
            use crossfire::TrySendError as Serr;
            match self.tx.poll_send(cx, item) {
                Ok(()) => Ready(Ok(())),
                Err(e) => match e {
                    Serr::Full(item) => {
                        self.item = Some(item);
                        Pending
                    }
                    Serr::Disconnected(item) => Ready(Err(SendError(item))),
                },
            }
        } else {
            // TODO should never happen
            Ready(Ok(()))
        }
    }
}

impl<T> Sender<T>
where
    T: Unpin + Send + 'static,
{
    pub fn send<'a>(&'a mut self, item: T) -> Sending<'a, T> {
        let tx = self.0.clone();
        Sending {
            fut: Box::new(async move { tx.send(item).await }),
            _p1: std::marker::PhantomData,
        }
    }

    pub fn try_send(&mut self, item: T) -> Result<(), SendError<T>> {
        match self.0.try_send(item) {
            Ok(()) => Ok(()),
            Err(x) => Err(SendError(x.into_inner())),
        }
    }
}

pub enum SendPollError<T> {
    Full(T),
    Closed(T),
}

impl<T> fmt::Debug for SendPollError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendPollError::Full(_) => fmt.debug_tuple("Full").finish(),
            SendPollError::Closed(_) => fmt.debug_tuple("Closed").finish(),
        }
    }
}

impl<T> fmt::Display for SendPollError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

#[derive(Debug)]
pub struct SendCloseError;

impl fmt::Display for SendCloseError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

pub trait SendPoll<T> {
    fn poll_send(self: Pin<&mut Self>, item: T, cx: &mut Context<'_>) -> Result<(), SendPollError<T>>;
    fn poll_send_unpin(&mut self, item: T, cx: &mut Context<'_>) -> Result<(), SendPollError<T>>;
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<(), SendCloseError>;
    fn poll_close_unpin(&mut self, cx: &mut Context<'_>) -> Result<(), SendCloseError>;
}

impl<T: Unpin + Send + 'static> SendPoll<T> for Sender<T> {
    fn poll_send(mut self: Pin<&mut Self>, item: T, cx: &mut Context<'_>) -> Result<(), SendPollError<T>> {
        match self.0.try_send(item) {
            Ok(()) => Ok(()),
            Err(e) => match e {
                mpsc::error::TrySendError::Full(x) => Err(SendPollError::Full(x)),
                mpsc::error::TrySendError::Closed(x) => Err(SendPollError::Closed(x)),
            },
        }
    }

    fn poll_send_unpin(&mut self, item: T, cx: &mut Context<'_>) -> Result<(), SendPollError<T>> {
        Pin::new(self).poll_send(item, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<(), SendCloseError> {
        Ok(())
    }

    fn poll_close_unpin(&mut self, cx: &mut Context<'_>) -> Result<(), SendCloseError> {
        Pin::new(self).poll_close(cx)
    }
}

#[derive(Debug)]
pub struct RecvError {}

impl fmt::Display for RecvError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

pub struct Receiving<'a, T> {
    fut: Box<dyn Future<Output = Option<T>>>,
}

impl<'a, T> Future for Receiving<'a, T>
where
    T: Unpin + Send + 'static,
{
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use Poll::*;
        match self.rx.poll_next_unpin(cx) {
            Ready(Some(item)) => Ready(Ok(item)),
            Ready(None) => Ready(Err(RecvError {})),
            Pending => Pending,
        }
    }
}

impl<T> Receiver<T>
where
    T: Unpin + Send + 'static,
{
    pub fn recv(&mut self) -> Receiving<'_, T> {
        // TODO write this in different way, can not clone the receiver.
        Receiving { fut: Bx::new(TODO) }
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
