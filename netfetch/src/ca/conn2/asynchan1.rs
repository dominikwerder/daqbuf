use futures::Stream;
use futures::StreamExt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct Sender<T>(crossfire::MAsyncTx<T>, (), String);

pub struct Receiver<T>(crossfire::MAsyncRx<T>, crossfire::stream::AsyncStream<T>, String);

pub fn bounded<T: Unpin + Send + 'static, S: Into<String>>(n: usize, tag: S) -> (Sender<T>, Receiver<T>) {
    let tag = tag.into();
    let (tx, rx) = crossfire::mpmc::bounded_async(n);
    (
        Sender(tx.clone(), (), tag.clone()),
        Receiver(rx.clone(), rx.into_stream(), tag),
    )
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Sender").field("tag", &self.2).finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Receiver").field("tag", &self.2).finish()
    }
}

impl<T> Clone for Sender<T>
where
    T: Unpin + Send + 'static,
{
    fn clone(&self) -> Self {
        // let sink = self.0.clone().into_sink();
        let sink = ();
        Self(self.0.clone(), sink, self.2.clone())
    }
}

impl<T> Clone for Receiver<T>
where
    T: Unpin + Send + 'static,
{
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.0.clone().into_stream(), self.2.clone())
    }
}

impl<T> Stream for Receiver<T>
where
    T: Unpin + Send + 'static,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.1.poll_next_unpin(cx)
    }
}

pub struct SendError<T>(T);

pub struct Sending<'a, T> {
    // tx: &'a mut crossfire::sink::AsyncSink<T>,
    tx: crossfire::sink::AsyncSink<T>,
    item: Option<T>,
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
    pub fn send(&mut self, item: T) -> Sending<'_, T> {
        Sending {
            // tx: &mut self.1,
            tx: self.0.clone().into_sink(),
            item: Some(item),
            _p1: std::marker::PhantomData,
        }
    }

    pub fn try_send(&mut self, item: T) -> Result<(), SendError<T>> {
        match self.0.clone().into_blocking().try_send(item) {
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
    fn poll_send(self: Pin<&mut Self>, item: T, cx: &mut Context<'_>) -> Result<(), SendPollError<T>> {
        use crossfire::TrySendError;
        let mut sink = self.0.clone().into_sink();
        // let sink = self.1;
        match sink.poll_send(cx, item) {
            Ok(()) => Ok(()),
            Err(e) => match e {
                TrySendError::Full(item) => Err(SendPollError::Full(item)),
                TrySendError::Disconnected(item) => Err(SendPollError::Closed(item)),
            },
        }
    }

    fn poll_send_unpin(&mut self, item: T, cx: &mut Context<'_>) -> Result<(), SendPollError<T>> {
        Pin::new(self).poll_send(item, cx)
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Result<(), SendCloseError> {
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
    rx: &'a mut crossfire::stream::AsyncStream<T>,
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
        Receiving { rx: &mut self.1 }
    }
}

#[allow(unused)]
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
