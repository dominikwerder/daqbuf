use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

pub enum SendError<T> {
    Full(T),
    Closed(T),
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Full(_) => fmt.debug_tuple("Full").finish(),
            SendError::Closed(_) => fmt.debug_tuple("Closed").finish(),
        }
    }
}

impl<T> SendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            SendError::Full(x) => x,
            SendError::Closed(x) => x,
        }
    }
}

pub struct CtChanRc<T> {
    inner: Arc<RwLock<CtChan<T>>>,
}

impl<T> fmt::Debug for CtChanRc<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner.try_read().unwrap(), fmt)
    }
}

impl<T> Clone for CtChanRc<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> CtChanRc<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(CtChan::new())),
        }
    }

    pub fn poll_send_unpin(&self, item: T, cx: &mut Context) -> Result<(), SendError<T>> {
        self.inner.try_write().unwrap().poll_send_unpin(item, cx)
    }

    pub fn poll_next_unpin(&self, cx: &mut Context) -> Poll<Option<T>> {
        self.inner.try_write().unwrap().poll_next_unpin(cx)
    }

    pub fn inner(&self) -> &RwLock<CtChan<T>> {
        &self.inner
    }
}

pub struct CtChan<T> {
    buf: VecDeque<T>,
    waker_tx: Option<Waker>,
    waker_rx: Option<Waker>,
    closed: bool,
}

impl<T> fmt::Debug for CtChan<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("CtChan").field("len", &self.buf.len()).finish()
    }
}

impl<T> CtChan<T> {
    pub fn new() -> Self {
        Self {
            buf: VecDeque::with_capacity(16),
            waker_tx: None,
            waker_rx: None,
            closed: false,
        }
    }

    pub fn has_space(&self) -> bool {
        self.closed == false && self.buf.len() < self.buf.capacity()
    }

    pub fn poll_send_unpin(&mut self, item: T, cx: &mut Context) -> Result<(), SendError<T>> {
        if self.closed {
            Err(SendError::Closed(item))
        } else {
            let n = self.buf.len();
            if n < self.buf.capacity() {
                if n == 0 {
                    if let Some(w) = self.waker_rx.take() {
                        w.wake();
                    }
                }
                self.buf.push_back(item);
                if self.buf.len() >= self.buf.capacity() {
                    self.waker_tx = Some(cx.waker().clone());
                }
                Ok(())
            } else {
                Err(SendError::Full(item))
            }
        }
    }

    pub fn poll_next_unpin(&mut self, cx: &mut Context) -> Poll<Option<T>> {
        use Poll::*;
        let n = self.buf.len();
        if n <= 1 {
            self.waker_rx = Some(cx.waker().clone());
        }
        if let Some(x) = self.buf.pop_front() {
            if n >= self.buf.capacity() {
                if let Some(w) = self.waker_tx.take() {
                    w.wake();
                }
            }
            Ready(Some(x))
        } else {
            if self.closed { Ready(None) } else { Pending }
        }
    }

    pub fn send(&mut self, item: T) -> CtChanSending<'_, T> {
        CtChanSending {
            chan: self,
            item: Some(item),
        }
    }
}

pub struct CtChanSending<'a, T> {
    chan: &'a mut CtChan<T>,
    item: Option<T>,
}

impl<'a, T> fmt::Debug for CtChanSending<'a, T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("CtChanSending").field("chan", &self.chan).finish()
    }
}

impl<'a, T> Future for CtChanSending<'a, T>
where
    T: Unpin,
{
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(item) = this.item.take() {
            match this.chan.poll_send_unpin(item, cx) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(e) => {
                    this.item = Some(e.into_inner());
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
