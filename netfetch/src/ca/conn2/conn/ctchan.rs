use std::collections::VecDeque;
use std::fmt;
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
}
