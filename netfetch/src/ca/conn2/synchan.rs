use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

// Want to pass a Receiver for proto message specific to each Cid/Sid.
// Therefore, CaConn needs to hold a registry of all Sender by Cid/Sid.
// Everything should be non-Scync.

// Allow some buffering to improve flow.

pub fn bounded<T>(n: u32) -> (Sender<T>, Receiver<T>)
where
    T: Unpin + std::marker::Send,
{
    let shr = Shared {
        qu: VecDeque::new(),
        rx_waker: None,
        tx_waker: None,
        qu_max: n,
    };
    let shr = Arc::new(RwLock::new(shr));
    let tx = Sender { shr: shr.clone() };
    let rx = Receiver { shr: shr.clone() };
    (tx, rx)
}

#[derive(Debug)]
pub struct Sender<T>
where
    T: std::marker::Send,
{
    shr: Arc<RwLock<Shared<T>>>,
}

impl<T> Sender<T>
where
    T: std::marker::Send,
{
    pub fn send(&mut self, item: T) -> Sending<'_, T> {
        Sending {
            tx: self,
            item: Some(item),
        }
    }

    pub fn try_send(&mut self, item: T, cx: &mut Context) -> Result<(), SendError<T>> {
        let mut shr = self.shr.try_write().unwrap();
        if shr.qu_max == 0 {
            Err(SendError::Closed(item))
        } else {
            let ql = shr.qu.len();
            if ql < shr.qu_max as _ {
                shr.qu.push_back(item);
                if ql == 0 {
                    if let Some(waker) = shr.rx_waker.take() {
                        trace!("Send:Push:RxWake");
                        waker.wake();
                        Ok(())
                    } else {
                        trace!("Send:Push:Quiet");
                        // nothing to do here
                        Ok(())
                    }
                } else {
                    trace!("Send:Push:More");
                    // nothing to do here
                    Ok(())
                }
            } else {
                trace!("Send:Full");
                shr.tx_waker = Some(cx.waker().clone());
                Err(SendError::Full(item))
            }
        }
    }

    pub fn set_waker(&mut self, waker: &Waker) {
        let mut shr = self.shr.try_write().unwrap();
        if shr.tx_waker.is_none() {
            shr.tx_waker = Some(waker.clone());
        }
    }
}

#[derive(Debug)]
pub struct Sending<'a, T>
where
    T: std::marker::Send,
{
    tx: &'a mut Sender<T>,
    item: Option<T>,
}

impl<'a, T> Future for Sending<'a, T>
where
    T: Unpin + std::marker::Send,
{
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        trace!("Sending:poll_next");
        let self2 = self.get_mut();
        let mut shr = self2.tx.shr.try_write().unwrap();
        if shr.qu_max == 0 {
            if let Some(item) = self2.item.take() {
                trace!("Send:Closed:Item");
                Ready(Err(SendError::Closed(item)))
            } else {
                trace!("Send:Closed:Nothing");
                panic!("logic")
            }
        } else {
            let ql = shr.qu.len();
            if ql < shr.qu_max as _ {
                match self2.item.take() {
                    Some(item) => {
                        shr.qu.push_back(item);
                        if ql == 0 {
                            if let Some(waker) = shr.rx_waker.take() {
                                trace!("Send:Push:RxWake");
                                waker.wake();
                            } else {
                                trace!("Send:Push:Quiet");
                                // nothing to do here
                            }
                        } else {
                            trace!("Send:Push:More");
                            // nothing to do here
                        }
                        Ready(Ok(()))
                    }
                    None => {
                        trace!("Send:Push:Nothing");
                        panic!("logic")
                    }
                }
            } else {
                trace!("Send:Full");
                shr.tx_waker = Some(cx.waker().clone());
                Pending
            }
        }
    }
}

pub enum SendError<T> {
    Full(T),
    Closed(T),
}

impl<T> SendError<T> {
    pub fn is_closed(&self) -> bool {
        match self {
            SendError::Full(_) => false,
            SendError::Closed(_) => true,
        }
    }

    pub fn into_inner(self) -> T {
        match self {
            SendError::Full(x) => x,
            SendError::Closed(x) => x,
        }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Full(_) => write!(fmt, "SendError::Full"),
            SendError::Closed(_) => write!(fmt, "SendError::Closed"),
        }
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    shr: Arc<RwLock<Shared<T>>>,
}

impl<T> Receiver<T> {
    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv { rx: self }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        trace!("Receiver:poll");
        let mut shr = self.shr.try_write().unwrap();
        if let Some(x) = shr.qu.pop_front() {
            trace!("Receiver:PopQueue");
            Ready(Ok(x))
        } else {
            shr.rx_waker = Some(cx.waker().clone());
            if let Some(x) = shr.tx_waker.take() {
                trace!("Receiver:Pending:TxWake");
                x.wake();
                Pending
            } else {
                trace!("Receiver:Pending:Idle");
                // Sender is not waiting.
                Pending
            }
        }
    }
}

pub struct Recv<'a, T> {
    rx: &'a mut Receiver<T>,
}

impl<'a, T> Future for Recv<'a, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        trace!("Recv:poll");
        let mut shr = self.rx.shr.try_write().unwrap();
        if let Some(x) = shr.qu.pop_front() {
            trace!("Recv:PopQueue");
            Ready(Ok(x))
        } else {
            shr.rx_waker = Some(cx.waker().clone());
            if let Some(x) = shr.tx_waker.take() {
                trace!("Recv:Pending:TxWake");
                x.wake();
                Pending
            } else {
                trace!("Recv:Pending:Idle");
                // Sender is not waiting.
                Pending
            }
        }
    }
}

#[derive(Debug)]
pub enum RecvError {
    Closed,
}

#[derive(Debug)]
struct Shared<T> {
    qu: VecDeque<T>,
    rx_waker: Option<Waker>,
    tx_waker: Option<Waker>,
    qu_max: u32,
}
