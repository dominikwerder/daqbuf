use hashbrown::HashMap;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use stats::rand_xoshiro::rand_core::SeedableRng;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

static ID_REG: LazyLock<Mutex<(HashMap<u32, u32>, Xoshiro128PlusPlus, u32)>> =
    LazyLock::new(|| Mutex::new((HashMap::new(), Xoshiro128PlusPlus::from_os_rng(), 0)));

fn gen_next(reg: &LazyLock<Mutex<(HashMap<u32, u32>, Xoshiro128PlusPlus, u32)>>) -> (u32, u32) {
    let mut g = reg.lock().unwrap();
    loop {
        use stats::rand_xoshiro::rand_core::RngCore;
        let k = g.1.next_u32() & 0x7fffffff;
        break if g.0.try_insert(k, k).is_err() {
            continue;
        } else {
            g.2 += 1;
            (k, g.2)
        };
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdOwned(u32);

impl IdOwned {
    pub fn new() -> Self {
        let (_a, b) = gen_next(&ID_REG);
        Self(b)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

// Want to pass a Receiver for proto message specific to each Cid/Sid.
// Therefore, CaConn needs to hold a registry of all Sender by Cid/Sid.
// Everything should be non-Scync.

// Allow some buffering to improve flow.

pub fn bounded<T, S: Into<String>>(n: u32, tag: S) -> (Sender<T>, Receiver<T>)
where
    T: Unpin + std::marker::Send,
{
    let shr = Shared {
        qu: VecDeque::new(),
        rx_waker: None,
        tx_waker: None,
        qu_max: n,
        id: IdOwned::new(),
        tag: tag.into(),
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
        panic!("unused");
        Sending {
            tx: self,
            item: Some(item),
        }
    }

    pub fn try_send(&mut self, item: T, cx: &mut Context) -> Result<(), SendError<T>> {
        let mut shr = self.shr.try_write().unwrap();
        let id = shr.id.to_u32();
        if shr.qu_max == 0 {
            Err(SendError::Closed(item))
        } else {
            let ql = shr.qu.len();
            if ql < shr.qu_max as _ {
                shr.qu.push_back(item);
                if ql == 0 {
                    if let Some(waker) = shr.rx_waker.take() {
                        trace!("Send:Push:RxWake  shr-id {} {}", id, shr.tag);
                        waker.wake();
                        Ok(())
                    } else {
                        trace!("Send:Push:Quiet  shr-id {} {}", id, shr.tag);
                        // nothing to do here
                        Ok(())
                    }
                } else {
                    trace!(
                        "Send:Push:More  shr-id {} {}  {ql} {ql2}",
                        id,
                        shr.tag,
                        ql2 = shr.qu.len()
                    );
                    // nothing to do here
                    Ok(())
                }
            } else {
                trace!("Send:Full  shr-id {} {}", id, shr.tag);
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
        let self2 = self.get_mut();
        let mut shr = self2.tx.shr.try_write().unwrap();
        let id = shr.id.to_u32();
        trace!("Sending:poll_next  shr-id {} {}", id, shr.tag);
        if shr.qu_max == 0 {
            if let Some(item) = self2.item.take() {
                trace!("Send:Closed:Item  shr-id {} {}", id, shr.tag);
                Ready(Err(SendError::Closed(item)))
            } else {
                trace!("Send:Closed:Nothing  shr-id {} {}", id, shr.tag);
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
                                trace!("Send:Push:RxWake  shr-id {} {}", id, shr.tag);
                                waker.wake();
                            } else {
                                trace!("Send:Push:Quiet  shr-id {} {}", id, shr.tag);
                                // nothing to do here
                            }
                        } else {
                            trace!("Send:Push:More  shr-id {} {}", id, shr.tag);
                            // nothing to do here
                        }
                        Ready(Ok(()))
                    }
                    None => {
                        trace!("Send:Push:Nothing  shr-id {} {}", id, shr.tag);
                        panic!("logic")
                    }
                }
            } else {
                trace!("Send:Full  shr-id {} {}", id, shr.tag);
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

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self { shr: self.shr.clone() }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        let mut shr = self.shr.try_write().unwrap();
        let id = shr.id.to_u32();
        trace!("Receiver:poll  shr-id {} {}", id, shr.tag);
        if let Some(x) = shr.qu.pop_front() {
            trace!("Receiver:PopQueue  shr-id {} {}", id, shr.tag);
            Ready(Ok(x))
        } else {
            shr.rx_waker = Some(cx.waker().clone());
            if let Some(x) = shr.tx_waker.take() {
                trace!("Receiver:Pending:TxWake  shr-id {} {}", id, shr.tag);
                x.wake();
                Pending
            } else {
                trace!("Receiver:Pending:Idle  shr-id {} {}", id, shr.tag);
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
        let mut shr = self.rx.shr.try_write().unwrap();
        let id = shr.id.to_u32();
        trace!("Recv:poll  shr-id {} {}", id, shr.tag);
        if let Some(x) = shr.qu.pop_front() {
            trace!("Recv:PopQueue  shr-id {} {}", id, shr.tag);
            Ready(Ok(x))
        } else {
            shr.rx_waker = Some(cx.waker().clone());
            if let Some(x) = shr.tx_waker.take() {
                trace!("Recv:Pending:TxWake  shr-id {} {}", id, shr.tag);
                x.wake();
                Pending
            } else {
                trace!("Recv:Pending:Idle  shr-id {} {}", id, shr.tag);
                Pending
            }
        }
    }
}

#[derive(Debug)]
pub enum RecvError {
    Closed,
}

impl fmt::Display for RecvError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

#[derive(Debug)]
struct Shared<T> {
    qu: VecDeque<T>,
    rx_waker: Option<Waker>,
    tx_waker: Option<Waker>,
    qu_max: u32,
    id: IdOwned,
    tag: String,
}
