use crate::log;
use async_channel::Receiver;
use async_channel::Sender;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::FromLogItem;
use items_0::streamitem::LogItem;
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(PushError, "PushError"),
    enum variants {
        Full,
    },
);

pub struct RequestLogItemBuffer {
    buf: VecDeque<LogItem>,
}

impl RequestLogItemBuffer {
    pub fn new() -> Self {
        Self { buf: VecDeque::new() }
    }

    pub fn push_back(&mut self, item: LogItem) -> Result<(), PushError> {
        self.buf.push_back(item);
        Ok(())
    }
}

pub struct LogQueueFutureWrap<F> {
    #[allow(unused)]
    reqid: String,
    fut: Pin<Box<F>>,
    logbuf: RequestLogItemBuffer,
}

impl<F> LogQueueFutureWrap<F> {
    pub fn new(reqid: String, fut: F) -> Self {
        let fut = Box::pin(fut);
        let logbuf = RequestLogItemBuffer::new();
        Self { reqid, fut, logbuf }
    }
}

impl<F> Future for LogQueueFutureWrap<F>
where
    F: Future,
{
    type Output = <F as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let self2 = self.as_mut().get_mut();
        LOG_QUEUE_3
            .try_with(|x| {
                let mut h = x.try_borrow_mut().unwrap();
                h.logbuf = &mut self2.logbuf as *mut _;
                h.push_v0 = log_push_v0;
            })
            .unwrap();
        let ret = self2.fut.poll_unpin(cx);
        LOG_QUEUE_3
            .try_with(|x| {
                let mut h = x.try_borrow_mut().unwrap();
                h.push_v0 = log_push_noop;
            })
            .unwrap();
        ret
    }
}

thread_local! {
    static LOG_QUEUE_0: RefCell<VecDeque<u32>>  = const { RefCell::new(VecDeque::new()) };
    static LOG_QUEUE_1: *mut VecDeque<u32>  = const { std::ptr::null_mut() };
    static LOG_QUEUE_2: LogQueueFnPtrs  = const { LogQueueFnPtrs::default() };
    static LOG_QUEUE_3: RefCell<LogQueueFnPtrs>  = const { RefCell::new(LogQueueFnPtrs::default()) };
    static LOG_QUEUE_4: RefCell<LogQueueFnPtrsB>  = const { RefCell::new(LogQueueFnPtrsB::default()) };
}

pub struct LogQueueFnPtrs {
    logbuf: *mut RequestLogItemBuffer,
    push_v0: fn(*mut RequestLogItemBuffer, LogItem) -> Result<(), PushError>,
}

impl LogQueueFnPtrs {
    const fn default() -> Self {
        Self {
            logbuf: std::ptr::null_mut(),
            push_v0: log_push_noop,
        }
    }
}

pub struct LogQueueFnPtrsB {
    log_tx: *const LogItemMuxTx,
    push_v0: fn(*const LogItemMuxTx, LogItem) -> Result<(), PushError>,
}

impl LogQueueFnPtrsB {
    const fn default() -> Self {
        Self {
            log_tx: std::ptr::null_mut(),
            push_v0: log_tx_send_noop,
        }
    }
}

#[allow(unused)]
fn push_log_item_queue_3(item: LogItem) -> Result<(), PushError> {
    LOG_QUEUE_3
        .try_with(|x| {
            let h = x.try_borrow().unwrap();
            (h.push_v0)(h.logbuf, item)
        })
        .unwrap()
}

fn push_log_item_queue_4(item: LogItem) -> () {
    // TODO dismiss messages in worst case and raise a flag instead.
    LOG_QUEUE_4
        .try_with(|x| {
            let h = x.try_borrow().unwrap();
            (h.push_v0)(h.log_tx, item)
        })
        .unwrap()
        .unwrap()
}

pub fn push_log_item(item: LogItem) -> () {
    push_log_item_queue_4(item)
}

fn log_push_noop(_: *mut RequestLogItemBuffer, _: LogItem) -> Result<(), PushError> {
    Ok(())
}

fn log_push_v0(logbuf: *mut RequestLogItemBuffer, item: LogItem) -> Result<(), PushError> {
    unsafe { &mut *logbuf }.push_back(item)
}

fn log_tx_send_noop(_: *const LogItemMuxTx, _: LogItem) -> Result<(), PushError> {
    Ok(())
}

fn log_tx_send_real(log_tx: *const LogItemMuxTx, item: LogItem) -> Result<(), PushError> {
    if let Some(tx) = unsafe { &*log_tx }.log_tx1.as_ref() {
        match tx.try_send(item) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("--------------------   log_tx_send_real  Err {e}");
            }
        }
    } else {
        eprintln!("--------------------   log_tx_send_real  NO TX");
    }
    Ok(())
}

struct LogItemMuxTx {
    #[allow(unused)]
    reqid: String,
    log_tx1: Option<Pin<Box<Sender<LogItem>>>>,
    #[allow(unused)]
    log_tx2: Option<kanal::AsyncSender<LogItem>>,
}

pub struct LogItemMux<S> {
    tx: UnsafeCell<LogItemMuxTx>,
    inp: Option<S>,
    log_rx1: Option<Pin<Box<Receiver<LogItem>>>>,
    #[allow(unused)]
    log_rx2: Option<kanal::AsyncReceiver<LogItem>>,
}

impl<S> LogItemMux<S> {
    pub fn new(inp: S, reqid: String) -> Self
    where
        S: Stream + Unpin,
        <S as Stream>::Item: FromLogItem,
    {
        let (log_tx1, log_rx1) = async_channel::bounded(4000);
        let (log_tx2, log_rx2) = kanal::bounded_async(4000);
        Self {
            tx: UnsafeCell::new(LogItemMuxTx {
                reqid,
                log_tx1: Some(Box::pin(log_tx1)),
                log_tx2: Some(log_tx2),
            }),
            inp: Some(inp),
            log_rx1: Some(Box::pin(log_rx1)),
            log_rx2: Some(log_rx2),
        }
    }
}

impl<S> Stream for LogItemMux<S>
where
    S: Stream + Unpin,
    <S as Stream>::Item: FromLogItem,
{
    type Item = <S as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let self2 = self.as_mut().get_mut();
        LOG_QUEUE_4
            .try_with(|x| {
                let mut h = x.try_borrow_mut().unwrap();
                h.log_tx = self2.tx.get_mut();
                h.push_v0 = log_tx_send_real;
            })
            .unwrap();
        let ret = loop {
            let mut have_progress = false;
            let mut have_pending = false;
            if let Some(s) = self2.log_rx1.as_mut() {
                match s.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        if true {
                            let item = <S as Stream>::Item::from_log_item(x);
                            break Ready(Some(item));
                        } else {
                            log::trace!("LogItemMux  log_rx1  got {x:?}");
                            have_progress = true;
                        }
                    }
                    Ready(None) => {
                        log::trace!("LogItemMux  log_rx1  got None");
                        self2.log_rx1 = None;
                        have_progress = true;
                    }
                    Pending => {
                        have_pending = true;
                    }
                }
            }
            if let Some(s) = self2.inp.as_mut() {
                match s.poll_next_unpin(cx) {
                    Ready(Some(x)) => break Ready(Some(x)),
                    Ready(None) => {
                        self2.inp = None;
                        if let Some(rx) = self2.log_rx1.as_ref() {
                            log::trace!("LogItemMux  closing log_rx1");
                            rx.close();
                        }
                        have_progress = true;
                    }
                    Pending => {
                        have_pending = true;
                    }
                }
            }
            break if have_progress {
                continue;
            } else if have_pending {
                Pending
            } else if self2.inp.is_none() && self2.log_rx1.is_none() {
                log::trace!("LogItemMux  both inputs None");
                Ready(None)
            } else {
                log::error!("no progress no pending");
                panic!("no progress no pending")
            };
        };
        LOG_QUEUE_4
            .try_with(|x| {
                let mut h = x.try_borrow_mut().unwrap();
                h.push_v0 = log_tx_send_noop;
            })
            .unwrap();
        ret
    }
}
