mod channelhandler;

use crate::ca::conn2::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::conn::channelheap::channelhandler::ChannelHandler;
use crate::ca::conn2::progpend::HaveProgressPending;
use crate::ca::conn2::synchan;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto::CaMsg;
use futures_util::FutureExt;
use hashbrown::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHeap"),
    enum variants {
        ChannelHandler(#[from] channelhandler::Error),
        CaInput(#[from] synchan::RecvError),
        Msg(String),
    },
);

#[derive(Debug)]
struct ChannelEntry {
    handler: ChannelHandler,
    tx: synchan::Sender<CaMsg>,
    waker: task::Waker,
}

#[derive(Debug)]
enum State {
    Running,
    Done,
}

mod waker1 {
    use crate::ca::conn2::caids::Cid;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering::AcqRel;
    use std::task;

    struct WakeData {
        cid: Cid,
        cnt: AtomicU32,
        wk1: task::Waker,
        wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>,
    }

    impl WakeData {
        fn cid(&self) -> Cid {
            self.cid.clone()
        }
    }

    fn clone(d: *const ()) -> task::RawWaker {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace!("waker1:clone  cid {}", data.cid());
        let rw = {
            let data = data.clone();
            data.cnt.fetch_add(1, AcqRel);
            let data = Arc::into_raw(data) as *const ();
            task::RawWaker::new(data, &VTABLE)
        };
        let _ = Arc::into_raw(data);
        rw
    }

    fn wake(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace!("waker1:wake  cid {}", data.cid());
        data.wakeup_cids.insert(data.cid(), ());
        data.wk1.wake_by_ref();
        let _ = Arc::into_raw(data);
    }

    fn wake_by_ref(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace!("waker1:wake_by_ref  cid {}", data.cid());
        data.wakeup_cids.insert(data.cid(), ());
        data.wk1.wake_by_ref();
        let _ = Arc::into_raw(data);
    }

    fn drop(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace!("waker1:drop  cid {}", data.cid());
        data.cnt.fetch_sub(1, AcqRel);
        std::mem::drop(data);
    }

    pub fn waker(cid: Cid, wk1: task::Waker, wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>) -> task::Waker {
        let data = WakeData {
            cid,
            cnt: AtomicU32::new(1),
            wk1,
            wakeup_cids,
        };
        let data = Arc::new(data);
        let data = Arc::into_raw(data) as *const ();
        let rw = task::RawWaker::new(data, &VTABLE);
        unsafe { task::Waker::from_raw(rw) }
    }

    const VTABLE: task::RawWakerVTable = task::RawWakerVTable::new(clone, wake, wake_by_ref, drop);
}

#[derive(Debug)]
pub struct ChannelHeap {
    state: State,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_rx: synchan::Receiver<CaMsg>,
    by_cid: HashMap<Cid, ChannelEntry>,
    inp_buf: VecDeque<CaMsg>,
    wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>,
}

impl ChannelHeap {
    pub fn new(proto_tx: asynchan::Sender<CaMsg>, proto_rx: synchan::Receiver<CaMsg>) -> Self {
        Self {
            state: State::Running,
            proto_tx,
            proto_rx,
            by_cid: HashMap::new(),
            inp_buf: VecDeque::with_capacity(8),
            wakeup_cids: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub fn channel_add(&mut self, conf: ChannelConfig, cx: &mut Context) {
        trace!("channel_add {conf:?}");
        let (tx, rx) = synchan::bounded(12, "ChannelHeap-channeladd");
        let mut handler = ChannelHandler::new(conf, self.proto_tx.clone(), rx);
        let cid = handler.cid();
        if self.by_cid.contains_key(&cid) {
            error!("ChannelHeap::channel_add: channel with cid {cid:?} already in map");
            return;
        }
        let waker = waker1::waker(cid.clone(), cx.waker().clone(), self.wakeup_cids.clone());
        let mut cx2 = task::Context::from_waker(&waker);
        loop {
            use Poll::*;
            break match Pin::new(&mut handler).poll_unpin(&mut cx2) {
                Ready(x) => {
                    trace!("ChannelHeap:channel_add:Poll:Done");
                    match x {
                        Ok(()) => {
                            continue;
                        }
                        Err(e) => {
                            trace!("ChannelHeap:channel_add:Poll:Err {e}");
                            // TODO
                        }
                    }
                }
                Pending => {}
            };
        }
        let e = ChannelEntry { handler, tx, waker };
        self.by_cid.insert(cid, e);
    }
}

impl Future for ChannelHeap {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use Poll::*;
        'main: loop {
            let mut hpp = HaveProgressPending::new();
            match &self.state {
                State::Running => {
                    if self.inp_buf.len() < self.inp_buf.capacity() {
                        match self.proto_rx.poll_unpin(cx) {
                            Ready(Ok(item)) => {
                                trace!("ChannelHeap:CaInp:Rx:Ok");
                                hpp.mark_progress();
                                self.inp_buf.push_back(item);
                            }
                            Ready(Err(e)) => {
                                trace!("ChannelHeap:CaInp:Rx:Err");
                                hpp.mark_progress();
                                self.state = State::Done;
                                break Ready(Err(e.into()));
                            }
                            Pending => {
                                trace!("ChannelHeap:CaInp:Rx:Pending");
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        // Maybe nothing to do here?
                    }
                    if self.inp_buf.len() != 0 {
                        // Distribute input to channels.
                        while let Some(item) = self.inp_buf.pop_front() {
                            // Some message can be dispatched by Cid.
                            // Others need translation from Subid.
                            if let Some(cid) = item.cid() {
                                let cid = Cid::new(cid);
                                if let Some(e) = self.by_cid.get_mut(&cid) {
                                    match e.tx.try_send(item, cx) {
                                        Ok(()) => {
                                            trace!("ChannelHeap:Dispatch:Sent {cid}");
                                            hpp.mark_progress();
                                        }
                                        Err(synchan::SendError::Full(item)) => {
                                            trace!("ChannelHeap:Dispatch:Pending {cid}");
                                            self.inp_buf.push_front(item);
                                            hpp.mark_pending();
                                        }
                                        Err(synchan::SendError::Closed(item)) => {
                                            trace!("ChannelHeap:Dispatch:Closed {cid}");
                                            self.inp_buf.push_front(item);
                                            hpp.mark_progress();
                                            self.state = State::Done;
                                            warn!("TODO handle closed channel handler gracefully");
                                            let msg = format!("ChannelHeap: channel handler for cid {cid} closed");
                                            break 'main Ready(Err(Error::Msg(msg)));
                                        }
                                    }
                                } else {
                                    hpp.mark_progress();
                                    warn!("ChannelHeap: no channel handler for cid {cid}");
                                }
                            } else {
                                hpp.mark_progress();
                                warn!("ChannelHeap: message without cid: {item:?}");
                                warn!("TODO check for subid and ioid");
                            }
                        }
                    }
                    // TODO also wake up those for which we just discovered input.
                    let self2 = self.as_mut().get_mut();
                    for e in self2.wakeup_cids.iter() {
                        let cid = e.key();
                        if let Some(h) = self2.by_cid.get_mut(cid) {
                            let cx2 = &mut Context::from_waker(&h.waker);
                            match h.handler.poll_unpin(cx2) {
                                Ready(Ok(())) => {
                                    hpp.mark_progress();
                                    trace!("ChannelHeap:Handler:Done {cid}");
                                }
                                Ready(Err(e)) => {
                                    hpp.mark_progress();
                                    self2.state = State::Done;
                                    warn!("TODO handle closed channel handler gracefully {cid}");
                                    let msg = format!("ChannelHeap:Handler error {cid} {e}");
                                    break 'main Ready(Err(Error::Msg(msg)));
                                }
                                Pending => {
                                    hpp.mark_pending();
                                    trace!("ChannelHeap:Handler:Pending {cid}");
                                }
                            }
                        } else {
                            warn!("ChannelHeap: no channel handler for wakeup cid {cid}");
                            hpp.mark_progress();
                        }
                    }
                    self2.wakeup_cids.clear();
                }
                State::Done => {}
            }
            break if hpp.have_progress() {
                trace!("HPP:Progress");
                continue;
            } else if hpp.have_pending() {
                trace!("HPP:Pending");
                Pending
            } else {
                trace!("HPP:Done");
                Ready(Ok(()))
            };
        }
    }
}
