mod channelhandler;

use crate::ca::conn2::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::caids::Subid;
use crate::ca::conn2::conn::channelheap::channelhandler::ChannelHandler;
use crate::ca::conn2::progpend::HaveProgressPending;
use crate::ca::conn2::synchan;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto::CaMsg;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

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
        trace!("waker1:clone  {}", data.cid());
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
        trace!("waker1:wake  {}", data.cid());
        data.wakeup_cids.insert(data.cid(), ());
        data.wk1.wake_by_ref();
        let _ = Arc::into_raw(data);
    }

    fn wake_by_ref(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace!("waker1:wake_by_ref  {}", data.cid());
        data.wakeup_cids.insert(data.cid(), ());
        data.wk1.wake_by_ref();
        let _ = Arc::into_raw(data);
    }

    fn drop(d: *const ()) {
        let data = unsafe { Arc::<WakeData>::from_raw(d as _) };
        trace!("waker1:drop  {}", data.cid());
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
pub enum ItemInner {
    ScyllaWrite,
}

#[derive(Debug)]
pub struct ChannelHeapItem {
    // Only for performance measurement:
    ts_create: Instant,
    inner: ItemInner,
}

#[derive(Debug)]
pub enum ChHeapCmd {
    RegisterSubid(Cid, Subid, asynchan::Sender<u32>),
}

#[derive(Debug)]
pub struct ChannelHeap {
    state: State,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_rx: asynchan::Receiver<CaMsg>,
    by_cid: HashMap<Cid, ChannelEntry>,
    by_subid: HashMap<Subid, Cid>,
    inp_buf: VecDeque<CaMsg>,
    wakeup_cids: Arc<dashmap::DashMap<Cid, ()>>,
    ch_hp_tx: asynchan::Sender<ChHeapCmd>,
    ch_hp_rx: asynchan::Receiver<ChHeapCmd>,
}

impl ChannelHeap {
    pub fn new(proto_tx: asynchan::Sender<CaMsg>, proto_rx: asynchan::Receiver<CaMsg>) -> Self {
        let (ch_hp_tx, ch_hp_rx) = asynchan::bounded(12);
        Self {
            state: State::Running,
            proto_tx,
            proto_rx,
            by_cid: HashMap::new(),
            by_subid: HashMap::new(),
            inp_buf: VecDeque::with_capacity(8),
            wakeup_cids: Arc::new(dashmap::DashMap::new()),
            ch_hp_tx,
            ch_hp_rx,
        }
    }

    pub fn channel_add(&mut self, conf: ChannelConfig, cx: &mut Context) {
        trace!("channel_add {conf:?}");
        let (tx, rx) = asynchan::bounded(12, "ChannelHeap-channeladd");
        let mut handler = ChannelHandler::new(conf, self.proto_tx.clone(), rx, self.ch_hp_tx.clone());
        let cid = handler.cid();
        if self.by_cid.contains_key(&cid) {
            error!("ChannelHeap::channel_add: channel with cid {cid:?} already in map");
            return;
        }
        let waker = waker1::waker(cid.clone(), cx.waker().clone(), self.wakeup_cids.clone());
        let mut cx2 = task::Context::from_waker(&waker);
        // TODO factor out polling into struct fn and call same from poll_next and channel_add.
        // That is, because also here so many possibilities can occur.
        loop {
            use Poll::*;
            break match Pin::new(&mut handler).poll_next_unpin(&mut cx2) {
                Ready(Some(x)) => {
                    trace!("ChannelHeap:channel_add:Poll:Done");
                    match x {
                        Ok(item) => {
                            trace!("ChannelHeap:channel_add:Poll:Ok");
                            error!(
                                "ChannelHeap:channel_add:Poll:Ok  TODO ChannelHeap:channel_add:Poll:Ok must here do something with item"
                            );
                            continue;
                        }
                        Err(e) => {
                            trace!("ChannelHeap:channel_add:Poll:Err {e}");
                            // TODO
                        }
                    }
                }
                Ready(None) => {
                    trace!("ChannelHeap:channel_add:Poll:Done");
                    error!("ChannelHeap:channel_add:Poll:Done  TODO must handle ChannelHandler finish");
                }
                Pending => {
                    trace!("ChannelHeap:channel_add:Poll:Pending");
                    error!("ChannelHeap:channel_add:Poll:Pending  TODO must mark Pending");
                }
            };
        }
        let e = ChannelEntry { handler, tx, waker };
        self.by_cid.insert(cid, e);
    }
}

impl Stream for ChannelHeap {
    type Item = Result<ChannelHeapItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'main: loop {
            let mut hpp = HaveProgressPending::new();
            match &self.state {
                State::Running => {
                    match self.ch_hp_rx.poll_next_unpin(cx) {
                        Ready(Some(cmd)) => {
                            trace!("ChannelHeap:ChHpCmd:Got");
                            hpp.mark_progress();
                            match cmd {
                                ChHeapCmd::RegisterSubid(cid, subid, mut resp_tx) => {
                                    trace!("ChannelHeap:ChHpCmd:RegisterSubid  {cid}  {subid}");
                                    let mut existed = false;
                                    self.by_subid
                                        .entry(subid.clone())
                                        .and_modify(|_| {
                                            existed = true;
                                        })
                                        .or_insert_with(|| cid.clone());
                                    if existed {
                                        warn!(
                                            "ChannelHeap:ChHpCmd:RegisterSubid:SubidExists  {subid}  TODO handle error"
                                        );
                                    } else {
                                        trace!("ChannelHeap:ChHpCmd:RegisterSubid:Ok  {subid}");
                                        match resp_tx.try_send(1) {
                                            Ok(()) => {}
                                            Err(_) => {
                                                // TODO should never happen
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ready(None) => {
                            trace!("ChannelHeap:ChHpCmd:Done");
                            hpp.mark_progress();
                            // TODO handle closed command channel
                        }
                        Pending => {
                            trace!("ChannelHeap:ChHpCmd:Pending");
                            hpp.mark_pending();
                        }
                    }
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
                                break Ready(Some(Err(e.into())));
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
                        let self2 = self.as_mut().get_mut();
                        while let Some(item) = self2.inp_buf.pop_front() {
                            // Some message can be dispatched by Cid.
                            // Others need translation from Subid.
                            if let Some(cid) = item.cid() {
                                let cid = Cid::new(cid);
                                if let Some(e) = self2.by_cid.get_mut(&cid) {
                                    match e.tx.try_send(item, cx) {
                                        Ok(()) => {
                                            trace!("ChannelHeap:Dispatch:Sent {cid}");
                                            hpp.mark_progress();
                                        }
                                        Err(synchan::SendError::Full(item)) => {
                                            trace!("ChannelHeap:Dispatch:Pending {cid}");
                                            self2.inp_buf.push_front(item);
                                            hpp.mark_pending();
                                        }
                                        Err(synchan::SendError::Closed(item)) => {
                                            trace!("ChannelHeap:Dispatch:Closed {cid}");
                                            self2.inp_buf.push_front(item);
                                            hpp.mark_progress();
                                            self2.state = State::Done;
                                            warn!("TODO handle closed channel handler gracefully");
                                            let msg = format!("ChannelHeap: channel handler for cid {cid} closed");
                                            break 'main Ready(Some(Err(Error::Msg(msg))));
                                        }
                                    }
                                } else {
                                    hpp.mark_progress();
                                    warn!("ChannelHeap: no channel handler for cid {cid}");
                                }
                            } else if let Some(subid) = item.subid() {
                                if let Some(cid) = self2.by_subid.get(&Subid::new(subid)) {
                                    if let Some(e) = self2.by_cid.get_mut(cid) {
                                        match e.tx.try_send(item, cx) {
                                            Ok(()) => {
                                                trace!("ChannelHeap:Dispatch:Sent {cid}");
                                                hpp.mark_progress();
                                            }
                                            Err(synchan::SendError::Full(item)) => {
                                                trace!("ChannelHeap:Dispatch:Pending {cid}");
                                                self2.inp_buf.push_front(item);
                                                hpp.mark_pending();
                                            }
                                            Err(synchan::SendError::Closed(item)) => {
                                                trace!("ChannelHeap:Dispatch:Closed {cid}");
                                                self2.inp_buf.push_front(item);
                                                hpp.mark_progress();
                                                self2.state = State::Done;
                                                warn!("TODO handle closed channel handler gracefully");
                                                let msg = format!("ChannelHeap: channel handler for cid closed");
                                                break 'main Ready(Some(Err(Error::Msg(msg))));
                                            }
                                        }
                                    } else {
                                        hpp.mark_progress();
                                        warn!("ChannelHeap: no channel handler for cid");
                                    }
                                } else {
                                    warn!("ChannelHeap: no cid found for subid");
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
                        trace!("ChannelHeap: waking cid {}", e.key());
                    }
                    for e in self2.wakeup_cids.iter() {
                        let cid = e.key();
                        // trace!("ChannelHeap: waking cid {}", e.key());
                        if let Some(h) = self2.by_cid.get_mut(cid) {
                            let cx2 = &mut Context::from_waker(&h.waker);
                            match h.handler.poll_next_unpin(cx2) {
                                Ready(Some(x)) => match x {
                                    Ok(item) => {
                                        trace!("ChannelHeap:Handler:Some {cid}");
                                        let item = match item.inner {
                                            channelhandler::ItemInner::ScyllaWrite => ChannelHeapItem {
                                                ts_create: item.ts_create,
                                                inner: ItemInner::ScyllaWrite,
                                            },
                                        };
                                        hpp.mark_progress();
                                        break 'main Ready(Some(Ok(item)));
                                    }
                                    Err(e) => {
                                        hpp.mark_progress();
                                        // TODO must not go into error state.
                                        // TODO clean up this channel handler.
                                        // TODO report upstream about failed channel.
                                        // TODO upstream should try to remove and re-add the channel a few times.
                                        // TODO if that does not work, remove the connection, invalidate
                                        // all those channels, and try again all channels.
                                        self2.state = State::Done;
                                        warn!(
                                            "ChannelHeap:Handler:Done  TODO handle closed channel handler gracefully {cid}"
                                        );
                                        let msg = format!("ChannelHeap:Handler error {cid} {e}");
                                        break 'main Ready(Some(Err(Error::Msg(msg))));
                                    }
                                },
                                Ready(None) => {
                                    trace!("ChannelHeap:Handler:Finished {cid}");
                                    error!("TODO handle finished channel handler gracefully {cid}");
                                    hpp.mark_progress();
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
                Ready(None)
            };
        }
    }
}
