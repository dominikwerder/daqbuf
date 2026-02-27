use crate::ca::conn2::asynchan;
use crate::ca::progpend::HaveProgressPending;
use crate::throttletrace::ThrottleTrace;
use async_channel::Receiver;
use ca_proto::ca::proto;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use libc::c_int;
use proto::CaMsg;
use proto::CaMsgTy;
use proto::HeadInfo;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::io::unix::AsyncFd;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "FindIoc"),
    enum variants {
        SocketCreate,
        SocketConvertTokio,
        BroadcastEnable,
        NonblockEnable,
        SocketBind,
        SendFailure,
        ReadFailure,
        ReadEmpty,
        Proto(#[from] proto::Error),
        Slidebuf(#[from] slidebuf::Error),
        IO(#[from] std::io::Error),
    },
);

struct SockBox(c_int);

impl Drop for SockBox {
    fn drop(self: &mut Self) {
        if self.0 != -1 {
            unsafe {
                libc::close(self.0);
                self.0 = -1;
            }
        }
    }
}

static BATCH_ID: AtomicUsize = AtomicUsize::new(0);
static SEARCH_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct BatchId(u32);

impl BatchId {
    fn next() -> Self {
        Self(BATCH_ID.fetch_add(1, Ordering::AcqRel) as u32)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct SearchId(u32);

impl SearchId {
    fn next() -> Self {
        Self(SEARCH_ID.fetch_add(1, Ordering::AcqRel) as u32)
    }
}

struct SearchBatch {
    ts_beg: Instant,
    tgts: VecDeque<usize>,
    channels: Vec<String>,
    sids: Vec<SearchId>,
    done: Vec<bool>,
    txs: Vec<Option<asynchan::Sender<FindIocRes>>>,
}

pub struct OptResTx(Option<Pin<Box<asynchan::Sender<FindIocRes>>>>);

impl fmt::Debug for OptResTx {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_tuple("OptResTx").field(&self.0.is_some()).finish()
    }
}

impl OptResTx {
    pub fn new_empty() -> Self {
        Self(None)
    }

    pub fn new_tx(tx: asynchan::Sender<FindIocRes>) -> Self {
        Self(Some(Box::pin(tx)))
    }

    pub fn takeit(&mut self) -> OptResTx {
        OptResTx(self.0.take())
    }

    pub fn into_inner(self) -> Option<Pin<Box<asynchan::Sender<FindIocRes>>>> {
        self.0
    }
}

#[derive(Debug)]
pub struct FindIocRes {
    channel: String,
    response_addr: Option<SocketAddrV4>,
    addr: Option<SocketAddrV4>,
    dt: Duration,
}

impl FindIocRes {
    pub fn from_results(
        channel: String,
        response_addr: Option<SocketAddrV4>,
        addr: Option<SocketAddrV4>,
        dt: Duration,
    ) -> Self {
        Self {
            channel,
            response_addr,
            addr,
            dt,
        }
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn addr(&self) -> Option<SocketAddrV4> {
        self.addr.clone()
    }

    pub fn response_addr(&self) -> Option<SocketAddrV4> {
        self.response_addr.clone()
    }

    pub fn dt(&self) -> Duration {
        self.dt.clone()
    }
}

fn _assert_traits() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    _assert_send::<FindIocRes>();
    _assert_send::<asynchan::Sender<u32>>();
    _assert_send::<asynchan::Sender<FindIocRes>>();
    // _assert_send::<asynchan::Sender<std::rc::Rc<u32>>>();
    // _assert_sync::<FindIocRes>();
}

pub struct FindIocStream {
    tgts: Vec<SocketAddrV4>,
    channels_input: Option<Pin<Box<Receiver<(String, asynchan::Sender<FindIocRes>)>>>>,
    in_flight: BTreeMap<BatchId, SearchBatch>,
    in_flight_max: usize,
    channels_per_batch: usize,
    batch_run_max: Duration,
    bid_by_sid: BTreeMap<SearchId, BatchId>,
    batch_send_queue: VecDeque<BatchId>,
    sock: SockBox,
    afd: AsyncFd<i32>,
    buf1: Vec<u8>,
    send_addr: SocketAddrV4,
    out_queue: VecDeque<(FindIocRes, asynchan::Sender<FindIocRes>)>,
    ping: Option<Pin<Box<tokio::time::Sleep>>>,
    bids_all_done: BTreeMap<BatchId, ()>,
    bids_timed_out: BTreeMap<BatchId, ()>,
    sids_done: BTreeMap<SearchId, ()>,
    result_for_done_sid_count: u64,
    #[allow(unused)]
    thr_msg_0: ThrottleTrace,
    #[allow(unused)]
    thr_msg_1: ThrottleTrace,
    #[allow(unused)]
    thr_msg_2: ThrottleTrace,
}

impl FindIocStream {
    pub fn new(
        channels_input: Receiver<(String, asynchan::Sender<FindIocRes>)>,
        tgts: Vec<SocketAddrV4>,
        #[allow(unused)] blacklist: Vec<SocketAddrV4>,
        batch_run_max: Duration,
        in_flight_max: usize,
        batch_size: usize,
    ) -> Self {
        let sock = unsafe { Self::create_socket() }.unwrap();
        let afd = AsyncFd::new(sock.0).unwrap();
        Self {
            tgts,
            channels_input: Some(Box::pin(channels_input)),
            in_flight: BTreeMap::new(),
            in_flight_max,
            channels_per_batch: batch_size,
            batch_run_max,
            bid_by_sid: BTreeMap::new(),
            batch_send_queue: VecDeque::new(),
            sock,
            afd,
            buf1: vec![0; 1024],
            send_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 5064),
            out_queue: VecDeque::new(),
            ping: Some(Box::pin(tokio::time::sleep(Duration::from_millis(200)))),
            bids_all_done: BTreeMap::new(),
            bids_timed_out: BTreeMap::new(),
            sids_done: BTreeMap::new(),
            result_for_done_sid_count: 0,
            thr_msg_0: ThrottleTrace::new(Duration::from_millis(1000)),
            thr_msg_1: ThrottleTrace::new(Duration::from_millis(1000)),
            thr_msg_2: ThrottleTrace::new(Duration::from_millis(1000)),
        }
    }

    pub fn quick_state(&self) -> String {
        format!(
            "channels_input {:?}  in_flight {}  bid_by_sid {}  out_queue {}  result_for_done_sid_count {}  bids_timed_out {}",
            self.channels_input.as_ref().map(|x| x.len()),
            self.in_flight.len(),
            self.bid_by_sid.len(),
            self.out_queue.len(),
            self.result_for_done_sid_count,
            self.bids_timed_out.len()
        )
    }

    fn buf_and_batch(&mut self, bid: &BatchId) -> Option<(&mut Vec<u8>, &mut SearchBatch)> {
        match self.in_flight.get_mut(bid) {
            Some(batch) => Some((&mut self.buf1, batch)),
            None => None,
        }
    }

    unsafe fn create_socket() -> Result<SockBox, Error> {
        let ec = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
        if ec == -1 {
            return Err(Error::SocketCreate);
        }
        let sock = SockBox(ec);
        {
            let opt: libc::c_int = 1;
            let ec = unsafe {
                libc::setsockopt(
                    sock.0,
                    libc::SOL_SOCKET,
                    libc::SO_BROADCAST,
                    &opt as *const _ as _,
                    std::mem::size_of::<libc::c_int>() as _,
                )
            };
            if ec == -1 {
                return Err(Error::BroadcastEnable);
            }
        }
        {
            let ec = unsafe { libc::fcntl(sock.0, libc::F_SETFL, libc::O_NONBLOCK) };
            if ec == -1 {
                return Err(Error::NonblockEnable);
            }
        }
        let ip: [u8; 4] = [0, 0, 0, 0];
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: 0,
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(ip),
            },
            sin_zero: [0; 8],
        };
        let addr_len = std::mem::size_of::<libc::sockaddr_in>();
        let ec = unsafe { libc::bind(sock.0, &addr as *const _ as _, addr_len as _) };
        if ec == -1 {
            return Err(Error::SocketBind);
        }
        {
            let mut addr = libc::sockaddr_in {
                sin_family: libc::AF_INET as u16,
                sin_port: 0,
                sin_addr: libc::in_addr { s_addr: 0 },
                sin_zero: [0; 8],
            };
            let mut addr_len = std::mem::size_of::<libc::sockaddr_in>();
            let ec = unsafe { libc::getsockname(sock.0, &mut addr as *mut _ as _, &mut addr_len as *mut _ as _) };
            if ec == -1 {
                error!("getsockname {}", ec);
                return Err(Error::SocketConvertTokio);
            } else {
                if true {
                    let ipv4 = Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes());
                    let tcp_port = u16::from_be(addr.sin_port);
                    debug!("bound local socket to {} port {}", ipv4, tcp_port);
                }
            }
        }
        Ok(sock)
    }

    unsafe fn try_send(sock: i32, addr: &SocketAddrV4, buf: &[u8]) -> Poll<Result<(), Error>> {
        let ip = addr.ip().octets();
        let port = addr.port();
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(ip),
            },
            sin_zero: [0; 8],
        };
        let addr_len = std::mem::size_of::<libc::sockaddr_in>();
        let ec = unsafe {
            libc::sendto(
                sock,
                &buf[0] as *const _ as _,
                buf.len() as _,
                0,
                &addr as *const _ as _,
                addr_len as _,
            )
        };
        if ec == -1 {
            let errno = unsafe { *libc::__errno_location() };
            if errno == libc::EAGAIN {
                return Poll::Pending;
            } else {
                return Poll::Ready(Err(Error::SendFailure));
            }
        }
        Poll::Ready(Ok(()))
    }

    unsafe fn try_read(sock: i32) -> Poll<Result<(SocketAddrV4, Vec<(SearchId, SocketAddrV4)>), Error>> {
        let tsnow = Instant::now();
        let mut saddr_mem = [0u8; std::mem::size_of::<libc::sockaddr>()];
        let mut saddr_len: libc::socklen_t = saddr_mem.len() as _;
        let mut buf = vec![0u8; 1024];
        let ec = unsafe {
            libc::recvfrom(
                sock,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                libc::O_NONBLOCK,
                &mut saddr_mem as *mut _ as _,
                &mut saddr_len as *mut _ as _,
            )
        };
        if ec == -1 {
            let errno = unsafe { *libc::__errno_location() };
            if errno == libc::EAGAIN {
                return Poll::Pending;
            } else {
                return Poll::Ready(Err(Error::ReadFailure));
            }
        } else if ec < 0 {
            // stats.ca_udp_io_error().inc();
            error!("unexpected received {}", ec);
            Poll::Ready(Err(Error::ReadFailure))
        } else if ec == 0 {
            // stats.ca_udp_io_empty().inc();
            Poll::Ready(Err(Error::ReadEmpty))
        } else {
            // stats.ca_udp_io_recv().inc();
            let saddr2: libc::sockaddr_in = unsafe { std::mem::transmute_copy(&saddr_mem) };
            let parsed = Self::parse_response(saddr2, ec as _, &buf, tsnow)?;
            Poll::Ready(Ok(parsed))
        }
    }

    fn parse_response(
        saddr2: libc::sockaddr_in,
        ec: usize,
        buf: &[u8],
        tsnow: Instant,
    ) -> Result<(SocketAddrV4, Vec<(SearchId, SocketAddrV4)>), Error> {
        let src_addr = Ipv4Addr::from(saddr2.sin_addr.s_addr.to_ne_bytes());
        let src_port = u16::from_be(saddr2.sin_port);
        if false {
            let mut s1 = String::new();
            for i in 0..ec {
                s1.extend(format!(" {:02x}", buf[i]).chars());
            }
            debug!("received answer {}", s1);
            debug!(
                "received answer string {}",
                String::from_utf8_lossy(buf[..ec as usize].into())
            );
        }
        if ec > 2048 {
            // TODO handle if we get a too large answer.
            error!("received packet too large");
            panic!();
        }
        let mut nb = slidebuf::SlideBuf::new(2048);
        nb.put_slice(&buf[..ec as usize])?;
        let mut msgs = Vec::new();
        let mut accounted = 0;
        loop {
            let n = nb.data().len();
            if n == 0 {
                break;
            }
            if n < 16 {
                error!("incomplete message, not enough for header");
                break;
            }
            let hi = HeadInfo::from_netbuf(&mut nb)?;
            if hi.cmdid() == 0 && hi.payload_len() == 0 {
            } else if hi.cmdid() == 6 && hi.payload_len() == 8 {
            } else {
                info!("cmdid {}  payload {}", hi.cmdid(), hi.payload_len());
            }
            if nb.data().len() < hi.payload_len() as usize {
                error!("incomplete message, missing payload");
                break;
            }
            let msg = CaMsg::from_proto_infos(&hi, nb.data(), tsnow, 32)?;
            nb.adv(hi.payload_len() as usize)?;
            msgs.push(msg);
            accounted += 16 + hi.payload_len();
        }
        if accounted != ec as u32 {
            // stats.ca_udp_unaccounted_data().inc();
            debug!("unaccounted data  ec {}  accounted {}", ec, accounted);
        }
        if msgs.len() < 1 {
            // stats.ca_udp_warn().inc();
            debug!("received answer without messages");
        }
        if msgs.len() == 1 {
            // stats.ca_udp_warn().inc();
            debug!("received answer with single message: {:?}", msgs);
        }
        let mut good = true;
        if let CaMsgTy::VersionRes(v) = msgs[0].ty {
            if v != 13 {
                warn!("bad version in search response: {}", v);
                good = false;
            }
        } else {
            // stats.ca_udp_first_msg_not_version().inc();
        }
        // trace2!("recv  {:?}  {:?}", src_addr, msgs);
        let mut res = Vec::new();
        if good {
            // because of bad java CA implementation, consider also the first message
            for msg in &msgs[0..] {
                match &msg.ty {
                    CaMsgTy::VersionRes(_) => {}
                    CaMsgTy::SearchRes(k) => {
                        let addr = SocketAddrV4::new(src_addr, k.tcp_port);
                        res.push((SearchId(k.id), addr));
                    }
                    _ => {
                        // stats.ca_udp_error().inc();
                        warn!("try_read: unknown message received  {:?}", msg.ty);
                    }
                }
            }
        }
        Ok((SocketAddrV4::new(src_addr, src_port), res))
    }

    fn serialize_batch(buf: &mut Vec<u8>, batch: &SearchBatch) {
        buf.extend_from_slice(&[0, 0, 0, 0]);
        buf.extend_from_slice(&[0, 0, 0, 13]);
        buf.extend_from_slice(&[0, 0, 0, 0]);
        buf.extend_from_slice(&[0, 0, 0, 0]);
        for (sid, ch) in batch.sids.iter().zip(batch.channels.iter()) {
            use bytes::BufMut;
            let chb = ch.as_bytes();
            let npadded = (chb.len() + 1 + 7) / 8 * 8;
            let npad = npadded - chb.len();
            buf.put_u16(0x06);
            buf.put_u16(npadded as _);
            buf.put_u16(0);
            buf.put_u16(13);
            buf.put_u32(sid.0);
            buf.put_u32(sid.0);
            buf.extend_from_slice(chb);
            buf.extend_from_slice(&vec![0u8; npad]);
        }
    }

    fn create_in_flight(&mut self, chns: Vec<(String, asynchan::Sender<FindIocRes>)>) {
        let bid = BatchId::next();
        let mut sids = Vec::new();
        let mut chs = Vec::new();
        let mut txs = Vec::new();
        for (ch, tx) in chns {
            let sid = SearchId::next();
            self.bid_by_sid.insert(sid.clone(), bid.clone());
            sids.push(sid);
            chs.push(ch);
            txs.push(Some(tx));
        }
        let n = chs.len();
        let batch = SearchBatch {
            ts_beg: Instant::now(),
            channels: chs,
            tgts: self.tgts.iter().enumerate().map(|x| x.0).collect(),
            sids,
            done: vec![false; n],
            txs,
        };
        self.in_flight.insert(bid.clone(), batch);
        self.batch_send_queue.push_back(bid);
        // stats.ca_udp_batch_created().inc();
    }

    fn handle_result(&mut self, src: SocketAddrV4, res: Vec<(SearchId, SocketAddrV4)>) {
        let tsnow = Instant::now();
        let mut sids_remove = Vec::new();
        for (sid, addr) in res {
            self.sids_done.insert(sid.clone(), ());
            match self.bid_by_sid.get(&sid) {
                Some(bid) => {
                    sids_remove.push(sid.clone());
                    match self.in_flight.get_mut(bid) {
                        Some(batch) => {
                            let mut found_sid = false;
                            for (i2, s2) in batch.sids.iter().enumerate() {
                                if s2 == &sid {
                                    found_sid = true;
                                    batch.done[i2] = true;
                                    match batch.channels.get(i2) {
                                        Some(ch) => {
                                            if let Some(tx) = batch.txs[i2].take() {
                                                let dt = tsnow.saturating_duration_since(batch.ts_beg);
                                                let res = FindIocRes {
                                                    channel: ch.into(),
                                                    response_addr: Some(src.clone()),
                                                    addr: Some(addr),
                                                    dt,
                                                };
                                                // trace!("udp search response {res:?}");
                                                // stats.ca_udp_recv_result().inc();
                                                self.out_queue.push_back((res, tx));
                                            } else {
                                                info!("result for {ch} but no tx");
                                            }
                                        }
                                        None => {
                                            // stats.ca_udp_logic_error().inc();
                                            error!(
                                                "logic error  batch sids / channels lens:  {} vs {}",
                                                batch.sids.len(),
                                                batch.channels.len()
                                            );
                                        }
                                    }
                                }
                            }
                            if !found_sid {
                                error!("can not find sid {:?} in batch {:?}", sid, bid);
                            }
                            let all_done = batch.done.iter().all(|x| *x);
                            if all_done {
                                self.bids_all_done.insert(bid.clone(), ());
                                self.in_flight.remove(bid);
                            }
                        }
                        None => {
                            // TODO analyze reasons
                            error!("no batch for {:?}", bid);
                        }
                    }
                }
                None => {
                    // TODO analyze reasons
                    if self.sids_done.contains_key(&sid) {
                        self.result_for_done_sid_count += 1;
                    } else {
                        error!("no bid for {:?}", sid);
                    }
                }
            }
        }
        for sid in sids_remove {
            self.bid_by_sid.remove(&sid);
        }
    }

    fn clear_timed_out(&mut self) {
        let tsnow = Instant::now();
        let mut bids = Vec::new();
        let mut sids = Vec::new();
        let mut chns = Vec::new();
        let mut dts = Vec::new();
        let mut txs = Vec::new();
        for (bid, batch) in &mut self.in_flight {
            let dt = tsnow.saturating_duration_since(batch.ts_beg);
            if dt > self.batch_run_max {
                self.bids_timed_out.insert(bid.clone(), ());
                for (i2, sid) in batch.sids.iter().enumerate() {
                    if batch.done[i2] == false {
                        // debug!("Timeout: {bid:?} {}", batch.channels[i2]);
                        if let Some(tx) = batch.txs[i2].take() {
                            sids.push(sid.clone());
                            chns.push(batch.channels[i2].clone());
                            dts.push(dt);
                            txs.push(tx);
                        } else if batch.done[i2] == false {
                            warn!("batch not yet done, but no tx");
                        }
                        // stats.ca_udp_recv_timeout().inc();
                    }
                }
                bids.push(bid.clone());
            }
        }
        for (((sid, ch), dt), tx) in sids.into_iter().zip(chns).zip(dts).zip(txs) {
            debug!("timed out  {ch}");
            let res = FindIocRes {
                response_addr: None,
                channel: ch,
                addr: None,
                dt,
            };
            self.out_queue.push_back((res, tx));
            self.bid_by_sid.remove(&sid);
        }
        for bid in bids {
            self.in_flight.remove(&bid);
        }
    }

    fn get_input_up_to_batch_max(&mut self, cx: &mut Context) -> Poll<Vec<(String, asynchan::Sender<FindIocRes>)>> {
        use Poll::*;
        let mut ret = Vec::new();
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(rx) = self.channels_input.as_mut() {
                match rx.poll_next_unpin(cx) {
                    Ready(Some(item)) => {
                        hpp.mark_progress();
                        trace!("get_input_up_to_batch_max  {}", item.0);
                        ret.push(item);
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self.channels_input = None;
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            break if ret.len() >= self.channels_per_batch {
                Ready(ret)
            } else if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(Vec::new())
            };
        }
    }

    fn ready_for_end_of_stream(&self) -> bool {
        self.channels_input.is_none() && self.in_flight.is_empty() && self.out_queue.is_empty()
    }

    fn out_item(&mut self) -> Option<VecDeque<(FindIocRes, asynchan::Sender<FindIocRes>)>> {
        if self.out_queue.is_empty() {
            None
        } else {
            let ret = std::mem::replace(&mut self.out_queue, VecDeque::new());
            Some(ret)
        }
    }

    fn refill_some(&mut self, cx: &mut Context) -> Poll<Option<()>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            if self.in_flight.len() >= self.in_flight_max {
                break Ready(Some(()));
            }
            match self.get_input_up_to_batch_max(cx) {
                Ready(chns) => {
                    if chns.len() == 0 {
                    } else {
                        hpp.mark_progress();
                        self.create_in_flight(chns);
                    }
                }
                Pending => {
                    hpp.mark_pending();
                }
            }
            break if hpp.have_progress() {
                // TODO refactor such that we do not go over limits when looping
                // continue;
                Ready(Some(()))
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}

impl Stream for FindIocStream {
    type Item = Result<VecDeque<(FindIocRes, asynchan::Sender<FindIocRes>)>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.channels_input.is_none() {
            debug!("{}", self.quick_state());
        }
        // self.thr_msg_0.trigger("FindIocStream::poll_next", &[]);
        loop {
            let mut hpp = HaveProgressPending::new();
            if let Some(fut) = self.ping.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(_) => {
                        hpp.mark_progress();
                        if self.ready_for_end_of_stream() {
                            self.ping = None;
                        } else {
                            self.ping = Some(Box::pin(tokio::time::sleep(Duration::from_millis(200))));
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if let Some(x) = self.out_item() {
                break Ready(Some(Ok(x)));
            }
            self.clear_timed_out();
            if self.buf1.is_empty() {
            } else {
                match self.afd.poll_write_ready(cx) {
                    Ready(Ok(mut g)) => match unsafe { Self::try_send(self.sock.0, &self.send_addr, &self.buf1) } {
                        Ready(Ok(())) => {
                            hpp.mark_progress();
                            self.buf1.clear();
                        }
                        Ready(Err(e)) => {
                            hpp.mark_progress();
                            error!("FindIocStream {}", e);
                        }
                        Pending => {
                            hpp.mark_pending();
                            g.clear_ready();
                            // TODO count for stats
                            // warn!("socket seemed ready for write, but is not");
                        }
                    },
                    Ready(Err(e)) => {
                        hpp.mark_progress();
                        error!("poll_write_ready {}", e);
                        // TODO should we abort?
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            while self.buf1.is_empty() {
                match self.batch_send_queue.pop_front() {
                    Some(bid) => {
                        match self.buf_and_batch(&bid) {
                            Some((buf1, batch)) => match batch.tgts.pop_front() {
                                Some(tgtix) => {
                                    Self::serialize_batch(buf1, batch);
                                    trace!("serialized for search {:?}", batch.channels);
                                    match self.tgts.get(tgtix) {
                                        Some(tgt) => {
                                            let tgt = tgt.clone();
                                            self.send_addr = tgt.clone();
                                            self.batch_send_queue.push_back(bid);
                                            hpp.mark_progress();
                                        }
                                        None => {
                                            self.buf1.clear();
                                            self.batch_send_queue.push_back(bid);
                                            hpp.mark_progress();
                                            error!("tgtix does not exist");
                                        }
                                    }
                                }
                                None => {
                                    hpp.mark_progress();
                                }
                            },
                            None => {
                                if self.bids_all_done.contains_key(&bid) {
                                    // Already answered from another target
                                    //trace!("bid {bid:?} from batch send queue not in flight  AND  all done");
                                } else {
                                    warn!("bid {:?} from batch send queue not in flight  NOT done", bid);
                                }
                                hpp.mark_progress();
                            }
                        }
                    }
                    None => break,
                }
            }
            if self.channels_input.is_some() {
                match self.refill_some(cx) {
                    Ready(Some(())) => {
                        hpp.mark_progress();
                    }
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if self.ready_for_end_of_stream() {
            } else {
                match self.afd.poll_read_ready(cx) {
                    Ready(Ok(mut g)) => {
                        // debug!("BLOCK AA");
                        match unsafe { Self::try_read(self.sock.0) } {
                            Ready(Ok((src, res))) => {
                                self.handle_result(src, res);
                                if self.ready_for_end_of_stream() {
                                    debug!("ready_for_end_of_stream  continue after handle_result");
                                }
                                hpp.mark_progress();
                            }
                            Ready(Err(e)) => {
                                error!("try_read {}", e);
                                break Ready(Some(Err(e)));
                            }
                            Pending => {
                                g.clear_ready();
                                hpp.mark_pending();
                            }
                        }
                    }
                    Ready(Err(e)) => {
                        error!("poll_read_ready {}", e);
                        break Ready(Some(Err(e.into())));
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                info!("FindIocStream  Done");
                Ready(None)
            };
        }
    }
}

impl futures::stream::FusedStream for FindIocStream {
    fn is_terminated(&self) -> bool {
        false
    }
}
