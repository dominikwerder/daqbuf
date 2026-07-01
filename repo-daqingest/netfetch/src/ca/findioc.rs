use crate::asynchan;
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
use taskrun::tokio::time::Sleep;
use taskrun::tokio::time::sleep;
use tokio::io::unix::AsyncFd;

const BATCH_IVL: Duration = Duration::from_millis(100);

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

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
    tgts: VecDeque<usize>,
    channels: Vec<String>,
    sids: Vec<SearchId>,
}

struct ChannelSearchRequest {
    chn: String,
    sid: SearchId,
    beg: Instant,
    tx: Option<asynchan::Sender<FindIocRes>>,
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
    batch_cur: Option<SearchBatch>,
    batch_len_max: usize,
    search_timeout: Duration,
    sock: SockBox,
    afd: AsyncFd<i32>,
    send_job: Option<(SocketAddrV4, Vec<u8>)>,
    send_idle: Option<(Vec<u8>,)>,
    out_queue: VecDeque<(FindIocRes, asynchan::Sender<FindIocRes>)>,
    ping: Option<Pin<Box<tokio::time::Sleep>>>,
    chn_reqs: VecDeque<ChannelSearchRequest>,
    batch_ivl: Option<Pin<Box<Sleep>>>,
    is_done: bool,
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
        search_timeout: Duration,
        batch_len_max: usize,
    ) -> Self {
        let sock = unsafe { Self::create_socket() }.unwrap();
        let afd = AsyncFd::new(sock.0).unwrap();
        info!("search targets:");
        for x in &tgts {
            info!("  {x}");
        }
        Self {
            tgts,
            channels_input: Some(Box::pin(channels_input)),
            batch_cur: None,
            batch_len_max,
            search_timeout,
            sock,
            afd,
            send_job: None,
            send_idle: Some((Vec::with_capacity(2048),)),
            out_queue: VecDeque::new(),
            ping: Some(Box::pin(tokio::time::sleep(Duration::from_millis(200)))),
            chn_reqs: VecDeque::with_capacity(2),
            batch_ivl: None,
            is_done: false,
            thr_msg_0: ThrottleTrace::new(Duration::from_millis(1000)),
            thr_msg_1: ThrottleTrace::new(Duration::from_millis(1000)),
            thr_msg_2: ThrottleTrace::new(Duration::from_millis(1000)),
        }
    }

    pub fn quick_state(&self) -> String {
        format!(
            "channels_input {:?}  out_queue {}",
            self.channels_input.as_ref().map(|x| x.len()),
            self.out_queue.len(),
        )
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
        let addrc = libc::sockaddr_in {
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
                &addrc as *const _ as _,
                addr_len as _,
            )
        };
        if ec == -1 {
            let errno = unsafe { *libc::__errno_location() };
            if errno == libc::EAGAIN {
                Poll::Pending
            } else {
                Poll::Ready(Err(Error::SendFailure))
            }
        } else {
            trace!("try_send  sent  to {addr}  buf len {}\n", buf.len());
            Poll::Ready(Ok(()))
        }
    }

    unsafe fn try_read(sock: i32) -> Poll<Result<(SocketAddrV4, Vec<(SearchId, SocketAddrV4)>), Error>> {
        let tsnow = Instant::now();
        let mut saddr_mem = [0u8; std::mem::size_of::<libc::sockaddr>()];
        let mut saddr_len: libc::socklen_t = saddr_mem.len() as _;
        let mut buf = vec![0u8; 2048];
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
            let src_addr = Ipv4Addr::from(saddr2.sin_addr.s_addr.to_ne_bytes());
            let src_port = u16::from_be(saddr2.sin_port);
            let src = SocketAddrV4::new(src_addr, src_port);
            trace!("RECEIVED  {}  {}", src, String::from_utf8_lossy(&buf[..(ec as usize)]));
            let parsed = Self::parse_response(src, ec as _, &buf, tsnow)?;
            Poll::Ready(Ok(parsed))
        }
    }

    fn parse_response(
        src: SocketAddrV4,
        ec: usize,
        buf: &[u8],
        tsnow: Instant,
    ) -> Result<(SocketAddrV4, Vec<(SearchId, SocketAddrV4)>), Error> {
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
            trace!("RECEIVED  from {src}  msg {msg:?}");
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
        // trace2!("recv  {}  {:?}", src, msgs);
        let mut res = Vec::new();
        if good {
            // because of bad java CA implementation, consider also the first message
            for msg in &msgs[0..] {
                match &msg.ty {
                    CaMsgTy::VersionRes(_) => {}
                    CaMsgTy::SearchRes(k) => {
                        let ip = if k.addr == 0xffffffff {
                            *src.ip()
                        } else {
                            Ipv4Addr::from_octets(k.addr.to_be_bytes())
                        };
                        let addr = SocketAddrV4::new(ip, k.tcp_port);
                        trace!("src {}  addr {} {}", src, k.addr, addr);
                        res.push((SearchId(k.id), addr));
                    }
                    _ => {
                        // stats.ca_udp_error().inc();
                        warn!("try_read: unknown message received  {:?}", msg.ty);
                    }
                }
            }
        }
        Ok((src, res))
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

    fn handle_result(&mut self, src: SocketAddrV4, res: Vec<(SearchId, SocketAddrV4)>) {
        let tsnow = Instant::now();
        for (sid, addr) in res {
            for x in self.chn_reqs.iter_mut() {
                if sid == x.sid {
                    let n = &x.chn;
                    trace!("Found  {sid:?}  {n}  {addr}");
                    if let Some(tx) = x.tx.take() {
                        let dt = tsnow.saturating_duration_since(x.beg);
                        let res = FindIocRes {
                            channel: x.chn.clone(),
                            response_addr: Some(src.clone()),
                            addr: Some(addr),
                            dt,
                        };
                        self.out_queue.push_back((res, tx));
                    }
                }
            }
        }
    }

    fn clear_timed_out(&mut self) {
        let tsnow = Instant::now();
        for x in self.chn_reqs.iter_mut() {
            if x.tx.is_some() {
                let dt = tsnow.saturating_duration_since(x.beg);
                if dt > self.search_timeout {
                    if let Some(tx) = x.tx.take() {
                        let res = FindIocRes {
                            response_addr: None,
                            channel: x.chn.clone(),
                            addr: None,
                            dt,
                        };
                        self.out_queue.push_back((res, tx));
                    }
                }
            }
        }
    }

    fn out_item(&mut self) -> Option<VecDeque<(FindIocRes, asynchan::Sender<FindIocRes>)>> {
        if self.out_queue.is_empty() {
            None
        } else {
            let ret = std::mem::replace(&mut self.out_queue, VecDeque::new());
            Some(ret)
        }
    }

    fn refill_some(&mut self, cx: &mut Context, tsnow: Instant) -> Poll<Option<()>> {
        let selfname = "get_input_up_to_batch_max";
        use Poll::*;
        let mut ret = Vec::new();
        let mut hpp2 = HaveProgressPending::new();
        loop {
            let mut hpp = HaveProgressPending::new();
            if ret.len() < self.batch_len_max
                && let Some(rx) = self.channels_input.as_mut()
            {
                match rx.poll_next_unpin(cx) {
                    Ready(Some(item)) => {
                        hpp.mark_progress();
                        trace!("{selfname}  {}", item.0);
                        ret.push(item);
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self.channels_input = None;
                    }
                    Pending => {
                        hpp.mark_pending();
                        hpp2.mark_pending();
                    }
                }
            } else {
            }
            break if hpp.have_progress() {
                continue;
            } else {
                ()
            };
        }
        if ret.len() != 0 {
            // let bid = BatchId::next();
            let mut chns = Vec::new();
            let mut sids = Vec::new();
            for (ch, tx) in ret {
                let sid = SearchId::next();
                chns.push(ch.clone());
                sids.push(sid.clone());
                self.chn_reqs.push_back(ChannelSearchRequest {
                    chn: ch,
                    sid,
                    beg: tsnow,
                    tx: Some(tx),
                });
            }
            let batch = SearchBatch {
                tgts: self.tgts.iter().enumerate().map(|x| x.0).collect(),
                channels: chns,
                sids,
            };
            self.batch_cur = Some(batch);
            Ready(Some(()))
        } else if hpp2.have_pending() {
            Pending
        } else {
            Ready(None)
        }
    }
}

impl Stream for FindIocStream {
    type Item = Result<VecDeque<(FindIocRes, asynchan::Sender<FindIocRes>)>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfname = "FindIocStream::poll_next";
        if self.channels_input.is_none() {
            debug!("{}", self.quick_state());
        }
        // self.thr_msg_0.trigger("FindIocStream::poll_next", &[]);
        loop {
            trace!("{selfname}");
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            if let Some(x) = self2.out_item() {
                break Ready(Some(Ok(x)));
            }
            if let Some(fut) = self2.ping.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(()) => {
                        hpp.mark_progress();
                        self2.clear_timed_out();
                        if self2.channels_input.is_some() {
                            self2.ping = Some(Box::pin(tokio::time::sleep(Duration::from_millis(500))));
                        } else {
                            self2.ping = None;
                        }
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if let Some(job) = self2.send_job.as_mut() {
                if let Some(fut) = self2.batch_ivl.as_mut() {
                    match fut.poll_unpin(cx) {
                        Ready(()) => {
                            hpp.mark_progress();
                            self2.batch_ivl = None
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                } else {
                    let (addr, buf1) = job;
                    match self2.afd.poll_write_ready(cx) {
                        Ready(Ok(mut g)) => {
                            match unsafe { Self::try_send(self2.sock.0, addr, buf1) } {
                                Ready(Ok(())) => {
                                    hpp.mark_progress();
                                    g.clear_ready();
                                    let s = String::from_utf8_lossy(buf1);
                                    self2.send_idle = Some((std::mem::replace(buf1, Vec::new()),));
                                    self2.send_job = None;
                                    self2.batch_ivl = Some(Box::pin(sleep(BATCH_IVL)));
                                }
                                Ready(Err(e)) => {
                                    hpp.mark_progress();
                                    g.clear_ready();
                                    self2.send_idle = Some((std::mem::replace(buf1, Vec::new()),));
                                    self2.send_job = None;
                                    error!("{selfname}  {e}");
                                    break Ready(Some(Err(Error::SendFailure)));
                                }
                                Pending => {
                                    hpp.mark_pending();
                                    g.clear_ready();
                                    // TODO count for stats
                                    info!("{selfname}  socket seemed ready for write, but is not");
                                }
                            }
                        }
                        Ready(Err(e)) => {
                            hpp.mark_progress();
                            error!("{selfname}  poll_write_ready {e}");
                            self2.send_idle = Some((std::mem::replace(buf1, Vec::new()),));
                            self2.send_job = None;
                            break Ready(Some(Err(Error::SendFailure)));
                        }
                        Pending => {
                            hpp.mark_pending();
                        }
                    }
                }
            } else if let Some(batch) = self2.batch_cur.as_mut() {
                if let Some((mut buf1,)) = self2.send_idle.take() {
                    match batch.tgts.pop_front() {
                        Some(tgtix) => match self2.tgts.get(tgtix) {
                            Some(tgt) => {
                                hpp.mark_progress();
                                buf1.clear();
                                Self::serialize_batch(&mut buf1, batch);
                                trace!("{selfname}  serialized for search {:?}", batch.channels);
                                let tgt = tgt.clone();
                                self2.send_job = Some((tgt.clone(), buf1));
                            }
                            None => {
                                hpp.mark_progress();
                                self2.send_idle = Some((buf1,));
                                error!("{selfname}  tgtix does not exist");
                            }
                        },
                        None => {
                            hpp.mark_progress();
                            self2.send_idle = Some((buf1,));
                            self2.batch_cur = None;
                        }
                    }
                } else {
                    error!("{selfname}  send buffer lost");
                    break Ready(Some(Err(Error::SendFailure)));
                }
            } else if self2.channels_input.is_some() {
                let tsnow = Instant::now();
                match self2.refill_some(cx, tsnow) {
                    Ready(Some(())) => {
                        hpp.mark_progress();
                    }
                    Ready(None) => {}
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            }
            if self2.channels_input.is_some() {
                match self2.afd.poll_read_ready(cx) {
                    Ready(Ok(mut g)) => match unsafe { Self::try_read(self2.sock.0) } {
                        Ready(Ok((src, res))) => {
                            self2.handle_result(src, res);
                            hpp.mark_progress();
                        }
                        Ready(Err(e)) => {
                            error!("{selfname}  try_read {e}");
                            break Ready(Some(Err(e)));
                        }
                        Pending => {
                            g.clear_ready();
                            hpp.mark_pending();
                        }
                    },
                    Ready(Err(e)) => {
                        error!("{selfname}  poll_read_ready {e}");
                        break Ready(Some(Err(e.into())));
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                }
            } else {
                // If input closes, we stop reading. Could also continue for some time.
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                info!("FindIocStream  Done");
                self2.is_done = true;
                Ready(None)
            };
        }
    }
}

impl futures::stream::FusedStream for FindIocStream {
    fn is_terminated(&self) -> bool {
        self.is_done
    }
}
