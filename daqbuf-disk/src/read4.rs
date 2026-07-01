use bytes::BytesMut;
use daqbuf_err as err;
use err::Error;
use netpod::log::*;
use std::os::unix::prelude::RawFd;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Once;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::sync::mpsc;

static READ4: AtomicPtr<Read4> = AtomicPtr::new(std::ptr::null_mut());

const DO_TRACE: bool = false;

pub struct ReadTask {
    fd: RawFd,
    buflen: u64,
    read_queue_len: usize,
    results: mpsc::Sender<Result<ReadResult, Error>>,
}

pub struct ReadResult {
    pub buf: BytesMut,
    pub eof: bool,
}

pub struct Read4 {
    jobs_tx: mpsc::Sender<ReadTask>,
    rtx: crossbeam::channel::Sender<mpsc::Receiver<ReadTask>>,
    threads_max: AtomicUsize,
    can_not_publish: AtomicUsize,
}

impl Read4 {
    pub fn get() -> &'static Self {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let (jtx, jrx) = mpsc::channel(512);
            let (rtx, rrx) = crossbeam::channel::bounded(32);
            let read4 = Read4 {
                jobs_tx: jtx,
                rtx,
                threads_max: AtomicUsize::new(32),
                can_not_publish: AtomicUsize::new(0),
            };
            let b = Box::new(read4);
            let ptr = Box::into_raw(b);
            READ4.store(ptr, Ordering::Release);
            let ptr = READ4.load(Ordering::Acquire);
            let h = unsafe { &*ptr };
            if let Err(_) = h.rtx.send(jrx) {
                error!("Read4 INIT: can not enqueue main job reader");
            }
            for wid in 0..16 {
                let rrx = rrx.clone();
                tokio::task::spawn_blocking(move || h.read_worker(wid, rrx));
            }
        });
        let ptr = READ4.load(Ordering::Acquire);
        unsafe { &*ptr }
    }

    pub fn threads_max(&self) -> usize {
        self.threads_max.load(Ordering::Acquire)
    }

    pub fn set_threads_max(&self, max: usize) {
        self.threads_max.store(max, Ordering::Release);
    }

    pub async fn read(
        &self,
        fd: RawFd,
        buflen: u64,
        read_queue_len: usize,
    ) -> Result<mpsc::Receiver<Result<ReadResult, Error>>, Error> {
        let (tx, rx) = mpsc::channel(32);
        let rt = ReadTask {
            fd,
            buflen,
            read_queue_len,
            results: tx,
        };
        match self.jobs_tx.send(rt).await {
            Ok(_) => Ok(rx),
            Err(e) => Err(Error::with_msg(format!("can not send read job task: {e}"))),
        }
    }

    fn read_worker(&self, wid: u32, rrx: crossbeam::channel::Receiver<mpsc::Receiver<ReadTask>>) {
        loop {
            while wid as usize >= self.threads_max.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(4000));
            }
            match rrx.recv() {
                Ok(mut jrx) => match jrx.blocking_recv() {
                    Some(rt) => match self.rtx.send(jrx) {
                        Ok(_) => self.read_worker_job(wid, rt),
                        Err(e) => {
                            error!("can not return the job receiver: wid {wid}  {e}");
                            return;
                        }
                    },
                    None => {
                        let _ = self.rtx.send(jrx);
                        return;
                    }
                },
                Err(e) => {
                    error!("read_worker sees: wid {wid}  {e}");
                    return;
                }
            }
        }
    }

    fn read_worker_job(&self, wid: u32, rt: ReadTask) {
        let fd = rt.fd;
        let ec = unsafe { libc::lseek(fd, 0, libc::SEEK_CUR) };
        if ec == -1 {
            let errno = unsafe { *libc::__errno_location() };
            let msg = format!("seek error  wid {wid}  fd {fd}  errno {errno}");
            error!("{}", msg);
            let e = Error::with_msg_no_trace(msg);
            match rt.results.blocking_send(Err(e)) {
                Ok(_) => {}
                Err(_) => {
                    self.can_not_publish.fetch_add(1, Ordering::AcqRel);
                    error!("Can not publish error");
                }
            }
            return;
        }
        let mut rpos = ec as u64;
        let mut apos = rpos / rt.buflen * rt.buflen;
        let mut prc = 0;
        loop {
            let ts1 = Instant::now();
            while apos < rpos + rt.read_queue_len as u64 * rt.buflen {
                if DO_TRACE {
                    trace!("READAHEAD  wid {wid}  fd {fd}  apos {apos}");
                }
                let n = unsafe { libc::readahead(fd, apos as _, rt.buflen as _) };
                if n == -1 {
                    let errno = unsafe { *libc::__errno_location() };
                    let msg = format!("READAHEAD  ERROR  wid {wid}  errno {errno}  fd {fd}  apos {apos}");
                    warn!("{}", msg);
                    // TODO use a more structured error
                    let e = Error::with_msg_no_trace(msg);
                    match rt.results.blocking_send(Err(e)) {
                        Ok(_) => {}
                        Err(_) => {
                            self.can_not_publish.fetch_add(1, Ordering::AcqRel);
                            warn!("can not publish the read result  wid {wid}");
                        }
                    }
                } else {
                    apos += rt.buflen;
                }
            }
            if DO_TRACE {
                trace!("READ  wid {wid}  fd {fd}  rpos {rpos}");
            }
            let mut buf = BytesMut::with_capacity(rt.buflen as usize);
            let bufptr = buf.as_mut_ptr() as _;
            let buflen = buf.capacity() as _;
            let ec = unsafe { libc::read(fd, bufptr, buflen) };
            prc += 1;
            if ec == -1 {
                let errno = unsafe { *libc::__errno_location() };
                {
                    let msg = format!("READ  ERROR  wid {wid}  errno {errno}  fd {fd}  offset {rpos}");
                    warn!("{}", msg);
                    // TODO use a more structured error
                    let e = Error::with_msg_no_trace(msg);
                    match rt.results.blocking_send(Err(e)) {
                        Ok(_) => {}
                        Err(_) => {
                            self.can_not_publish.fetch_add(1, Ordering::AcqRel);
                            warn!("can not publish the read result  wid {wid}");
                            return;
                        }
                    }
                }
            } else if ec == 0 {
                debug!("READ  EOF  wid {wid}  prc {prc}  fd {fd}  offset {rpos}  prc {prc}");
                let rr = ReadResult { buf, eof: true };
                match rt.results.blocking_send(Ok(rr)) {
                    Ok(_) => {}
                    Err(_) => {
                        self.can_not_publish.fetch_add(1, Ordering::AcqRel);
                        warn!("can not publish the read result  wid {wid}");
                        return;
                    }
                }
                return;
            } else if ec > 0 {
                if ec as usize > buf.capacity() {
                    error!("READ  TOOLARGE  wid {wid}  ec {ec}  fd {fd}  offset {rpos}  prc {prc}");
                    return;
                } else {
                    rpos += ec as u64;
                    unsafe { buf.set_len(buf.len() + (ec as usize)) };
                    {
                        let ts2 = Instant::now();
                        let dur = ts2.duration_since(ts1);
                        let dms = 1e3 * dur.as_secs_f32();
                        if DO_TRACE {
                            trace!("READ  DONE  wid {wid}  ec {ec}  fd {fd}  prc {prc}  dms {dms:.2}");
                        }
                        let rr = ReadResult { buf, eof: false };
                        match rt.results.blocking_send(Ok(rr)) {
                            Ok(_) => {}
                            Err(_) => {
                                self.can_not_publish.fetch_add(1, Ordering::AcqRel);
                                warn!("can not publish the read result  wid {wid}");
                                return;
                            }
                        }
                    }
                }
            } else {
                error!("READ  UNEXPECTED  wid {wid}  ec {ec}  fd {fd}  offset {rpos}");
                return;
            }
        }
    }
}
