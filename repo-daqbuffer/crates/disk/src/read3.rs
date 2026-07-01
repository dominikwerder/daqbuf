use bytes::BytesMut;
use daqbuf_err as err;
use err::Error;
use netpod::log::*;
use std::os::unix::prelude::RawFd;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Once;
use std::time::{Duration, Instant};
use taskrun::tokio;
use tokio::sync::{mpsc, oneshot};

const DO_TRACE: bool = false;

static READ3: AtomicPtr<Read3> = AtomicPtr::new(std::ptr::null_mut());

pub struct ReadTask {
    fd: RawFd,
    pos: u64,
    count: u64,
    rescell: oneshot::Sender<Result<ReadResult, Error>>,
}

pub struct ReadResult {
    pub buf: BytesMut,
    pub eof: bool,
}

pub struct Read3 {
    jobs_tx: mpsc::Sender<ReadTask>,
    rtx: crossbeam::channel::Sender<mpsc::Receiver<ReadTask>>,
    threads_max: AtomicUsize,
    can_not_publish: AtomicUsize,
}

impl Read3 {
    pub fn get() -> &'static Self {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let (jtx, jrx) = mpsc::channel(512);
            let (rtx, rrx) = crossbeam::channel::bounded(32);
            let read3 = Read3 {
                jobs_tx: jtx,
                rtx,
                threads_max: AtomicUsize::new(32),
                can_not_publish: AtomicUsize::new(0),
            };
            let b = Box::new(read3);
            let ptr = Box::into_raw(b);
            READ3.store(ptr, Ordering::Release);
            let ptr = READ3.load(Ordering::Acquire);
            let h = unsafe { &*ptr };
            if let Err(_) = h.rtx.send(jrx) {
                error!("Read3 INIT: can not enqueue main job reader");
            }
            for wid in 0..128 {
                let rrx = rrx.clone();
                tokio::task::spawn_blocking(move || h.read_worker(wid, rrx));
            }
        });
        let ptr = READ3.load(Ordering::Acquire);
        unsafe { &*ptr }
    }

    pub fn threads_max(&self) -> usize {
        self.threads_max.load(Ordering::Acquire)
    }

    pub fn set_threads_max(&self, max: usize) {
        self.threads_max.store(max, Ordering::Release);
    }

    pub async fn read(&self, fd: RawFd, pos: u64, count: u64) -> Result<ReadResult, Error> {
        let (tx, rx) = oneshot::channel();
        let rt = ReadTask {
            fd,
            pos,
            count,
            rescell: tx,
        };
        match self.jobs_tx.send(rt).await {
            Ok(_) => match rx.await {
                Ok(res) => res,
                Err(e) => Err(Error::with_msg(format!("can not receive read task result: {e}"))),
            },
            Err(e) => Err(Error::with_msg(format!("can not send read job task: {e}"))),
        }
    }

    fn read_worker(&self, wid: u32, rrx: crossbeam::channel::Receiver<mpsc::Receiver<ReadTask>>) {
        'outer: loop {
            while wid as usize >= self.threads_max.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(4000));
            }
            match rrx.recv() {
                Ok(mut jrx) => match jrx.blocking_recv() {
                    Some(rt) => match self.rtx.send(jrx) {
                        Ok(_) => self.read_worker_job(wid, rt),
                        Err(e) => {
                            error!("can not return the job receiver: wid {wid}  {e}");
                            break 'outer;
                        }
                    },
                    None => {
                        let _ = self.rtx.send(jrx);
                        break 'outer;
                    }
                },
                Err(e) => {
                    error!("read_worker sees: wid {wid}  {e}");
                    break 'outer;
                }
            }
        }
    }

    fn read_worker_job(&self, wid: u32, rt: ReadTask) {
        let ts1 = Instant::now();
        let mut prc = 0;
        let fd = rt.fd;
        let mut rpos = rt.pos;
        let mut buf = BytesMut::with_capacity(rt.count as usize);
        let mut writable = rt.count as usize;
        let rr = loop {
            if DO_TRACE {
                trace!("do pread  fd {fd}  count {writable}  offset {rpos}  wid {wid}");
            }
            let ec = unsafe { libc::pread(fd, buf.as_mut_ptr() as _, writable, rpos as i64) };
            prc += 1;
            if ec == -1 {
                let errno = unsafe { *libc::__errno_location() };
                if errno == libc::EINVAL {
                    debug!("pread  EOF  fd {fd}  count {writable}  offset {rpos}  wid {wid}");
                    let rr = ReadResult { buf, eof: true };
                    break Ok(rr);
                } else {
                    warn!("pread  ERROR  errno {errno}  fd {fd}  count {writable}  offset {rpos}  wid {wid}");
                    // TODO use a more structured error
                    let e = Error::with_msg_no_trace(format!(
                        "pread  ERROR  errno {errno}  fd {fd}  count {writable}  offset {rpos}  wid {wid}"
                    ));
                    break Err(e);
                }
            } else if ec == 0 {
                debug!("pread  EOF  fd {fd}  count {writable}  offset {rpos}  wid {wid}  prc {prc}");
                let rr = ReadResult { buf, eof: true };
                break Ok(rr);
            } else if ec > 0 {
                if ec as usize > writable {
                    error!("pread  TOOLARGE  ec {ec}  fd {fd}  count {writable}  offset {rpos}  wid {wid}  prc {prc}");
                    return;
                } else {
                    rpos += ec as u64;
                    writable -= ec as usize;
                    unsafe { buf.set_len(buf.len() + (ec as usize)) };
                    if writable == 0 {
                        let ts2 = Instant::now();
                        let dur = ts2.duration_since(ts1);
                        let dms = 1e3 * dur.as_secs_f32();
                        if DO_TRACE {
                            trace!("pread  DONE  ec {ec}  fd {fd}  wid {wid}  prc {prc}  dms {dms:.2}");
                        }
                        let rr = ReadResult { buf, eof: false };
                        break Ok(rr);
                    }
                }
            } else {
                error!(
                    "pread  UNEXPECTED  ec {}  fd {}  count {}  offset {rpos}  wid {wid}",
                    ec, rt.fd, writable
                );
                return;
            }
        };
        match rt.rescell.send(rr) {
            Ok(_) => {}
            Err(_) => {
                self.can_not_publish.fetch_add(1, Ordering::AcqRel);
                warn!("can not publish the read result  wid {wid}");
            }
        }
    }
}
