#[cfg(test)]
pub mod aggtest;
pub mod binnedstream;
pub mod channelconfig;
pub mod datagen;
pub mod dataopen;
pub mod decode;
pub mod eventchunker;
pub mod eventchunkermultifile;
pub mod eventfilter;
pub mod frame;
pub mod index;
pub mod merge;
pub mod paths;
pub mod raw;
pub mod read3;
pub mod read4;
pub mod streamlog;

pub use parse;

use async_channel::Receiver;
use async_channel::Sender;
use bytes::BytesMut;
use daqbuf_err as err;
use err::Error;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use futures_util::future::FusedFuture;
use netpod::ByteOrder;
use netpod::DiskIoTune;
use netpod::DtNano;
use netpod::ReadSys;
use netpod::ScalarType;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::log::*;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;
use std::future::Future;
use std::io::SeekFrom;
use std::mem;
use std::os::unix::prelude::AsRawFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use streams::dtflags::ARRAY;
use streams::dtflags::BIG_ENDIAN;
use streams::dtflags::COMPRESSION;
use streams::dtflags::SHAPE;
use streams::filechunkread::FileChunkRead;
use taskrun::tokio;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::ReadBuf;
use tokio::sync::mpsc;
use tracing::Instrument;

// TODO move to databuffer-specific crate
// TODO duplicate of SfChFetchInfo?
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SfDbChConf {
    pub channel: SfDbChannel,
    pub keyspace: u8,
    pub time_bin_size: DtNano,
    pub scalar_type: ScalarType,
    pub compression: bool,
    pub shape: Shape,
    pub array: bool,
    pub byte_order: ByteOrder,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggQuerySingleChannel {
    pub channel_config: SfDbChConf,
    pub timebin: u32,
    pub tb_file_count: u32,
    pub buffer_size: u32,
}

pub struct Fopen1 {
    #[allow(dead_code)]
    opts: OpenOptions,
    fut: Pin<Box<dyn Future<Output = Result<File, std::io::Error>>>>,
    term: bool,
}

impl Fopen1 {
    pub fn new(path: PathBuf) -> Self {
        let fut = Box::pin(async {
            let mut o1 = OpenOptions::new();
            let o2 = o1.read(true);
            let res = o2.open(path);
            res.await
        }) as Pin<Box<dyn Future<Output = Result<File, std::io::Error>>>>;
        let _fut2: Box<dyn Future<Output = u32>> = Box::new(async { 123 });
        Self {
            opts: OpenOptions::new(),
            fut,
            term: false,
        }
    }
}

impl Future for Fopen1 {
    type Output = Result<File, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let g = self.fut.as_mut();
        match g.poll(cx) {
            Poll::Ready(Ok(k)) => {
                self.term = true;
                Poll::Ready(Ok(k))
            }
            Poll::Ready(Err(k)) => {
                self.term = true;
                Poll::Ready(Err(k.into()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl FusedFuture for Fopen1 {
    fn is_terminated(&self) -> bool {
        self.term
    }
}

unsafe impl Send for Fopen1 {}

pub struct FileContentStream {
    file: File,
    cap: usize,
    disk_io_tune: DiskIoTune,
    read_going: bool,
    buf: BytesMut,
    ts1: Instant,
    nlog: usize,
    done: bool,
    complete: bool,
    total_read: usize,
}

impl Drop for FileContentStream {
    fn drop(&mut self) {
        debug!("FileContentStream  total_read {}  cap {}", self.total_read, self.cap);
    }
}

impl FileContentStream {
    pub fn self_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        let cap = disk_io_tune.read_buffer_len;
        let buf = Self::prepare_buf(cap);
        Self {
            file,
            cap,
            disk_io_tune,
            read_going: false,
            buf,
            ts1: Instant::now(),
            nlog: 0,
            done: false,
            complete: false,
            total_read: 0,
        }
    }

    fn prepare_buf(cap: usize) -> BytesMut {
        let mut buf = BytesMut::with_capacity(cap);
        unsafe {
            // SAFETY if we got here, then we have the required capacity
            buf.set_len(buf.capacity());
        }
        buf
    }

    fn mut_file_and_buf(&mut self) -> (&mut File, &mut BytesMut) {
        (&mut self.file, &mut self.buf)
    }
}

impl Stream for FileContentStream {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("{} poll_next on complete", Self::self_name())
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                if !self.read_going {
                    self.read_going = true;
                    self.ts1 = Instant::now();
                    // TODO remove
                    // std::thread::sleep(Duration::from_millis(10));
                }
                let (file, buf) = self.mut_file_and_buf();
                let mut rb = ReadBuf::new(buf.as_mut());
                futures_util::pin_mut!(file);
                match AsyncRead::poll_read(file, cx, &mut rb) {
                    Ready(Ok(())) => {
                        let nread = rb.filled().len();
                        if nread < rb.capacity() {
                            debug!("read less than capacity  {} vs {}", nread, rb.capacity());
                        }
                        let cap = self.cap;
                        let mut buf = mem::replace(&mut self.buf, Self::prepare_buf(cap));
                        buf.truncate(nread);
                        self.read_going = false;
                        self.total_read += nread;
                        let ts2 = Instant::now();
                        if nread == 0 {
                            let ret = FileChunkRead::with_buf_dur(buf, ts2.duration_since(self.ts1));
                            self.done = true;
                            Ready(Some(Ok(ret)))
                        } else {
                            let ret = FileChunkRead::with_buf_dur(buf, ts2.duration_since(self.ts1));
                            if false && self.nlog < 6 {
                                self.nlog += 1;
                                debug!("{:?}  ret {:?}", self.disk_io_tune, ret);
                            }
                            Ready(Some(Ok(ret)))
                        }
                    }
                    Ready(Err(e)) => {
                        self.done = true;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

fn start_read5(
    path: PathBuf,
    file: File,
    tx: async_channel::Sender<Result<FileChunkRead, Error>>,
    disk_io_tune: DiskIoTune,
    reqid: String,
) -> Result<(), Error> {
    warn!("start_read5");
    let fut = async move {
        let mut file = file;
        let pos_beg = match file.stream_position().await {
            Ok(x) => x,
            Err(e) => {
                error!("stream_position  {e}  {path:?}");
                if let Err(_) = tx
                    .send(Err(Error::with_msg_no_trace(format!("seek error {path:?}"))))
                    .await
                {
                    error!("broken channel");
                }
                return;
            }
        };
        let mut pos = pos_beg;
        debug!("read5  begin  {disk_io_tune:?}");
        loop {
            let mut buf = BytesMut::new();
            buf.resize(disk_io_tune.read_buffer_len, 0);
            match tokio::time::timeout(Duration::from_millis(8000), file.read(&mut buf)).await {
                Ok(Ok(n)) => {
                    if n == 0 {
                        //info!("read5 EOF  pos_beg {pos_beg}  pos {pos}  path {path:?}");
                        break;
                    }
                    pos += n as u64;
                    buf.truncate(n);
                    let item = FileChunkRead::with_buf(buf);
                    match tx.send(Ok(item)).await {
                        Ok(()) => {}
                        Err(_) => {
                            //error!("broken channel");
                            break;
                        }
                    }
                }
                Ok(Err(e)) => match tx.send(Err(e.into())).await {
                    Ok(()) => {
                        break;
                    }
                    Err(_) => {
                        //error!("broken channel");
                        break;
                    }
                },
                Err(_) => {
                    let msg = format!("I/O timeout  pos_beg {pos_beg}  pos {pos}  path {path:?}");
                    error!("{msg}");
                    let e = Error::with_msg_no_trace(msg);
                    match tx.send(Err(e)).await {
                        Ok(()) => {}
                        Err(_e) => {
                            //error!("broken channel");
                            break;
                        }
                    }
                    break;
                }
            }
        }
        let n = pos - pos_beg;
        debug!("read5  done  {n}");
    };
    let span = tracing::span!(tracing::Level::INFO, "read5", reqid);
    let fut = fut.instrument(span);
    tokio::task::spawn(fut);
    Ok(())
}

pub struct FileContentStream5 {
    rx: async_channel::Receiver<Result<FileChunkRead, Error>>,
}

impl FileContentStream5 {
    pub fn new(path: PathBuf, file: File, disk_io_tune: DiskIoTune, reqid: String) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(32);
        start_read5(path, file, tx, disk_io_tune, reqid)?;
        let ret = Self { rx };
        Ok(ret)
    }
}

impl Stream for FileContentStream5 {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        unsafe { self.map_unchecked_mut(|x| &mut x.rx) }.poll_next_unpin(cx)
    }
}

enum FCS2 {
    Idle,
    Reading(
        (
            Box<BytesMut>,
            Pin<Box<dyn Future<Output = Result<usize, Error>> + Send>>,
        ),
    ),
}

pub struct FileContentStream2 {
    fcs: FCS2,
    file: Pin<Box<File>>,
    disk_io_tune: DiskIoTune,
    done: bool,
    complete: bool,
}

impl FileContentStream2 {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        let file = Box::pin(file);
        Self {
            fcs: FCS2::Idle,
            file,
            disk_io_tune,
            done: false,
            complete: false,
        }
    }

    #[allow(unused_mut)]
    fn make_reading(&mut self) {
        let mut buf = Box::new(BytesMut::with_capacity(self.disk_io_tune.read_buffer_len));
        // let bufref = unsafe { &mut *((&mut buf as &mut BytesMut) as *mut BytesMut) };
        let bufref: &mut BytesMut = err::todoval();
        let fileref = unsafe { &mut *((&mut self.file) as *mut Pin<Box<File>>) };
        let fut = AsyncReadExt::read_buf(fileref, bufref).map_err(|e| e.into());
        self.fcs = FCS2::Reading((buf, Box::pin(fut)));
    }
}

impl Stream for FileContentStream2 {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("{} poll_next on complete", Self::type_name())
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                match self.fcs {
                    FCS2::Idle => {
                        self.make_reading();
                        continue;
                    }
                    FCS2::Reading((ref mut buf, ref mut fut)) => match fut.poll_unpin(cx) {
                        Ready(Ok(n)) => {
                            let buf2 = std::mem::replace(buf as &mut BytesMut, BytesMut::new());
                            let item = FileChunkRead::with_buf(buf2);
                            if n == 0 {
                                self.done = true;
                            } else {
                                self.make_reading();
                            }
                            Ready(Some(Ok(item)))
                        }
                        Ready(Err(e)) => {
                            self.done = true;
                            Ready(Some(Err(e.into())))
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}

enum FCS3 {
    GetPosition,
    ReadingSimple,
    Reading,
}

enum ReadStep {
    Fut(Pin<Box<dyn Future<Output = Result<read3::ReadResult, Error>> + Send>>),
    Res(Result<read3::ReadResult, Error>),
}

pub struct FileContentStream3 {
    fcs: FCS3,
    file: Pin<Box<File>>,
    file_pos: u64,
    eof: bool,
    disk_io_tune: DiskIoTune,
    get_position_fut: Pin<Box<dyn Future<Output = Result<u64, Error>> + Send>>,
    read_fut: Pin<Box<dyn Future<Output = Result<read3::ReadResult, Error>> + Send>>,
    reads: VecDeque<ReadStep>,
    done: bool,
    complete: bool,
}

impl FileContentStream3 {
    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        let mut file = Box::pin(file);
        let ffr = unsafe {
            let ffr = Pin::get_unchecked_mut(file.as_mut());
            std::mem::transmute::<&mut File, &'static mut File>(ffr)
        };
        let ff = ffr
            .seek(SeekFrom::Current(0))
            .map_err(|_| Error::with_msg_no_trace(format!("Seek error")));
        Self {
            fcs: FCS3::GetPosition,
            file,
            file_pos: 0,
            eof: false,
            disk_io_tune,
            get_position_fut: Box::pin(ff),
            read_fut: Box::pin(futures_util::future::ready(Err(Error::with_msg_no_trace(format!(
                "dummy"
            ))))),
            reads: VecDeque::new(),
            done: false,
            complete: false,
        }
    }
}

impl Stream for FileContentStream3 {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                match self.fcs {
                    FCS3::GetPosition => match self.get_position_fut.poll_unpin(cx) {
                        Ready(Ok(k)) => {
                            info!("current file pos: {k}");
                            self.file_pos = k;
                            if false {
                                let fd = self.file.as_raw_fd();
                                let count = self.disk_io_tune.read_buffer_len as u64;
                                self.read_fut = Box::pin(read3::Read3::get().read(fd, self.file_pos, count));
                                self.file_pos += count;
                                self.fcs = FCS3::ReadingSimple;
                            } else {
                                self.fcs = FCS3::Reading;
                            }
                            continue;
                        }
                        Ready(Err(e)) => {
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                        Pending => Pending,
                    },
                    FCS3::ReadingSimple => match self.read_fut.poll_unpin(cx) {
                        Ready(Ok(res)) => {
                            if res.eof {
                                let item = FileChunkRead::with_buf(res.buf);
                                self.done = true;
                                Ready(Some(Ok(item)))
                            } else {
                                let item = FileChunkRead::with_buf(res.buf);
                                let fd = self.file.as_raw_fd();
                                let count = self.disk_io_tune.read_buffer_len as u64;
                                self.read_fut = Box::pin(read3::Read3::get().read(fd, self.file_pos, count));
                                self.file_pos += count;
                                Ready(Some(Ok(item)))
                            }
                        }
                        Ready(Err(e)) => {
                            self.done = true;
                            Ready(Some(Err(e)))
                        }
                        Pending => Pending,
                    },
                    FCS3::Reading => {
                        while !self.eof && self.reads.len() < self.disk_io_tune.read_queue_len {
                            let fd = self.file.as_raw_fd();
                            let pos = self.file_pos;
                            let count = self.disk_io_tune.read_buffer_len as u64;
                            trace!("create ReadTask  fd {fd}  pos {pos}  count {count}");
                            let r3 = read3::Read3::get();
                            let fut = r3.read(fd, pos, count);
                            self.reads.push_back(ReadStep::Fut(Box::pin(fut)));
                            self.file_pos += count;
                        }
                        for e in &mut self.reads {
                            match e {
                                ReadStep::Fut(k) => match k.poll_unpin(cx) {
                                    Ready(k) => {
                                        trace!("received a result");
                                        *e = ReadStep::Res(k);
                                    }
                                    Pending => {}
                                },
                                ReadStep::Res(_) => {}
                            }
                        }
                        if let Some(ReadStep::Res(_)) = self.reads.front() {
                            if let Some(ReadStep::Res(res)) = self.reads.pop_front() {
                                trace!("pop front result");
                                match res {
                                    Ok(rr) => {
                                        if rr.eof {
                                            if self.eof {
                                                trace!("see EOF in ReadResult  AGAIN");
                                            } else {
                                                debug!("see EOF in ReadResult  SET OUR FLAG");
                                                self.eof = true;
                                            }
                                        }
                                        let res = FileChunkRead::with_buf(rr.buf);
                                        Ready(Some(Ok(res)))
                                    }
                                    Err(e) => {
                                        error!("received ReadResult error: {e}");
                                        self.done = true;
                                        let e = Error::with_msg(format!("I/O error: {e}"));
                                        Ready(Some(Err(e)))
                                    }
                                }
                            } else {
                                self.done = true;
                                let e = Error::with_msg(format!("logic error"));
                                error!("{e}");
                                Ready(Some(Err(e)))
                            }
                        } else if let None = self.reads.front() {
                            debug!("empty read fut queue, end");
                            self.done = true;
                            continue;
                        } else {
                            trace!("read fut queue Pending");
                            Pending
                        }
                    }
                }
            };
        }
    }
}

enum FCS4 {
    Init,
    Setup,
    Reading,
}

pub struct FileContentStream4 {
    fcs: FCS4,
    file: Pin<Box<File>>,
    disk_io_tune: DiskIoTune,
    setup_fut:
        Option<Pin<Box<dyn Future<Output = Result<mpsc::Receiver<Result<read4::ReadResult, Error>>, Error>> + Send>>>,
    inp: Option<mpsc::Receiver<Result<read4::ReadResult, Error>>>,
    recv_fut: Pin<Box<dyn Future<Output = Option<Result<read4::ReadResult, Error>>> + Send>>,
    done: bool,
    complete: bool,
}

impl FileContentStream4 {
    pub fn new(file: File, disk_io_tune: DiskIoTune) -> Self {
        let file = Box::pin(file);
        Self {
            fcs: FCS4::Init,
            file,
            disk_io_tune,
            setup_fut: None,
            inp: None,
            recv_fut: Box::pin(futures_util::future::ready(Some(Err(Error::with_msg_no_trace(
                format!("dummy"),
            ))))),
            done: false,
            complete: false,
        }
    }
}

impl Stream for FileContentStream4 {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll_next on complete")
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else {
                match self.fcs {
                    FCS4::Init => {
                        let read4 = read4::Read4::get();
                        let fd = self.file.as_raw_fd();
                        let buflen = self.disk_io_tune.read_buffer_len as u64;
                        let fut = read4.read(fd, buflen, self.disk_io_tune.read_queue_len);
                        self.setup_fut = Some(Box::pin(fut) as _);
                        self.fcs = FCS4::Setup;
                        continue;
                    }
                    FCS4::Setup => match self.setup_fut.as_mut().unwrap().poll_unpin(cx) {
                        Ready(k) => match k {
                            Ok(k) => {
                                self.setup_fut = None;
                                self.fcs = FCS4::Reading;
                                self.inp = Some(k);
                                // TODO
                                let rm = self.inp.as_mut().unwrap();
                                let rm = unsafe {
                                    std::mem::transmute::<
                                        &mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                        &'static mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                    >(rm)
                                };
                                self.recv_fut = Box::pin(rm.recv()) as _;
                                continue;
                            }
                            Err(e) => {
                                self.done = true;
                                let e = Error::with_msg_no_trace(format!("init failed {e:?}"));
                                Ready(Some(Err(e)))
                            }
                        },
                        Pending => Pending,
                    },
                    FCS4::Reading => match self.recv_fut.poll_unpin(cx) {
                        Ready(k) => match k {
                            Some(k) => match k {
                                Ok(k) => {
                                    // TODO
                                    let rm = self.inp.as_mut().unwrap();
                                    let rm = unsafe {
                                        std::mem::transmute::<
                                            &mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                            &'static mut mpsc::Receiver<Result<read4::ReadResult, Error>>,
                                        >(rm)
                                    };
                                    self.recv_fut = Box::pin(rm.recv()) as _;
                                    let item = FileChunkRead::with_buf(k.buf);
                                    Ready(Some(Ok(item)))
                                }
                                Err(e) => {
                                    self.done = true;
                                    let e = Error::with_msg_no_trace(format!("init failed {e:?}"));
                                    Ready(Some(Err(e)))
                                }
                            },
                            None => {
                                self.done = true;
                                continue;
                            }
                        },
                        Pending => Pending,
                    },
                }
            };
        }
    }
}

async fn file_tokio_to_std(file: File) -> std::fs::File {
    file.into_std().await
}

struct BlockingTaskIntoChannel {
    convert_file_fut: Option<Pin<Box<dyn Future<Output = std::fs::File> + Send>>>,
    tx: Option<Sender<Result<FileChunkRead, Error>>>,
    rx: Receiver<Result<FileChunkRead, Error>>,
    cap: usize,
    reqid: String,
}

impl BlockingTaskIntoChannel {
    fn new(file: File, disk_io_tune: DiskIoTune, reqid: String) -> Self {
        let (tx, rx) = async_channel::bounded(disk_io_tune.read_queue_len);
        Self {
            convert_file_fut: Some(Box::pin(file_tokio_to_std(file))),
            tx: Some(tx),
            rx,
            cap: disk_io_tune.read_buffer_len,
            reqid,
        }
    }

    fn readloop(mut file: std::fs::File, cap: usize, tx: Sender<Result<FileChunkRead, Error>>, reqid: String) {
        let span = tracing::span!(tracing::Level::DEBUG, "bticrl", reqid);
        let _spg = span.entered();
        loop {
            use std::io::Read;
            let ts1 = Instant::now();
            let mut buf = BytesMut::with_capacity(cap);
            unsafe {
                // SAFETY we can always size up to capacity
                buf.set_len(buf.capacity());
            }
            match file.read(&mut buf) {
                Ok(n) => {
                    if n == 0 {
                        tx.close();
                        break;
                    } else if n > buf.capacity() {
                        let msg = format!("blocking_task_into_channel read more than buffer cap");
                        error!("{msg}");
                        match tx.send_blocking(Err(Error::with_msg_no_trace(msg))) {
                            Ok(()) => (),
                            Err(e) => {
                                error!("blocking_task_into_channel can not send Err into channel {e}");
                            }
                        }
                        break;
                    } else {
                        let ts2 = Instant::now();
                        unsafe {
                            // SAFETY we checked before that n <= capacity
                            buf.set_len(n);
                        }
                        let item = FileChunkRead::with_buf_dur(buf, ts2.duration_since(ts1));
                        match tx.send_blocking(Ok(item)) {
                            Ok(()) => (),
                            Err(_) => {
                                // TODO
                                // Receiver most likely disconnected.
                                // error!("blocking_task_into_channel can not send into channel {e}");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    match tx.send_blocking(Err(e.into())) {
                        Ok(()) => (),
                        Err(e) => {
                            error!("blocking_task_into_channel can not send Err into channel {e}");
                        }
                    }
                    break;
                }
            }
        }
    }

    fn setup(&mut self, file: std::fs::File) {
        let cap = self.cap;
        let tx = self.tx.take().unwrap();
        let reqid = self.reqid.clone();
        taskrun::tokio::task::spawn_blocking(move || Self::readloop(file, cap, tx, reqid));
    }
}

impl Stream for BlockingTaskIntoChannel {
    type Item = Result<FileChunkRead, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let convert_file_fut = &mut unsafe { self.as_mut().get_unchecked_mut() }.convert_file_fut;
        if let Some(cff) = convert_file_fut {
            match cff.poll_unpin(cx) {
                Ready(file) => {
                    *convert_file_fut = None;
                    let this = unsafe { self.as_mut().get_unchecked_mut() };
                    this.setup(file);
                    cx.waker().wake_by_ref();
                    Pending
                }
                Pending => Pending,
            }
        } else {
            let mut rx = unsafe { self.map_unchecked_mut(|x| &mut x.rx) };
            match rx.poll_next_unpin(cx) {
                Ready(Some(Ok(item))) => Ready(Some(Ok(item))),
                Ready(Some(Err(e))) => Ready(Some(Err(e))),
                Ready(None) => Ready(None),
                Pending => Pending,
            }
        }
    }
}

fn blocking_task_into_channel(
    _path: PathBuf,
    file: File,
    disk_io_tune: DiskIoTune,
    reqid: String,
) -> Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>> {
    Box::pin(BlockingTaskIntoChannel::new(file, disk_io_tune, reqid)) as _
}

pub fn file_content_stream<S>(
    path: PathBuf,
    file: File,
    disk_io_tune: DiskIoTune,
    reqid: S,
) -> Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>
where
    S: Into<String>,
{
    if let ReadSys::BlockingTaskIntoChannel = disk_io_tune.read_sys {
    } else {
        warn!("reading via {:?}", disk_io_tune.read_sys);
    }
    let reqid = reqid.into();
    debug!("file_content_stream  disk_io_tune {disk_io_tune:?}");
    match disk_io_tune.read_sys {
        ReadSys::TokioAsyncRead => {
            let s = FileContentStream::new(file, disk_io_tune);
            Box::pin(s) as Pin<Box<dyn Stream<Item = _> + Send>>
        }
        ReadSys::Read2 => {
            let s = FileContentStream2::new(file, disk_io_tune);
            Box::pin(s) as _
        }
        ReadSys::Read3 => {
            let s = FileContentStream3::new(file, disk_io_tune);
            Box::pin(s) as _
        }
        ReadSys::Read4 => {
            let s = FileContentStream4::new(file, disk_io_tune);
            Box::pin(s) as _
        }
        ReadSys::Read5 => {
            let s = FileContentStream5::new(path, file, disk_io_tune, reqid).unwrap();
            Box::pin(s) as _
        }
        ReadSys::BlockingTaskIntoChannel => blocking_task_into_channel(path, file, disk_io_tune, reqid),
    }
}

trait ChannelConfigExt {
    fn dtflags(&self) -> u8;
}

impl ChannelConfigExt for SfDbChConf {
    fn dtflags(&self) -> u8 {
        let mut ret = 0;
        if self.compression {
            ret |= COMPRESSION;
        }
        match self.shape {
            Shape::Scalar => {}
            Shape::Wave(_) => {
                ret |= SHAPE;
            }
            Shape::Image(_, _) => {
                ret |= SHAPE;
            }
        }
        if self.byte_order.is_be() {
            ret |= BIG_ENDIAN;
        }
        if self.array {
            ret |= ARRAY;
        }
        ret
    }
}
