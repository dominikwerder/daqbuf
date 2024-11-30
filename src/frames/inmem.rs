use crate::slidebuf::SlideBuf;
use bytes::Bytes;
use futures_util::pin_mut;
use futures_util::Stream;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::SitemErrTy;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::TERM_FRAME_TYPE_ID;
use items_2::framable::INMEM_FRAME_FOOT;
use items_2::framable::INMEM_FRAME_HEAD;
use items_2::framable::INMEM_FRAME_MAGIC;
use items_2::inmem::InMemoryFrame;
use netpod::log::*;
use netpod::ByteSize;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "InMem")]
pub enum Error {
    Input,
    Slidebuf(#[from] crate::slidebuf::Error),
    IO(#[from] std::io::Error),
    LessThanNeedMin,
    LessThanHeader,
    HugeFrame(u32),
    BadMagic(u32),
    TryFromSlice(#[from] std::array::TryFromSliceError),
    BadCrc,
    EnoughInputNothingParsed,
}

pub type BoxedBytesStream = Pin<Box<dyn Stream<Item = Result<Bytes, SitemErrTy>> + Send>>;

macro_rules! trace2 { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ); }

/// Interprets a byte stream as length-delimited frames.
///
/// Emits each frame as a single item. Therefore, each item must fit easily into memory.
pub struct InMemoryFrameStream<T, E>
where
    T: Stream<Item = Result<Bytes, E>> + Unpin,
{
    inp: T,
    // TODO since we moved to input stream of Bytes, we have the danger that the ring buffer
    // is not large enough. Actually, this should rather use a RopeBuf with incoming owned bufs.
    buf: SlideBuf,
    need_min: usize,
    done: bool,
    complete: bool,
    inp_bytes_consumed: u64,
}

impl<T, E> InMemoryFrameStream<T, E>
where
    T: Stream<Item = Result<Bytes, E>> + Unpin,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: T, bufcap: ByteSize) -> Self {
        Self {
            inp,
            buf: SlideBuf::new(bufcap.bytes() as usize),
            need_min: INMEM_FRAME_HEAD,
            done: false,
            complete: false,
            inp_bytes_consumed: 0,
        }
    }

    fn poll_upstream(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<usize, Error>> {
        trace2!("poll_upstream");
        use Poll::*;
        // use tokio::io::AsyncRead;
        // use tokio::io::ReadBuf;
        // let mut buf = ReadBuf::new(self.buf.available_writable_area(self.need_min.saturating_sub(self.buf.len()))?);
        let inp = &mut self.inp;
        pin_mut!(inp);
        match inp.poll_next(cx) {
            Ready(Some(Ok(x))) => match self.buf.available_writable_area(x.len()) {
                Ok(dst) => {
                    dst[..x.len()].copy_from_slice(&x);
                    self.buf.wadv(x.len())?;
                    Ready(Ok(x.len()))
                }
                Err(e) => Ready(Err(e.into())),
            },
            Ready(Some(Err(_e))) => Ready(Err(Error::Input)),
            Ready(None) => Ready(Ok(0)),
            Pending => Pending,
        }
        // match AsyncRead::poll_read(inp, cx, &mut buf) {
        //     Ready(Ok(())) => {
        //         let n = buf.filled().len();
        //         self.buf.wadv(n)?;
        //         trace2!("recv bytes {}", n);
        //         Ready(Ok(n))
        //     }
        //     Ready(Err(e)) => Ready(Err(e.into())),
        //     Pending => Pending,
        // }
    }

    // Try to consume bytes to parse a frame.
    // Update the need_min to the most current state.
    // Must only be called when at least `need_min` bytes are available.
    fn parse(&mut self) -> Result<Option<InMemoryFrame>, Error> {
        let buf = self.buf.data();
        if buf.len() < self.need_min {
            return Err(Error::LessThanNeedMin);
        }
        if buf.len() < INMEM_FRAME_HEAD {
            return Err(Error::LessThanHeader);
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into()?);
        let encid = u32::from_le_bytes(buf[4..8].try_into()?);
        let tyid = u32::from_le_bytes(buf[8..12].try_into()?);
        let len = u32::from_le_bytes(buf[12..16].try_into()?);
        let payload_crc_exp = u32::from_le_bytes(buf[16..20].try_into()?);
        if magic != INMEM_FRAME_MAGIC {
            let n = buf.len().min(64);
            let u = String::from_utf8_lossy(&buf[0..n]);
            let msg = format!(
                "InMemoryFrameAsyncReadStream  tryparse  incorrect magic: {}  buf as utf8: {:?}",
                magic, u
            );
            error!("{msg}");
            return Err(Error::BadMagic(magic));
        }
        if len > 1024 * 1024 * 50 {
            let msg = format!(
                "InMemoryFrameAsyncReadStream  tryparse  huge buffer  len {}  self.inp_bytes_consumed {}",
                len, self.inp_bytes_consumed
            );
            error!("{msg}");
            return Err(Error::HugeFrame(len));
        }
        let lentot = INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + len as usize;
        if buf.len() < lentot {
            // TODO count cases in production
            self.need_min = lentot;
            return Ok(None);
        }
        let p1 = INMEM_FRAME_HEAD + len as usize;
        let mut h = crc32fast::Hasher::new();
        h.update(&buf[..p1]);
        let frame_crc = h.finalize();
        let mut h = crc32fast::Hasher::new();
        h.update(&buf[INMEM_FRAME_HEAD..p1]);
        let payload_crc = h.finalize();
        let frame_crc_ind = u32::from_le_bytes(buf[p1..p1 + 4].try_into()?);
        let payload_crc_match = payload_crc_exp == payload_crc;
        let frame_crc_match = frame_crc_ind == frame_crc;
        if !frame_crc_match || !payload_crc_match {
            let _ss = String::from_utf8_lossy(&buf[..buf.len().min(256)]);
            let msg = format!(
                "InMemoryFrameAsyncReadStream  tryparse  crc mismatch A  {}  {}",
                payload_crc_match, frame_crc_match,
            );
            error!("{msg}");
            let e = Error::BadCrc;
            return Err(e);
        }
        self.inp_bytes_consumed += lentot as u64;
        // TODO metrics
        //trace!("parsed frame well  len {}", len);
        let ret = InMemoryFrame {
            len,
            tyid,
            encid,
            buf: Bytes::from(buf[INMEM_FRAME_HEAD..p1].to_vec()),
        };
        self.buf.adv(lentot)?;
        self.need_min = INMEM_FRAME_HEAD;
        Ok(Some(ret))
    }
}

impl<T, E> Stream for InMemoryFrameStream<T, E>
where
    T: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Sitemty<InMemoryFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = span!(Level::INFO, "InMemRd");
        let _spanguard = span.enter();
        loop {
            break if self.complete {
                panic!("{} poll_next on complete", Self::type_name())
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.buf.len() >= self.need_min {
                match self.parse() {
                    Ok(None) => {
                        if self.buf.len() >= self.need_min {
                            self.done = true;
                            let e = Error::EnoughInputNothingParsed;
                            Ready(Some(Err(sitem_err2_from_string(e))))
                        } else {
                            continue;
                        }
                    }
                    Ok(Some(item)) => {
                        if item.tyid() == TERM_FRAME_TYPE_ID {
                            self.done = true;
                            continue;
                        } else {
                            let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                            Ready(Some(item))
                        }
                    }
                    Err(e) => {
                        self.done = true;
                        Ready(Some(Err(sitem_err2_from_string(e))))
                    }
                }
            } else {
                match self.as_mut().poll_upstream(cx) {
                    Ready(Ok(n1)) => {
                        if n1 == 0 {
                            self.done = true;
                            continue;
                        } else {
                            continue;
                        }
                    }
                    Ready(Err(e)) => {
                        error!(
                            "poll_upstream  need_min {}  buf {:?}  {:?}",
                            self.need_min, self.buf, e
                        );
                        self.done = true;
                        Ready(Some(Err(sitem_err2_from_string(e))))
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
