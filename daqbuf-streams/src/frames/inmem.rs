use crate::log;
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
use items_2::framable::INMEM_FRAME_HEAD;
use items_2::inmem::InMemoryFrame;
use netpod::ByteSize;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "InMem"),
    enum variants {
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
        InMemParse(#[from] items_2::inmem::Error),
    },
);

pub type BoxedBytesStream = Pin<Box<dyn Stream<Item = Result<Bytes, SitemErrTy>> + Send>>;

macro_rules! trace2 { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ); }

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
        let inp = &mut self.inp;
        pin_mut!(inp);
        match inp.poll_next(cx) {
            Ready(Some(Ok(x))) => match self.buf.available_writable_area(x.len()) {
                Ok(dst) => {
                    dst[..x.len()].copy_from_slice(&x);
                    self.buf.wadv(x.len())?;
                    Ready(Ok(x.len()))
                }
                Err(e) => {
                    log::error!(
                        "{}  {:?}  inp len {}  need_min {}",
                        e,
                        self.buf,
                        x.len(),
                        self.need_min
                    );
                    Ready(Err(e.into()))
                }
            },
            Ready(Some(Err(_e))) => Ready(Err(Error::Input)),
            Ready(None) => Ready(Ok(0)),
            Pending => Pending,
        }
    }

    // Try to consume bytes to parse a frame.
    // Update the need_min to the most current state.
    // Must only be called when at least `need_min` bytes are available.
    fn parse(&mut self) -> Result<Option<InMemoryFrame>, Error> {
        let buf = self.buf.data();
        if buf.len() < self.need_min {
            return Err(Error::LessThanNeedMin);
        }
        use items_2::inmem::ParseResult;
        match InMemoryFrame::parse(buf) {
            Ok(x) => match x {
                ParseResult::NotEnoughData(n) => {
                    self.need_min = n;
                    Ok(None)
                }
                ParseResult::Parsed(lentot, val) => {
                    self.buf.adv(lentot)?;
                    self.need_min = INMEM_FRAME_HEAD;
                    self.inp_bytes_consumed += lentot as u64;
                    Ok(Some(val))
                }
            },
            Err(e) => Err(e.into()),
        }
    }
}

impl<T, E> Stream for InMemoryFrameStream<T, E>
where
    T: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Sitemty<InMemoryFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = log::span!(log::Level::INFO, "InMemRd");
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
                        log::error!(
                            "poll_upstream  need_min {}  buf {:?}  {:?}",
                            self.need_min,
                            self.buf,
                            e
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
