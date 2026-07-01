use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::Stream;
use futures_util::StreamExt;
use netpod::log::*;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

const FRAME_HEAD_LEN: usize = 16;
const FRAME_PAYLOAD_MAX: u32 = 1024 * 1024 * 8;
const BUF_MAX: usize = (FRAME_HEAD_LEN + FRAME_PAYLOAD_MAX as usize) * 2;

macro_rules! trace_parse { ($($arg:expr),*) => ( if false { trace!($($arg),*); } ); }

autoerr::create_error_v1!(
    name(Error, "StreamFramedBytes"),
    enum variants {
        DataInput,
        FrameTooLarge,
    },
);

pub type BoxedFramedBytesStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

// TODO move this type decl because it is not specific to cbor
pub type SitemtyFramedBytesStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

pub enum State {
    Reading,
    Done,
}

pub struct FramedBytesStream<S> {
    inp: S,
    buf: BytesMut,
    state: State,
}

impl<S, E> FramedBytesStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Error>,
{
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            buf: BytesMut::with_capacity(1024 * 256),
            state: State::Reading,
        }
    }

    fn try_parse(&mut self) -> Result<Option<Bytes>, Error> {
        trace_parse!("try_parse  self.buf.len() {}", self.buf.len());
        if self.buf.len() < FRAME_HEAD_LEN {
            return Ok(None);
        }
        let n = u32::from_le_bytes(self.buf[..4].try_into().map_err(|_| Error::DataInput)?);
        trace_parse!("try_parse  n {}", n);
        if n > FRAME_PAYLOAD_MAX {
            let e = Error::FrameTooLarge;
            return Err(e);
        }
        let frame_len = FRAME_HEAD_LEN + n as usize;
        trace_parse!("try_parse  frame_len {}", frame_len);
        assert!(self.buf.len() <= self.buf.capacity());
        if self.buf.capacity() < frame_len {
            let add_max = BUF_MAX - self.buf.capacity().min(BUF_MAX);
            let nadd =
                ((frame_len.min(FRAME_PAYLOAD_MAX as usize) - self.buf.len()) * 2).min(add_max);
            self.buf.reserve(nadd);
        }
        let adv = (frame_len + 7) / 8 * 8;
        trace_parse!("try_parse  adv {}", adv);
        if self.buf.len() < adv {
            Ok(None)
        } else {
            self.buf.advance(FRAME_HEAD_LEN);
            let buf = self.buf.split_to(n as usize);
            self.buf.advance(adv - frame_len);
            Ok(Some(buf.freeze()))
        }
    }
}

impl<S, E> Stream for FramedBytesStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Error>,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &self.state {
                State::Reading => match self.try_parse() {
                    Ok(Some(x)) => Ready(Some(Ok(x))),
                    Ok(None) => match self.inp.poll_next_unpin(cx) {
                        Ready(Some(x)) => match x {
                            Ok(x) => {
                                self.buf.put_slice(&x);
                                continue;
                            }
                            Err(e) => {
                                self.state = State::Done;
                                Ready(Some(Err(e.into())))
                            }
                        },
                        Ready(None) => {
                            if self.buf.len() > 0 {
                                warn!(
                                    "remaining bytes in input buffer, input closed  len {}",
                                    self.buf.len()
                                );
                            }
                            self.state = State::Done;
                            Ready(None)
                        }
                        Pending => Pending,
                    },
                    Err(e) => {
                        self.state = State::Done;
                        Ready(Some(Err(e)))
                    }
                },
                State::Done => Ready(None),
            };
        }
    }
}
