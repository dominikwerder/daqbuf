use crate::log::*;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::future::ready;
use futures_util::stream;
use futures_util::Stream;
use futures_util::StreamExt;

fn pad_to_8(buf: &mut Vec<u8>) {
    let npadded = (7 + buf.len()) & (!0x7);
    let npad = npadded - buf.len();
    buf.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0][..npad]);
}

pub fn bytes_chunks_to_framed<S, T, E>(stream: S) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<T, E>>,
    T: Into<Bytes>,
    E: std::error::Error,
{
    stream
        // TODO unify this map to padded bytes for both json and cbor output
        .flat_map(|x| match x {
            Ok(y) => {
                let buf = y.into();
                let n = buf.len();
                if n == 0 {
                    info!("skip framing of zero sized chunk");
                    stream::iter([Ok(Bytes::new()), Ok(Bytes::new()), Ok(Bytes::new())])
                } else {
                    let adv = (n + 7) & (!0x7);
                    let pad = adv - n;
                    let mut b2 = BytesMut::with_capacity(16);
                    b2.put_u32_le(n as u32);
                    b2.put_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
                    let mut b3 = BytesMut::with_capacity(16);
                    b3.put_slice(&[0, 0, 0, 0, 0, 0, 0, 0][..pad]);
                    stream::iter([Ok(b2.freeze()), Ok(buf), Ok(b3.freeze())])
                }
            }
            Err(e) => {
                error!("{}", e);
                stream::iter([Ok(Bytes::new()), Ok(Bytes::new()), Ok(Bytes::new())])
            }
        })
        .filter(|x| {
            if let Ok(x) = x {
                ready(x.len() > 0)
            } else {
                ready(true)
            }
        })
}

// TODO move this, it's also used by binned.
pub fn bytes_chunks_to_len_framed_str<S, T, E>(stream: S) -> impl Stream<Item = Result<String, E>>
where
    S: Stream<Item = Result<T, E>>,
    T: Into<String>,
    E: std::error::Error,
{
    stream
        .flat_map(|x| match x {
            Ok(y) => {
                use std::fmt::Write;
                let s = y.into();
                let mut b2 = String::with_capacity(16);
                write!(b2, "{:15}\n", s.len()).unwrap();
                stream::iter([Ok::<_, E>(b2), Ok(s), Ok(String::from("\n"))])
            }
            Err(e) => {
                error!("{}", e);
                stream::iter([Ok(String::new()), Ok(String::new()), Ok(String::new())])
            }
        })
        .filter(|x| {
            if let Ok(x) = x {
                ready(x.len() > 0)
            } else {
                ready(true)
            }
        })
}
