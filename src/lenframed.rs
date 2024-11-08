use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::future;
use futures_util::stream;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::WithLen;

pub fn length_framed<S, T, E>(inp: S) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<T, E>>,
    T: WithLen + Into<Bytes>,
{
    inp.map(|x| match x {
        Ok(x) => {
            let n = x.len() as u32;
            let mut buf1 = BytesMut::with_capacity(8);
            buf1.put_u32_le(n);
            [Some(Ok(buf1.freeze())), Some(Ok(x.into()))]
        }
        Err(e) => [Some(Err(e)), None],
    })
    .map(|x| stream::iter(x))
    .flatten()
    .filter_map(|x| future::ready(x))
}
