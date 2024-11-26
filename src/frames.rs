pub mod eventsfromframes;
pub mod inmem;

use bytes::Bytes;
use bytes::BytesMut;
use futures_util::Stream;
use futures_util::StreamExt;
use items_2::framable::Framable;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "FramedStreamError")]
pub enum Error {
    MakeFrame(#[from] items_2::framable::Error),
}

pub fn frameable_stream_to_bytes_stream<S, T>(stream: S) -> impl Stream<Item = Result<Bytes, Error>>
where
    S: Stream<Item = T>,
    T: Framable,
{
    stream.map(|x| {
        x.make_frame_dyn()
            .map(BytesMut::freeze)
            .map_err(|e| e.into())
    })
}
