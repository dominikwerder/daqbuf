use bytes::Bytes;
use futures_util::Stream;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::io::AsyncRead;

autoerr::create_error_v1!(
    name(Error, "TcpReadAsBytes"),
    enum variants {
        IO(#[from] std::io::Error),
    },
);

pub struct TcpReadAsBytes<INP> {
    inp: INP,
}

impl<INP> TcpReadAsBytes<INP> {
    pub fn new(inp: INP) -> Self {
        Self { inp }
    }
}

impl<INP> Stream for TcpReadAsBytes<INP>
where
    INP: AsyncRead + Unpin,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut buf1 = vec![0; 512];
        let mut buf2 = tokio::io::ReadBuf::new(&mut buf1);
        match tokio::io::AsyncRead::poll_read(Pin::new(&mut self.inp), cx, &mut buf2) {
            Ready(Ok(())) => {
                let n = buf2.filled().len();
                if n == 0 {
                    Ready(None)
                } else {
                    buf1.truncate(n);
                    let item = Bytes::from(buf1);
                    Ready(Some(Ok(item)))
                }
            }
            Ready(Err(e)) => Ready(Some(Err(e.into()))),
            Pending => Pending,
        }
    }
}
