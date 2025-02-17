use async_channel::Receiver;
use async_channel::Sender;
use futures_util::Stream;
use futures_util::StreamExt;
use pin_project_lite::pin_project;
use std::pin::pin;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "ChannelCombine"),
    enum variants {
        Logic,
    },
);

pin_project! {
    pub struct ChannelCombineAB<T> {
        #[pin]
        a: Receiver<T>,
        #[pin]
        b: Receiver<T>,
        #[pin]
        tx: Sender<T>,
    }
}

impl<T> ChannelCombineAB<T>
where
    T: Send + Unpin + 'static,
{
    pub fn new(a: Receiver<T>, b: Receiver<T>) -> Receiver<T> {
        let capdef = 10;
        let cap = a
            .capacity()
            .unwrap_or(capdef)
            .max(b.capacity().unwrap_or(capdef));
        let (tx, rx) = async_channel::bounded(cap);
        tokio::spawn(Self::run(Self::inner_new(a, b, tx)));
        rx
    }

    fn inner_new(a: Receiver<T>, b: Receiver<T>, tx: Sender<T>) -> Self {
        Self { a, b, tx }
    }

    async fn run(obj: Self) {
        let mut o2 = pin!(obj);
        while let Some(x) = o2.next().await {
            match o2.tx.send(x).await {
                Ok(()) => {}
                Err(_) => {
                    break;
                }
            }
        }
    }
}

impl<T> Stream for ChannelCombineAB<T>
where
    T: 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let mut this = self.project();
        match this.a.poll_next_unpin(cx) {
            Ready(x) => Ready(x),
            Pending => match this.b.poll_next_unpin(cx) {
                Ready(x) => Ready(x),
                Pending => Pending,
            },
        }
    }
}
