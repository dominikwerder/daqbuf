use async_channel::Send;
use async_channel::Sender;
use futures_util::Future;
use futures_util::Stream;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "ItemClone")]
pub enum Error {}

#[pin_project::pin_project]
pub struct Itemclone<'a, T, INP>
where
    T: 'static,
{
    #[pin]
    sender: Pin<Box<Sender<T>>>,
    #[pin]
    inp: INP,
    send_fut: Option<Pin<Box<Send<'a, T>>>>,
}

impl<'a, T, INP> Itemclone<'a, T, INP> {
    pub fn new(inp: INP, sender: Sender<T>) -> Self
    where
        INP: Stream<Item = T> + Unpin,
        T: Clone + Unpin,
    {
        let sender = Box::pin(sender);
        Self {
            sender,
            inp,
            send_fut: None,
        }
    }
}

unsafe fn extend<'a, 'b, T>(t: &'a mut T) -> &'b mut T {
    core::mem::transmute(t)
}

impl<'a, T, INP> Itemclone<'a, T, INP>
where
    INP: Stream<Item = T> + Unpin,
    T: Clone + Unpin,
{
    fn poll_fresh(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<T, Error>>> {
        use Poll::*;
        let selfproj = self.as_mut().project();
        match selfproj.inp.poll_next(cx) {
            Ready(Some(item)) => {
                let r1 = selfproj.sender.get_mut().as_mut().get_mut();
                let sender = unsafe { extend(r1) };
                let fut = sender.send(item.clone());
                *selfproj.send_fut = Some(Box::pin(fut));
                Ready(Some(Ok(item)))
            }
            Ready(None) => {
                self.sender.close();
                Ready(None)
            }
            Pending => Pending,
        }
    }

    fn send_copy(fut: Pin<&mut Send<T>>, cx: &mut Context) -> Poll<Result<(), Error>> {
        use Poll::*;
        match fut.poll(cx) {
            Ready(Ok(())) => Ready(Ok(())),
            Ready(Err(_)) => todo!("can not send copy"),
            Pending => Pending,
        }
    }
}

impl<'a, T, INP> Stream for Itemclone<'a, T, INP>
where
    INP: Stream<Item = T> + Unpin,
    T: Clone + Unpin,
{
    type Item = Result<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfproj = self.as_mut().project();
        match selfproj.send_fut {
            Some(fut) => {
                let fut = fut.as_mut();
                match Self::send_copy(fut, cx) {
                    Ready(Ok(())) => self.poll_fresh(cx),
                    Ready(Err(e)) => Ready(Some(Err(e))),
                    Pending => Pending,
                }
            }
            None => self.poll_fresh(cx),
        }
    }
}
