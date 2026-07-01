use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct Enumerate2<T> {
    inp: T,
    cnt: usize,
}

impl<T> Enumerate2<T> {
    pub fn new(inp: T) -> Self {
        Self { inp, cnt: 0 }
    }
}

impl<T> Stream for Enumerate2<T>
where
    T: Stream + Unpin,
{
    type Item = (usize, <T as Stream>::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                let i = self.cnt;
                self.cnt += 1;
                Ready(Some((i, item)))
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct Then2<T, F, Fut> {
    inp: Pin<Box<T>>,
    f: Pin<Box<F>>,
    fut: Option<Pin<Box<Fut>>>,
}

impl<T, F, Fut> Then2<T, F, Fut>
where
    T: Stream,
    F: Fn(<T as Stream>::Item) -> Fut,
{
    pub fn new(inp: T, f: F) -> Self {
        Self {
            inp: Box::pin(inp),
            f: Box::pin(f),
            fut: None,
        }
    }

    fn prepare_fut(&mut self, item: <T as Stream>::Item) {
        self.fut = Some(Box::pin((self.f)(item)));
    }
}

/*impl<T, F, Fut> Unpin for Then2<T, F, Fut>
where
    T: Unpin,
    F: Unpin,
    Fut: Unpin,
{
}*/

impl<T, F, Fut> Stream for Then2<T, F, Fut>
where
    T: Stream,
    F: Fn(<T as Stream>::Item) -> Fut,
    Fut: Future,
{
    type Item = <Fut as Future>::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(fut) = self.fut.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(item) => {
                        self.fut = None;
                        Ready(Some(item))
                    }
                    Pending => Pending,
                }
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => {
                        self.prepare_fut(item);
                        continue;
                    }
                    Ready(None) => Ready(None),
                    Pending => Pending,
                }
            };
        }
    }
}

pub struct VecStream<T> {
    inp: VecDeque<T>,
}

impl<T> VecStream<T> {
    pub fn new(inp: VecDeque<T>) -> Self {
        Self { inp }
    }
}

impl<T> Stream for VecStream<T>
where
    T: Unpin,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let Some(item) = self.inp.pop_front() {
            Ready(Some(item))
        } else {
            Ready(None)
        }
    }
}
