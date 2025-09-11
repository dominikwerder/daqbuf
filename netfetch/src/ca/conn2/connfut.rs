use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct ConnFutResource {}

#[must_use = "futures do nothing unless you await or poll them"]
pub trait ConnFuture {
    type Output;

    fn poll(self: Pin<&mut Self>, cfres: &mut ConnFutResource, cx: &mut Context) -> Poll<Self::Output>;
}

struct A;

impl Future for A {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}
