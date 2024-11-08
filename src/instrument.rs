use futures_util::Stream;
use futures_util::StreamExt;
use netpod::log::tracing;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[pin_project::pin_project]
pub struct InstrumentStream<S> {
    #[pin]
    inp: S,
    #[pin]
    span: tracing::Span,
}

impl<S> InstrumentStream<S> {
    pub fn new(inp: S, span: tracing::Span) -> Self {
        Self { inp, span }
    }
}

impl<S> Stream for InstrumentStream<S>
where
    S: Stream,
{
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let _spg = this.span.enter();
        this.inp.poll_next_unpin(cx)
    }
}
