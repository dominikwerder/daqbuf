mod futs;
mod streamtask;

pub use futs::FutShutdown;
use futures_util::Stream;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "ConnSet"),
    enum variants {
        Logic,
    },
);

#[derive(Debug)]
pub struct ConnSet {}

impl ConnSet {
    pub fn new() -> Self {
        // streamtask::run_in_task();
        ConnSet {}
    }

    pub async fn shutdown(&self) -> FutShutdown {
        todo!()
    }
}

impl Stream for ConnSet {
    type Item = Result<(), Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
