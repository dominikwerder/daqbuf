use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "ConnSetFut"),
    enum variants {
        Logic,
    },
);

#[derive(Debug)]
pub struct FutShutdown {}

impl FutShutdown {
    pub fn new() -> Self {
        FutShutdown {}
    }
}

impl Future for FutShutdown {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
