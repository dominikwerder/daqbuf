use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "Http3Support"),
    enum variants {
        NoRuntime,
        IO(#[from] std::io::Error),
    },
);

pub struct Http3Support {}

impl Http3Support {
    pub async fn new(_bind_addr: SocketAddr) -> Result<Self, Error> {
        let ret = Self {};
        Ok(ret)
    }

    pub async fn wait_idle(&self) -> () {
        ()
    }
}

impl Future for Http3Support {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        todo!()
    }
}
