use crate::ca::conn2::asynchan;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::CidOwned;
use crate::ca::conn2::progpend::HaveProgressPending;
use crate::ca::conn2::synchan;
use crate::conf::ChannelConfig;
use ca_proto::ca::proto;
use ca_proto::ca::proto::CaMsg;
use futures_util::FutureExt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelHandler"),
    enum variants {
        ProtoTxClosed,
        SynRecv(#[from] synchan::RecvError),
    },
);

#[derive(Debug)]
struct Init {}

struct Creating {
    fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
}

impl fmt::Debug for Creating {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Creating").finish()
    }
}

#[derive(Debug)]
enum State {
    Init,
    Creating(Creating),
    Running,
    Done,
}

async fn channel_create(cid: u32, name: String, tx: asynchan::Sender<CaMsg>, tsnow: Instant) -> Result<(), Error> {
    let msg = CaMsg::from_ty_ts(
        proto::CaMsgTy::CreateChan(proto::CreateChan {
            cid,
            channel: name.into(),
        }),
        tsnow,
    );
    tx.send(msg).await.map_err(|_| Error::ProtoTxClosed)?;
    Ok(())
}

#[derive(Debug)]
pub struct ChannelHandler {
    state: State,
    cid: CidOwned,
    conf: ChannelConfig,
    proto_tx: asynchan::Sender<CaMsg>,
    proto_rx: synchan::Receiver<CaMsg>,
}

impl ChannelHandler {
    pub fn new(conf: ChannelConfig, proto_tx: asynchan::Sender<CaMsg>, proto_rx: synchan::Receiver<CaMsg>) -> Self {
        let cid = CidOwned::new();
        trace!("ChannelHandler::new  {cid:?}  {conf:?}");

        // TODO to send the channel create message, I need to be in some async function.
        // Issue:
        // Even if this handler attempts a send, but hits Pending, then ChannelHeap will get
        // woken up at some point, but how does ChannelHeap know to poll this ChannelHandler again?
        // When polling, ChannelHeap must pass a specific Waker.

        Self {
            state: State::Init,
            cid,
            conf,
            proto_tx,
            proto_rx,
        }
    }

    pub fn cid(&self) -> Cid {
        self.cid.to_cid()
    }
}

impl Future for ChannelHandler {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            let tsnow = Instant::now();
            let mut hpp = HaveProgressPending::new();
            match &mut self.state {
                State::Init => {
                    let fut = channel_create(self.cid.to_u32(), self.conf.name().into(), self.proto_tx.clone(), tsnow)
                        .boxed();
                    self.state = State::Creating(Creating { fut });
                    hpp.mark_progress();
                }
                State::Creating(st1) => {
                    trace!("ChannelHandler:Creating");
                    match st1.fut.as_mut().poll(cx) {
                        Ready(Ok(())) => {
                            trace!("ChannelHandler:Creating:Ready:Ok");
                            self.state = State::Running;
                            hpp.mark_progress();
                        }
                        Ready(Err(e)) => {
                            trace!("ChannelHandler:Creating:Ready:Err {e}");
                            self.state = State::Done;
                            hpp.mark_progress();
                            break Ready(Err(e));
                        }
                        Pending => {
                            trace!("ChannelHandler:Creating:Pending");
                            hpp.mark_pending();
                        }
                    }
                }
                State::Running => {
                    match self.proto_rx.poll_unpin(cx) {
                        Ready(Ok(item)) => {
                            trace!("ChannelHandler:Running:Ready:Ok {item:?}");
                            // TODO process item
                            hpp.mark_progress();
                        }
                        Ready(Err(e)) => {
                            trace!("ChannelHandler:Running:Ready:Err {e}");
                            // TODO handle closed channel
                            self.state = State::Done;
                            hpp.mark_progress();
                            break Ready(Err(e.into()));
                        }
                        Pending => {
                            trace!("ChannelHandler:Running:Pending");
                            hpp.mark_pending();
                        }
                    }
                }
                State::Done => {}
            };
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                trace!("HPP:Done");
                Ready(Ok(()))
            };
        }
    }
}
