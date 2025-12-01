use crate::ca::conn2::progpend::HaveProgressPending;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use futures_util::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Connected"),
    enum variants {
        IO(#[from] std::io::Error),
        Proto(#[from] ca_proto::ca::proto::Error),
    },
);

#[derive(Debug)]
enum State {
    Running,
    Done,
}

#[derive(Debug)]
pub struct ProtoPusher {
    state: State,
    proto: CaProto,
    out_rx: Option<Pin<Box<async_channel::Receiver<CaMsg>>>>,
    alt: u16,
}

impl ProtoPusher {
    pub fn new(proto: CaProto, out_rx: async_channel::Receiver<CaMsg>) -> Self {
        // let (out_tx, out_rx) = async_channel::bounded(120);
        Self {
            state: State::Running,
            proto,
            out_rx: Some(Box::pin(out_rx)),
            // out_tx,
            // inp_tx,
            alt: 0,
        }
    }

    fn try_01(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        hpp: &mut HaveProgressPending,
    ) -> Option<<Self as Stream>::Item> {
        use Poll::*;
        match self.as_mut().proto.poll_next_unpin(cx) {
            Ready(Some(Ok(x))) => {
                hpp.have_progress();
                Some(Ok(x))
            }
            Ready(Some(Err(e))) => {
                hpp.have_progress();
                Some(Err(e.into()))
            }
            Ready(None) => {
                self.state = State::Done;
                hpp.have_progress();
                None
            }
            Pending => {
                hpp.have_pending();
                None
            }
        }
    }

    fn try_02(mut self: Pin<&mut Self>, cx: &mut Context<'_>, hpp: &mut HaveProgressPending) -> Option<CaMsg> {
        use Poll::*;
        if self.proto.proto_out_len() < 20 {
            if let Some(rx) = &mut self.out_rx {
                match rx.poll_next_unpin(cx) {
                    Ready(Some(x)) => {
                        hpp.have_progress();
                        self.as_mut().proto.push_out(x);
                        None
                    }
                    Ready(None) => {
                        hpp.have_progress();
                        self.out_rx = None;
                        None
                    }
                    Pending => {
                        hpp.have_pending();
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl Stream for ProtoPusher {
    type Item = Result<CaItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            break match &self.state {
                State::Running => {
                    if self.alt == 0 {
                        self.alt = 1;
                        if let Some(item) = self.as_mut().try_01(cx, &mut hpp) {
                            break Ready(Some(item));
                        }
                        self.as_mut().try_02(cx, &mut hpp);
                    } else {
                        self.alt = 0;
                        self.as_mut().try_02(cx, &mut hpp);
                        if let Some(item) = self.as_mut().try_01(cx, &mut hpp) {
                            break Ready(Some(item));
                        }
                    }
                    if hpp.is_progress() {
                        continue;
                    } else if hpp.is_pending() {
                        Pending
                    } else {
                        Ready(None)
                    }
                }
                State::Done => {
                    break Ready(None);
                }
            };
        }
    }
}
