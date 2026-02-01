use crate::ca::conn2::asynchan;
use crate::ca::progpend::HaveProgressPending;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use futures::Stream;
use futures::StreamExt;
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
    out_rx: Option<asynchan::Receiver<CaMsg>>,
    alt: u16,
}

impl ProtoPusher {
    pub fn new(proto: CaProto, out_rx: asynchan::Receiver<CaMsg>) -> Self {
        Self {
            state: State::Running,
            proto,
            out_rx: Some(out_rx),
            alt: 0,
        }
    }

    pub fn close(&mut self) {
        self.out_rx = None;
    }

    fn try_01(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        hpp: &mut HaveProgressPending,
    ) -> Option<<Self as Stream>::Item> {
        use Poll::*;
        match self.as_mut().proto.poll_next_unpin(cx) {
            Ready(Some(Ok(x))) => {
                hpp.mark_progress();
                Some(Ok(x))
            }
            Ready(Some(Err(e))) => {
                hpp.mark_progress();
                Some(Err(e.into()))
            }
            Ready(None) => {
                self.state = State::Done;
                hpp.mark_progress();
                None
            }
            Pending => {
                hpp.mark_pending();
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
                        hpp.mark_progress();
                        self.as_mut().proto.push_out(x);
                        None
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self.out_rx = None;
                        None
                    }
                    Pending => {
                        hpp.mark_pending();
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
                    if hpp.have_progress() {
                        continue;
                    } else if hpp.have_pending() {
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
