use crate::asynbuf;
use crate::asynbuf::AsynBuf;
use crate::asynbuf::TsMark;
use crate::asynchan;
use crate::ca::progpend::HaveProgressPending;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }

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
    proto_out_buf: AsynBuf<CaMsg>,
    ts_mark_try_01: TsMark,
    ts_mark_try_02: TsMark,
}

impl ProtoPusher {
    pub fn new(proto: CaProto) -> Self {
        Self {
            state: State::Running,
            proto,
            proto_out_buf: AsynBuf::new(16),
            ts_mark_try_01: TsMark::new("try_01".into()),
            ts_mark_try_02: TsMark::new("try_02".into()),
        }
    }

    pub fn inp_push_try(self: Pin<&mut Self>, item: CaMsg, cx: &mut Context<'_>) -> asynbuf::PushRes<CaMsg> {
        let self2 = self.get_mut();
        let v = &mut self2.proto_out_buf;
        // let w1 = &mut self2.waker_1;
        // let w2 = &mut self2.waker_2;
        let x = v.push_back(item);
        match &x {
            asynbuf::PushRes::First => {
                // if let Some(w) = w1.take() {
                //     w.wake();
                // }
            }
            asynbuf::PushRes::Done => {}
            asynbuf::PushRes::Full(_) => {
                // *w2 = Some(cx.waker().clone());
            }
        }
        x
    }

    fn poll_proto(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        hpp: &mut HaveProgressPending,
    ) -> Option<<Self as Stream>::Item> {
        use Poll::*;
        let self2 = self.get_mut();
        let ts_mark = &mut self2.ts_mark_try_01;
        match self2.proto.poll_next_unpin(cx) {
            Ready(Some(Ok(x))) => {
                hpp.mark_progress();
                ts_mark.hit_some();
                Some(Ok(x))
            }
            Ready(Some(Err(e))) => {
                hpp.mark_progress();
                ts_mark.hit_some();
                Some(Err(e.into()))
            }
            Ready(None) => {
                self2.state = State::Done;
                ts_mark.hit_none();
                hpp.mark_progress();
                None
            }
            Pending => {
                hpp.mark_pending();
                ts_mark.hit_pending();
                None
            }
        }
    }

    pub fn status_socket(&mut self) -> serde_json::Value {
        use serde_json::json;
        match self.proto.get_read_stats_v1() {
            Ok(x) => json!({
                "socket_buffer_len": x.0,
                "tcp_read_bytes": x.1,
                "buf_rlen": x.2,
            }),
            Err(e) => json!({
                "error": e.to_string(),
            }),
        }
    }

    pub(super) fn dump_state_poll(&self) -> serde_json::Value {
        use serde_json::json;
        let st = match &self.state {
            State::Running => json!({"Running": {}}),
            State::Done => json!({"Done": {}}),
        };
        let js = json!({
            "state": st,
            "ts_mark_try_01": &self.ts_mark_try_01,
            "ts_mark_try_02": &self.ts_mark_try_02,
        });
        js
    }
}

impl Stream for ProtoPusher {
    type Item = Result<CaItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut hpp = HaveProgressPending::new();
            match &self.state {
                State::Running => {
                    if let Some(item) = self.as_mut().poll_proto(cx, &mut hpp) {
                        break Ready(Some(item));
                    }
                    break if hpp.have_progress() {
                        continue;
                    } else if hpp.have_pending() {
                        Pending
                    } else {
                        Ready(None)
                    };
                }
                State::Done => break Ready(None),
            };
        }
    }
}
