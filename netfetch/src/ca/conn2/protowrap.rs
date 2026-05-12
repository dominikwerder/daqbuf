const INP_BUF_CAP: usize = 3;

use crate::asynbuf;
use crate::asynchan;
use crate::ca::progpend::HaveProgressPending;
use asynbuf::AsynBuf;
use asynbuf::TsMark;
use ca_proto::ca::proto::CaItem;
use ca_proto::ca::proto::CaMsg;
use ca_proto::ca::proto::CaProto;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

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
            proto_out_buf: AsynBuf::new(INP_BUF_CAP),
            ts_mark_try_01: TsMark::new("try_01".into()),
            ts_mark_try_02: TsMark::new("try_02".into()),
        }
    }

    pub fn is_space(&self) -> bool {
        self.proto_out_buf.is_space()
    }

    pub fn out_len(&self) -> usize {
        self.proto_out_buf.len()
    }

    pub fn push_back_or_drop(&mut self, item: CaMsg) {
        self.proto_out_buf.push_back(item);
    }

    fn poll_proto(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        let self2 = self.get_mut();
        let ts_mark = &mut self2.ts_mark_try_01;
        match self2.proto.poll_next_unpin(cx) {
            Ready(Some(Ok(x))) => {
                ts_mark.hit_some();
                Ready(Some(Ok(x)))
            }
            Ready(Some(Err(e))) => {
                ts_mark.hit_some();
                Ready(Some(Err(e.into())))
            }
            Ready(None) => {
                self2.state = State::Done;
                ts_mark.hit_none();
                Ready(None)
            }
            Pending => {
                ts_mark.hit_pending();
                Pending
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
                    while self.proto.proto_out_space()
                        && let Some(x) = self.proto_out_buf.pop_front()
                    {
                        self.proto.push_out(x);
                    }
                    match self.as_mut().poll_proto(cx) {
                        Ready(Some(x)) => {
                            hpp.mark_progress();
                            match x {
                                Ok(x) => break Ready(Some(Ok(x))),
                                Err(e) => break Ready(Some(Err(e))),
                            }
                        }
                        Ready(None) => {}
                        Pending => {
                            hpp.mark_pending();
                        }
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
