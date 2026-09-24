use crate::asynchan;
use crate::ca::conn2::caids::CaDbrTy;
use crate::ca::conn2::caids::Ioid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::conn::channelheap::ProtoRxItem;
use crate::ca::connset2::connset::channeltrace;
use ca_proto::ca::proto;
use netpod::Shape;
use serde_helper::ToSerde;
use serde_json::json;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

pub enum CaSubFutItem {
    Error,
    ChannelTrace(channeltrace::ChannelTraceItem),
    CaMsgOut(proto::CaMsg),
}

pub trait CaSubFut: fmt::Debug + Send {
    // type Error: std::error::Error;
    fn awaits_ioid(&self) -> Option<Ioid>;
    fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem>;
    fn inp_done(&mut self);
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<CaSubFutItem>>;
    fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<CaSubFutItem>>;
}

#[derive(Debug, ToSerde)]
pub(super) struct ReadNotifyAdhoc {
    sid: Sid,
    dbrty: CaDbrTy,
    shape: Shape,
    ioid: Ioid,
    #[to_serde(skip)]
    sent: Option<Instant>,
    #[to_serde(len)]
    inp_buf: VecDeque<ProtoRxItem>,
    inp_done: bool,
    done: bool,
    #[to_serde(skip)]
    tx: asynchan::Sender<serde_json::Value>,
}

// RunningItem::CaMsgOut

impl ReadNotifyAdhoc {
    pub fn new(sid: Sid, dbrty: CaDbrTy, shape: Shape, ioid: Ioid, tx: asynchan::Sender<serde_json::Value>) -> Self {
        debug!("new");
        // taskrun::tokio::time::sleep(Duration::from_millis(4000));
        Self {
            sid,
            dbrty,
            shape,
            ioid,
            sent: None,
            inp_buf: VecDeque::with_capacity(8),
            inp_done: false,
            done: false,
            tx,
        }
    }
}

impl CaSubFut for ReadNotifyAdhoc {
    fn awaits_ioid(&self) -> Option<Ioid> {
        Some(self.ioid.clone())
    }

    fn inp_push_try(&mut self, item: ProtoRxItem) -> Option<ProtoRxItem> {
        let v = &mut self.inp_buf;
        if v.len() < v.capacity() {
            v.push_back(item);
            None
        } else {
            Some(item)
        }
    }

    fn inp_done(&mut self) {
        self.inp_done = true;
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<CaSubFutItem>> {
        use Poll::*;
        let tsnow = Instant::now();
        loop {
            break if self.done {
                Ready(None)
            } else if let Some(sent) = self.sent {
                if let Some(msg) = self.inp_buf.pop_front() {
                    let (m1, m2) = msg.msg.into_parts();
                    match m2 {
                        proto::CaMsgTy::ReadNotifyRes(msg) => {
                            let v = serde_json::to_value(&msg).unwrap_or_else(|e| json!({"error": e.to_string()}));
                            let js = json!({
                                "ok": true,
                                "value": v,
                            });
                            if self.tx.try_send(js).is_err() {
                                // TODO metrics
                            }
                            let item = channeltrace::ChannelTraceItem::new(
                                channeltrace::ChannelTraceItemInner::CaProto(channeltrace::CaProto::ReadNotifyRes(
                                    channeltrace::ReadNotifyRes::new(sent.elapsed()),
                                )),
                            );
                            self.done = true;
                            Ready(Some(CaSubFutItem::ChannelTrace(item)))
                        }
                        _ => continue,
                    }
                } else if self.inp_done {
                    Ready(None)
                } else if sent + Duration::from_millis(4000) < tsnow {
                    self.done = true;
                    let item = channeltrace::ChannelTraceItem::new(channeltrace::ChannelTraceItemInner::CaProto(
                        channeltrace::CaProto::ReadNotifyTimeout,
                    ));
                    Ready(Some(CaSubFutItem::ChannelTrace(item)))
                } else {
                    Pending
                }
            } else {
                self.sent = Some(tsnow);
                let msg = proto::CaMsg::from_ty_ts(
                    proto::CaMsgTy::ReadNotify(proto::ReadNotify {
                        data_type: self.dbrty.to_u16(),
                        data_count: self.shape.to_ca_count().unwrap_or(0),
                        sid: self.sid.to_u32(),
                        ioid: self.ioid.to_u32(),
                    }),
                    tsnow,
                );
                let item = CaSubFutItem::CaMsgOut(msg);
                Ready(Some(item))
            };
        }
    }

    fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<CaSubFutItem>> {
        Pin::new(self).poll_next(cx)
    }
}
