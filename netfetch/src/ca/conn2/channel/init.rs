use super::AcceptMessageResult;
use crate::ca::conn2::caids::Cid;
use crate::ca::conn2::caids::Sid;
use crate::ca::conn2::channel::SharedResources;
use crate::ca::conn2::synchan;
use crate::ca::conn2::todoval;
use crate::conf::ChannelConfig;
use async_channel::Sender;
use async_channel::TrySendError;
use ca_proto::ca::proto;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use serde::Serialize;
use series::ChannelStatusSeriesId;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ChannelError"),
    enum variants {
        ChannelSend,
        Netpod(#[from] netpod::Error),
    },
);

#[derive(Serialize)]
enum State {
    Init,
    #[serde(with = "serde_open_send")]
    OpenSend(
        Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
        VecDeque<proto::CaMsg>,
    ),
    OpenRecv,
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            State::Init => write!(fmt, "Init"),
            State::OpenSend(_, v) => write!(fmt, "OpenSend({})", v.len()),
            State::OpenRecv => write!(fmt, "OpenRecv"),
        }
    }
}

mod serde_open_send {
    use super::Error;
    use super::Pin;
    use super::proto;
    use serde::Serializer;
    use std::collections::VecDeque;

    pub fn serialize<S>(
        _: &Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
        _: &VecDeque<proto::CaMsg>,
        ser: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ser.serialize_str("OpenSend")
    }
}

async fn tx_send(tx: Sender<proto::CaMsg>, item: proto::CaMsg) -> Result<(), Error> {
    tx.send(item).await.map_err(|_| Error::ChannelSend)
}

#[derive(Debug, Serialize)]
pub struct TryOpen {
    state: State,
    conf: ChannelConfig,
    cssid: ChannelStatusSeriesId,
    ress: SharedResources,
    // #[serde(serialize_with = "ser_try_open_msgbuf")]
    // msgbuf: VecDeque<proto::CaMsg>,
}

fn ser_try_open_msgbuf<S>(v: &VecDeque<proto::CaMsg>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    ser.serialize_u32(v.len() as u32)
}

impl TryOpen {
    pub fn new(conf: ChannelConfig, cssid: ChannelStatusSeriesId, ress: SharedResources) -> Self {
        Self {
            state: State::Init,
            conf,
            cssid,
            ress,
            // msgbuf: VecDeque::new(),
        }
    }

    pub fn dismantle(self) -> (ChannelConfig, ChannelStatusSeriesId, SharedResources) {
        (self.conf, self.cssid, self.ress)
    }

    pub fn process_ca_msg(
        mut self: Pin<&mut Self>,
        cx: Context,
        msg: proto::CaMsg,
    ) -> Result<AcceptMessageResult, Error> {
        // use Poll::*;
        match &self.state {
            State::Init | State::OpenSend(..) => {
                warn!("go message during Init or OpenSend ty {:?}", msg.ty);
                Ok(AcceptMessageResult::Accepted(()))
            }
            State::OpenRecv => {
                // process message right here.
                // self.msgbuf.push(msg);
                match &msg.ty {
                    proto::CaMsgTy::CreateChanRes(x) => {}
                    proto::CaMsgTy::CreateChanFail(x) => {
                        // TODO
                        // Here, must indicate that the address could be wrong!
                        // The channel status must be "Fail" so that ConnSet can decide to re-search.
                        // TODO how to transition the channel state? Any invariants or simply write to the map?
                        let cid = Cid::new(x.cid);
                        let failinfo = format!("name {}  cid {}", self.conf.name(), cid.to_u32());
                        // let item = CaConnEvent {
                        //     ts: tsnow,
                        //     value: CaConnEventValue::ChannelCreateFail(failinfo),
                        // };
                        // TODO push out that status event

                        // TODO return channel state change together with failure type so
                        // that upstream can act on it.
                    }
                    _ => {
                        warn!("got unexpected message during TryOpen: {:?}", msg.ty);
                    }
                }
                Ok(AcceptMessageResult::Accepted(()))
            }
        }
    }

    // fn process_buffered(mut self: Pin<&mut Self>, cx: Context) -> Result<AcceptMessageResult, Error> {}

    fn handle_create_chan_res(&mut self, k: proto::CreateChanRes, tsnow: Instant) -> Result<(), Error> {
        /*
        TODO
        - Remember the server-provided Sid. Need to hand that also to next state.
        - If it is a enum channel, fetch enum variants.
            But this could be also the responsibility of the next state.
        - Also spawn a ChannelInfoQuery to find/register the series.
            For enum, this can be done in parallel.
        - Maybe this state can be content without enum variants or series register.
            We should just cause state change and let next state move on from here.
        */
        let cid = Cid::new(k.cid);
        let sid = Sid::new(k.sid);
        if k.data_type > 6 {
            error!("CreateChanRes with unexpected data_type {}", k.data_type);
        }
        // Ask for DBR_TIME_...
        let ca_dbr_type = k.data_type + 14;
        let scalar_type = ScalarType::from_ca_id(k.data_type)?;
        let shape = Shape::from_ca_count(k.data_count)?;

        let stnow = std::time::SystemTime::now();
        let (acc_msp, _) = TsMs::from_system_time(stnow).to_grid_02(netpod::EMIT_ACCOUNTING_SNAP);
        // let created_state = CreatedState {
        //     cssid,
        //     cid,
        //     sid,
        //     ca_dbr_type,
        //     ca_dbr_count: k.data_count,
        //     ts_created: tsnow,
        //     ts_alive_last: tsnow,
        //     ts_activity_last: tsnow,
        //     st_activity_last: stnow,
        //     insert_item_ivl_ema: IntervalEma::new(),
        //     item_recv_ivl_ema: IntervalEma::new(),
        //     insert_recv_ivl_last: tsnow,
        //     muted_before: 0,
        //     recv_count: 0,
        //     recv_bytes: 0,
        //     stwin_ts: 0,
        //     stwin_count: 0,
        //     stwin_bytes: 0,
        //     acc_recv: AccountingInfo::new(acc_msp),
        //     acc_st: AccountingInfo::new(acc_msp),
        //     acc_mt: AccountingInfo::new(acc_msp),
        //     acc_lt: AccountingInfo::new(acc_msp),
        //     dw_st_last: SystemTime::UNIX_EPOCH,
        //     dw_mt_last: SystemTime::UNIX_EPOCH,
        //     dw_lt_last: SystemTime::UNIX_EPOCH,
        //     val_lst_st: serde_json::Value::Null,
        //     val_lst_mt: serde_json::Value::Null,
        //     val_lst_lt: serde_json::Value::Null,
        //     scalar_type: scalar_type.clone(),
        //     shape: shape.clone(),
        //     name: conf.conf.name().into(),
        //     enum_str_table: None,
        //     ts_recv_value_status_emit_next: Instant::now(),
        // };
        // if series::dbg::dbg_chn(created_state.name()) {
        //     info!(
        //         "handle_create_chan_res  {:?}  {}",
        //         created_state.cid,
        //         created_state.name()
        //     );
        // }
        // match &scalar_type {
        //     ScalarType::Enum => {
        //         // TODO channel created, now fetch enum variants, later make writer
        //         let fut = enumfetch::EnumFetch::new(created_state, self);
        //         // TODO should always check if the slot is free.
        //         let ioid = fut.ioid();
        //         let x = Box::pin(fut);
        //         self.handler_by_ioid.insert(ioid, Some(x));
        //     }
        //     _ => {
        //         let backend = self.backend.clone();
        //         let channel_name = created_state.name().into();
        //         // TODO create a channel for the answer.
        //         // Keep only a certain max number of channels in-flight because have to poll on them.
        //         // TODO register the channel for the answer.
        //         let (tx, rx) = async_channel::bounded(8);
        //         let item = ChannelInfoQuery {
        //             backend,
        //             channel: channel_name,
        //             kind: SeriesKind::CaStatus,
        //             scalar_type: ScalarType::I16,
        //             shape: Shape::Scalar,
        //             tx: Box::pin(tx),
        //         };
        //         self.channel_info_query_qu.push_back(item);
        //         self.channel_info_query_res_rxs.push_back((Box::pin(rx), cid));
        //         *chst = ChannelState::FetchCaStatusSeries(MakingSeriesWriterState {
        //             tsbeg: tsnow,
        //             channel: created_state,
        //             series_status: SeriesId::new(0),
        //         });
        //     }
        // }
        Ok(())
    }
}

impl Future for TryOpen {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            let this = &mut *self;
            break match &mut this.state {
                State::Init => {
                    let hostname = this.ress.local_epics_hostname.clone();
                    let mut msgs = VecDeque::new();
                    let tsnow = Instant::now();
                    // TODO generate a cid.
                    let cid = Cid::new(todoval());
                    // let sid = Sid::new(k.sid);
                    // let cssid = cssid.clone();
                    let name = self.conf.name();
                    let msg = proto::CaMsg::from_ty_ts(
                        proto::CaMsgTy::CreateChan(proto::CreateChan {
                            cid: cid.to_u32(),
                            channel: name.into(),
                        }),
                        tsnow,
                    );
                    msgs.push_back(msg);
                    // TODO optional during dev emit to status series that we try to connect.
                    self.state = State::OpenSend(None, msgs);
                    continue;
                }
                State::OpenSend(futopt, msgs) => {
                    if let Some(fut) = futopt {
                        match fut.as_mut().poll(cx) {
                            Ready(x) => {
                                *futopt = None;
                                match x {
                                    Ok(()) => continue,
                                    Err(e) => Ready(Err(e)),
                                }
                            }
                            Pending => Pending,
                        }
                    } else if let Some(item) = msgs.pop_front() {
                        match this.ress.proto_out.tx().try_send(item) {
                            Ok(()) => {
                                continue;
                            }
                            Err(TrySendError::Full(item)) => {
                                let tx = this.ress.proto_out.tx().clone();
                                let fut2 = tx_send(tx, item);
                                let fut2 = Box::pin(fut2);
                                *futopt = Some(fut2);
                                continue;
                            }
                            Err(TrySendError::Closed(_)) => Ready(Err(Error::ChannelSend)),
                        }
                    } else {
                        self.state = State::OpenRecv;
                        continue;
                    }
                }
                State::OpenRecv => {
                    // TODO wait for message.
                    // We do not poll here.
                    Pending
                }
            };
        }
    }
}
