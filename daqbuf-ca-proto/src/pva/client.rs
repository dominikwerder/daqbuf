use super::Error;
use super::field::PvaStruct;
use super::proto::AsyncWriteRead;
use super::proto::ChannelReq;
use super::proto::ChannelRequestInit;
use super::proto::ChannelRequestOp;
use super::proto::ConnValidRes;
use super::proto::CreateChannelReq;
use super::proto::DestroyChannel;
use super::proto::PvaItem;
use super::proto::PvaMsgTy;
use super::proto::PvaProto;
use super::proto::PvaRequest;
use super::pvdata::StatusKind;
use super::value::PvaDelta;
use super::value::PvaStructValue;
use futures_util::StreamExt;
use std::sync::Arc;

pub const CLIENT_CID: u32 = 1;
pub const REQUEST_ID: u32 = 1;

fn status_ok(kind: StatusKind) -> bool {
    matches!(kind, StatusKind::Ok | StatusKind::Warning)
}

pub async fn get_once<T: AsyncWriteRead>(
    tcp: T,
    remote_name: String,
    pv: &str,
    pv_request: PvaRequest,
    array_truncate: usize,
) -> Result<PvaStructValue, Error> {
    let mut proto = PvaProto::new(tcp, None, remote_name, array_truncate);
    let mut server_cid = None;
    loop {
        let item = match proto.next().await {
            Some(x) => x?,
            None => return Err(Error::NeitherPendingNorProgress),
        };
        let msg = match item {
            PvaItem::Empty => continue,
            PvaItem::Msg(m) => m,
        };
        match msg.ty {
            PvaMsgTy::ConnectionValidationReq(_) => {
                proto.push_out(PvaMsgTy::ConnectionValidationRes(ConnValidRes::anonymous()));
            }
            PvaMsgTy::ConnectionValidated(status) => {
                if !status_ok(status.kind) {
                    return Err(Error::LogicError);
                }
                proto.push_out(PvaMsgTy::CreateChannelReq(CreateChannelReq {
                    channels: vec![ChannelReq {
                        client_cid: CLIENT_CID,
                        name: pv.to_string(),
                    }],
                }));
            }
            PvaMsgTy::CreateChannelRes(res) => {
                if res.client_cid != CLIENT_CID {
                    continue;
                }
                if !status_ok(res.status.kind) {
                    return Err(Error::LogicError);
                }
                server_cid = Some(res.server_cid);
                proto.push_out(PvaMsgTy::ChannelGetInit(ChannelRequestInit {
                    server_cid: res.server_cid,
                    request_id: REQUEST_ID,
                    pv_request: pv_request.clone(),
                }));
            }
            PvaMsgTy::ChannelGetInitRes(res) => {
                if res.request_id != REQUEST_ID {
                    continue;
                }
                if !status_ok(res.status.kind) {
                    return Err(Error::LogicError);
                }
                let sc = server_cid.ok_or(Error::LogicError)?;
                proto.push_out(PvaMsgTy::ChannelGet(ChannelRequestOp {
                    server_cid: sc,
                    request_id: REQUEST_ID,
                }));
            }
            PvaMsgTy::ChannelGetRes(res) => {
                if res.request_id != REQUEST_ID {
                    continue;
                }
                if !status_ok(res.status.kind) {
                    return Err(Error::LogicError);
                }
                let delta = res.delta.ok_or(Error::LogicError)?;
                let full = delta.into_full().ok_or(Error::LogicError)?;
                if let Some(sc) = server_cid {
                    proto.push_out(PvaMsgTy::DestroyRequest(ChannelRequestOp {
                        server_cid: sc,
                        request_id: REQUEST_ID,
                    }));
                    proto.push_out(PvaMsgTy::DestroyChannelReq(DestroyChannel {
                        server_cid: sc,
                        client_cid: CLIENT_CID,
                    }));
                }
                return Ok(full);
            }
            _ => {}
        }
    }
}

pub struct MonitorSession {
    proto: PvaProto,
    server_cid: u32,
    ty: Arc<PvaStruct>,
}

impl MonitorSession {
    pub fn ty(&self) -> &Arc<PvaStruct> {
        &self.ty
    }

    pub async fn next_delta(&mut self) -> Result<Option<PvaDelta>, Error> {
        loop {
            let item = match self.proto.next().await {
                Some(x) => x?,
                None => return Ok(None),
            };
            let msg = match item {
                PvaItem::Empty => continue,
                PvaItem::Msg(m) => m,
            };
            match msg.ty {
                PvaMsgTy::MonitorUpdate(u) => {
                    if u.request_id != REQUEST_ID {
                        continue;
                    }
                    return Ok(Some(u.delta));
                }
                PvaMsgTy::MonitorFinal(f) => {
                    if f.request_id != REQUEST_ID {
                        continue;
                    }
                    return Ok(None);
                }
                _ => {}
            }
        }
    }

    pub async fn close(mut self) -> Result<(), Error> {
        self.proto.push_out(PvaMsgTy::DestroyRequest(ChannelRequestOp {
            server_cid: self.server_cid,
            request_id: REQUEST_ID,
        }));
        self.proto.push_out(PvaMsgTy::DestroyChannelReq(DestroyChannel {
            server_cid: self.server_cid,
            client_cid: CLIENT_CID,
        }));
        if let Some(Err(e)) = self.proto.next().await {
            return Err(e);
        }
        Ok(())
    }
}

pub async fn open_monitor<T: AsyncWriteRead>(
    tcp: T,
    remote_name: String,
    pv: &str,
    pv_request: PvaRequest,
    array_truncate: usize,
) -> Result<MonitorSession, Error> {
    let mut proto = PvaProto::new(tcp, None, remote_name, array_truncate);
    let mut server_cid = None;
    loop {
        let item = match proto.next().await {
            Some(x) => x?,
            None => return Err(Error::NeitherPendingNorProgress),
        };
        let msg = match item {
            PvaItem::Empty => continue,
            PvaItem::Msg(m) => m,
        };
        match msg.ty {
            PvaMsgTy::ConnectionValidationReq(_) => {
                proto.push_out(PvaMsgTy::ConnectionValidationRes(ConnValidRes::anonymous()));
            }
            PvaMsgTy::ConnectionValidated(status) => {
                if !status_ok(status.kind) {
                    return Err(Error::LogicError);
                }
                proto.push_out(PvaMsgTy::CreateChannelReq(CreateChannelReq {
                    channels: vec![ChannelReq {
                        client_cid: CLIENT_CID,
                        name: pv.to_string(),
                    }],
                }));
            }
            PvaMsgTy::CreateChannelRes(res) => {
                if res.client_cid != CLIENT_CID {
                    continue;
                }
                if !status_ok(res.status.kind) {
                    return Err(Error::LogicError);
                }
                server_cid = Some(res.server_cid);
                proto.push_out(PvaMsgTy::MonitorInit(ChannelRequestInit {
                    server_cid: res.server_cid,
                    request_id: REQUEST_ID,
                    pv_request: pv_request.clone(),
                }));
            }
            PvaMsgTy::MonitorInitRes(res) => {
                if res.request_id != REQUEST_ID {
                    continue;
                }
                if !status_ok(res.status.kind) {
                    return Err(Error::LogicError);
                }
                let sc = server_cid.ok_or(Error::LogicError)?;
                let ty = res.ty.ok_or(Error::LogicError)?;
                proto.push_out(PvaMsgTy::MonitorStart(ChannelRequestOp {
                    server_cid: sc,
                    request_id: REQUEST_ID,
                }));
                return Ok(MonitorSession {
                    proto,
                    server_cid: sc,
                    ty,
                });
            }
            _ => {}
        }
    }
}
