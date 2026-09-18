use super::Error;
use super::field::IntroRegistry;
use super::field::PvaField;
use super::field::PvaStruct;
use super::pvdata::BitSet;
use super::pvdata::Endian;
use super::pvdata::PVA_PAYLOAD_LEN_MAX;
use super::pvdata::Reader;
use super::pvdata::Status;
use super::pvdata::Writer;
use super::search::Beacon;
use super::search::SearchReq;
use super::search::SearchRes;
use super::value::PvaDelta;
use super::value::PvaStructValue;
use futures_util::AsyncRead;
use futures_util::AsyncWrite;
use futures_util::Stream;
use netpod::log;
use slidebuf::SlideBuf;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::str;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace_in_out { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

pub const PVA_MAGIC: u8 = 0xca;
pub const PVA_VERSION: u8 = 1;
pub const PVA_TCP_PORT: u16 = 5075;
pub const PVA_UDP_PORT: u16 = 5076;
pub const PVA_INPUT_BUF_CAP: usize = 1024 * 1024 * 40;
pub const PVA_OUTPUT_BUF_CAP: usize = 1024 * 256;
pub const PVA_SEGMENT_LEN_MAX: usize = 1024 * 1024 * 64;

pub const FLAG_CONTROL: u8 = 0x01;
pub const FLAG_SEGMENT_MASK: u8 = 0x30;
pub const FLAG_FROM_SERVER: u8 = 0x40;
pub const FLAG_BIG_ENDIAN: u8 = 0x80;

pub const SEGMENT_NONE: u8 = 0;
pub const SEGMENT_FIRST: u8 = 1;
pub const SEGMENT_LAST: u8 = 2;
pub const SEGMENT_MIDDLE: u8 = 3;

pub const CMD_BEACON: u8 = 0x00;
pub const CMD_CONNECTION_VALIDATION: u8 = 0x01;
pub const CMD_ECHO: u8 = 0x02;
pub const CMD_SEARCH: u8 = 0x03;
pub const CMD_SEARCH_RESPONSE: u8 = 0x04;
pub const CMD_AUTHNZ: u8 = 0x05;
pub const CMD_ACL_CHANGE: u8 = 0x06;
pub const CMD_CREATE_CHANNEL: u8 = 0x07;
pub const CMD_DESTROY_CHANNEL: u8 = 0x08;
pub const CMD_CONNECTION_VALIDATED: u8 = 0x09;
pub const CMD_GET: u8 = 0x0a;
pub const CMD_PUT: u8 = 0x0b;
pub const CMD_PUT_GET: u8 = 0x0c;
pub const CMD_MONITOR: u8 = 0x0d;
pub const CMD_ARRAY: u8 = 0x0e;
pub const CMD_DESTROY_REQUEST: u8 = 0x0f;
pub const CMD_PROCESS: u8 = 0x10;
pub const CMD_GET_FIELD: u8 = 0x11;
pub const CMD_MESSAGE: u8 = 0x12;
pub const CMD_RPC: u8 = 0x14;
pub const CMD_CANCEL_REQUEST: u8 = 0x15;
pub const CMD_ORIGIN_TAG: u8 = 0x16;

pub const CTRL_MARK_TOTAL_BYTES_SENT: u8 = 0x00;
pub const CTRL_ACK_TOTAL_BYTES_RECEIVED: u8 = 0x01;
pub const CTRL_SET_BYTE_ORDER: u8 = 0x02;
pub const CTRL_ECHO_REQUEST: u8 = 0x03;
pub const CTRL_ECHO_RESPONSE: u8 = 0x04;

pub const SUB_GET: u8 = 0x00;
pub const SUB_STOP: u8 = 0x04;
pub const SUB_INIT: u8 = 0x08;
pub const SUB_DESTROY: u8 = 0x10;
pub const SUB_GET_PUT: u8 = 0x40;
pub const SUB_START: u8 = 0x44;
pub const SUB_PIPELINE: u8 = 0x80;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PvaHead {
    pub version: u8,
    pub flags: u8,
    pub command: u8,
    pub payload_size: u32,
}

impl PvaHead {
    pub fn parse(b: &[u8]) -> Result<Self, Error> {
        if b.len() < 8 {
            return Err(Error::NotEnoughInput(8, b.len()));
        }
        if b[0] != PVA_MAGIC {
            return Err(Error::BadMagic(b[0]));
        }
        let flags = b[2];
        let payload_size = if flags & FLAG_BIG_ENDIAN != 0 {
            u32::from_be_bytes([b[4], b[5], b[6], b[7]])
        } else {
            u32::from_le_bytes([b[4], b[5], b[6], b[7]])
        };
        Ok(Self {
            version: b[1],
            flags,
            command: b[3],
            payload_size,
        })
    }

    pub fn is_control(&self) -> bool {
        self.flags & FLAG_CONTROL != 0
    }

    pub fn segment(&self) -> u8 {
        (self.flags & FLAG_SEGMENT_MASK) >> 4
    }

    pub fn from_server(&self) -> bool {
        self.flags & FLAG_FROM_SERVER != 0
    }

    pub fn endian(&self) -> Endian {
        if self.flags & FLAG_BIG_ENDIAN != 0 {
            Endian::Big
        } else {
            Endian::Little
        }
    }

    pub fn write(&self, out: &mut Vec<u8>) {
        out.push(PVA_MAGIC);
        out.push(self.version);
        out.push(self.flags);
        out.push(self.command);
        if self.flags & FLAG_BIG_ENDIAN != 0 {
            out.extend_from_slice(&self.payload_size.to_be_bytes());
        } else {
            out.extend_from_slice(&self.payload_size.to_le_bytes());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnValidReq {
    pub server_buffer_size: i32,
    pub server_intro_registry_max_size: i16,
    pub auth_nz: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnValidRes {
    pub client_buffer_size: i32,
    pub client_intro_registry_max_size: i16,
    pub connection_qos: i16,
    pub auth_nz: String,
}

impl ConnValidRes {
    pub fn anonymous() -> Self {
        Self {
            client_buffer_size: PVA_INPUT_BUF_CAP as i32,
            client_intro_registry_max_size: 0x7fff,
            connection_qos: 0,
            auth_nz: "anonymous".into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelReq {
    pub client_cid: u32,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateChannelReq {
    pub channels: Vec<ChannelReq>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateChannelRes {
    pub client_cid: u32,
    pub server_cid: u32,
    pub status: Status,
    pub access_rights: Option<i16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestroyChannel {
    pub server_cid: u32,
    pub client_cid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetFieldReq {
    pub server_cid: u32,
    pub request_id: u32,
    pub sub_field: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetFieldRes {
    pub request_id: u32,
    pub status: Status,
    pub field: Option<PvaField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelRequestInit {
    pub server_cid: u32,
    pub request_id: u32,
    pub pv_request: PvaRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelRequestOp {
    pub server_cid: u32,
    pub request_id: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestInitRes {
    pub request_id: u32,
    pub status: Status,
    pub ty: Option<Arc<PvaStruct>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChannelGetRes {
    pub request_id: u32,
    pub status: Status,
    pub delta: Option<PvaDelta>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MonitorUpdate {
    pub request_id: u32,
    pub delta: PvaDelta,
    pub overrun: BitSet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestIdOp {
    pub request_id: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageCmd {
    pub request_id: u32,
    pub message_type: u8,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PvaMsgTy {
    SetByteOrder(Endian, bool),
    MarkTotalBytesSent(u32),
    AckTotalBytesReceived(u32),
    EchoRequestControl(u32),
    EchoResponseControl(u32),
    Echo(Vec<u8>),
    ConnectionValidationReq(ConnValidReq),
    ConnectionValidationRes(ConnValidRes),
    ConnectionValidated(Status),
    Search(SearchReq),
    SearchRes(SearchRes),
    Beacon(Beacon),
    CreateChannelReq(CreateChannelReq),
    CreateChannelRes(CreateChannelRes),
    DestroyChannelReq(DestroyChannel),
    DestroyChannelRes(DestroyChannel),
    GetFieldReq(GetFieldReq),
    GetFieldRes(GetFieldRes),
    ChannelGetInit(ChannelRequestInit),
    ChannelGetInitRes(RequestInitRes),
    ChannelGet(ChannelRequestOp),
    ChannelGetRes(ChannelGetRes),
    MonitorInit(ChannelRequestInit),
    MonitorInitRes(RequestInitRes),
    MonitorStart(ChannelRequestOp),
    MonitorStop(ChannelRequestOp),
    MonitorUpdate(MonitorUpdate),
    MonitorFinal(RequestIdOp),
    CancelRequest(ChannelRequestOp),
    DestroyRequest(ChannelRequestOp),
    Message(MessageCmd),
    Unhandled(u8),
}

impl PvaMsgTy {
    pub fn command(&self) -> u8 {
        use PvaMsgTy::*;
        match self {
            SetByteOrder(..) => CTRL_SET_BYTE_ORDER,
            MarkTotalBytesSent(..) => CTRL_MARK_TOTAL_BYTES_SENT,
            AckTotalBytesReceived(..) => CTRL_ACK_TOTAL_BYTES_RECEIVED,
            EchoRequestControl(..) => CTRL_ECHO_REQUEST,
            EchoResponseControl(..) => CTRL_ECHO_RESPONSE,
            Echo(..) => CMD_ECHO,
            ConnectionValidationReq(..) => CMD_CONNECTION_VALIDATION,
            ConnectionValidationRes(..) => CMD_CONNECTION_VALIDATION,
            ConnectionValidated(..) => CMD_CONNECTION_VALIDATED,
            Search(..) => CMD_SEARCH,
            SearchRes(..) => CMD_SEARCH_RESPONSE,
            Beacon(..) => CMD_BEACON,
            CreateChannelReq(..) => CMD_CREATE_CHANNEL,
            CreateChannelRes(..) => CMD_CREATE_CHANNEL,
            DestroyChannelReq(..) => CMD_DESTROY_CHANNEL,
            DestroyChannelRes(..) => CMD_DESTROY_CHANNEL,
            GetFieldReq(..) => CMD_GET_FIELD,
            GetFieldRes(..) => CMD_GET_FIELD,
            ChannelGetInit(..) => CMD_GET,
            ChannelGetInitRes(..) => CMD_GET,
            ChannelGet(..) => CMD_GET,
            ChannelGetRes(..) => CMD_GET,
            MonitorInit(..) => CMD_MONITOR,
            MonitorInitRes(..) => CMD_MONITOR,
            MonitorStart(..) => CMD_MONITOR,
            MonitorStop(..) => CMD_MONITOR,
            MonitorUpdate(..) => CMD_MONITOR,
            MonitorFinal(..) => CMD_MONITOR,
            CancelRequest(..) => CMD_CANCEL_REQUEST,
            DestroyRequest(..) => CMD_DESTROY_REQUEST,
            Message(..) => CMD_MESSAGE,
            Unhandled(x) => *x,
        }
    }

    pub fn control_data(&self) -> Option<u32> {
        use PvaMsgTy::*;
        match self {
            SetByteOrder(_, perm) => Some(if *perm { 1 } else { 0 }),
            MarkTotalBytesSent(x) => Some(*x),
            AckTotalBytesReceived(x) => Some(*x),
            EchoRequestControl(x) => Some(*x),
            EchoResponseControl(x) => Some(*x),
            _ => None,
        }
    }

    pub fn write_payload(&self, w: &mut Writer) -> Result<(), Error> {
        use PvaMsgTy::*;
        match self {
            Echo(b) => w.raw(b),
            ConnectionValidationRes(v) => {
                w.i32(v.client_buffer_size);
                w.i16(v.client_intro_registry_max_size);
                w.i16(v.connection_qos);
                w.string(&v.auth_nz);
            }
            ConnectionValidationReq(v) => {
                w.i32(v.server_buffer_size);
                w.i16(v.server_intro_registry_max_size);
                w.size(v.auth_nz.len());
                for x in &v.auth_nz {
                    w.string(x);
                }
            }
            Search(v) => v.write(w),
            SearchRes(v) => v.write(w),
            Beacon(v) => v.write(w),
            CreateChannelReq(v) => {
                w.u16(v.channels.len() as u16);
                for c in &v.channels {
                    w.u32(c.client_cid);
                    w.string(&c.name);
                }
            }
            DestroyChannelReq(v) => {
                w.u32(v.server_cid);
                w.u32(v.client_cid);
            }
            GetFieldReq(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
                w.string(&v.sub_field);
            }
            ChannelGetInit(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
                w.u8(SUB_INIT);
                v.pv_request.write(w);
            }
            MonitorInit(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
                w.u8(SUB_INIT);
                v.pv_request.write(w);
            }
            ChannelGet(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
                w.u8(SUB_GET_PUT);
            }
            MonitorStart(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
                w.u8(SUB_START);
            }
            MonitorStop(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
                w.u8(SUB_STOP);
            }
            CancelRequest(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
            }
            DestroyRequest(v) => {
                w.u32(v.server_cid);
                w.u32(v.request_id);
            }
            Message(v) => {
                w.u32(v.request_id);
                w.u8(v.message_type);
                w.string(&v.message);
            }
            _ => return Err(Error::MsgNotSerializable),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PvaMsg {
    pub ty: PvaMsgTy,
    ts: Instant,
}

impl PvaMsg {
    pub fn new(ty: PvaMsgTy) -> Self {
        Self { ty, ts: Instant::now() }
    }

    pub fn ts(&self) -> Instant {
        self.ts
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PvaItem {
    Empty,
    Msg(PvaMsg),
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PvaRequest {
    nodes: Vec<(String, PvaRequest)>,
}

impl PvaRequest {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn nodes(&self) -> &[(String, PvaRequest)] {
        &self.nodes
    }

    pub fn entry(&mut self, name: &str) -> &mut PvaRequest {
        let i = match self.nodes.iter().position(|x| x.0 == name) {
            Some(i) => i,
            None => {
                self.nodes.push((name.into(), PvaRequest::default()));
                self.nodes.len() - 1
            }
        };
        &mut self.nodes[i].1
    }

    pub fn parse(s: &str) -> Result<Self, Error> {
        let b = s.as_bytes();
        let mut i = 0;
        let mut ret = PvaRequest::default();
        parse_request_list(b, &mut i, &mut ret)?;
        skip_ws(b, &mut i);
        if i != b.len() {
            return Err(Error::BadRequestString);
        }
        Ok(ret)
    }

    pub fn from_field(f: &PvaField) -> Result<Self, Error> {
        match f {
            PvaField::Struct(s) => {
                let mut nodes = Vec::with_capacity(s.len());
                for (n, c) in s.names().iter().zip(s.fields()) {
                    nodes.push((n.clone(), Self::from_field(c)?));
                }
                Ok(Self { nodes })
            }
            _ => Err(Error::ExpectedStructIntrospection),
        }
    }

    pub fn to_struct(&self) -> Arc<PvaStruct> {
        let mut names = Vec::with_capacity(self.nodes.len());
        let mut fields = Vec::with_capacity(self.nodes.len());
        for (n, c) in &self.nodes {
            names.push(n.clone());
            fields.push(PvaField::Struct(c.to_struct()));
        }
        Arc::new(PvaStruct::new(String::new(), names, fields))
    }

    pub fn to_field(&self) -> PvaField {
        PvaField::Struct(self.to_struct())
    }

    pub fn write(&self, w: &mut Writer) {
        self.to_field().write(w);
    }
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && b[*i] == b' ' {
        *i += 1;
    }
}

fn parse_request_list(b: &[u8], i: &mut usize, out: &mut PvaRequest) -> Result<(), Error> {
    loop {
        skip_ws(b, i);
        if *i >= b.len() || b[*i] == b')' {
            break;
        }
        parse_request_item(b, i, out)?;
        skip_ws(b, i);
        if *i < b.len() && b[*i] == b',' {
            *i += 1;
            continue;
        }
        break;
    }
    Ok(())
}

fn parse_request_item(b: &[u8], i: &mut usize, out: &mut PvaRequest) -> Result<(), Error> {
    let start = *i;
    while *i < b.len() && !matches!(b[*i], b',' | b'(' | b')' | b'.' | b' ') {
        *i += 1;
    }
    if *i == start {
        return Err(Error::BadRequestString);
    }
    let name = str::from_utf8(&b[start..*i]).map_err(|_| Error::BadRequestString)?;
    let child = out.entry(name);
    if *i < b.len() && b[*i] == b'.' {
        *i += 1;
        return parse_request_item(b, i, child);
    }
    skip_ws(b, i);
    if *i < b.len() && b[*i] == b'(' {
        *i += 1;
        parse_request_list(b, i, child)?;
        skip_ws(b, i);
        if *i >= b.len() || b[*i] != b')' {
            return Err(Error::BadRequestString);
        }
        *i += 1;
    }
    Ok(())
}

struct ParseCtx<'a> {
    reg: &'a mut IntroRegistry,
    reqs: &'a mut HashMap<u32, Arc<PvaStruct>>,
    array_truncate: usize,
}

fn control_msg(head: &PvaHead) -> Result<PvaMsgTy, Error> {
    let ret = match head.command {
        CTRL_MARK_TOTAL_BYTES_SENT => PvaMsgTy::MarkTotalBytesSent(head.payload_size),
        CTRL_ACK_TOTAL_BYTES_RECEIVED => PvaMsgTy::AckTotalBytesReceived(head.payload_size),
        CTRL_SET_BYTE_ORDER => PvaMsgTy::SetByteOrder(head.endian(), head.payload_size != 0),
        CTRL_ECHO_REQUEST => PvaMsgTy::EchoRequestControl(head.payload_size),
        CTRL_ECHO_RESPONSE => PvaMsgTy::EchoResponseControl(head.payload_size),
        x => PvaMsgTy::Unhandled(x),
    };
    Ok(ret)
}

fn parse_init_res(r: &mut Reader, request_id: u32, ctx: &mut ParseCtx) -> Result<RequestInitRes, Error> {
    let status = r.status()?;
    let ty = if status.has_data() {
        match ctx.reg.parse_field(r)? {
            Some(PvaField::Struct(s)) => {
                ctx.reqs.insert(request_id, s.clone());
                Some(s)
            }
            Some(_) => return Err(Error::ExpectedStructIntrospection),
            None => None,
        }
    } else {
        None
    };
    Ok(RequestInitRes { request_id, status, ty })
}

fn parse_delta(r: &mut Reader, request_id: u32, ctx: &mut ParseCtx) -> Result<PvaDelta, Error> {
    let ty = match ctx.reqs.get(&request_id) {
        Some(x) => x.clone(),
        None => return Err(Error::NoIntrospectionForRequest(request_id)),
    };
    let changed = r.bitset()?;
    PvaStructValue::decode_partial(&ty, &changed, r, ctx.reg, ctx.array_truncate)
}

fn parse_channel_request(r: &mut Reader, monitor: bool, ctx: &mut ParseCtx) -> Result<PvaMsgTy, Error> {
    let server_cid = r.u32()?;
    let request_id = r.u32()?;
    let sub = r.u8()?;
    if sub & SUB_INIT != 0 {
        let f = match ctx.reg.parse_field(r)? {
            Some(x) => x,
            None => return Err(Error::MissingIntrospection),
        };
        let v = ChannelRequestInit {
            server_cid,
            request_id,
            pv_request: PvaRequest::from_field(&f)?,
        };
        if monitor {
            Ok(PvaMsgTy::MonitorInit(v))
        } else {
            Ok(PvaMsgTy::ChannelGetInit(v))
        }
    } else {
        let v = ChannelRequestOp { server_cid, request_id };
        if !monitor {
            Ok(PvaMsgTy::ChannelGet(v))
        } else if sub == SUB_START {
            Ok(PvaMsgTy::MonitorStart(v))
        } else if sub & SUB_STOP != 0 {
            Ok(PvaMsgTy::MonitorStop(v))
        } else {
            Ok(PvaMsgTy::MonitorStart(v))
        }
    }
}

fn parse_payload(head: &PvaHead, payload: &[u8], ctx: &mut ParseCtx) -> Result<PvaMsgTy, Error> {
    let mut r = Reader::new(payload, head.endian());
    let r = &mut r;
    let ret = match head.command {
        CMD_ECHO => PvaMsgTy::Echo(r.rest().to_vec()),
        CMD_CONNECTION_VALIDATION => {
            if head.from_server() {
                let server_buffer_size = r.i32()?;
                let server_intro_registry_max_size = r.i16()?;
                let n = r.size_req()?;
                if n > 64 {
                    return Err(Error::ArrayTooLong(n));
                }
                let mut auth_nz = Vec::with_capacity(n);
                for _ in 0..n {
                    auth_nz.push(r.string()?);
                }
                PvaMsgTy::ConnectionValidationReq(ConnValidReq {
                    server_buffer_size,
                    server_intro_registry_max_size,
                    auth_nz,
                })
            } else {
                PvaMsgTy::ConnectionValidationRes(ConnValidRes {
                    client_buffer_size: r.i32()?,
                    client_intro_registry_max_size: r.i16()?,
                    connection_qos: r.i16()?,
                    auth_nz: r.string()?,
                })
            }
        }
        CMD_CONNECTION_VALIDATED => PvaMsgTy::ConnectionValidated(r.status()?),
        CMD_SEARCH => PvaMsgTy::Search(SearchReq::parse(r)?),
        CMD_SEARCH_RESPONSE => PvaMsgTy::SearchRes(SearchRes::parse(r)?),
        CMD_BEACON => PvaMsgTy::Beacon(Beacon::parse(r, ctx.reg, ctx.array_truncate)?),
        CMD_CREATE_CHANNEL => {
            if head.from_server() {
                let client_cid = r.u32()?;
                let server_cid = r.u32()?;
                let status = r.status()?;
                let access_rights = if !r.is_empty() { Some(r.i16()?) } else { None };
                PvaMsgTy::CreateChannelRes(CreateChannelRes {
                    client_cid,
                    server_cid,
                    status,
                    access_rights,
                })
            } else {
                let n = r.u16()? as usize;
                if n > 1024 {
                    return Err(Error::ArrayTooLong(n));
                }
                let mut channels = Vec::with_capacity(n);
                for _ in 0..n {
                    let client_cid = r.u32()?;
                    let name = r.string()?;
                    channels.push(ChannelReq { client_cid, name });
                }
                PvaMsgTy::CreateChannelReq(CreateChannelReq { channels })
            }
        }
        CMD_DESTROY_CHANNEL => {
            let v = DestroyChannel {
                server_cid: r.u32()?,
                client_cid: r.u32()?,
            };
            if head.from_server() {
                PvaMsgTy::DestroyChannelRes(v)
            } else {
                PvaMsgTy::DestroyChannelReq(v)
            }
        }
        CMD_GET_FIELD => {
            if head.from_server() {
                let request_id = r.u32()?;
                let status = r.status()?;
                let field = if status.has_data() {
                    ctx.reg.parse_field(r)?
                } else {
                    None
                };
                PvaMsgTy::GetFieldRes(GetFieldRes {
                    request_id,
                    status,
                    field,
                })
            } else {
                PvaMsgTy::GetFieldReq(GetFieldReq {
                    server_cid: r.u32()?,
                    request_id: r.u32()?,
                    sub_field: r.string()?,
                })
            }
        }
        CMD_GET if !head.from_server() => parse_channel_request(r, false, ctx)?,
        CMD_MONITOR if !head.from_server() => parse_channel_request(r, true, ctx)?,
        CMD_GET => {
            let request_id = r.u32()?;
            let sub = r.u8()?;
            if sub & SUB_INIT != 0 {
                PvaMsgTy::ChannelGetInitRes(parse_init_res(r, request_id, ctx)?)
            } else {
                let status = r.status()?;
                let delta = if status.has_data() {
                    Some(parse_delta(r, request_id, ctx)?)
                } else {
                    None
                };
                PvaMsgTy::ChannelGetRes(ChannelGetRes {
                    request_id,
                    status,
                    delta,
                })
            }
        }
        CMD_MONITOR => {
            let request_id = r.u32()?;
            let sub = r.u8()?;
            if sub & SUB_INIT != 0 {
                PvaMsgTy::MonitorInitRes(parse_init_res(r, request_id, ctx)?)
            } else if sub & SUB_DESTROY != 0 && r.is_empty() {
                ctx.reqs.remove(&request_id);
                PvaMsgTy::MonitorFinal(RequestIdOp { request_id })
            } else {
                let delta = parse_delta(r, request_id, ctx)?;
                let overrun = r.bitset()?;
                if sub & SUB_DESTROY != 0 {
                    ctx.reqs.remove(&request_id);
                }
                PvaMsgTy::MonitorUpdate(MonitorUpdate {
                    request_id,
                    delta,
                    overrun,
                })
            }
        }
        CMD_MESSAGE => PvaMsgTy::Message(MessageCmd {
            request_id: r.u32()?,
            message_type: r.u8()?,
            message: r.string()?,
        }),
        CMD_DESTROY_REQUEST => {
            let v = ChannelRequestOp {
                server_cid: r.u32()?,
                request_id: r.u32()?,
            };
            ctx.reqs.remove(&v.request_id);
            PvaMsgTy::DestroyRequest(v)
        }
        CMD_CANCEL_REQUEST => PvaMsgTy::CancelRequest(ChannelRequestOp {
            server_cid: r.u32()?,
            request_id: r.u32()?,
        }),
        x => {
            debug!("unhandled pva command {x:02x}");
            PvaMsgTy::Unhandled(x)
        }
    };
    Ok(ret)
}

pub fn write_message(out: &mut Vec<u8>, ty: &PvaMsgTy, endian: Endian) -> Result<(), Error> {
    let be = if let Endian::Big = endian { FLAG_BIG_ENDIAN } else { 0 };
    if let Some(data) = ty.control_data() {
        PvaHead {
            version: PVA_VERSION,
            flags: FLAG_CONTROL | be,
            command: ty.command(),
            payload_size: data,
        }
        .write(out);
        return Ok(());
    }
    let mut payload = Vec::new();
    {
        let mut w = Writer::new(&mut payload, endian);
        ty.write_payload(&mut w)?;
    }
    PvaHead {
        version: PVA_VERSION,
        flags: be,
        command: ty.command(),
        payload_size: payload.len() as u32,
    }
    .write(out);
    out.extend_from_slice(&payload);
    Ok(())
}

pub fn decode_datagram(b: &[u8], reg: &mut IntroRegistry, array_truncate: usize) -> Result<Vec<PvaMsgTy>, Error> {
    let mut reqs = HashMap::new();
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 8 <= b.len() {
        let head = PvaHead::parse(&b[pos..pos + 8])?;
        pos += 8;
        if head.is_control() {
            out.push(control_msg(&head)?);
            continue;
        }
        let n = head.payload_size as usize;
        if pos + n > b.len() {
            return Err(Error::NotEnoughInput(n, b.len() - pos));
        }
        let mut ctx = ParseCtx {
            reg,
            reqs: &mut reqs,
            array_truncate,
        };
        out.push(parse_payload(&head, &b[pos..pos + n], &mut ctx)?);
        pos += n;
    }
    Ok(out)
}

#[derive(Clone, Debug, PartialEq)]
enum PvaState {
    Head,
    Payload(PvaHead),
    Done,
}

impl PvaState {
    fn need_min(&self) -> usize {
        match self {
            PvaState::Head => 8,
            PvaState::Payload(h) => h.payload_size as usize,
            PvaState::Done => 8,
        }
    }
}

pub trait AsyncWriteRead: AsyncWrite + AsyncRead + Send + 'static {}

impl<T> AsyncWriteRead for T where T: AsyncWrite + AsyncRead + Send + 'static {}

pub struct PvaProto {
    tcp: Pin<Box<dyn AsyncWriteRead>>,
    raw_socket_fd: Option<i32>,
    tcp_eof: bool,
    remote_name: String,
    state: PvaState,
    buf: SlideBuf,
    outbuf: SlideBuf,
    out: VecDeque<PvaMsg>,
    array_truncate: usize,
    resqu: VecDeque<PvaItem>,
    conn_endian: Endian,
    seg: Vec<u8>,
    seg_head: Option<PvaHead>,
    scratch: Vec<u8>,
    intro_recv: IntroRegistry,
    reqs: HashMap<u32, Arc<PvaStruct>>,
    tcp_read_bytes: u64,
}

impl fmt::Debug for PvaProto {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("PvaProto")
            .field("raw_socket_fd", &self.raw_socket_fd)
            .field("tcp_eof", &self.tcp_eof)
            .field("remote_name", &self.remote_name)
            .field("state", &self.state)
            .field("buf", &self.buf)
            .field("outbuf", &self.outbuf)
            .field("out", &self.out)
            .field("array_truncate", &self.array_truncate)
            .field("resqu", &self.resqu)
            .field("conn_endian", &self.conn_endian)
            .field("seg", &self.seg.len())
            .field("seg_head", &self.seg_head)
            .field("reqs", &self.reqs.len())
            .field("tcp_read_bytes", &self.tcp_read_bytes)
            .finish()
    }
}

impl PvaProto {
    pub fn new<T: AsyncWriteRead>(
        tcp: T,
        raw_socket_fd: Option<i32>,
        remote_name: String,
        array_truncate: usize,
    ) -> Self {
        Self {
            tcp: Box::pin(tcp),
            raw_socket_fd,
            tcp_eof: false,
            remote_name,
            state: PvaState::Head,
            buf: SlideBuf::new(PVA_INPUT_BUF_CAP),
            outbuf: SlideBuf::new(PVA_OUTPUT_BUF_CAP),
            out: VecDeque::new(),
            array_truncate,
            resqu: VecDeque::with_capacity(256),
            conn_endian: Endian::Little,
            seg: Vec::new(),
            seg_head: None,
            scratch: Vec::new(),
            intro_recv: IntroRegistry::new(),
            reqs: HashMap::new(),
            tcp_read_bytes: 0,
        }
    }

    pub fn remote_name(&self) -> &str {
        &self.remote_name
    }

    pub fn conn_endian(&self) -> Endian {
        self.conn_endian
    }

    pub fn request_type(&self, request_id: u32) -> Option<Arc<PvaStruct>> {
        self.reqs.get(&request_id).cloned()
    }

    pub fn proto_out_space(&self) -> bool {
        self.out.len() < 20
    }

    pub fn push_out(&mut self, ty: PvaMsgTy) {
        if let PvaMsgTy::DestroyRequest(v) = &ty {
            self.reqs.remove(&v.request_id);
        }
        self.out.push_back(PvaMsg::new(ty));
    }

    fn attempt_output(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<usize, Error>> {
        use Poll::*;
        let this = self.as_mut().get_mut();
        let w = &mut this.tcp;
        let b = this.outbuf.data();
        let w = Pin::new(w);
        match w.poll_write(cx, b) {
            Ready(k) => match k {
                Ok(k) => {
                    trace_in_out!("written to tcp  {k}");
                    match self.outbuf.adv(k) {
                        Ok(()) => Ready(Ok(k)),
                        Err(e) => {
                            error!("advance error {:?}", e);
                            Ready(Err(e.into()))
                        }
                    }
                }
                Err(e) => {
                    error!("output write error {:?}", e);
                    Ready(Err(e.into()))
                }
            },
            Pending => Pending,
        }
    }

    fn stage_out(&mut self) -> Result<bool, Error> {
        let mut scratch = std::mem::take(&mut self.scratch);
        let mut ret = Ok(false);
        while let Some(msg) = self.out.front() {
            scratch.clear();
            if let Err(e) = write_message(&mut scratch, &msg.ty, self.conn_endian) {
                ret = Err(e);
                break;
            }
            let total = scratch.len();
            match self.outbuf.available_writable_area(total) {
                Ok(area) => {
                    if area.len() < total {
                        break;
                    }
                    area[..total].copy_from_slice(&scratch);
                }
                Err(_) => break,
            }
            self.outbuf.wadv(total)?;
            self.out.pop_front();
            ret = Ok(true);
        }
        self.scratch = scratch;
        ret
    }

    pub fn poll_outbound(mut self: Pin<&mut Self>, cx: &mut Context) -> Option<Poll<Option<Result<(), Error>>>> {
        use Poll::*;
        let mut have_pending = false;
        let mut have_progress = false;
        match self.as_mut().get_mut().stage_out() {
            Ok(x) => {
                if x {
                    have_progress = true;
                }
            }
            Err(e) => return Some(Ready(Some(Err(e)))),
        }
        while self.outbuf.len() != 0 {
            match Self::attempt_output(self.as_mut(), cx) {
                Ready(x) => match x {
                    Ok(n) => {
                        if n == 0 {
                            return Some(Ready(Some(Err(Error::LogicError))));
                        }
                        have_progress = true;
                    }
                    Err(e) => return Some(Ready(Some(Err(e)))),
                },
                Pending => {
                    have_pending = true;
                    break;
                }
            }
        }
        if have_progress {
            Some(Ready(Some(Ok(()))))
        } else if have_pending {
            Some(Pending)
        } else {
            None
        }
    }

    fn loop_body(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        use Poll::*;
        let mut have_pending = false;
        let mut have_progress = false;
        let tsnow = Instant::now();
        match self.as_mut().poll_outbound(cx) {
            Some(Ready(Some(x))) => match x {
                Ok(()) => {
                    have_progress = true;
                }
                Err(e) => return Err(e),
            },
            Some(Ready(None)) => {}
            Some(Pending) => {
                have_pending = true;
            }
            None => {}
        }
        let need_min = self.state.need_min();
        {
            let cap = self.buf.cap();
            if cap < need_min {
                let e = Error::BufferTooSmallForNeedMin(cap, need_min);
                warn!("{e}");
                return Err(e);
            }
        }
        loop {
            if self.tcp_eof {
                break;
            }
            let this = self.as_mut().get_mut();
            let tcp = Pin::new(&mut this.tcp);
            let buf = this.buf.available_writable_area(need_min)?;
            if buf.is_empty() {
                return Err(Error::NoReadBufferSpace);
            }
            break match tcp.poll_read(cx, buf) {
                Ready(k) => match k {
                    Ok(nf) => {
                        if nf == 0 {
                            debug!("peer done  {:?}  {:?}", self.remote_name, self.state);
                            self.tcp_eof = true;
                        } else {
                            trace_in_out!("received bytes {nf}");
                            self.buf.wadv(nf)?;
                            have_progress = true;
                            self.tcp_read_bytes = self.tcp_read_bytes.wrapping_add(nf as _);
                            continue;
                        }
                    }
                    Err(e) => return Err(e.into()),
                },
                Pending => {
                    have_pending = true;
                }
            };
        }
        while self.resqu.len() < self.resqu.capacity() {
            if self.buf.len() >= self.state.need_min() {
                if let Some(item) = self.parse_item(tsnow)? {
                    self.resqu.push_back(item);
                }
                have_progress = true;
            } else {
                break;
            }
        }
        if have_progress {
            Ok(Ready(()))
        } else if have_pending {
            Ok(Pending)
        } else {
            if self.tcp_eof {
                self.state = PvaState::Done;
                Ok(Ready(()))
            } else {
                Err(Error::NeitherPendingNorProgress)
            }
        }
    }

    fn parse_item(&mut self, tsnow: Instant) -> Result<Option<PvaItem>, Error> {
        let head = match &self.state {
            PvaState::Head => None,
            PvaState::Payload(h) => Some(h.clone()),
            PvaState::Done => return Err(Error::ParseAttemptInDoneState),
        };
        match head {
            None => self.parse_head(tsnow),
            Some(head) => self.parse_payload_state(head, tsnow),
        }
    }

    fn parse_head(&mut self, tsnow: Instant) -> Result<Option<PvaItem>, Error> {
        let head = PvaHead::parse(self.buf.read_bytes(8)?)?;
        if head.is_control() {
            let ty = control_msg(&head)?;
            if let PvaMsgTy::SetByteOrder(e, _) = &ty {
                self.conn_endian = *e;
            }
            return Ok(Some(PvaItem::Msg(PvaMsg { ty, ts: tsnow })));
        }
        if head.payload_size > PVA_PAYLOAD_LEN_MAX {
            return Err(Error::PayloadTooLarge(head.payload_size));
        }
        if head.payload_size == 0 && head.segment() == SEGMENT_NONE {
            let Self {
                intro_recv,
                reqs,
                array_truncate,
                ..
            } = self;
            let mut ctx = ParseCtx {
                reg: intro_recv,
                reqs,
                array_truncate: *array_truncate,
            };
            let ty = parse_payload(&head, &[], &mut ctx)?;
            return Ok(Some(PvaItem::Msg(PvaMsg { ty, ts: tsnow })));
        }
        self.state = PvaState::Payload(head);
        Ok(None)
    }

    fn parse_payload_state(&mut self, head: PvaHead, tsnow: Instant) -> Result<Option<PvaItem>, Error> {
        let n = head.payload_size as usize;
        let segf = head.segment();
        let ret = {
            let Self {
                buf,
                seg,
                seg_head,
                intro_recv,
                reqs,
                array_truncate,
                ..
            } = self;
            let mut ctx = ParseCtx {
                reg: intro_recv,
                reqs,
                array_truncate: *array_truncate,
            };
            if segf == SEGMENT_NONE {
                let ty = parse_payload(&head, buf.read_bytes(n)?, &mut ctx)?;
                Some(PvaItem::Msg(PvaMsg { ty, ts: tsnow }))
            } else if segf == SEGMENT_FIRST || segf == SEGMENT_MIDDLE {
                if segf == SEGMENT_FIRST {
                    seg.clear();
                    *seg_head = Some(head.clone());
                }
                match seg_head.as_ref() {
                    Some(h) => {
                        if h.command != head.command {
                            return Err(Error::SegmentCommandMismatch(h.command, head.command));
                        }
                    }
                    None => return Err(Error::SegmentWithoutStart),
                }
                if seg.len() + n > PVA_SEGMENT_LEN_MAX {
                    return Err(Error::SegmentTooLarge(seg.len() + n));
                }
                seg.extend_from_slice(buf.read_bytes(n)?);
                None
            } else {
                let h0 = match seg_head.take() {
                    Some(x) => x,
                    None => return Err(Error::SegmentWithoutStart),
                };
                if h0.command != head.command {
                    return Err(Error::SegmentCommandMismatch(h0.command, head.command));
                }
                seg.extend_from_slice(buf.read_bytes(n)?);
                let ty = parse_payload(&h0, seg, &mut ctx)?;
                seg.clear();
                Some(PvaItem::Msg(PvaMsg { ty, ts: tsnow }))
            }
        };
        self.state = PvaState::Head;
        Ok(ret)
    }
}

impl Stream for PvaProto {
    type Item = Result<PvaItem, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(item) = self.resqu.pop_front() {
                Ready(Some(Ok(item)))
            } else if let PvaState::Done = self.state {
                Ready(None)
            } else {
                let k = Self::loop_body(self.as_mut(), cx);
                match k {
                    Ok(Ready(())) => continue,
                    Ok(Pending) => Pending,
                    Err(e) => {
                        self.state = PvaState::Done;
                        Ready(Some(Err(e)))
                    }
                }
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pva::field::test::composite_pv;
    use crate::pva::merge::PvaMonitorMerge;
    use crate::pva::value::PvaValue;
    use std::io;
    use std::sync::Mutex;
    use std::task::Waker;

    struct TestSock {
        inp: Vec<u8>,
        pos: usize,
        out: Arc<Mutex<Vec<u8>>>,
    }

    impl TestSock {
        fn new(inp: Vec<u8>) -> Self {
            Self {
                inp,
                pos: 0,
                out: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn out_handle(&self) -> Arc<Mutex<Vec<u8>>> {
            self.out.clone()
        }
    }

    impl AsyncRead for TestSock {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
            let n = (self.inp.len() - self.pos).min(buf.len());
            let p = self.pos;
            buf[..n].copy_from_slice(&self.inp[p..p + n]);
            self.pos += n;
            Poll::Ready(Ok(n))
        }
    }

    impl AsyncWrite for TestSock {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
            self.out.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn drive(inp: Vec<u8>) -> Vec<PvaMsgTy> {
        let mut proto = PvaProto::new(TestSock::new(inp), None, "test".into(), usize::MAX);
        let mut cx = Context::from_waker(Waker::noop());
        let mut ret = Vec::new();
        loop {
            match Pin::new(&mut proto).poll_next(&mut cx) {
                Poll::Ready(Some(Ok(PvaItem::Msg(m)))) => ret.push(m.ty),
                Poll::Ready(Some(Ok(PvaItem::Empty))) => {}
                Poll::Ready(Some(Err(e))) => panic!("proto error {e}"),
                Poll::Ready(None) => break,
                Poll::Pending => panic!("unexpected pending"),
            }
        }
        ret
    }

    fn payload(f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut buf = Vec::new();
        f(&mut Writer::new(&mut buf, Endian::Little));
        buf
    }

    fn frame(command: u8, flags: u8, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        PvaHead {
            version: PVA_VERSION,
            flags,
            command,
            payload_size: body.len() as u32,
        }
        .write(&mut out);
        out.extend_from_slice(body);
        out
    }

    fn from_server(command: u8, body: &[u8]) -> Vec<u8> {
        frame(command, FLAG_FROM_SERVER, body)
    }

    fn set_byte_order() -> Vec<u8> {
        let mut out = Vec::new();
        PvaHead {
            version: PVA_VERSION,
            flags: FLAG_CONTROL | FLAG_FROM_SERVER,
            command: CTRL_SET_BYTE_ORDER,
            payload_size: 0,
        }
        .write(&mut out);
        out
    }

    fn monitor_init_res(request_id: u32) -> Vec<u8> {
        let b = payload(|w| {
            w.u32(request_id);
            w.u8(SUB_INIT);
            w.status(&Status::ok());
            PvaField::Struct(composite_pv()).write(w);
        });
        from_server(CMD_MONITOR, &b)
    }

    fn composite_body(w: &mut Writer, secs: i64, nanos: i32, pulse: i64, v1: f64, v2: f64, v3: &[f32]) {
        w.i64(secs);
        w.i32(nanos);
        w.i64(pulse);
        w.f64(v1);
        w.f64(v2);
        w.size(v3.len());
        for x in v3 {
            w.f32(*x);
        }
    }

    fn monitor_full(request_id: u32, secs: i64, pulse: i64, v1: f64) -> Vec<u8> {
        let mut changed = BitSet::new();
        changed.set(0);
        let b = payload(|w| {
            w.u32(request_id);
            w.u8(SUB_GET);
            w.bitset(&changed);
            composite_body(w, secs, 5, pulse, v1, 2.0, &[1., 2., 3.]);
            w.bitset(&BitSet::new());
        });
        from_server(CMD_MONITOR, &b)
    }

    fn monitor_delta(request_id: u32, secs: i64, v1: f64) -> Vec<u8> {
        let ty = composite_pv();
        let mut changed = BitSet::new();
        changed.set(ty.offset_of_path("timeStamp").unwrap());
        changed.set(ty.offset_of_path("value1").unwrap());
        let b = payload(|w| {
            w.u32(request_id);
            w.u8(SUB_GET);
            w.bitset(&changed);
            w.i64(secs);
            w.i32(7);
            w.f64(v1);
            w.bitset(&BitSet::new());
        });
        from_server(CMD_MONITOR, &b)
    }

    fn take_update(msg: &PvaMsgTy) -> &MonitorUpdate {
        match msg {
            PvaMsgTy::MonitorUpdate(x) => x,
            x => panic!("expected monitor update, got {x:?}"),
        }
    }

    #[test]
    fn handshake_prefix() {
        let mut inp = set_byte_order();
        inp.extend_from_slice(&from_server(
            CMD_CONNECTION_VALIDATION,
            &payload(|w| {
                w.i32(16384);
                w.i16(0x7fff);
                w.size(1);
                w.string("anonymous");
            }),
        ));
        inp.extend_from_slice(&from_server(
            CMD_CONNECTION_VALIDATED,
            &payload(|w| w.status(&Status::ok())),
        ));
        let msgs = drive(inp);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], PvaMsgTy::SetByteOrder(Endian::Little, false));
        match &msgs[1] {
            PvaMsgTy::ConnectionValidationReq(v) => {
                assert_eq!(v.server_buffer_size, 16384);
                assert_eq!(v.auth_nz, vec!["anonymous".to_string()]);
            }
            x => panic!("unexpected {x:?}"),
        }
        assert_eq!(msgs[2], PvaMsgTy::ConnectionValidated(Status::ok()));
    }

    #[test]
    fn control_message_between_application_messages() {
        let mut inp = monitor_init_res(1);
        let mut ctrl = Vec::new();
        PvaHead {
            version: PVA_VERSION,
            flags: FLAG_CONTROL | FLAG_FROM_SERVER,
            command: CTRL_MARK_TOTAL_BYTES_SENT,
            payload_size: 4242,
        }
        .write(&mut ctrl);
        inp.extend_from_slice(&ctrl);
        inp.extend_from_slice(&monitor_full(1, 10, 100, 1.5));
        let msgs = drive(inp);
        assert_eq!(msgs.len(), 3);
        assert!(matches!(msgs[0], PvaMsgTy::MonitorInitRes(_)));
        assert_eq!(msgs[1], PvaMsgTy::MarkTotalBytesSent(4242));
        assert!(matches!(msgs[2], PvaMsgTy::MonitorUpdate(_)));
    }

    #[test]
    fn segmented_message_reassembled() {
        let body = payload(|w| {
            w.u32(3);
            w.u8(SUB_INIT);
            w.status(&Status::ok());
            PvaField::Struct(composite_pv()).write(w);
        });
        let a = body.len() / 3;
        let b = 2 * body.len() / 3;
        let mut inp = frame(CMD_MONITOR, FLAG_FROM_SERVER | 0x10, &body[..a]);
        inp.extend_from_slice(&frame(CMD_MONITOR, FLAG_FROM_SERVER | 0x30, &body[a..b]));
        inp.extend_from_slice(&frame(CMD_MONITOR, FLAG_FROM_SERVER | 0x20, &body[b..]));
        let msgs = drive(inp);
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            PvaMsgTy::MonitorInitRes(v) => {
                assert_eq!(v.request_id, 3);
                assert_eq!(v.ty.as_ref().unwrap().id(), "my:composite:1.0");
            }
            x => panic!("unexpected {x:?}"),
        }
    }

    #[test]
    fn monitor_update_without_init_errors() {
        let mut proto = PvaProto::new(
            TestSock::new(monitor_full(1, 10, 100, 1.5)),
            None,
            "test".into(),
            usize::MAX,
        );
        let mut cx = Context::from_waker(Waker::noop());
        match Pin::new(&mut proto).poll_next(&mut cx) {
            Poll::Ready(Some(Err(Error::NoIntrospectionForRequest(1)))) => {}
            x => panic!("unexpected {x:?}"),
        }
    }

    #[test]
    fn monitor_consumed_as_deltas() {
        let mut inp = monitor_init_res(1);
        inp.extend_from_slice(&monitor_full(1, 10, 100, 1.5));
        inp.extend_from_slice(&monitor_delta(1, 11, 2.5));
        inp.extend_from_slice(&monitor_delta(1, 12, 3.5));
        let msgs = drive(inp);
        assert_eq!(msgs.len(), 4);

        let d0 = &take_update(&msgs[1]).delta;
        assert!(d0.is_full());
        assert_eq!(d0.get("value1"), Some(&PvaValue::F64(1.5)));
        assert_eq!(d0.get("pulseId"), Some(&PvaValue::I64(100)));
        assert_eq!(d0.get("value3"), Some(&PvaValue::F32Array(vec![1., 2., 3.])));

        for (i, exp) in [(2usize, 2.5f64), (3, 3.5)] {
            let d = &take_update(&msgs[i]).delta;
            assert!(!d.is_full());
            assert!(d.touches("value1"));
            assert!(d.touches("timeStamp"));
            assert!(!d.touches("pulseId"));
            assert!(!d.touches("value2"));
            assert!(!d.touches("value3"));
            assert_eq!(d.entries().len(), 2);
            assert_eq!(d.get("value1"), Some(&PvaValue::F64(exp)));
            assert_eq!(d.get("pulseId"), None);
            assert_eq!(d.get("value3"), None);
        }

        let ty = composite_pv();
        let d1 = &take_update(&msgs[2]).delta;
        assert_eq!(d1.entries()[0].0, ty.offset_of_path("timeStamp").unwrap());
        assert_eq!(d1.entries()[1].0, ty.offset_of_path("value1").unwrap());
        assert_eq!(d1.get("timeStamp.secondsPastEpoch"), Some(&PvaValue::I64(11)));
        assert_eq!(d1.get("timeStamp.nanoseconds"), Some(&PvaValue::I32(7)));
    }

    #[test]
    fn monitor_consumed_as_full_values() {
        let mut inp = monitor_init_res(1);
        inp.extend_from_slice(&monitor_full(1, 10, 100, 1.5));
        inp.extend_from_slice(&monitor_delta(1, 11, 2.5));
        inp.extend_from_slice(&monitor_delta(1, 12, 3.5));
        let msgs = drive(inp);

        let ty = match &msgs[0] {
            PvaMsgTy::MonitorInitRes(v) => v.ty.clone().unwrap(),
            x => panic!("unexpected {x:?}"),
        };
        let mut merge = PvaMonitorMerge::new(ty);
        assert!(!merge.has_value());

        let mut seen = Vec::new();
        for m in &msgs[1..] {
            merge.apply(&take_update(m).delta).unwrap();
            let v = merge.value();
            seen.push((
                v.get("timeStamp.secondsPastEpoch").unwrap().as_i64().unwrap(),
                v.get("pulseId").unwrap().as_i64().unwrap(),
                v.get("value1").unwrap().as_f64().unwrap(),
                v.get("value2").unwrap().as_f64().unwrap(),
                v.get("value3").unwrap().clone(),
            ));
        }
        let arr = PvaValue::F32Array(vec![1., 2., 3.]);
        assert_eq!(
            seen,
            vec![
                (10, 100, 1.5, 2.0, arr.clone()),
                (11, 100, 2.5, 2.0, arr.clone()),
                (12, 100, 3.5, 2.0, arr.clone()),
            ]
        );
        assert_eq!(merge.applied_count(), 3);
        let fin = merge.into_value();
        assert_eq!(fin.type_id(), "my:composite:1.0");
        assert_eq!(fin.get("timeStamp.nanoseconds"), Some(&PvaValue::I32(7)));
    }

    #[test]
    fn monitor_final_drops_introspection() {
        let mut inp = monitor_init_res(1);
        inp.extend_from_slice(&monitor_full(1, 10, 100, 1.5));
        inp.extend_from_slice(&from_server(
            CMD_MONITOR,
            &payload(|w| {
                w.u32(1);
                w.u8(SUB_DESTROY);
            }),
        ));
        let msgs = drive(inp);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2], PvaMsgTy::MonitorFinal(RequestIdOp { request_id: 1 }));
    }

    #[test]
    fn channel_get_response_decodes() {
        let b = payload(|w| {
            w.u32(5);
            w.u8(SUB_INIT);
            w.status(&Status::ok());
            PvaField::Struct(composite_pv()).write(w);
        });
        let mut inp = from_server(CMD_GET, &b);
        let mut changed = BitSet::new();
        changed.set(0);
        inp.extend_from_slice(&from_server(
            CMD_GET,
            &payload(|w| {
                w.u32(5);
                w.u8(SUB_GET_PUT);
                w.status(&Status::ok());
                w.bitset(&changed);
                composite_body(w, 1, 2, 3, 4., 5., &[6.]);
            }),
        ));
        let msgs = drive(inp);
        assert_eq!(msgs.len(), 2);
        match &msgs[1] {
            PvaMsgTy::ChannelGetRes(v) => {
                let d = v.delta.as_ref().unwrap();
                assert!(d.is_full());
                let full = d.clone().into_full().unwrap();
                assert_eq!(full.get("pulseId"), Some(&PvaValue::I64(3)));
                assert_eq!(full.get("value3"), Some(&PvaValue::F32Array(vec![6.])));
            }
            x => panic!("unexpected {x:?}"),
        }
    }

    #[test]
    fn unhandled_command_does_not_kill_connection() {
        let mut inp = from_server(CMD_RPC, &[1, 2, 3, 4]);
        inp.extend_from_slice(&from_server(
            CMD_CONNECTION_VALIDATED,
            &payload(|w| w.status(&Status::ok())),
        ));
        let msgs = drive(inp);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], PvaMsgTy::Unhandled(CMD_RPC));
        assert_eq!(msgs[1], PvaMsgTy::ConnectionValidated(Status::ok()));
    }

    #[test]
    fn outbound_messages_are_written() {
        let sock = TestSock::new(Vec::new());
        let outbuf = sock.out_handle();
        let mut proto = PvaProto::new(sock, None, "test".into(), usize::MAX);
        assert!(proto.proto_out_space());
        proto.push_out(PvaMsgTy::ConnectionValidationRes(ConnValidRes::anonymous()));
        proto.push_out(PvaMsgTy::CreateChannelReq(CreateChannelReq {
            channels: vec![ChannelReq {
                client_cid: 1,
                name: "SOME:PV".into(),
            }],
        }));
        proto.push_out(PvaMsgTy::MonitorInit(ChannelRequestInit {
            server_cid: 9,
            request_id: 1,
            pv_request: PvaRequest::parse("field(value1,timeStamp.secondsPastEpoch)").unwrap(),
        }));
        let mut cx = Context::from_waker(Waker::noop());
        match Pin::new(&mut proto).poll_next(&mut cx) {
            Poll::Ready(None) => {}
            x => panic!("unexpected {x:?}"),
        }
        let out = outbuf.lock().unwrap().clone();
        let mut reg = IntroRegistry::new();
        let back = decode_datagram(&out, &mut reg, usize::MAX).unwrap();
        assert_eq!(back.len(), 3);
        match &back[0] {
            PvaMsgTy::ConnectionValidationRes(v) => assert_eq!(v.auth_nz, "anonymous"),
            x => panic!("unexpected {x:?}"),
        }
        match &back[1] {
            PvaMsgTy::CreateChannelReq(v) => assert_eq!(v.channels[0].name, "SOME:PV"),
            x => panic!("unexpected {x:?}"),
        }
        match &back[2] {
            PvaMsgTy::MonitorInit(v) => {
                assert_eq!(v.server_cid, 9);
                assert_eq!(v.request_id, 1);
                assert_eq!(
                    v.pv_request,
                    PvaRequest::parse("field(value1,timeStamp.secondsPastEpoch)").unwrap()
                );
            }
            x => panic!("unexpected {x:?}"),
        }
    }

    #[test]
    fn pv_request_parse() {
        let r = PvaRequest::parse("field(value,timeStamp,alarm)").unwrap();
        assert_eq!(r.nodes().len(), 1);
        assert_eq!(r.nodes()[0].0, "field");
        assert_eq!(r.nodes()[0].1.nodes().len(), 3);
        let s = r.to_struct();
        assert_eq!(s.names(), &["field".to_string()]);
        let r2 = PvaRequest::parse("field(timeStamp.secondsPastEpoch,value1)").unwrap();
        let f = r2.to_struct();
        assert!(f.field_of_path("field.timeStamp.secondsPastEpoch").is_some());
        assert_eq!(PvaRequest::all().nodes().len(), 0);
        assert!(PvaRequest::parse("field(").is_err());
        assert!(PvaRequest::parse(",").is_err());
    }

    #[test]
    fn head_flags() {
        let h = PvaHead {
            version: 2,
            flags: FLAG_FROM_SERVER | 0x30 | FLAG_BIG_ENDIAN,
            command: CMD_MONITOR,
            payload_size: 7,
        };
        let mut b = Vec::new();
        h.write(&mut b);
        assert_eq!(&b[0..4], &[0xca, 2, h.flags, CMD_MONITOR]);
        assert_eq!(&b[4..8], &[0, 0, 0, 7]);
        let back = PvaHead::parse(&b).unwrap();
        assert_eq!(back, h);
        assert_eq!(back.segment(), SEGMENT_MIDDLE);
        assert!(back.from_server());
        assert!(!back.is_control());
        assert_eq!(back.endian(), Endian::Big);
        assert!(matches!(PvaHead::parse(&[0x11; 8]), Err(Error::BadMagic(0x11))));
    }
}
