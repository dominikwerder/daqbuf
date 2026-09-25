use hashbrown::HashMap;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddrV4;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug, Clone, Serialize)]
pub enum ConnectionTraceL1ItemInner {
    Ping,
    Pong(Duration),
    TcpConnectAttempt,
    TcpConnected(Duration),
    TcpConnectError(String),
    ConnectionError(String),
    EndOfStream,
}

impl ConnectionTraceL1ItemInner {
    fn oneline(&self) -> String {
        use ConnectionTraceL1ItemInner::*;
        match self {
            Ping => "ping".into(),
            Pong(lat) => format!("pong {:.0} ms", 1e3 * lat.as_secs_f32()),
            TcpConnectAttempt => "tcp connect".into(),
            TcpConnected(dt) => format!("tcp connected {:.0} ms", 1e3 * dt.as_secs_f32()),
            TcpConnectError(e) => format!("tcp connect error {e}"),
            ConnectionError(e) => format!("connection error {e}"),
            EndOfStream => "end of stream".into(),
        }
    }
}

impl ConnectionTraceL1ItemInner {
    pub fn to_channel_trace_inner(&self) -> Option<ChannelTraceItemInner> {
        use ConnectionTraceL1ItemInner::*;
        match self {
            Ping | Pong(_) => None,
            TcpConnectAttempt => Some(ChannelTraceItemInner::TcpConnectAttempt),
            TcpConnected(dt) => Some(ChannelTraceItemInner::TcpConnected(*dt)),
            TcpConnectError(e) => Some(ChannelTraceItemInner::TcpConnectError(e.clone())),
            ConnectionError(e) => Some(ChannelTraceItemInner::ConnectionError(e.clone())),
            EndOfStream => Some(ChannelTraceItemInner::EndOfStream),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ConnectionTraceL1Item {
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    pub ts: Instant,
    pub addr: SocketAddrV4,
    pub inner: ConnectionTraceL1ItemInner,
}

impl ConnectionTraceL1Item {
    pub fn oneline(&self) -> String {
        let s = self.inner.oneline();
        if s.chars().count() > 50 {
            s.chars().take(49).chain(['…']).collect()
        } else {
            s
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ConnectionTraceExportItem {
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    ts1: Instant,
    oneline: String,
    inner: ConnectionTraceL1ItemInner,
}

#[derive(Debug)]
pub enum ChannelTraceErrorInner {
    Boxed(Box<dyn std::error::Error + Send>),
    String(String),
}

#[derive(Debug)]
pub struct ChannelTraceError {
    err: ChannelTraceErrorInner,
}

impl Clone for ChannelTraceError {
    fn clone(&self) -> Self {
        let err = match &self.err {
            ChannelTraceErrorInner::Boxed(e) => ChannelTraceErrorInner::String(e.to_string()),
            ChannelTraceErrorInner::String(e) => ChannelTraceErrorInner::String(e.clone()),
        };
        Self { err }
    }
}

impl Serialize for ChannelTraceError {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.err {
            ChannelTraceErrorInner::Boxed(x) => ser.serialize_str(x.to_string().as_str()),
            ChannelTraceErrorInner::String(x) => ser.serialize_str(x.as_str()),
        }
    }
}

impl fmt::Display for ChannelTraceError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.err {
            ChannelTraceErrorInner::Boxed(x) => write!(fmt, "{x}"),
            ChannelTraceErrorInner::String(x) => write!(fmt, "{x}"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Created {
    #[serde(with = "serde_helper::serde_duration::serde_Duration_human")]
    latency: Duration,
}

impl Created {
    pub fn new(latency: Duration) -> Self {
        Self { latency }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadNotifyRes {
    #[serde(with = "serde_helper::serde_duration::serde_Duration_human")]
    latency: Duration,
}

impl ReadNotifyRes {
    pub fn new(latency: Duration) -> Self {
        Self { latency }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum CaProto {
    Created(Created),
    ReadNotify,
    ReadNotifyRes(ReadNotifyRes),
    ReadNotifyTimeout,
}

impl CaProto {
    fn oneline(&self) -> String {
        match self {
            CaProto::Created(x) => format!("created {:.0} ms", 1e3 * x.latency.as_secs_f32()),
            CaProto::ReadNotify => "read-notify".into(),
            CaProto::ReadNotifyRes(x) => format!("read-notify-res {:.0} ms", 1e3 * x.latency.as_secs_f32()),
            CaProto::ReadNotifyTimeout => "read-notify-timeout".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum ChannelTraceItemInner {
    Error(ChannelTraceError),
    CaProto(CaProto),
    TcpConnectAttempt,
    TcpConnected(Duration),
    TcpConnectError(String),
    ConnectionError(String),
    EndOfStream,
}

impl ChannelTraceItemInner {
    fn oneline(&self) -> String {
        match self {
            ChannelTraceItemInner::Error(e) => format!("error: {e}"),
            ChannelTraceItemInner::CaProto(x) => x.oneline(),
            ChannelTraceItemInner::TcpConnectAttempt => "tcp connect".into(),
            ChannelTraceItemInner::TcpConnected(dt) => format!("tcp connected {:.0} ms", 1e3 * dt.as_secs_f32()),
            ChannelTraceItemInner::TcpConnectError(e) => format!("tcp connect error {e}"),
            ChannelTraceItemInner::ConnectionError(e) => format!("connection error {e}"),
            ChannelTraceItemInner::EndOfStream => "end of stream".into(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChannelTraceItem {
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    ts: Instant,
    inner: ChannelTraceItemInner,
}

impl ChannelTraceItem {
    pub fn new(inner: ChannelTraceItemInner) -> Self {
        Self {
            ts: Instant::now(),
            inner,
        }
    }

    pub fn oneline(&self) -> String {
        let s = self.inner.oneline();
        if s.chars().count() > 50 {
            s.chars().take(49).chain(['…']).collect()
        } else {
            s
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChannelTraceL1Item {
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    ts: Instant,
    chname: String,
    inner: ChannelTraceItem,
}

impl ChannelTraceL1Item {
    pub fn new(chname: String, inner: ChannelTraceItem) -> Self {
        // serde_helper::serde_instant::serde_Instant_elapsed_ms
        Self {
            ts: Instant::now(),
            chname,
            inner,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChannelTraceL2Item {
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    ts: Instant,
    conn: SocketAddrV4,
    inner: ChannelTraceL1Item,
}

impl ChannelTraceL2Item {
    pub fn new(conn: SocketAddrV4, inner: ChannelTraceL1Item) -> Self {
        Self {
            ts: Instant::now(),
            conn,
            inner,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChannelTraceExportItem {
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    ts1: Instant,
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    ts2: Instant,
    #[serde(with = "serde_helper::serde_instant::serde_Instant_as_system_time")]
    ts3: Instant,
    oneline: String,
    chname: String,
    conn: SocketAddrV4,
    inner: ChannelTraceItemInner,
}

#[derive(Debug, Serialize)]
struct ChannelTraceStashChname {
    qu_channel: VecDeque<ChannelTraceL2Item>,
}

impl ChannelTraceStashChname {
    const fn cap() -> usize {
        100
    }

    fn new() -> Self {
        Self {
            qu_channel: VecDeque::with_capacity(Self::cap() + 20),
        }
    }

    fn push(&mut self, item: ChannelTraceL2Item) {
        self.qu_channel.push_back(item);
        if self.qu_channel.len() > Self::cap() {
            self.qu_channel = self.qu_channel.split_off(Self::cap() * 2 / 3);
        }
    }
}

#[derive(Debug, Serialize)]
struct ConnectionTraceStash {
    items: VecDeque<ConnectionTraceL1Item>,
}

impl ConnectionTraceStash {
    const fn cap() -> usize {
        100
    }

    fn new() -> Self {
        Self {
            items: VecDeque::with_capacity(Self::cap() + 20),
        }
    }

    fn push(&mut self, item: ConnectionTraceL1Item) {
        self.items.push_back(item);
        if self.items.len() > Self::cap() {
            self.items = self.items.split_off(Self::cap() * 2 / 3);
        }
    }
}

#[derive(Debug)]
pub(super) struct ChannelTraceStash {
    by_chname: HashMap<String, ChannelTraceStashChname>,
    by_connaddr: HashMap<SocketAddrV4, ConnectionTraceStash>,
}

impl ChannelTraceStash {
    pub(super) fn new() -> Self {
        Self {
            by_chname: HashMap::new(),
            by_connaddr: HashMap::new(),
        }
    }

    pub(super) fn push(&mut self, item: ChannelTraceL2Item) {
        if let Some(e) = self.by_chname.get_mut(&item.inner.chname) {
            e.push(item);
        } else {
            let k = item.inner.chname.clone();
            let mut st = ChannelTraceStashChname::new();
            st.push(item);
            self.by_chname.insert(k, st);
        }
    }
    pub(super) fn push_connection_trace(&mut self, item: ConnectionTraceL1Item) {
        if let Some(e) = self.by_connaddr.get_mut(&item.addr) {
            e.push(item);
        } else {
            let k = item.addr.clone();
            let mut st = ConnectionTraceStash::new();
            st.push(item);
            self.by_connaddr.insert(k, st);
        }
    }

    pub(super) fn dump(&self) -> String {
        format!("{:?}", self)
    }

    pub(super) fn get_json_value_for_connection(&self, addr: SocketAddrV4) -> serde_json::Value {
        if let Some(e) = self.by_connaddr.get(&addr) {
            let aa: Vec<_> = e
                .items
                .iter()
                .map(|x| ConnectionTraceExportItem {
                    ts1: x.ts,
                    oneline: x.inner.oneline(),
                    inner: x.inner.clone(),
                })
                .collect();
            serde_json::to_value(serde_json::json!({
                "trace": aa,
            }))
            .unwrap()
        } else {
            serde_json::Value::Null
        }
    }

    pub(super) fn get_json_value_for_channel(&self, chn: &str) -> serde_json::Value {
        if let Some(e) = self.by_chname.get(chn) {
            let aa: Vec<_> = e
                .qu_channel
                .iter()
                .map(|x| ChannelTraceExportItem {
                    ts1: x.inner.inner.ts,
                    ts2: x.inner.ts,
                    ts3: x.ts,
                    oneline: x.inner.inner.oneline(),
                    chname: x.inner.chname.clone(),
                    conn: x.conn,
                    inner: x.inner.inner.inner.clone(),
                })
                .collect();
            serde_json::to_value(serde_json::json!({
                "channel": chn,
                "channeltraceitems": aa,
            }))
            .unwrap()
        } else {
            serde_json::Value::Null
        }
    }
}
