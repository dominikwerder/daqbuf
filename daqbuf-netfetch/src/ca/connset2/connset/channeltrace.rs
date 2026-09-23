use hashbrown::HashMap;
use serde::Serialize;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
pub struct ChannelTraceError {
    err: Box<dyn std::error::Error + Send>,
}

impl Serialize for ChannelTraceError {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        ser.serialize_str(&self.err.to_string())
    }
}

#[derive(Debug, Serialize)]
pub struct Created {
    #[serde(with = "serde_helper::serde_duration::serde_Duration_human")]
    latency: Duration,
}

#[derive(Debug, Serialize)]
pub struct ReadNotifyRes {
    #[serde(with = "serde_helper::serde_duration::serde_Duration_human")]
    latency: Duration,
}

#[derive(Debug, Serialize)]
pub enum CaProto {
    Created(Created),
    Ping,
    Pong,
    ReadNotify,
    ReadNotifyRes(ReadNotifyRes),
}

#[derive(Debug, Serialize)]
pub enum ChannelTraceItemInner {
    Error(ChannelTraceError),
    CaProto(CaProto),
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

#[derive(Debug)]
pub(super) struct ChannelTraceStash {
    by_chname: HashMap<String, ChannelTraceStashChname>,
}

impl ChannelTraceStash {
    pub(super) fn new() -> Self {
        Self {
            by_chname: HashMap::new(),
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

    pub(super) fn dump(&self) -> String {
        format!("{:?}", self)
    }

    pub(super) fn get_json_value_for_channel(&self, chn: &str) -> serde_json::Value {
        if let Some(e) = self.by_chname.get(chn) {
            serde_json::to_value(&e).unwrap()
        } else {
            serde_json::Value::Null
        }
    }
}
