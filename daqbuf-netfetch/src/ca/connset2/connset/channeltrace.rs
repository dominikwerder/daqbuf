use hashbrown::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::time::Instant;

#[derive(Debug)]
pub struct ChannelTraceError {
    err: Box<dyn std::error::Error + Send>,
}

#[derive(Debug)]
pub enum ChannelTraceItemInner {
    Error(ChannelTraceError),
    Created,
    Ping,
    Pong,
}

#[derive(Debug)]
enum ChannelTraceCat {
    Error,
    Channel,
    ReadNotify,
    Monitor,
    MonitorUpdate,
    Ping,
}

#[derive(Debug)]
pub struct ChannelTraceItem {
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

#[derive(Debug)]
pub struct ChannelTraceL1Item {
    ts: Instant,
    chname: String,
    inner: ChannelTraceItem,
}

impl ChannelTraceL1Item {
    pub fn new(chname: String, inner: ChannelTraceItem) -> Self {
        Self {
            ts: Instant::now(),
            chname,
            inner,
        }
    }
}

#[derive(Debug)]
pub struct ChannelTraceL2Item {
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

#[derive(Debug)]
struct ChannelTraceStashChname {
    qu_channel: VecDeque<ChannelTraceL2Item>,
}

impl ChannelTraceStashChname {
    fn new() -> Self {
        Self {
            qu_channel: VecDeque::with_capacity(100),
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
        } else {
            self.by_chname.insert(item.inner.chname, ChannelTraceStashChname::new());
        }
    }

    pub(super) fn dump(&self) -> String {
        format!("{:?}", self)
    }
}
