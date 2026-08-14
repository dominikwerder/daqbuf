use hashbrown::HashMap;
use std::collections::VecDeque;
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
struct ChannelTraceStashChname {
    qu_channel: VecDeque<ChannelTraceItem>,
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

    pub(super) fn push(&mut self, item: ChannelTraceItem) {}

    pub(super) fn dump(&self) -> String {
        format!("{:?}", self)
    }
}
