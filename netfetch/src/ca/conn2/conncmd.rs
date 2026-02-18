use crate::conf::ChannelConfig;
use atomic::AtomicUsize;
use series::ChannelStatusSeriesId;
use std::sync::atomic;

#[derive(Debug)]
enum ConnCommandKind {
    ChannelAdd(ChannelConfig, ChannelStatusSeriesId),
    ChannelClose(String),
    Shutdown,
}

#[derive(Debug)]
struct ConnCommandDis {
    id: usize,
    kind: ConnCommandKind,
}

impl ConnCommandDis {
    pub fn channel_add(conf: ChannelConfig, cssid: ChannelStatusSeriesId) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelAdd(conf, cssid),
        }
    }

    pub fn channel_close(name: String) -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::ChannelClose(name),
        }
    }

    pub fn shutdown() -> Self {
        Self {
            id: Self::make_id(),
            kind: ConnCommandKind::Shutdown,
        }
    }

    fn make_id() -> usize {
        static ID: AtomicUsize = AtomicUsize::new(0);
        ID.fetch_add(1, atomic::Ordering::AcqRel)
    }

    pub fn id(&self) -> usize {
        self.id
    }
}
