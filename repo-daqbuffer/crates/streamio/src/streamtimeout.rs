#[cfg(test)]
mod test;

use std::sync::Arc;
use std::time::Duration;
use streams::streamtimeout::BoxedTimeoutFuture;
use streams::streamtimeout::StreamTimeout2;

#[derive(Clone)]
pub struct StreamTimeout {}

impl StreamTimeout {
    pub fn new() -> Self {
        Self {}
    }

    pub fn boxed() -> Box<dyn StreamTimeout2> {
        Box::new(Self::new())
    }

    pub fn arced() -> Arc<dyn StreamTimeout2> {
        Arc::new(Self::new())
    }
}

impl StreamTimeout2 for StreamTimeout {
    fn timeout_intervals(&self, ivl: Duration) -> BoxedTimeoutFuture {
        Box::pin(tokio::time::sleep(ivl))
    }
}
