use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use std::collections::VecDeque;
use std::time::Instant;

#[macro_export]
macro_rules! local_log_llog {
    ($this:expr, $($arg:tt)*) => {{
        $this.llog.push(format!($($arg)*));
    }};
}

pub use crate::local_log_llog as llog;

#[derive(Debug)]
pub struct LocalLog {
    enable: bool,
    buf: VecDeque<(Instant, String)>,
}

impl LocalLog {
    pub fn new() -> Self {
        Self {
            enable: false,
            buf: VecDeque::new(),
        }
    }

    pub fn enable(&mut self) {
        if !self.enable {
            self.enable = true;
            self.buf = VecDeque::with_capacity(32);
        }
    }

    pub fn disable(&mut self) {
        if self.enable {
            self.enable = false;
            self.buf = VecDeque::new();
        }
    }

    pub fn push(&mut self, s: String) {
        if self.buf.len() >= 120 {
            self.buf.truncate(99);
        }
        let ts = Instant::now();
        self.buf.push_back((ts, s));
    }

    pub fn to_vec_string(&self) -> Vec<(DateTime<Utc>, String)> {
        let tsnow = Instant::now();
        let stnow = Utc::now();
        self.buf
            .iter()
            .map(|(ts, s)| {
                let dt = tsnow.saturating_duration_since(*ts);
                (stnow - dt, s.clone())
            })
            .collect()
    }
}
