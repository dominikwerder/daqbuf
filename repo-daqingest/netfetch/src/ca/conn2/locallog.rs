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

pub type Entry = (u32, DateTime<Utc>, String);

#[derive(Debug)]
pub struct LocalLog {
    enable: bool,
    cnt: u32,
    buf: VecDeque<((), Entry)>,
}

impl LocalLog {
    pub fn new() -> Self {
        Self {
            enable: false,
            cnt: 0,
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
        self.check_truncate();
        let i = self.cnt;
        self.cnt += 1;
        let stnow = Utc::now();
        self.buf.push_back(((), (i, stnow, s)));
    }

    pub fn to_vec_string(&self) -> Vec<Entry> {
        self.buf.iter().map(|(ts, (i, st, s))| (*i, *st, s.clone())).collect()
    }

    pub fn pop(&mut self) -> Option<Entry> {
        if let Some((ts, (i, st, s))) = self.buf.pop_front() {
            Some((i, st, s))
        } else {
            None
        }
    }

    pub fn push_entry(&mut self, e: Entry) {
        self.check_truncate();
        self.buf.push_back(((), e));
    }

    fn check_truncate(&mut self) {
        if self.buf.len() >= 120 {
            self.buf.drain(..20).for_each(|_| ());
        }
    }
}
