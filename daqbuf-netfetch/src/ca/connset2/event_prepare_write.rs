use crate::ca::conn2::ChannelEventValue;
use hashbrown::HashMap;
use series::SeriesId;
use std::fmt;

#[derive(Debug)]
pub struct ChannelCollector {
    series: SeriesId,
    cnt: u64,
    cnt_delta: u64,
}

impl ChannelCollector {
    pub fn new(series: SeriesId) -> Self {
        Self {
            series,
            cnt: 0,
            cnt_delta: 0,
        }
    }

    pub fn ingest(&mut self, ev: ChannelEventValue) {
        self.cnt += 1;
        self.cnt_delta += 1;
    }
}

pub struct EventPrepareWrite {
    map: HashMap<SeriesId, ChannelCollector>,
}

impl fmt::Debug for EventPrepareWrite {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("EventPrepareWrite")
            .field("map.len", &self.map.len())
            .finish()
    }
}

impl EventPrepareWrite {
    pub fn new() -> Self {
        Self { map: HashMap::new() }
    }

    pub fn ingest(&mut self, ev: ChannelEventValue) {
        if let Some(r) = self.map.get_mut(&ev.series()) {
            r.ingest(ev);
        } else {
            let series = ev.series();
            let mut x = ChannelCollector::new(series);
            x.ingest(ev);
            self.map.insert(series, x);
        }
    }

    pub fn oneline(&mut self) -> EventPrepareWriteFmtOnline<'_> {
        let cnt_delta: u64 = self.map.values().map(|x| x.cnt_delta).sum();
        self.map.values_mut().for_each(|x| {
            x.cnt_delta = 0;
        });
        EventPrepareWriteFmtOnline { inner: self, cnt_delta }
    }
}

pub struct EventPrepareWriteFmtOnline<'a> {
    inner: &'a EventPrepareWrite,
    cnt_delta: u64,
}

impl<'a> fmt::Display for EventPrepareWriteFmtOnline<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let map = &self.inner.map;
        let mlen = map.len();
        let cnt_min = map.values().map(|x| x.cnt).min().unwrap_or(0);
        let cnt_max = map.values().map(|x| x.cnt).max().unwrap_or(0);
        let cnt_maxdiff = cnt_max - cnt_min;
        let cnt_sum: u64 = map.values().map(|x| x.cnt).sum();
        let cnt_avg = cnt_sum as f32 / map.len() as f32;
        let cnt_var: f32 = map.values().map(|x| (x.cnt as f32 - cnt_avg).powi(2)).sum();
        let cnt_dev = cnt_var.powf(0.5);
        let cnt_delta = self.cnt_delta;
        write!(
            fmt,
            "\n  mlen {mlen}  cnt_sum {cnt_sum:14}  cnt_delta {cnt_delta:6}  cnt_maxdiff {cnt_maxdiff:14}  cnt_avg {cnt_avg:18.2}  cnt_dev {cnt_dev:14.2}"
        )
    }
}
