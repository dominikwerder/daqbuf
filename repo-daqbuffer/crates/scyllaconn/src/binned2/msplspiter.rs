use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use netpod::DtMs;
use netpod::range::evrange::NanoRange;

#[derive(Debug, Clone)]
pub struct MspLspItem {
    pub msp: MspU32,
    pub lsp: LspU32,
}

#[derive(Debug)]
pub struct MspLspIter {
    range: NanoRange,
    pbp: PrebinnedPartitioning,
    mins: (MspU32, LspU32),
    maxs: (MspU32, LspU32),
    curs: (MspU32, LspU32),
}

impl MspLspIter {
    pub fn new_covering(range: NanoRange, pbp: PrebinnedPartitioning) -> Self {
        let mins = pbp.msp_lsp(range.beg_ts().to_ts_ms());
        let end1 = range.end_ts().to_ts_ms();
        let g = DtMs::from_ms_u64(pbp.bin_len().ms() - 1);
        let end2 = end1.add_dt_ms(g);
        let maxs = pbp.msp_lsp(end2);
        Self {
            range,
            pbp,
            mins,
            maxs,
            curs: mins,
        }
    }

    pub fn range(&self) -> NanoRange {
        self.range.clone()
    }

    pub fn mins(&self) -> (MspU32, LspU32) {
        self.mins
    }

    pub fn maxs(&self) -> (MspU32, LspU32) {
        self.maxs
    }
}

impl Iterator for MspLspIter {
    type Item = (MspU32, LspU32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.curs.0 >= self.maxs.0 && self.curs.1 >= self.maxs.1 {
            None
        } else {
            let msp = self.curs.0;
            let lsp = self.curs.1;
            self.curs.1.0 += 1;
            if self.curs.1.0 >= self.pbp.patch_len() {
                self.curs.1.0 = 0;
                self.curs.0.0 += 1;
            }
            Some((msp, lsp))
        }
    }
}

#[test]
fn test_iter_00() {
    let range = NanoRange::from_strings("2024-06-07T09:17:31Z", "2024-06-07T09:17:31Z").unwrap();
    let pbp = PrebinnedPartitioning::Sec1;
    assert_eq!(pbp.patch_len(), 1200);
    let it = MspLspIter::new_covering(range, pbp);
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 0);
}

#[test]
fn test_iter_01() {
    let range = NanoRange::from_strings("2024-06-07T09:17:31Z", "2024-06-07T09:17:32Z").unwrap();
    let pbp = PrebinnedPartitioning::Sec1;
    let it = MspLspIter::new_covering(range, pbp.clone());
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 1);
    let e = a.first().unwrap();
    assert_eq!(e.0.0, 1431459);
    assert_eq!(e.1.0, 1051);
    // let e = a.last().unwrap();
    // assert_eq!(e.0.0, 1431459);
    // assert_eq!(e.1.0, 1051);
    let ts_sec = pbp.patch_len() as u64 * a[0].0.0 as u64 + a[0].1.0 as u64;
    // time::UtcDateTime::new(time::Date::with, time)
    let ts1 = time::UtcDateTime::from_unix_timestamp(ts_sec as i64).unwrap();
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
    let ts2 = time::UtcDateTime::parse("2024-06-07T09:17:31Z", &fmt).unwrap();
    assert_eq!(ts1, ts2);
}

#[test]
fn test_iter_02() {
    let range = NanoRange::from_strings("2024-06-07T09:17:31Z", "2024-06-07T09:22:00Z").unwrap();
    let pbp = PrebinnedPartitioning::Sec1;
    let it = MspLspIter::new_covering(range, pbp.clone());
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 240 + 29);
    let e = &a[a.len() - 1];
    assert_eq!(e.0.0, 1431460);
    assert_eq!(e.1.0, 119);
}

#[test]
fn test_iter_03() {
    // Check clamped covering in correct direction
    let range = NanoRange::from_strings("2024-06-07T09:17:31.1Z", "2024-06-07T09:21:59.8Z").unwrap();
    let pbp = PrebinnedPartitioning::Sec1;
    let it = MspLspIter::new_covering(range, pbp.clone());
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 240 + 29);
    let e = &a[0];
    assert_eq!(e.0.0, 1431459);
    assert_eq!(e.1.0, 1051);
    let e = &a[a.len() - 1];
    assert_eq!(e.0.0, 1431460);
    assert_eq!(e.1.0, 119);
}

#[test]
fn test_iter_04() {
    // Check clamped covering in correct direction
    let range = NanoRange::from_strings("2024-06-07T09:17:31.9Z", "2024-06-07T09:21:59.1Z").unwrap();
    let pbp = PrebinnedPartitioning::Sec1;
    let it = MspLspIter::new_covering(range, pbp.clone());
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 240 + 29);
    let e = &a[0];
    assert_eq!(e.0.0, 1431459);
    assert_eq!(e.1.0, 1051);
    let e = &a[a.len() - 1];
    assert_eq!(e.0.0, 1431460);
    assert_eq!(e.1.0, 119);
}

#[test]
fn test_iter_05() {
    let range = NanoRange::from_strings("2025-05-05T12:00:00Z", "2025-05-08T00:00:00Z").unwrap();
    let pbp = PrebinnedPartitioning::Day1;
    let it = MspLspIter::new_covering(range, pbp.clone());
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 3);
    let _ = &a[0];
    let _ = &a[a.len() - 1];
}
