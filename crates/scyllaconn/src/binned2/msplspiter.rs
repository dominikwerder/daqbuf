use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use netpod::TsMs;
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
    ts: TsMs,
}

impl MspLspIter {
    pub fn new(range: NanoRange, pbp: PrebinnedPartitioning) -> Self {
        let ts = range.beg_ts().to_ts_ms();
        Self { range, pbp, ts }
    }
}

impl Iterator for MspLspIter {
    type Item = (MspU32, LspU32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.ts >= self.range.end_ts().to_ts_ms() {
            None
        } else {
            let x = self.pbp.msp_lsp(self.ts);
            let msp = MspU32(x.0);
            let lsp = LspU32(x.1);
            self.ts = self.ts.add_dt_ms(self.pbp.bin_len());
            Some((msp, lsp))
        }
    }
}

#[test]
fn test_iter_00() {
    let range = NanoRange::from_strings("2024-06-07T09:17:31Z", "2024-06-07T09:17:31Z").unwrap();
    let pbp = PrebinnedPartitioning::Sec1;
    assert_eq!(pbp.patch_len(), 1200);
    let it = MspLspIter::new(range, pbp);
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 0);
}

#[test]
fn test_iter_01() {
    let range = NanoRange::from_strings("2024-06-07T09:17:31Z", "2024-06-07T09:17:32Z").unwrap();
    let pbp = PrebinnedPartitioning::Sec1;
    assert_eq!(pbp.patch_len(), 1200);
    let it = MspLspIter::new(range, pbp.clone());
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].0.0, 1431459);
    assert_eq!(a[0].1.0, 1051);
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
    let it = MspLspIter::new(range, pbp.clone());
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
    let it = MspLspIter::new(range, pbp.clone());
    let a: Vec<_> = it.collect();
    assert_eq!(a.len(), 240 + 29);
    let e = &a[0];
    assert_eq!(e.0.0, 1431459);
    assert_eq!(e.1.0, 1051);
    let e = &a[a.len() - 1];
    assert_eq!(e.0.0, 1431460);
    assert_eq!(e.1.0, 119);
}
