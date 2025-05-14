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
    let mut it = MspLspIter::new(range, pbp);
    taskrun;
    for x in it {
        eprintln!("{:?}", x);
    }
}
