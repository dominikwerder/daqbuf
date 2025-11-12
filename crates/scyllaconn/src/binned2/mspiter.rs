use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use netpod::DtMs;
use netpod::range::evrange::NanoRange;

// lsp2 is meant exclusive.
#[derive(Debug, Clone)]
pub struct MspChunkerItem {
    pub msp: MspU32,
    pub lsp1: LspU32,
    pub lsp2: LspU32,
}

#[derive(Debug)]
pub struct MspChunker {
    pbp: PrebinnedPartitioning,
    #[allow(unused)]
    min: (MspU32, LspU32),
    max: (MspU32, LspU32),
    cur: (MspU32, LspU32),
}

impl MspChunker {
    pub fn new_covering(range: NanoRange, pbp: PrebinnedPartitioning) -> Self {
        let mins = pbp.msp_lsp(range.beg_ts().to_ts_ms());
        let end1 = range.end_ts().to_ts_ms();
        let g = DtMs::from_ms_u64(pbp.bin_len().ms() - 1);
        let end2 = end1.add_dt_ms(g);
        let maxs = pbp.msp_lsp(end2);
        Self {
            pbp,
            min: mins,
            max: maxs,
            cur: mins,
        }
    }

    pub fn from_min_max(pbp: PrebinnedPartitioning, min: (MspU32, LspU32), max: (MspU32, LspU32)) -> Self {
        let curs = min;
        Self {
            pbp,
            min,
            max,
            cur: curs,
        }
    }
}

impl Iterator for MspChunker {
    type Item = MspChunkerItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur.0 >= self.max.0 && self.cur.1 >= self.max.1 {
            None
        } else {
            if self.cur.0 < self.max.0 {
                let item = MspChunkerItem {
                    msp: self.cur.0,
                    lsp1: self.cur.1,
                    lsp2: LspU32(self.pbp.patch_len()),
                };
                self.cur.0.0 += 1;
                self.cur.1 = LspU32(0);
                Some(item)
            } else {
                if self.cur.1 < self.max.1 {
                    let item = MspChunkerItem {
                        msp: self.cur.0,
                        lsp1: self.cur.1,
                        lsp2: self.max.1,
                    };
                    self.cur.1 = self.max.1;
                    Some(item)
                } else {
                    None
                }
            }
        }
    }
}
