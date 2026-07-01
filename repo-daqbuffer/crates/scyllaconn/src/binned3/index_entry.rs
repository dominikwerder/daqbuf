use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use netpod::DtMs;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct IndexEntry {
    pub pbp: PrebinnedPartitioning,
    pub msp: MspU32,
    pub lsp: LspU32,
    pub binlen: DtMs,
}

pub fn check_good_order(v: &VecDeque<IndexEntry>) -> bool {
    for (a, b) in v.iter().skip(1).zip(v.iter()) {
        if a.binlen < b.binlen {
            return false;
        }
    }
    true
}
