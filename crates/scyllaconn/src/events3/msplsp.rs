use netpod::TsMs;
use netpod::TsNano;
use netpod::timeunits::MS;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MspEv(u64);

impl MspEv {
    pub fn to_i64(&self) -> i64 {
        self.0 as _
    }

    pub fn to_ms(&self) -> TsMs {
        TsMs(self.0)
    }

    pub fn lsp(&self, ts: TsNano) -> Option<LspEv> {
        let ns1 = ts.ns();
        let ns2 = self.0 * MS;
        if ns1 >= ns2 {
            let lsp = LspEv(ns1 - ns2);
            Some(lsp)
        } else {
            None
        }
    }
}

impl From<TsMs> for MspEv {
    fn from(value: TsMs) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LspEv(u64);

impl LspEv {
    pub fn from_i64(x: i64) -> Self {
        Self(x as _)
    }

    pub fn max() -> Self {
        Self(i64::MAX as _)
    }

    pub fn to_i64(&self) -> i64 {
        self.0 as _
    }
}
