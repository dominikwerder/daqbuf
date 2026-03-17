use netpod::TsMs;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MspEv(u64);

impl MspEv {
    pub fn to_i64(&self) -> i64 {
        self.0 as _
    }
}

impl From<TsMs> for MspEv {
    fn from(value: TsMs) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
