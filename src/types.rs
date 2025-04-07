use crate::rexport::serde;

#[derive(Debug, serde::Serialize)]
pub struct CounterU32 {
    v: u32,
}

impl CounterU32 {
    #[inline(always)]
    pub fn new() -> Self {
        Self { v: 0 }
    }

    #[inline(always)]
    pub fn inc(&mut self) {
        self.v += 1
    }

    #[inline(always)]
    pub fn add(&mut self, v: u32) {
        self.v += v
    }

    #[inline(always)]
    pub fn ingest(&mut self, rhs: Self) {
        self.v += rhs.v
    }
}
