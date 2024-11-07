#[derive(Debug)]
pub struct HistoLog2 {
    histo: [u64; 16],
    sub: usize,
}

impl HistoLog2 {
    pub fn new(sub: usize) -> Self {
        Self { histo: [0; 16], sub }
    }

    #[inline]
    pub fn ingest(&mut self, mut v: u32) {
        let mut po = 0;
        while v != 0 && po < 15 {
            v = v >> 1;
            po += 1;
        }
        let po = if po >= self.histo.len() + self.sub {
            self.histo.len() - 1
        } else {
            if po > self.sub {
                po - self.sub
            } else {
                0
            }
        };
        self.histo[po] += 1;
    }
}
