use items_0::Empty;
use items_0::Extendable;
use items_0::WithLen;
use serde::Deserialize;
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountingEvents {
    pub tss: VecDeque<u64>,
    pub count: VecDeque<u64>,
    pub bytes: VecDeque<u64>,
}

impl Empty for AccountingEvents {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            count: VecDeque::new(),
            bytes: VecDeque::new(),
        }
    }
}

impl WithLen for AccountingEvents {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl Extendable for AccountingEvents {
    fn extend_from(&mut self, src: &mut Self) {
        use core::mem::replace;
        let v = replace(&mut src.tss, VecDeque::new());
        self.tss.extend(v.into_iter());
        let v = replace(&mut src.count, VecDeque::new());
        self.count.extend(v.into_iter());
        let v = replace(&mut src.bytes, VecDeque::new());
        self.bytes.extend(v.into_iter());
    }
}
