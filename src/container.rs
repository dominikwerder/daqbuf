use crate::Events;

pub trait ByteEstimate {
    fn byte_estimate(&self) -> u64;
}

impl ByteEstimate for Box<dyn Events> {
    fn byte_estimate(&self) -> u64 {
        self.as_ref().byte_estimate()
    }
}
