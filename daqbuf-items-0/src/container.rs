use netpod::UnsupEvt;

pub trait ByteEstimate {
    fn byte_estimate(&self) -> u32;
}

impl ByteEstimate for UnsupEvt {
    fn byte_estimate(&self) -> u32 {
        200
    }
}

impl ByteEstimate for Vec<UnsupEvt> {
    fn byte_estimate(&self) -> u32 {
        200
    }
}
