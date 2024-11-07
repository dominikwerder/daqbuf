use bytes::Bytes;
use std::fmt;

pub struct InMemoryFrame {
    pub encid: u32,
    pub tyid: u32,
    pub len: u32,
    pub buf: Bytes,
}

impl InMemoryFrame {
    pub fn encid(&self) -> u32 {
        self.encid
    }
    pub fn tyid(&self) -> u32 {
        self.tyid
    }
    pub fn len(&self) -> u32 {
        self.len
    }
    pub fn buf(&self) -> &Bytes {
        &self.buf
    }
}

impl fmt::Debug for InMemoryFrame {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "InMemoryFrame {{ encid: {:x}  tyid: {:x}  len {} }}",
            self.encid, self.tyid, self.len
        )
    }
}
