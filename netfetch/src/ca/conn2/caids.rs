use serde::Serialize;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Cid(u32);

impl Cid {
    pub fn new(x: u32) -> Self {
        Self(x)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Subid(u32);

impl Subid {
    pub fn new(x: u32) -> Self {
        Self(x)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Sid(u32);

impl Sid {
    pub fn new(x: u32) -> Self {
        Self(x)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Ioid(u32);

impl Ioid {
    pub fn new(x: u32) -> Self {
        Self(x)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}
