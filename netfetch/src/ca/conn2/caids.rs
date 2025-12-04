use hashbrown::HashMap;
use serde::Serialize;
use stats::rand_xoshiro::Xoshiro128PlusPlus;
use stats::rand_xoshiro::rand_core::SeedableRng;
use std::fmt;
use std::sync::LazyLock;
use std::sync::Mutex;

static CID_REG: LazyLock<Mutex<(HashMap<u32, u32>, Xoshiro128PlusPlus)>> =
    LazyLock::new(|| Mutex::new((HashMap::new(), Xoshiro128PlusPlus::from_os_rng())));

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct CidOwned(u32);

impl CidOwned {
    pub fn new() -> Self {
        let mut g = CID_REG.lock().unwrap();
        loop {
            use stats::rand_xoshiro::rand_core::RngCore;
            let k = g.1.next_u32() & 0x7fffffff;
            break if g.0.try_insert(k, k).is_err() {
                continue;
            } else {
                Self(k)
            };
        }
    }

    pub fn to_cid(&self) -> Cid {
        Cid(self.0)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

impl Drop for CidOwned {
    fn drop(&mut self) {
        let mut g = CID_REG.lock().unwrap();
        g.0.remove(&self.0);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
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

impl fmt::Display for Cid {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Cid({})", self.0)
    }
}

impl fmt::Display for Sid {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Sid({})", self.0)
    }
}

impl fmt::Display for Subid {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Subid({})", self.0)
    }
}

impl fmt::Display for Ioid {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "Ioid({})", self.0)
    }
}
