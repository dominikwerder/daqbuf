use netpod::DtMs;
use netpod::TsMs;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

autoerr::create_error_v1!(
    name(Error, "BinMsp"),
    enum variants {
        PrebinnedPartitioningInvalid,
        BadDv1(u32),
        BadPbp(u32),
        BadBinLen(DtMs),
    },
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MspU32(pub u32);

impl MspU32 {
    pub fn to_db_i32(&self) -> i32 {
        self.0 as i32
    }

    pub fn from_db_i32(x: i32) -> Self {
        Self(x as u32)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }

    pub fn to_u64(&self) -> u64 {
        self.0 as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LspU32(pub u32);

impl LspU32 {
    pub fn to_db_i32(&self) -> i32 {
        self.0 as i32
    }

    pub fn from_db_i32(x: i32) -> Self {
        Self(x as u32)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BinlenU32(pub u32);

impl BinlenU32 {
    pub fn to_db_i32(&self) -> i32 {
        self.0 as i32
    }

    pub fn from_db_i32(x: i32) -> Self {
        Self(x as u32)
    }

    pub fn to_u32(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrebinnedPartitioning {
    Sec1,
    Sec10,
    Min1,
    Min10,
    Hour1,
    Day1,
}

impl fmt::Display for PrebinnedPartitioning {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl PrebinnedPartitioning {
    pub fn from_dv1_abs(dv1: u32) -> Result<Self, Error> {
        use PrebinnedPartitioning::*;
        match dv1 {
            0x104 => Ok(Sec1),
            0xa04 => Ok(Sec10),
            0x105 => Ok(Min1),
            0xa05 => Ok(Min10),
            0x106 => Ok(Hour1),
            0x107 => Ok(Day1),
            _ => Err(Error::BadDv1(dv1)),
        }
    }

    pub fn from_binlen(binlen: DtMs) -> Result<Self, Error> {
        use PrebinnedPartitioning::*;
        let ret = if binlen == DtMs::from_ms_u64(1000 * 1) {
            Some(Sec1)
        } else if binlen == DtMs::from_ms_u64(1000 * 10) {
            Some(Sec10)
        } else if binlen == DtMs::from_ms_u64(1000 * 60 * 1) {
            Some(Min1)
        } else if binlen == DtMs::from_ms_u64(1000 * 60 * 10) {
            Some(Min10)
        } else if binlen == DtMs::from_ms_u64(1000 * 60 * 60 * 1) {
            Some(Hour1)
        } else if binlen == DtMs::from_ms_u64(1000 * 60 * 60 * 24) {
            Some(Day1)
        } else {
            None
        };
        ret.ok_or(Error::BadBinLen(binlen))
    }

    pub fn bin_len(&self) -> DtMs {
        use PrebinnedPartitioning::*;
        match self {
            Sec1 => DtMs::from_ms_u64(1000 * 1),
            Sec10 => DtMs::from_ms_u64(1000 * 10),
            Min1 => DtMs::from_ms_u64(1000 * 60 * 1),
            Min10 => DtMs::from_ms_u64(1000 * 60 * 10),
            Hour1 => DtMs::from_ms_u64(1000 * 60 * 60 * 1),
            Day1 => DtMs::from_ms_u64(1000 * 60 * 60 * 24),
        }
    }

    pub fn bin_len_dv1_abs(&self) -> u32 {
        use PrebinnedPartitioning::*;
        match self {
            Sec1 => 0x100 | 0x04,
            Sec10 => 0xa00 | 0x04,
            Min1 => 0x100 | 0x05,
            Min10 => 0xa00 | 0x05,
            Hour1 => 0x100 | 0x06,
            Day1 => 0x100 | 0x07,
        }
    }

    pub fn patch_len(&self) -> u32 {
        use PrebinnedPartitioning::*;
        match self {
            Sec1 => 1200,
            Sec10 => 720,
            Min1 => 720,
            Min10 => 1008,
            Hour1 => 960,
            Day1 => 800,
        }
    }

    pub fn patch_dt(&self) -> DtMs {
        self.bin_len().mul(self.patch_len() as u64)
    }

    pub fn msp_lsp(&self, val: TsMs) -> (u32, u32) {
        let div1ms = self.patch_dt().ms();
        let div2ms = self.bin_len().ms();
        let valms = val.ms();
        let qu1 = valms / div1ms;
        let re1 = valms % div1ms;
        let qu2 = re1 / div2ms;
        let _re2 = re1 % div2ms;
        (qu1 as u32, qu2 as u32)
    }

    pub fn uses_index_min10(&self) -> bool {
        use PrebinnedPartitioning::*;
        match self {
            Sec1 => true,
            Sec10 => true,
            Min1 => true,
            Min10 => false,
            Hour1 => false,
            Day1 => false,
        }
    }

    pub fn db_ix(&self) -> u32 {
        use PrebinnedPartitioning::*;
        match self {
            Sec1 => 1,
            Sec10 => 2,
            Min1 => 3,
            Min10 => 4,
            Hour1 => 5,
            Day1 => 6,
        }
    }

    pub fn from_db_ix(x: u32) -> Result<Self, Error> {
        use PrebinnedPartitioning::*;
        match x {
            1 => Ok(Sec1),
            2 => Ok(Sec10),
            3 => Ok(Min1),
            4 => Ok(Min10),
            5 => Ok(Hour1),
            6 => Ok(Day1),
            _ => Err(Error::BadPbp(x)),
        }
    }

    pub fn msp_lsp_to_ts(&self, msp: MspU32, lsp: LspU32) -> TsMs {
        let n = self.patch_len() as u64 * msp.to_u64() + lsp.to_u32() as u64;
        let dt = self.bin_len().mul(n);
        TsMs::from_ms_u64(dt.ms())
    }

    pub fn lsp_inc(&self, msp: MspU32, lsp: LspU32) -> (MspU32, LspU32) {
        let mut m2 = msp;
        let mut l2 = lsp;
        l2.0 += 1;
        if l2.0 >= self.patch_len() {
            l2.0 = 0;
            m2.0 += 1;
        }
        (m2, l2)
    }

    pub fn from_str(s: &str) -> Result<Self, Error> {
        use PrebinnedPartitioning::*;
        match s {
            "Sec1" => Ok(Sec1),
            "Sec10" => Ok(Sec10),
            "Min1" => Ok(Min1),
            "Min10" => Ok(Min10),
            "Hour1" => Ok(Hour1),
            "Day1" => Ok(Day1),
            _ => Err(Error::PrebinnedPartitioningInvalid),
        }
    }

    pub fn to_str(&self) -> &'static str {
        use PrebinnedPartitioning::*;
        match self {
            Sec1 => "Sec1",
            Sec10 => "Sec10",
            Min1 => "Min1",
            Min10 => "Min10",
            Hour1 => "Hour1",
            Day1 => "Day1",
        }
    }
}

impl TryFrom<DtMs> for PrebinnedPartitioning {
    type Error = Error;

    fn try_from(value: DtMs) -> Result<Self, Self::Error> {
        use PrebinnedPartitioning::*;
        if value == DtMs::from_ms_u64(1000) {
            Ok(Sec1)
        } else if value == DtMs::from_ms_u64(1000 * 10) {
            Ok(Sec10)
        } else if value == DtMs::from_ms_u64(1000 * 60) {
            Ok(Min1)
        } else if value == DtMs::from_ms_u64(1000 * 60 * 10) {
            Ok(Min10)
        } else if value == DtMs::from_ms_u64(1000 * 60 * 60) {
            Ok(Hour1)
        } else if value == DtMs::from_ms_u64(1000 * 60 * 60 * 24) {
            Ok(Day1)
        } else {
            Err(Error::PrebinnedPartitioningInvalid)
        }
    }
}

#[test]
fn test_quo_rem_00() {
    let ts1 = TsMs::from_ms_u64(1000 * 60 * 60 * 24 * 817 + 17784239);
    let (qr, dt) = PrebinnedPartitioning::Day1.quo_rem_l1(ts1);
    // eprintln!("{:?}  {:?}", qr, dt);
    assert_eq!(qr.dv1, 0x107);
    assert_eq!(qr.quo, 1);
    assert_eq!(qr.rem, 17);
    assert_eq!(dt, DtMs::from_ms_u64(17784239));
}

#[test]
fn test_quo_rem_01() {
    let ts1 = TsMs::from_ms_u64(1000 * 60 * 60 * 24 * 817 + 17784239);
    let (qr, dt) = PrebinnedPartitioning::Day1.quo_rem_l1(ts1);
    let pbp = PrebinnedPartitioning::from_dv1_abs(qr.dv1).unwrap();
    eprintln!("{:?}", pbp);
    let tsms = pbp.patch_dt().ms() * qr.quo as u64 + pbp.bin_len().ms() * qr.rem as u64 + dt.ms();
    eprintln!("{:?}", tsms);
    eprintln!("{:?}", ts1.ms());
    panic!()
}
