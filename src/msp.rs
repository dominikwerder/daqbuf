use netpod::DtMs;
use netpod::TsMs;
use std::fmt;

autoerr::create_error_v1!(
    name(Error, "BinMsp"),
    enum variants {
        PrebinnedPartitioningInvalid,
        BadDv1(u32),
    },
);

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct QuoRem {
    dv1: u32,
    dv2: u32,
    quo: u32,
    rem: u32,
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

    pub fn batch_len(&self) -> u32 {
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

    pub fn batch_dt(&self) -> DtMs {
        self.bin_len().mul(self.batch_len() as u64)
    }

    // how to decide if we use multi-level index?
    // it is "this", the bin-len itself.
    pub fn quo_rem_l1(&self, val: TsMs) -> (QuoRem, DtMs) {
        let divms = self.batch_dt().ms();
        let dv1 = self.bin_len_dv1_abs();
        let dv2 = self.bin_len().ms();
        let valms = val.ms();
        let quo = valms / divms;
        let rrr = valms % divms;
        let rem = rrr / dv2;
        let rr2 = rrr % dv2;
        let quo_rem = QuoRem {
            dv1,
            dv2: dv2 as u32,
            quo: quo as u32,
            rem: rem as u32,
        };
        (quo_rem, DtMs::from_ms_u64(rr2))
    }

    pub fn uses_index_hour1(&self) -> bool {
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
    let tsms = pbp.batch_dt().ms() * qr.quo as u64 + pbp.bin_len().ms() * qr.rem as u64 + dt.ms();
    eprintln!("{:?}", tsms);
    eprintln!("{:?}", ts1.ms());
    panic!()
}
