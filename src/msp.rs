use netpod::DtMs;

autoerr::create_error_v1!(
    name(Error, "BinMsp"),
    enum variants {
        PrebinnedPartitioningInvalid,
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

impl PrebinnedPartitioning {
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

    pub fn msp_div(&self) -> DtMs {
        use PrebinnedPartitioning::*;
        if true {
            match self {
                Sec1 => DtMs::from_ms_u64(1000 * 60 * 20),
                Sec10 => DtMs::from_ms_u64(1000 * 60 * 60 * 2),
                Min1 => DtMs::from_ms_u64(1000 * 60 * 60 * 12),
                Min10 => DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 7),
                Hour1 => DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 40),
                Day1 => DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 800),
            }
        } else {
            match self {
                Sec1 => DtMs::from_ms_u64(1000 * 60 * 10),
                Sec10 => DtMs::from_ms_u64(1000 * 60 * 60 * 2),
                Min1 => DtMs::from_ms_u64(1000 * 60 * 60 * 8),
                Min10 => DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 4),
                Hour1 => DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 28),
                Day1 => DtMs::from_ms_u64(1000 * 60 * 60 * 24 * 400),
            }
        }
    }

    pub fn quo_rem(&self, val: DtMs) -> (u64, u32) {
        let valms = val.ms();
        let divms = self.msp_div().ms();
        let quo = valms / divms;
        let rem = (valms - divms * quo) / self.bin_len().ms();
        (quo, rem as u32)
    }

    pub fn off_max(&self) -> u32 {
        self.msp_div().ms() as u32 / self.bin_len().ms() as u32
    }

    pub fn clamp_off(&self, off: u32) -> u32 {
        self.off_max().min(off)
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
