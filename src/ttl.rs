use core::fmt;
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RetentionTime {
    Short,
    Medium,
    Long,
}

impl RetentionTime {
    pub fn debug_tag(&self) -> &'static str {
        use RetentionTime::*;
        match self {
            Short => "ST",
            Medium => "MT",
            Long => "LT",
        }
    }

    pub fn table_prefix(&self) -> &'static str {
        use RetentionTime::*;
        match self {
            Short => "st_",
            Medium => "mt_",
            Long => "lt_",
        }
    }

    pub fn ttl_events_d0(&self) -> Duration {
        let day = 60 * 60 * 24;
        let margin_max = Duration::from_secs(day * 2);
        let ttl = self.ttl_ts_msp();
        let margin = ttl / 10;
        let margin = if margin >= margin_max {
            margin_max
        } else {
            margin
        };
        ttl + margin
    }

    pub fn ttl_events_d1(&self) -> Duration {
        // TTL now depends only on RetentionTime, not on data type or shape.
        self.ttl_events_d0()
    }

    pub fn ttl_ts_msp(&self) -> Duration {
        let day = 60 * 60 * 24;
        match self {
            RetentionTime::Short => Duration::from_secs(day * 7),
            RetentionTime::Medium => Duration::from_secs(day * 31 * 13),
            RetentionTime::Long => Duration::from_secs(day * 31 * 12 * 17),
        }
    }

    pub fn ttl_binned(&self) -> Duration {
        // Current choice is to keep the TTL the same as for events
        self.ttl_events_d0()
    }

    pub fn ttl_channel_status(&self) -> Duration {
        // Current choice is to keep the TTL the same as for events
        self.ttl_events_d0()
    }

    pub fn to_index_db_i32(&self) -> i32 {
        self.to_index_db_u16() as i32
    }

    pub fn to_index_db_u16(&self) -> u16 {
        match self {
            RetentionTime::Short => 2,
            RetentionTime::Medium => 4,
            RetentionTime::Long => 12,
        }
    }

    pub fn from_index_db_u16(x: u16) -> Result<Self, Error> {
        match x {
            2 => Ok(Self::Short),
            4 => Ok(Self::Medium),
            12 => Ok(Self::Long),
            _ => Err(Error::Parse),
        }
    }

    pub fn from_str(x: &str) -> Result<Self, Error> {
        match x {
            "short" => Ok(Self::Short),
            "medium" => Ok(Self::Medium),
            "long" => Ok(Self::Long),
            _ => Err(Error::Parse),
        }
    }
}

impl fmt::Display for RetentionTime {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            RetentionTime::Short => "short",
            RetentionTime::Medium => "medium",
            RetentionTime::Long => "long",
        };
        fmt.write_str(s)
    }
}

autoerr::create_error_v1!(
    name(Error, "TtlError"),
    enum variants {
        Parse,
    },
);

impl FromStr for RetentionTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ret = match s {
            "short" => Self::Short,
            "medium" => Self::Medium,
            "long" => Self::Long,
            _ => return Err(Error::Parse),
        };
        Ok(ret)
    }
}
