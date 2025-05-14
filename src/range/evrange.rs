use crate::AppendToUrl;
use crate::Dim0Kind;
use crate::Error;
use crate::FromUrl;
use crate::MS;
use crate::TsNano;
use crate::query::PulseRangeQuery;
use crate::query::TimeRangeQuery;
use crate::timeunits::SEC;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TimeRange {
    Time {
        beg: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    Pulse {
        beg: u64,
        end: u64,
    },
    Nano {
        beg: u64,
        end: u64,
    },
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct NanoRange {
    pub beg: u64,
    pub end: u64,
}

impl fmt::Debug for NanoRange {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if true {
            let beg = TsNano(self.beg);
            let end = TsNano(self.end);
            write!(
                fmt,
                "NanoRange {{ beg: {}, end: {} }}",
                beg.fmt(),
                end.fmt()
            )
        } else if false {
            let beg = TsNano(self.beg);
            let end = TsNano(self.end);
            fmt.debug_struct("NanoRange")
                .field("beg", &beg)
                .field("end", &end)
                .finish()
        } else {
            let beg = chrono::Utc
                .timestamp_opt((self.beg / SEC) as i64, (self.beg % SEC) as u32)
                .earliest();
            let end = chrono::Utc
                .timestamp_opt((self.end / SEC) as i64, (self.end % SEC) as u32)
                .earliest();
            if let (Some(a), Some(b)) = (beg, end) {
                fmt.debug_struct("NanoRange")
                    .field("beg", &a)
                    .field("end", &b)
                    .finish()
            } else {
                fmt.debug_struct("NanoRange")
                    .field("beg", &beg)
                    .field("end", &end)
                    .finish()
            }
        }
    }
}

impl fmt::Display for NanoRange {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl NanoRange {
    pub fn from_date_time(beg: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            beg: beg.timestamp_nanos_opt().unwrap_or(0) as u64,
            end: end.timestamp_nanos_opt().unwrap_or(0) as u64,
        }
    }

    pub fn from_strings(beg: &str, end: &str) -> Result<Self, Error> {
        // let format = time::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
        // let beg = time::UtcDateTime::parse(beg, &format)?;
        // let beg = beg.into();
        let beg = beg.parse::<DateTime<Utc>>()?;
        let end = end.parse::<DateTime<Utc>>()?;
        Ok(Self::from_date_time(beg, end))
    }

    pub fn from_ms_u64(beg: u64, end: u64) -> Self {
        Self {
            beg: MS * beg,
            end: MS * end,
        }
    }

    pub fn from_ns_u64(beg: u64, end: u64) -> Self {
        Self { beg, end }
    }

    pub fn delta(&self) -> u64 {
        self.end - self.beg
    }

    pub fn beg(&self) -> u64 {
        self.beg
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    pub fn beg_ts(&self) -> TsNano {
        TsNano::from_ns(self.beg)
    }

    pub fn end_ts(&self) -> TsNano {
        TsNano::from_ns(self.end)
    }
}

impl From<(u64, u64)> for NanoRange {
    fn from(value: (u64, u64)) -> Self {
        Self {
            beg: value.0,
            end: value.1,
        }
    }
}

impl TryFrom<&SeriesRange> for NanoRange {
    type Error = Error;

    fn try_from(val: &SeriesRange) -> Result<NanoRange, Self::Error> {
        match val {
            SeriesRange::TimeRange(x) => Ok(x.clone()),
            SeriesRange::PulseRange(_) => Err(Error::NotTimerange),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PulseRange {
    pub beg: u64,
    pub end: u64,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub enum SeriesRange {
    TimeRange(NanoRange),
    PulseRange(PulseRange),
}

impl SeriesRange {
    pub fn dim0kind(&self) -> Dim0Kind {
        match self {
            SeriesRange::TimeRange(_) => Dim0Kind::Time,
            SeriesRange::PulseRange(_) => Dim0Kind::Pulse,
        }
    }

    pub fn is_time(&self) -> bool {
        match self {
            SeriesRange::TimeRange(_) => true,
            SeriesRange::PulseRange(_) => false,
        }
    }

    pub fn is_pulse(&self) -> bool {
        match self {
            SeriesRange::TimeRange(_) => false,
            SeriesRange::PulseRange(_) => true,
        }
    }

    pub fn beg_u64(&self) -> u64 {
        match self {
            SeriesRange::TimeRange(x) => x.beg,
            SeriesRange::PulseRange(x) => x.beg,
        }
    }

    pub fn end_u64(&self) -> u64 {
        match self {
            SeriesRange::TimeRange(x) => x.end,
            SeriesRange::PulseRange(x) => x.end,
        }
    }

    pub fn delta_u64(&self) -> u64 {
        match self {
            SeriesRange::TimeRange(x) => x.end - x.beg,
            SeriesRange::PulseRange(x) => x.end - x.beg,
        }
    }

    pub fn to_time(&self) -> Option<NanoRange> {
        match self {
            SeriesRange::TimeRange(x) => Some(x.clone()),
            SeriesRange::PulseRange(_) => None,
        }
    }
}

impl fmt::Debug for SeriesRange {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SeriesRange::TimeRange(range) => write!(fmt, "SeriesRange::TimeRange {{ {} }}", range),
            SeriesRange::PulseRange(_) => write!(fmt, "SeriesRange::PulseRange {{ .. }}"),
        }
    }
}

impl From<NanoRange> for SeriesRange {
    fn from(k: NanoRange) -> Self {
        Self::TimeRange(k)
    }
}

impl From<PulseRange> for SeriesRange {
    fn from(k: PulseRange) -> Self {
        Self::PulseRange(k)
    }
}

impl FromUrl for SeriesRange {
    type Error = Error;

    fn from_url(url: &url::Url) -> Result<Self, Self::Error> {
        let pairs = crate::get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let ret = if let Ok(x) = TimeRangeQuery::from_pairs(pairs) {
            SeriesRange::TimeRange(x.into())
        } else if let Ok(x) = PulseRangeQuery::from_pairs(pairs) {
            SeriesRange::PulseRange(x.into())
        } else {
            return Err(Error::MissingTimerange);
        };
        Ok(ret)
    }
}

impl AppendToUrl for SeriesRange {
    fn append_to_url(&self, url: &mut Url) {
        match self {
            SeriesRange::TimeRange(k) => TimeRangeQuery::from(k).append_to_url(url),
            SeriesRange::PulseRange(k) => PulseRangeQuery::from(k).append_to_url(url),
        }
    }
}
