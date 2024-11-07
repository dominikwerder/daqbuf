pub mod api1;
pub mod datetime;
pub mod prebinned;

use daqbuf_err as err;

use crate::get_url_query_pairs;
use crate::log::*;
use crate::AggKind;
use crate::AppendToUrl;
use crate::FromUrl;
use crate::HasBackend;
use crate::HasTimeout;
use crate::NanoRange;
use crate::NetpodError;
use crate::PulseRange;
use crate::SfDbChannel;
use crate::ToNanos;
use crate::DATETIME_FMT_6MS;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use err::Error;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CacheUsage {
    Use,
    Ignore,
    Recreate,
    V0NoCache,
}

impl CacheUsage {
    pub fn query_param_value(&self) -> String {
        match self {
            CacheUsage::Use => "use",
            CacheUsage::Ignore => "ignore",
            CacheUsage::Recreate => "recreate",
            CacheUsage::V0NoCache => "v0nocache",
        }
        .into()
    }

    // Missing query parameter is not an error
    pub fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Option<Self>, NetpodError> {
        pairs
            .get("cacheUsage")
            .map(|k| {
                if k == "use" {
                    Ok(Some(CacheUsage::Use))
                } else if k == "ignore" {
                    Ok(Some(CacheUsage::Ignore))
                } else if k == "recreate" {
                    Ok(Some(CacheUsage::Recreate))
                } else if k == "v0nocache" {
                    Ok(Some(CacheUsage::V0NoCache))
                } else {
                    Err(NetpodError::BadCacheUsage(k.clone()))?
                }
            })
            .unwrap_or(Ok(None))
    }

    pub fn from_string(s: &str) -> Result<Self, Error> {
        let ret = if s == "ignore" {
            CacheUsage::Ignore
        } else if s == "recreate" {
            CacheUsage::Recreate
        } else if s == "use" {
            CacheUsage::Use
        } else if s == "v0nocache" {
            CacheUsage::V0NoCache
        } else {
            return Err(Error::with_msg(format!("can not interpret cache usage string: {}", s)));
        };
        Ok(ret)
    }

    pub fn is_cache_write(&self) -> bool {
        match self {
            CacheUsage::Use => true,
            CacheUsage::Ignore => false,
            CacheUsage::Recreate => true,
            CacheUsage::V0NoCache => false,
        }
    }

    pub fn is_cache_read(&self) -> bool {
        match self {
            CacheUsage::Use => true,
            CacheUsage::Ignore => false,
            CacheUsage::Recreate => false,
            CacheUsage::V0NoCache => false,
        }
    }
}

impl fmt::Display for CacheUsage {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.query_param_value())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeRangeQuery {
    range: NanoRange,
}

fn parse_time(v: &str) -> Result<DateTime<Utc>, NetpodError> {
    if let Ok(x) = v.parse() {
        Ok(x)
    } else {
        if v.ends_with("ago") {
            let d = humantime::parse_duration(&v[..v.len() - 3]).map_err(|_| NetpodError::BadTimerange)?;
            Ok(Utc::now() - d)
        } else {
            Err(NetpodError::BadTimerange)
        }
    }
}

impl FromUrl for TimeRangeQuery {
    type Error = NetpodError;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        if let (Some(beg), Some(end)) = (pairs.get("begDate"), pairs.get("endDate")) {
            let ret = Self {
                range: NanoRange {
                    beg: parse_time(beg)?.to_nanos(),
                    end: parse_time(end)?.to_nanos(),
                },
            };
            Ok(ret)
        } else if let (Some(beg), Some(end)) = (pairs.get("begNs"), pairs.get("endNs")) {
            let ret = Self {
                range: NanoRange {
                    beg: beg.parse()?,
                    end: end.parse()?,
                },
            };
            Ok(ret)
        } else {
            Err(NetpodError::MissingTimerange)
        }
    }
}

impl AppendToUrl for TimeRangeQuery {
    fn append_to_url(&self, url: &mut Url) {
        let date_fmt = DATETIME_FMT_6MS;
        let mut g = url.query_pairs_mut();
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64).format(date_fmt).to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64).format(date_fmt).to_string(),
        );
    }
}

impl From<TimeRangeQuery> for NanoRange {
    fn from(k: TimeRangeQuery) -> Self {
        Self {
            beg: k.range.beg,
            end: k.range.end,
        }
    }
}

impl From<&NanoRange> for TimeRangeQuery {
    fn from(k: &NanoRange) -> Self {
        Self {
            range: NanoRange { beg: k.beg, end: k.end },
        }
    }
}

impl From<&PulseRange> for PulseRangeQuery {
    fn from(k: &PulseRange) -> Self {
        Self {
            range: PulseRange { beg: k.beg, end: k.end },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulseRangeQuery {
    range: PulseRange,
}

impl FromUrl for PulseRangeQuery {
    type Error = NetpodError;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        if let (Some(beg), Some(end)) = (pairs.get("begPulse"), pairs.get("endPulse")) {
            let ret = Self {
                range: PulseRange {
                    beg: beg.parse()?,
                    end: end.parse()?,
                },
            };
            Ok(ret)
        } else {
            Err(NetpodError::MissingQueryParameters)
        }
    }
}

impl AppendToUrl for PulseRangeQuery {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("begPulse", &self.range.beg.to_string());
        g.append_pair("endPulse", &self.range.end.to_string());
    }
}

impl From<PulseRangeQuery> for PulseRange {
    fn from(k: PulseRangeQuery) -> Self {
        Self {
            beg: k.range.beg,
            end: k.range.end,
        }
    }
}

pub fn binning_scheme_append_to_url(agg_kind: &AggKind, url: &mut Url) {
    let mut g = url.query_pairs_mut();
    match agg_kind {
        AggKind::EventBlobs => {
            g.append_pair("binningScheme", "eventBlobs");
        }
        AggKind::TimeWeightedScalar => {
            g.append_pair("binningScheme", "timeWeightedScalar");
        }
        AggKind::Plain => {
            g.append_pair("binningScheme", "fullValue");
        }
        AggKind::DimXBins1 => {
            g.append_pair("binningScheme", "unweightedScalar");
        }
        AggKind::DimXBinsN(n) => {
            g.append_pair("binningScheme", "binnedX");
            g.append_pair("binnedXcount", &format!("{}", n));
        }
        AggKind::PulseIdDiff => {
            g.append_pair("binningScheme", "pulseIdDiff");
        }
    }
}

// Absent AggKind is not considered an error.
pub fn agg_kind_from_binning_scheme(pairs: &BTreeMap<String, String>) -> Result<Option<AggKind>, NetpodError> {
    let key = "binningScheme";
    if let Some(s) = pairs.get(key) {
        let ret = if s == "eventBlobs" {
            AggKind::EventBlobs
        } else if s == "fullValue" {
            AggKind::Plain
        } else if s == "timeWeightedScalar" {
            AggKind::TimeWeightedScalar
        } else if s == "unweightedScalar" {
            AggKind::DimXBins1
        } else if s == "binnedX" {
            let u = pairs.get("binnedXcount").map_or("1", |k| k).parse()?;
            AggKind::DimXBinsN(u)
        } else if s == "pulseIdDiff" {
            AggKind::PulseIdDiff
        } else {
            return Err(NetpodError::MissingBinningScheme);
        };
        Ok(Some(ret))
    } else {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
pub struct ChannelStateEventsQuery {
    channel: SfDbChannel,
    range: NanoRange,
}

impl ChannelStateEventsQuery {
    pub fn new(channel: SfDbChannel, range: NanoRange) -> Self {
        Self { channel, range }
    }

    pub fn range(&self) -> &NanoRange {
        &self.range
    }

    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.series = Some(series);
    }

    pub fn channel_mut(&mut self) -> &mut SfDbChannel {
        &mut self.channel
    }
}

impl HasBackend for ChannelStateEventsQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
    }
}

impl HasTimeout for ChannelStateEventsQuery {
    fn timeout(&self) -> Option<Duration> {
        None
    }
}

impl FromUrl for ChannelStateEventsQuery {
    type Error = NetpodError;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let beg_date = pairs.get("begDate").ok_or_else(|| NetpodError::MissingTimerange)?;
        let end_date = pairs.get("endDate").ok_or_else(|| NetpodError::MissingTimerange)?;
        let ret = Self {
            channel: SfDbChannel::from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
        };
        let self_name = std::any::type_name::<Self>();
        debug!("{self_name}::from_url  {ret:?}");
        Ok(ret)
    }
}

impl AppendToUrl for ChannelStateEventsQuery {
    fn append_to_url(&self, url: &mut Url) {
        self.channel.append_to_url(url);
        let mut g = url.query_pairs_mut();
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64)
                .format(DATETIME_FMT_6MS)
                .to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64)
                .format(DATETIME_FMT_6MS)
                .to_string(),
        );
    }
}
