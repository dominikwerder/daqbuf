use crate::transform::TransformQuery;
use netpod::get_url_query_pairs;
use netpod::log::*;
use netpod::query::CacheUsage;
use netpod::range::evrange::SeriesRange;
use netpod::ttl::RetentionTime;
use netpod::AppendToUrl;
use netpod::BinnedRange;
use netpod::BinnedRangeEnum;
use netpod::ByteSize;
use netpod::DtMs;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use netpod::SfDbChannel;
use serde::Deserialize;
use serde::Serialize;
use series::msp::PrebinnedPartitioning;
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

autoerr::create_error_v1!(
    name(Error, "BinnedQuery"),
    enum variants {
        BadInt(#[from] std::num::ParseIntError),
        MultipleBinCountBinWidth,
        BadUseRt,
        Netpod(#[from] netpod::Error),
        Transform(#[from] crate::transform::Error),
    },
);

mod serde_option_vec_duration {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
    use std::time::Duration;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct HumantimeDuration {
        #[serde(with = "humantime_serde")]
        inner: Duration,
    }

    pub fn serialize<S>(val: &Option<Vec<Duration>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(vec) => {
                // humantime_serde::serialize(&t, ser)
                let t: Vec<_> = vec
                    .iter()
                    .map(|&x| HumantimeDuration { inner: x })
                    .collect();
                serde::Serialize::serialize(&t, ser)
            }
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'a, D>(de: D) -> Result<Option<Vec<Duration>>, D::Error>
    where
        D: Deserializer<'a>,
    {
        let t: Option<Vec<HumantimeDuration>> = serde::Deserialize::deserialize(de)?;
        Ok(t.map(|v| v.iter().map(|x| x.inner).collect()))
    }
}

#[allow(unused)]
fn vec_is_empty<T>(x: &Vec<T>) -> bool {
    x.len() == 0
}

fn string_is_empty(x: &String) -> bool {
    x.len() == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinnedQuery {
    channel: SfDbChannel,
    range: SeriesRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bin_count: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde"
    )]
    bin_width: Option<Duration>,
    #[serde(
        default = "TransformQuery::default_time_binned",
        skip_serializing_if = "TransformQuery::is_default_time_binned"
    )]
    transform: TransformQuery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_usage: Option<CacheUsage>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_option_vec_duration"
    )]
    subgrids: Option<Vec<Duration>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde",
        rename = "contentTimeout"
    )]
    timeout_content: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    buf_len_disk_io: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    disk_stats_every: Option<ByteSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merger_out_len_max: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scylla_read_queue_len: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    test_do_wasm: Option<String>,
    #[serde(default, skip_serializing_if = "string_is_empty")]
    log_level: String,
    #[serde(default, skip_serializing_if = "string_is_empty")]
    log_items: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stats_items: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    use_rt: Option<RetentionTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_from_events: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_from_prebinned: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_rebin: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    datahub_bin_as_waveform: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pbd_enable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pbd_rts_pbp_block: Option<Vec<Vec<u8>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pbd_evs: Option<bool>,
}

impl BinnedQuery {
    pub fn new(channel: SfDbChannel, range: SeriesRange, bin_count: u32) -> Self {
        Self {
            channel,
            range,
            bin_count: Some(bin_count),
            bin_width: None,
            transform: TransformQuery::default_time_binned(),
            cache_usage: None,
            subgrids: None,
            buf_len_disk_io: None,
            disk_stats_every: None,
            timeout_content: None,
            merger_out_len_max: None,
            scylla_read_queue_len: None,
            test_do_wasm: None,
            log_level: String::new(),
            log_items: String::new(),
            stats_items: None,
            use_rt: None,
            allow_from_events: None,
            allow_from_prebinned: None,
            allow_rebin: None,
            datahub_bin_as_waveform: None,
            pbd_enable: None,
            pbd_rts_pbp_block: None,
            pbd_evs: None,
        }
    }

    pub fn range(&self) -> &SeriesRange {
        &self.range
    }

    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }

    pub fn bin_count(&self) -> Option<u32> {
        self.bin_count
    }

    pub fn bin_width(&self) -> Option<Duration> {
        self.bin_width
    }

    pub fn transform(&self) -> &TransformQuery {
        &self.transform
    }

    pub fn cache_usage(&self) -> Option<CacheUsage> {
        self.cache_usage.clone()
    }

    pub fn disk_stats_every(&self) -> ByteSize {
        match &self.disk_stats_every {
            Some(x) => x.clone(),
            None => ByteSize(1024 * 1024 * 4),
        }
    }

    pub fn buf_len_disk_io(&self) -> usize {
        match self.buf_len_disk_io {
            Some(x) => x,
            None => 1024 * 16,
        }
    }

    pub fn timeout_content(&self) -> Option<Duration> {
        self.timeout_content
    }

    pub fn subgrids(&self) -> Option<&[Duration]> {
        self.subgrids.as_ref().map(|x| x.as_slice())
    }

    pub fn merger_out_len_max(&self) -> Option<u32> {
        self.merger_out_len_max
    }

    pub fn scylla_read_queue_len(&self) -> Option<u32> {
        self.scylla_read_queue_len
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.set_series(series);
    }

    pub fn channel_mut(&mut self) -> &mut SfDbChannel {
        &mut self.channel
    }

    pub fn set_cache_usage(&mut self, k: CacheUsage) {
        self.cache_usage = Some(k);
    }

    pub fn set_disk_stats_every(&mut self, k: ByteSize) {
        self.disk_stats_every = Some(k);
    }

    // Currently only for testing
    pub fn set_timeout(&mut self, k: Duration) {
        self.timeout_content = Some(k);
    }

    pub fn set_buf_len_disk_io(&mut self, k: usize) {
        self.buf_len_disk_io = Some(k);
    }

    pub fn for_time_weighted_scalar(self) -> Self {
        let mut v = self;
        v.transform = TransformQuery::for_time_weighted_scalar();
        v
    }

    pub fn test_do_wasm(&self) -> Option<&str> {
        match &self.test_do_wasm {
            Some(x) => Some(&x),
            None => None,
        }
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn log_items(&self) -> &str {
        &self.log_items
    }

    pub fn stats_items(&self) -> Option<u32> {
        self.stats_items
    }

    pub fn use_rt(&self) -> Option<RetentionTime> {
        self.use_rt.clone()
    }

    pub fn allow_from_events(&self) -> Option<bool> {
        self.allow_from_events.clone()
    }

    pub fn allow_from_prebinned(&self) -> Option<bool> {
        self.allow_from_prebinned.clone()
    }

    pub fn allow_rebin(&self) -> Option<bool> {
        self.allow_rebin.clone()
    }

    pub fn datahub_bin_as_waveform(&self) -> Option<bool> {
        self.datahub_bin_as_waveform.clone()
    }

    pub fn covering_range(&self) -> Result<BinnedRangeEnum, Error> {
        match &self.range {
            SeriesRange::TimeRange(range) => match self.bin_width {
                Some(dt) => {
                    if self.bin_count.is_some() {
                        Err(Error::MultipleBinCountBinWidth)
                    } else {
                        let ret = BinnedRangeEnum::Time(BinnedRange::covering_range_time(
                            range.clone(),
                            DtMs::from_ms_u64(dt.as_millis() as u64),
                        )?);
                        Ok(ret)
                    }
                }
                None => {
                    let bc = self.bin_count.unwrap_or(20);
                    let ret = BinnedRangeEnum::covering_range(self.range.clone(), bc)?;
                    Ok(ret)
                }
            },
            SeriesRange::PulseRange(_) => todo!(),
        }
    }

    pub fn pbd_enable(&self) -> Option<bool> {
        self.pbd_enable.clone()
    }

    pub fn pbd_rts_pbp_block(&self) -> Option<Vec<Vec<u8>>> {
        self.pbd_rts_pbp_block.clone()
    }

    pub fn pbd_evs(&self) -> Option<bool> {
        self.pbd_evs.clone()
    }
}

impl HasBackend for BinnedQuery {
    fn backend(&self) -> &str {
        self.channel.backend()
    }
}

impl HasTimeout for BinnedQuery {
    fn timeout(&self) -> Option<Duration> {
        self.timeout_content
    }
}

impl FromUrl for BinnedQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let ret = Self {
            channel: SfDbChannel::from_pairs(&pairs)?,
            range: SeriesRange::from_pairs(pairs)?,
            bin_count: pairs.get("binCount").and_then(|x| x.parse().ok()),
            bin_width: pairs
                .get("binWidth")
                .and_then(|x| humantime::parse_duration(x).ok()),
            transform: TransformQuery::from_pairs(pairs)?,
            cache_usage: CacheUsage::from_pairs(&pairs)?,
            buf_len_disk_io: pairs
                .get("bufLenDiskIo")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            disk_stats_every: pairs
                .get("diskStatsEveryKb")
                .map(|k| k.parse().ok())
                .unwrap_or(None)
                .map(ByteSize::from_kb),
            timeout_content: pairs
                .get("contentTimeout")
                .and_then(|x| humantime::parse_duration(x).ok()),
            subgrids: pairs.get("subgrids").map(|x| {
                x.split(",")
                    .filter_map(|x| humantime::parse_duration(x).ok())
                    .collect()
            }),
            merger_out_len_max: pairs
                .get("mergerOutLenMax")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            scylla_read_queue_len: pairs
                .get("scyllaReadQueueLen")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            test_do_wasm: pairs.get("testDoWasm").map(|x| String::from(x)),
            log_level: pairs.get("log_level").map_or(String::new(), String::from),
            log_items: pairs.get("log_items").map_or(String::new(), String::from),
            stats_items: pairs.get("stats_items").and_then(|x| x.parse().ok()),
            use_rt: pairs.get("useRt").map_or(Ok(None), |k| {
                k.parse().map(Some).map_err(|_| Error::BadUseRt)
            })?,
            allow_from_events: pairs
                .get("allow_from_events")
                .and_then(|x| x.parse::<bool>().ok()),
            allow_from_prebinned: pairs
                .get("allow_from_prebinned")
                .and_then(|x| x.parse::<bool>().ok()),
            allow_rebin: pairs
                .get("allow_rebin")
                .and_then(|x| x.parse::<bool>().ok()),
            datahub_bin_as_waveform: pairs
                .get("private_datahub_bin_as_waveform")
                .and_then(|x| x.parse::<bool>().ok()),
            pbd_enable: pairs.get("pbd_enable").and_then(|x| x.parse().ok()),
            pbd_rts_pbp_block: pairs
                .get("pbd_rts_pbp_block")
                .and_then(|x| serde_json::from_str(x).ok()),
            pbd_evs: pairs.get("pbd_evs").and_then(|x| x.parse().ok()),
        };
        debug!("BinnedQuery::from_url  {:?}", ret);
        Ok(ret)
    }
}

impl AppendToUrl for BinnedQuery {
    fn append_to_url(&self, url: &mut Url) {
        self.channel.append_to_url(url);
        self.range.append_to_url(url);
        {
            let mut g = url.query_pairs_mut();
            if let Some(x) = self.bin_count {
                g.append_pair("binCount", &format!("{}", x));
            }
            if let Some(x) = self.bin_width {
                if x < Duration::from_secs(1) {
                    g.append_pair("binWidth", &format!("{:.0}ms", x.subsec_millis()));
                } else if x < Duration::from_secs(60) {
                    g.append_pair("binWidth", &format!("{:.0}s", x.as_secs_f64()));
                } else if x < Duration::from_secs(60 * 60) {
                    g.append_pair("binWidth", &format!("{:.0}m", x.as_secs() / 60));
                } else if x < Duration::from_secs(60 * 60 * 24) {
                    g.append_pair("binWidth", &format!("{:.0}h", x.as_secs() / 60 / 60));
                } else {
                    g.append_pair("binWidth", &format!("{:.0}d", x.as_secs() / 60 / 60 / 24));
                }
            }
        }
        self.transform.append_to_url(url);
        let mut g = url.query_pairs_mut();
        if let Some(x) = &self.cache_usage {
            g.append_pair("cacheUsage", &x.query_param_value());
        }
        if let Some(x) = &self.timeout_content {
            g.append_pair("contentTimeout", &format!("{:.0}ms", 1e3 * x.as_secs_f64()));
        }
        if let Some(x) = &self.subgrids {
            let s: String = x
                .iter()
                .map(|&x| humantime::format_duration(x).to_string())
                .fold(String::new(), |mut a, x| {
                    if a.len() != 0 {
                        a.push_str(",");
                    }
                    a.push_str(&x);
                    a
                });
            g.append_pair("subgrids", &s);
        }
        if let Some(x) = self.buf_len_disk_io {
            g.append_pair("bufLenDiskIo", &format!("{}", x));
        }
        if let Some(x) = &self.disk_stats_every {
            g.append_pair("diskStatsEveryKb", &format!("{}", x.bytes() / 1024));
        }
        if let Some(x) = self.merger_out_len_max.as_ref() {
            g.append_pair("mergerOutLenMax", &format!("{}", x));
        }
        if let Some(x) = self.scylla_read_queue_len.as_ref() {
            g.append_pair("scyllaReadQueueLen", &x.to_string());
        }
        if let Some(x) = &self.test_do_wasm {
            g.append_pair("testDoWasm", &x);
        }
        if self.log_level.len() != 0 {
            g.append_pair("log_level", &self.log_level);
        }
        if self.log_items.len() != 0 {
            g.append_pair("log_items", &self.log_items);
        }
        if let Some(x) = self.use_rt.as_ref() {
            g.append_pair("useRt", &x.to_string());
        }
        if let Some(x) = self.allow_from_events.as_ref() {
            g.append_pair("allow_from_events", &x.to_string());
        }
        if let Some(x) = self.allow_from_prebinned.as_ref() {
            g.append_pair("allow_from_prebinned", &x.to_string());
        }
        if let Some(x) = self.allow_rebin.as_ref() {
            g.append_pair("allow_rebin", &x.to_string());
        }
        if let Some(x) = self.datahub_bin_as_waveform.as_ref() {
            g.append_pair("private_datahub_bin_as_waveform", &x.to_string());
        }
        if let Some(x) = self.pbd_enable.as_ref() {
            g.append_pair("pbd_enable", &x.to_string());
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinWriteIndexQuery {
    channel: SfDbChannel,
    range: SeriesRange,
    #[serde(default)]
    log_level: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rt: Option<RetentionTime>,
    pbp: PrebinnedPartitioning,
}

impl BinWriteIndexQuery {
    pub fn range(&self) -> &SeriesRange {
        &self.range
    }

    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }

    pub fn retention_time(&self) -> RetentionTime {
        self.rt.clone().unwrap_or(RetentionTime::Short)
    }

    pub fn prebinned_partitioning(&self) -> PrebinnedPartitioning {
        self.pbp.clone()
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }
}

impl HasBackend for BinWriteIndexQuery {
    fn backend(&self) -> &str {
        self.channel.backend()
    }
}

impl FromUrl for BinWriteIndexQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let ret = Self {
            channel: SfDbChannel::from_pairs(&pairs)?,
            range: SeriesRange::from_pairs(pairs)?,
            log_level: pairs.get("log_level").map_or(String::new(), String::from),
            rt: pairs.get("rt").map_or(Ok(None), |k| {
                k.parse::<u16>()
                    .map(RetentionTime::from_index_db_u16)
                    .map_err(|_| Error::BadUseRt)?
                    .map(Some)
                    .map_err(|_| Error::BadUseRt)
            })?,
            pbp: pairs
                .get("pbp")
                .and_then(|x| x.parse().ok())
                .and_then(|x| PrebinnedPartitioning::from_db_ix(x).ok())
                .unwrap_or(PrebinnedPartitioning::Day1),
        };
        let selfname = std::any::type_name::<Self>();
        debug!("{}::from_pairs  {:?}", selfname, ret);
        Ok(ret)
    }
}

impl AppendToUrl for BinWriteIndexQuery {
    fn append_to_url(&self, url: &mut Url) {
        self.channel.append_to_url(url);
        self.range.append_to_url(url);
        let mut g = url.query_pairs_mut();
        if self.log_level.len() != 0 {
            g.append_pair("log_level", &self.log_level);
        }
        if let Some(x) = self.rt.as_ref() {
            g.append_pair("rt", &x.to_index_db_u16().to_string());
        }
        g.append_pair("pbp", &self.pbp.db_ix().to_string());
    }
}
