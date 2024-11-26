use super::binned::BinnedQuery;
use crate::transform::TransformQuery;
use netpod::get_url_query_pairs;
use netpod::is_false;
use netpod::query::api1::Api1Query;
use netpod::query::PulseRangeQuery;
use netpod::query::TimeRangeQuery;
use netpod::range::evrange::SeriesRange;
use netpod::ttl::RetentionTime;
use netpod::AppendToUrl;
use netpod::ByteSize;
use netpod::ChannelTypeConfigGen;
use netpod::DiskIoTune;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use netpod::SfDbChannel;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;
use url::Url;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "EventsQuery")]
pub enum Error {
    BadInt(#[from] std::num::ParseIntError),
    MissingTimerange,
    BadQuery,
    Transform(#[from] crate::transform::Error),
    Netpod(#[from] netpod::NetpodError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlainEventsQuery {
    channel: SfDbChannel,
    range: SeriesRange,
    #[serde(default, skip_serializing_if = "is_false", rename = "oneBeforeRange")]
    one_before_range: bool,
    #[serde(default, skip_serializing_if = "is_false", rename = "begExcl")]
    beg_excl: bool,
    #[serde(default = "TransformQuery::default_events")]
    #[serde(skip_serializing_if = "TransformQuery::is_default_events")]
    transform: TransformQuery,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde",
        rename = "contentTimeout"
    )]
    timeout_content: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    events_max: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bytes_max: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_large_result: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde"
    )]
    event_delay: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_batch_len: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    buf_len_disk_io: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    do_test_main_error: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    do_test_stream_error: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    test_do_wasm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merger_out_len_max: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scylla_read_queue_len: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    create_errors: Vec<String>,
    #[serde(default)]
    log_level: String,
    #[serde(default)]
    use_rt: Option<RetentionTime>,
    querymarker: String,
}

impl PlainEventsQuery {
    pub fn new<R>(channel: SfDbChannel, range: R) -> Self
    where
        R: Into<SeriesRange>,
    {
        Self {
            channel,
            range: range.into(),
            beg_excl: false,
            one_before_range: false,
            transform: TransformQuery::default_events(),
            timeout_content: None,
            events_max: None,
            bytes_max: None,
            allow_large_result: None,
            event_delay: None,
            stream_batch_len: None,
            buf_len_disk_io: None,
            do_test_main_error: false,
            do_test_stream_error: false,
            test_do_wasm: None,
            merger_out_len_max: None,
            scylla_read_queue_len: None,
            create_errors: Vec::new(),
            log_level: String::new(),
            use_rt: None,
            querymarker: String::new(),
        }
    }

    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }

    pub fn range(&self) -> &SeriesRange {
        &self.range
    }

    pub fn one_before_range(&self) -> bool {
        self.one_before_range || self.transform.need_one_before_range()
    }

    pub fn transform(&self) -> &TransformQuery {
        &self.transform
    }

    pub fn buf_len_disk_io(&self) -> usize {
        self.buf_len_disk_io.unwrap_or(1024 * 8)
    }

    pub fn timeout_content(&self) -> Option<Duration> {
        self.timeout_content
    }

    pub fn timeout_content_or_default(&self) -> Duration {
        self.timeout_content.unwrap_or(Duration::from_millis(3000))
    }

    pub fn events_max(&self) -> u64 {
        self.events_max.map_or_else(
            || {
                if self.allow_large_result_or_def() {
                    1000 * 500
                } else {
                    1000 * 80
                }
            },
            |x| x.min(1000 * 1000 * 4),
        )
    }

    pub fn bytes_max(&self) -> u64 {
        self.bytes_max.map_or_else(
            || {
                if self.allow_large_result_or_def() {
                    1024 * 1024 * 80
                } else {
                    1024 * 1024 * 8
                }
            },
            |x| x.min(1024 * 1024 * 400),
        )
    }

    pub fn allow_large_result_or_def(&self) -> bool {
        self.allow_large_result.unwrap_or(false)
    }

    pub fn event_delay(&self) -> &Option<Duration> {
        &self.event_delay
    }

    pub fn merger_out_len_max(&self) -> Option<u32> {
        self.merger_out_len_max
    }

    pub fn scylla_read_queue_len(&self) -> Option<u32> {
        self.scylla_read_queue_len
    }

    pub fn do_test_main_error(&self) -> bool {
        self.do_test_main_error
    }

    pub fn do_test_stream_error(&self) -> bool {
        self.do_test_stream_error
    }

    pub fn test_do_wasm(&self) -> Option<&str> {
        match &self.test_do_wasm {
            Some(x) => Some(&x),
            None => None,
        }
    }

    pub fn set_series_id(&mut self, series: u64) {
        self.channel.set_series(series);
    }

    pub fn set_do_test_main_error(&mut self, k: bool) {
        self.do_test_main_error = k;
    }

    pub fn set_do_test_stream_error(&mut self, k: bool) {
        self.do_test_stream_error = k;
    }

    pub fn for_time_weighted_scalar(mut self) -> Self {
        self.transform = TransformQuery::for_time_weighted_scalar();
        self
    }

    pub fn for_pulse_id_diff(mut self) -> Self {
        self.transform = TransformQuery::for_pulse_id_diff();
        self
    }

    pub fn is_event_blobs(&self) -> bool {
        self.transform.is_event_blobs()
    }

    pub fn need_value_data(&self) -> bool {
        self.transform.need_value_data()
    }

    pub fn create_errors_contains(&self, x: &str) -> bool {
        self.create_errors.contains(&String::from(x))
    }

    pub fn summary_short(&self) -> String {
        format!(
            "PlainEventsQuery {{ chn: {}, range: {:?} }}",
            self.channel().name(),
            self.range()
        )
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn use_rt(&self) -> Option<RetentionTime> {
        self.use_rt.clone()
    }
}

impl HasBackend for PlainEventsQuery {
    fn backend(&self) -> &str {
        self.channel.backend()
    }
}

impl HasTimeout for PlainEventsQuery {
    fn timeout(&self) -> Option<Duration> {
        PlainEventsQuery::timeout_content(self)
    }
}

impl FromUrl for PlainEventsQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let range = if let Ok(x) = TimeRangeQuery::from_pairs(pairs) {
            SeriesRange::TimeRange(x.into())
        } else if let Ok(x) = PulseRangeQuery::from_pairs(pairs) {
            SeriesRange::PulseRange(x.into())
        } else {
            return Err(Error::MissingTimerange);
        };
        let ret = Self {
            channel: SfDbChannel::from_pairs(pairs)?,
            range,
            one_before_range: pairs.get("oneBeforeRange").map_or(false, |x| x == "true"),
            beg_excl: pairs.get("begExcl").map_or(false, |x| x == "true"),
            transform: TransformQuery::from_pairs(pairs)?,
            timeout_content: pairs
                .get("contentTimeout")
                .and_then(|x| humantime::parse_duration(x).ok()),
            events_max: pairs.get("eventsMax").map_or(None, |k| k.parse().ok()),
            bytes_max: pairs.get("bytesMax").map_or(None, |k| k.parse().ok()),
            allow_large_result: pairs
                .get("allowLargeResult")
                .map_or(None, |x| x.parse().ok()),
            event_delay: pairs.get("eventDelay").map_or(Ok(None), |k| {
                k.parse::<u64>()
                    .map(|x| Duration::from_millis(x))
                    .map(|k| Some(k))
            })?,
            stream_batch_len: pairs
                .get("streamBatchLen")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            buf_len_disk_io: pairs
                .get("bufLenDiskIo")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            do_test_main_error: pairs
                .get("doTestMainError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|_| Error::BadQuery)?,
            do_test_stream_error: pairs
                .get("doTestStreamError")
                .map_or("false", |k| k)
                .parse()
                .map_err(|_| Error::BadQuery)?,
            // test_do_wasm: pairs
            //     .get("testDoWasm")
            //     .map(|x| x.parse::<bool>().ok())
            //     .unwrap_or(None)
            //     .unwrap_or(false),
            test_do_wasm: pairs.get("testDoWasm").map(|x| String::from(x)),
            merger_out_len_max: pairs
                .get("mergerOutLenMax")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            scylla_read_queue_len: pairs
                .get("scyllaReadQueueLen")
                .map_or(Ok(None), |k| k.parse().map(|k| Some(k)))?,
            create_errors: pairs
                .get("create_errors")
                .map(|x| x.split(",").map(|x| x.to_string()).collect())
                .unwrap_or(Vec::new()),
            log_level: pairs.get("log_level").map_or(String::new(), String::from),
            use_rt: pairs.get("useRt").map_or(Ok(None), |k| {
                k.parse().map(Some).map_err(|_| Error::BadQuery)
            })?,
            querymarker: pairs
                .get("querymarker")
                .map_or(String::new(), |x| x.to_string()),
        };
        Ok(ret)
    }
}

impl AppendToUrl for PlainEventsQuery {
    fn append_to_url(&self, url: &mut Url) {
        match &self.range {
            SeriesRange::TimeRange(k) => TimeRangeQuery::from(k).append_to_url(url),
            SeriesRange::PulseRange(_) => todo!(),
        }
        self.channel.append_to_url(url);
        let mut g = url.query_pairs_mut();
        g.append_pair("oneBeforeRange", &self.one_before_range().to_string());
        if self.beg_excl {
            g.append_pair("begExcl", "true");
        }
        g.append_pair("querymarker", &self.querymarker);
        drop(g);
        self.transform.append_to_url(url);
        let mut g = url.query_pairs_mut();
        if let Some(x) = &self.timeout_content {
            g.append_pair("contentTimeout", &format!("{:.0}ms", 1e3 * x.as_secs_f64()));
        }
        if let Some(x) = self.events_max.as_ref() {
            g.append_pair("eventsMax", &x.to_string());
        }
        if let Some(x) = self.bytes_max.as_ref() {
            g.append_pair("bytesMax", &x.to_string());
        }
        if let Some(x) = self.allow_large_result {
            g.append_pair("allowLargeResult", &x.to_string());
        }
        if let Some(x) = self.event_delay.as_ref() {
            g.append_pair("eventDelay", &format!("{:.0}", x.as_secs_f64() * 1e3));
        }
        if let Some(x) = self.stream_batch_len.as_ref() {
            g.append_pair("streamBatchLen", &x.to_string());
        }
        if let Some(x) = self.buf_len_disk_io.as_ref() {
            g.append_pair("bufLenDiskIo", &x.to_string());
        }
        if self.do_test_main_error {
            g.append_pair("doTestMainError", "true");
        }
        if self.do_test_stream_error {
            g.append_pair("doTestStreamError", "true");
        }
        if let Some(x) = &self.test_do_wasm {
            g.append_pair("testDoWasm", &x);
        }
        if let Some(x) = self.merger_out_len_max.as_ref() {
            g.append_pair("mergerOutLenMax", &x.to_string());
        }
        if let Some(x) = self.scylla_read_queue_len.as_ref() {
            g.append_pair("scyllaReadQueueLen", &x.to_string());
        }
        if self.create_errors.len() != 0 {
            g.append_pair("create_errors", &self.create_errors.join(","));
        }
        if self.log_level.len() != 0 {
            g.append_pair("log_level", &self.log_level);
        }
        if let Some(x) = self.use_rt.as_ref() {
            g.append_pair("useRt", &x.to_string());
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsSubQuerySelect {
    ch_conf: ChannelTypeConfigGen,
    range: SeriesRange,
    one_before_range: bool,
    transform: TransformQuery,
    wasm1: Option<String>,
}

impl EventsSubQuerySelect {
    pub fn new(
        ch_info: ChannelTypeConfigGen,
        range: SeriesRange,
        one_before_range: bool,
        transform: TransformQuery,
    ) -> Self {
        Self {
            ch_conf: ch_info,
            range,
            one_before_range,
            transform,
            wasm1: None,
        }
    }

    pub fn wasm1(&self) -> Option<&str> {
        match &self.wasm1 {
            Some(x) => Some(&x),
            None => None,
        }
    }

    pub fn set_wasm1(&mut self, x: String) {
        self.wasm1 = Some(x);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsSubQuerySettings {
    timeout: Option<Duration>,
    events_max: Option<u64>,
    bytes_max: Option<u64>,
    event_delay: Option<Duration>,
    stream_batch_len: Option<usize>,
    buf_len_disk_io: Option<usize>,
    queue_len_disk_io: Option<usize>,
    create_errors: Vec<String>,
    use_rt: Option<RetentionTime>,
    merger_out_len_max: Option<u32>,
    scylla_read_queue_len: Option<u32>,
}

impl EventsSubQuerySettings {
    pub fn merger_out_len_max(&self) -> Option<u32> {
        self.merger_out_len_max
    }

    pub fn scylla_read_queue_len(&self) -> Option<u32> {
        self.scylla_read_queue_len
    }
}

impl Default for EventsSubQuerySettings {
    fn default() -> Self {
        Self {
            timeout: None,
            events_max: None,
            bytes_max: None,
            event_delay: None,
            stream_batch_len: None,
            buf_len_disk_io: None,
            queue_len_disk_io: None,
            create_errors: Vec::new(),
            use_rt: None,
            merger_out_len_max: None,
            scylla_read_queue_len: None,
        }
    }
}

impl From<&PlainEventsQuery> for EventsSubQuerySettings {
    fn from(value: &PlainEventsQuery) -> Self {
        Self {
            timeout: value.timeout_content(),
            events_max: value.events_max,
            bytes_max: value.bytes_max,
            event_delay: value.event_delay,
            stream_batch_len: value.stream_batch_len,
            buf_len_disk_io: value.buf_len_disk_io,
            // TODO add to query
            queue_len_disk_io: None,
            create_errors: value.create_errors.clone(),
            use_rt: value.use_rt(),
            merger_out_len_max: value.merger_out_len_max(),
            scylla_read_queue_len: value.scylla_read_queue_len(),
        }
    }
}

impl From<&BinnedQuery> for EventsSubQuerySettings {
    fn from(value: &BinnedQuery) -> Self {
        Self {
            timeout: value.timeout(),
            // TODO ?
            events_max: None,
            bytes_max: None,
            event_delay: None,
            stream_batch_len: None,
            buf_len_disk_io: None,
            // TODO add to query
            queue_len_disk_io: None,
            create_errors: Vec::new(),
            use_rt: value.use_rt(),
            merger_out_len_max: value.merger_out_len_max(),
            scylla_read_queue_len: value.scylla_read_queue_len(),
        }
    }
}

impl From<&Api1Query> for EventsSubQuerySettings {
    fn from(value: &Api1Query) -> Self {
        let disk_io_tune = value.disk_io_tune();
        Self {
            timeout: value.timeout(),
            // TODO ?
            events_max: None,
            bytes_max: None,
            event_delay: None,
            stream_batch_len: None,
            buf_len_disk_io: Some(disk_io_tune.read_buffer_len),
            queue_len_disk_io: Some(disk_io_tune.read_queue_len),
            create_errors: Vec::new(),
            use_rt: None,
            merger_out_len_max: None,
            scylla_read_queue_len: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsSubQuery {
    select: EventsSubQuerySelect,
    settings: EventsSubQuerySettings,
    ty: String,
    reqid: String,
    log_level: String,
}

impl EventsSubQuery {
    pub fn from_parts(
        select: EventsSubQuerySelect,
        settings: EventsSubQuerySettings,
        reqid: String,
        log_level: String,
    ) -> Self {
        Self {
            select,
            settings,
            ty: "EventsSubQuery".into(),
            reqid,
            log_level,
        }
    }

    pub fn backend(&self) -> &str {
        &self.select.ch_conf.backend()
    }

    pub fn name(&self) -> &str {
        &self.select.ch_conf.name()
    }

    pub fn range(&self) -> &SeriesRange {
        &self.select.range
    }

    pub fn need_one_before_range(&self) -> bool {
        self.select.one_before_range | self.transform().need_one_before_range()
    }

    pub fn transform(&self) -> &TransformQuery {
        &self.select.transform
    }

    pub fn ch_conf(&self) -> &ChannelTypeConfigGen {
        &self.select.ch_conf
    }

    pub fn timeout(&self) -> Duration {
        self.settings
            .timeout
            .unwrap_or(Duration::from_millis(10000))
    }

    pub fn events_max(&self) -> u64 {
        self.settings.events_max.unwrap_or(1024 * 128)
    }

    pub fn event_delay(&self) -> &Option<Duration> {
        &self.settings.event_delay
    }

    pub fn disk_io_tune(&self) -> DiskIoTune {
        let mut tune = DiskIoTune::default();
        if let Some(x) = self.settings.buf_len_disk_io {
            tune.read_buffer_len = x;
        }
        tune
    }

    pub fn inmem_bufcap(&self) -> ByteSize {
        // TODO should depend on the type of backend: only imagebuffer needs large size.
        ByteSize::from_kb(1024 * 30)
    }

    // A rough indication on how many bytes this request is allowed to return. Otherwise, the result should
    // be a partial result.
    pub fn bytes_max(&self) -> u64 {
        self.settings.events_max.unwrap_or(1024 * 512)
    }

    pub fn is_event_blobs(&self) -> bool {
        self.select.transform.is_event_blobs()
    }

    pub fn need_value_data(&self) -> bool {
        self.select.transform.need_value_data()
    }

    pub fn create_errors_contains(&self, x: &str) -> bool {
        self.settings.create_errors.contains(&String::from(x))
    }

    pub fn reqid(&self) -> &str {
        &self.reqid
    }

    pub fn wasm1(&self) -> Option<&str> {
        self.select.wasm1()
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn use_rt(&self) -> Option<RetentionTime> {
        self.settings.use_rt.clone()
    }

    pub fn merger_out_len_max(&self) -> Option<u32> {
        self.settings.merger_out_len_max()
    }

    pub fn settings(&self) -> &EventsSubQuerySettings {
        &self.settings
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Frame1Parts {
    query: EventsSubQuery,
}

impl Frame1Parts {
    pub fn new(query: EventsSubQuery) -> Self {
        Self { query }
    }

    pub fn parts(self) -> (EventsSubQuery,) {
        (self.query,)
    }
}

#[test]
fn parse_frame1() {
    let inp = r##"{"query":{"select":{"ch_conf":{"Scylla":{"backend":"swissfel-daqbuf-ca","series":2367705320261409690,"scalar_type":"ChannelStatus","shape":[],"name":"SLGRE-LI2C03_CH6:TEMP"}},"range":{"TimeRange":{"beg":1695736001000000000,"end":1695736301000000000}},"transform":{"event":"ValueFull","time_binning":"None"},"wasm1":null},"settings":{"timeout":null,"events_max":200000,"event_delay":null,"stream_batch_len":null,"buf_len_disk_io":null,"queue_len_disk_io":null,"create_errors":[]},"ty":"EventsSubQuery","reqid":"3ea23209"}}"##;
    // TODO assert
    let _v: Frame1Parts = serde_json::from_str(inp).unwrap();
}
