pub mod channelstatus;
pub mod hex;
pub mod histo;
pub mod query;
pub mod range;
pub mod status;
pub mod stream_impl_tracer;
pub mod streamext;
pub mod ttl;

#[allow(unused)]
#[inline(always)]
pub fn is_log_direct() -> bool {
    static ONCE: LazyLock<bool> =
        LazyLock::new(|| std::env::var("LOG_DIRECT").map_or(false, |x| x.parse().unwrap_or(false)));
    *ONCE
}

pub mod log_macros_branch {
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_trace {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::trace!($($arg)*)
            } else {
                $crate::log::trace!($($arg)*)
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_debug {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::debug!($($arg)*)
            } else {
                $crate::log::debug!($($arg)*)
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_info {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::info!($($arg)*)
            } else {
                $crate::log::info!($($arg)*)
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_warn {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::warn!($($arg)*)
            } else {
                $crate::log::warn!($($arg)*)
            }
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! branch_error {
        ($($arg:tt)*) => {
            if $crate::is_log_direct() {
                $crate::log_direct::error!($($arg)*)
            } else {
                $crate::log::error!($($arg)*)
            }
        };
    }
    pub use branch_debug as debug;
    pub use branch_error as error;
    pub use branch_info as info;
    pub use branch_trace as trace;
    pub use branch_warn as warn;
    pub use tracing::{self, event, span, Level};
}

pub mod log_macros {
    #[allow(unused)]
    #[macro_export]
    macro_rules! trace {
        ($fmt:expr) => {
            eprintln!(concat!("TRACE ", $fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!(concat!("TRACE ", $fmt), $($arg)*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! debug {
        ($fmt:expr) => {
            eprintln!(concat!("DEBUG ", $fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!(concat!("DEBUG ", $fmt), $($arg)*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! info {
        ($fmt:expr) => {
            eprintln!(concat!("INFO  ", $fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!(concat!("INFO  ", $fmt), $($arg)*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! warn {
        ($fmt:expr) => {
            eprintln!(concat!("WARN  ", $fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!(concat!("WARN  ", $fmt), $($arg)*);
        };
    }
    #[allow(unused)]
    #[macro_export]
    macro_rules! error {
        ($fmt:expr) => {
            eprintln!(concat!("ERROR ", $fmt));
        };
        ($fmt:expr, $($arg:tt)*) => {
            eprintln!(concat!("ERROR ", $fmt), $($arg)*);
        };
    }
}

pub mod log {
    pub use tracing::{self, event, span, Level};
    pub use tracing::{debug, error, info, trace, warn};
}

pub mod log_direct {
    pub use crate::{debug, error, info, trace, warn};
    pub use tracing::{self, event, span, Level};
}

use bytes::Bytes;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use futures_util::Stream;
use futures_util::StreamExt;
use http::Request;
use http::Uri;
use range::evrange::NanoRange;
use range::evrange::PulseRange;
use range::evrange::SeriesRange;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsVal;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;
use std::sync::LazyLock;
use std::sync::Once;
use std::sync::RwLock;
use std::sync::RwLockWriteGuard;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use timeunits::*;
use url::Url;

pub const APP_JSON: &str = "application/json";
pub const APP_JSON_LINES: &str = "application/jsonlines";
pub const APP_OCTET: &str = "application/octet-stream";
pub const APP_CBOR_FRAMED: &str = "application/cbor-framed";
pub const APP_JSON_FRAMED: &str = "application/json-framed";
pub const ACCEPT_ALL: &str = "*/*";
pub const X_DAQBUF_REQID: &str = "x-daqbuffer-request-id";
pub const HEADER_NAME_REQUEST_ID: &str = "requestid";

pub const CONNECTION_STATUS_DIV: DtMs = DtMs::from_ms_u64(1000 * 60 * 60);
// pub const TS_MSP_GRID_UNIT: DtMs = DtMs::from_ms_u64(1000 * 10);
// pub const TS_MSP_GRID_SPACING: u64 = 6 * 2;

pub const EMIT_ACCOUNTING_SNAP: DtMs = DtMs::from_ms_u64(1000 * 60 * 10);

pub const DATETIME_FMT_0MS: &str = "%Y-%m-%dT%H:%M:%SZ";
pub const DATETIME_FMT_3MS: &str = "%Y-%m-%dT%H:%M:%S.%3fZ";
pub const DATETIME_FMT_6MS: &str = "%Y-%m-%dT%H:%M:%S.%6fZ";
pub const DATETIME_FMT_9MS: &str = "%Y-%m-%dT%H:%M:%S.%9fZ";

const TEST_BACKEND: &str = "testbackend-00";

#[allow(non_upper_case_globals)]
pub const trigger: [&'static str; 0] = [
    //
    // "S30CB05-VMCP-A010:PRESSURE",
    // "ATSRF-CAV:TUN-DETUNING-REL-ACT",
    // "S30CB14-KBOC-HPPI1:PI-OUT",
];

pub const TRACE_SERIES_ID: [u64; 1] = [
    //
    4985969403507503043,
];

pub struct OnDrop<F>
where
    F: FnOnce() -> (),
{
    f: Option<F>,
}

impl<F> OnDrop<F>
where
    F: FnOnce() -> (),
{
    pub fn new(f: F) -> Self {
        Self { f: Some(f) }
    }
}

impl<F> Drop for OnDrop<F>
where
    F: FnOnce() -> (),
{
    fn drop(&mut self) {
        self.f.take().map(|x| x());
    }
}

pub fn is_false<T>(x: T) -> bool
where
    T: std::borrow::Borrow<bool>,
{
    *x.borrow() == false
}

pub trait CmpZero {
    fn is_zero(&self) -> bool;
}

impl CmpZero for u32 {
    fn is_zero(&self) -> bool {
        *self == 0
    }
}
impl CmpZero for usize {
    fn is_zero(&self) -> bool {
        *self == 0
    }
}

autoerr::create_error_v1!(
    name(Error, "NetpodError"),
    enum variants {
        NodeMissing,
        UnknownSeriesKind(i64),
        BadInt(#[from] std::num::ParseIntError),
        ChronoParse(#[from] chrono::ParseError),
        HumantimeDurationParse(#[from] humantime::DurationError),
        MissingQueryParameters,
        MissingSeries,
        MissingBackend,
        MissingTimerange,
        BadTimerange,
        NoSeriesNoName,
        BadScalarTypeIndex(i64),
        UnsupportedDtype(u8),
        JsonParse(#[from] serde_json::Error),
        BadScalarTypeVariant(String),
        BadScalarTypeCaId(u16),
        ScalarTypeNotInCa,
        MissingScalarType,
        MissingShape,
        MissingBinningScheme,
        BadCacheUsage(String),
        TimelikeBinWidthImpossibleForPulseRange,
        BinCountTooLarge,
        BinCountTooSmall,
        BinnedNoGridMatch,
        NotTimerange,
        AggKind,
        TypeMismatch,
        ShapeBad,
        InputBad,
    },
);

autoerr::create_error_v1!(
    name(AsyncChannelError, "AsyncChannelError"),
    enum variants {
        Send,
        Recv,
    },
);

pub struct BodyStream<E>
where
    E: std::error::Error,
{
    pub inner: Box<dyn Stream<Item = Result<Bytes, E>> + Send + Unpin>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub enum SeriesKind {
    ChannelStatus,
    ChannelData,
    CaStatus,
}

impl SeriesKind {
    pub fn to_db_i16(&self) -> i16 {
        use SeriesKind::*;
        match self {
            ChannelStatus => 1,
            ChannelData => 2,
            CaStatus => 3,
        }
    }

    pub fn from_db_i16(x: i16) -> Result<Self, Error> {
        let ret = match x {
            1 => Self::ChannelStatus,
            2 => Self::ChannelData,
            3 => Self::CaStatus,
            _ => return Err(Error::UnknownSeriesKind(x as i64)),
        };
        Ok(ret)
    }
}

impl Default for SeriesKind {
    fn default() -> Self {
        SeriesKind::ChannelData
    }
}

impl FromUrl for SeriesKind {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let ret = pairs
            .get("seriesKind")
            .and_then(|x| match x.as_str() {
                "channelStatus" => Some(Self::ChannelStatus),
                "channelData" => Some(Self::ChannelData),
                "caStatus" => Some(Self::CaStatus),
                _ => None,
            })
            .unwrap_or(Self::default());
        Ok(ret)
    }
}

impl AppendToUrl for SeriesKind {
    fn append_to_url(&self, url: &mut Url) {
        let s = match self {
            SeriesKind::ChannelStatus => "channelStatus",
            SeriesKind::ChannelData => "channelData",
            SeriesKind::CaStatus => "caStatus",
        };
        let mut g = url.query_pairs_mut();
        g.append_pair("seriesKind", &s);
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScalarType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    BOOL,
    STRING,
    Enum,
}

impl fmt::Debug for ScalarType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_variant_str())
    }
}

impl fmt::Display for ScalarType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_variant_str())
    }
}

impl Serialize for ScalarType {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S::Error: serde::ser::Error,
    {
        use ScalarType::*;
        match self {
            U8 => ser.serialize_str("u8"),
            U16 => ser.serialize_str("u16"),
            U32 => ser.serialize_str("u32"),
            U64 => ser.serialize_str("u64"),
            I8 => ser.serialize_str("i8"),
            I16 => ser.serialize_str("i16"),
            I32 => ser.serialize_str("i32"),
            I64 => ser.serialize_str("i64"),
            F32 => ser.serialize_str("f32"),
            F64 => ser.serialize_str("f64"),
            BOOL => ser.serialize_str("bool"),
            STRING => ser.serialize_str("string"),
            Enum => ser.serialize_str("enum"),
        }
    }
}

struct ScalarTypeVis;

impl<'de> serde::de::Visitor<'de> for ScalarTypeVis {
    type Value = ScalarType;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("a string describing the ScalarType variant")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        use ScalarType::*;
        let s = value.to_lowercase();
        let ret = match s.as_str() {
            "u8" => U8,
            "u16" => U16,
            "u32" => U32,
            "u64" => U64,
            "i8" => I8,
            "i16" => I16,
            "i32" => I32,
            "i64" => I64,
            "f32" => F32,
            "f64" => F64,
            "bool" => BOOL,
            "string" => STRING,
            "enum" => Enum,
            k => return Err(E::custom(format!("can not understand variant {k:?}"))),
        };
        Ok(ret)
    }
}

impl<'de> Deserialize<'de> for ScalarType {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        de.deserialize_str(ScalarTypeVis)
    }
}

pub trait HasScalarType {
    fn scalar_type(&self) -> ScalarType;
}

impl ScalarType {
    pub fn from_dtype_index(ix: u8) -> Result<Self, Error> {
        use ScalarType::*;
        let g = match ix {
            0 => BOOL,
            1 => BOOL,
            3 => U8,
            5 => U16,
            8 => U32,
            10 => U64,
            2 => I8,
            4 => I16,
            7 => I32,
            9 => I64,
            11 => F32,
            12 => F64,
            13 => STRING,
            15 => Enum,
            _ => return Err(Error::UnsupportedDtype(ix)),
        };
        Ok(g)
    }

    pub fn to_variant_str(&self) -> &'static str {
        use ScalarType::*;
        match self {
            U8 => "u8",
            U16 => "u16",
            U32 => "u32",
            U64 => "u64",
            I8 => "i8",
            I16 => "i16",
            I32 => "i32",
            I64 => "i64",
            F32 => "f32",
            F64 => "f64",
            BOOL => "bool",
            STRING => "string",
            Enum => "enum",
        }
    }

    pub fn from_variant_str(s: &str) -> Result<Self, Error> {
        use ScalarType::*;
        let ret = match s {
            "u8" => U8,
            "u16" => U16,
            "u32" => U32,
            "u64" => U64,
            "i8" => I8,
            "i16" => I16,
            "i32" => I32,
            "i64" => I64,
            "f32" => F32,
            "f64" => F64,
            "bool" => BOOL,
            "string" => STRING,
            "enum" => Enum,
            _ => return Err(Error::BadScalarTypeVariant(s.into())),
        };
        Ok(ret)
    }

    pub fn to_bsread_str(&self) -> &'static str {
        use ScalarType::*;
        match self {
            U8 => "uint8",
            U16 => "uint16",
            U32 => "uint32",
            U64 => "uint64",
            I8 => "int8",
            I16 => "int16",
            I32 => "int32",
            I64 => "int64",
            F32 => "float32",
            F64 => "float64",
            BOOL => "bool",
            STRING => "string",
            Enum => "enum",
        }
    }

    pub fn from_bsread_str(s: &str) -> Result<Self, Error> {
        use ScalarType::*;
        let ret = match s {
            "uint8" => U8,
            "uint16" => U16,
            "uint32" => U32,
            "uint64" => U64,
            "int8" => I8,
            "int16" => I16,
            "int32" => I32,
            "int64" => I64,
            "float" => F32,
            "double" => F64,
            "float32" => F32,
            "float64" => F64,
            "bool" => BOOL,
            "string" => STRING,
            "enum" => Enum,
            _ => return Err(Error::BadScalarTypeVariant(s.into())),
        };
        Ok(ret)
    }

    pub fn from_ca_id(k: u16) -> Result<Self, Error> {
        use ScalarType::*;
        let ret = match k {
            0 => STRING,
            1 => I16,
            2 => F32,
            3 => Enum,
            4 => I8,
            5 => I32,
            6 => F64,
            _ => return Err(Error::BadScalarTypeCaId(k)),
        };
        Ok(ret)
    }

    pub fn to_ca_id(&self) -> Result<u16, Error> {
        use ScalarType::*;
        let ret = match self {
            I8 => 4,
            I16 => 1,
            I32 => 5,
            F32 => 2,
            F64 => 6,
            STRING => 0,
            Enum => 3,
            _ => return Err(Error::ScalarTypeNotInCa),
        };
        Ok(ret)
    }

    pub fn from_archeng_db_str(s: &str) -> Result<Self, Error> {
        use ScalarType::*;
        let ret = match s {
            "I8" => I8,
            "I16" => I16,
            "I32" => I32,
            "I64" => I64,
            "F32" => F32,
            "F64" => F64,
            _ => return Err(Error::BadScalarTypeVariant(s.into())),
        };
        Ok(ret)
    }

    pub fn from_scylla_i32(k: i32) -> Result<Self, Error> {
        if k < 0 || k > u8::MAX as i32 {
            return Err(Error::BadScalarTypeIndex(k as i64));
        }
        Self::from_dtype_index(k as u8)
    }

    // TODO this is useless for strings and enums.
    pub fn bytes(&self) -> u8 {
        use ScalarType::*;
        match self {
            U8 => 1,
            U16 => 2,
            U32 => 4,
            U64 => 8,
            I8 => 1,
            I16 => 2,
            I32 => 4,
            I64 => 8,
            F32 => 4,
            F64 => 8,
            BOOL => 1,
            STRING => 1,
            Enum => 2,
        }
    }

    pub fn index(&self) -> u8 {
        use ScalarType::*;
        match self {
            U8 => 3,
            U16 => 5,
            U32 => 8,
            U64 => 10,
            I8 => 2,
            I16 => 4,
            I32 => 7,
            I64 => 9,
            F32 => 11,
            F64 => 12,
            BOOL => 0,
            STRING => 13,
            Enum => 15,
        }
    }

    pub fn to_scylla_table_name_id(&self) -> &'static str {
        self.to_variant_str()
    }

    pub fn to_scylla_i32(&self) -> i32 {
        self.index() as i32
    }

    pub fn from_url_str(s: &str) -> Result<Self, Error> {
        let ret = serde_json::from_str(&format!("\"{s}\""))?;
        Ok(ret)
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct StringFix<const N: usize> {
    data: [char; N],
    len: u8,
}

impl<const N: usize> StringFix<N> {
    pub fn new() -> Self {
        Self {
            data: [char::REPLACEMENT_CHARACTER; N],
            len: 0,
        }
    }

    pub fn string(&self) -> String {
        self.data[..self.len as usize].iter().map(|x| *x).collect()
    }
}

impl<const N: usize, T> From<T> for StringFix<N>
where
    T: AsRef<str>,
{
    fn from(x: T) -> Self {
        let sl = x.as_ref();
        let sl = &sl[0..sl.len().min(N)];
        let mut ret = Self::new();
        for (i, ch) in sl.chars().enumerate() {
            ret.data[i] = ch;
        }
        ret.len = sl.len() as u8;
        ret
    }
}

impl<const N: usize> From<StringFix<N>> for String {
    fn from(x: StringFix<N>) -> Self {
        x.data[0..x.len as _].iter().collect()
    }
}

mod string_fix_impl_serde {
    use crate::StringFix;
    use serde::de::Visitor;
    use serde::Deserialize;
    use serde::Serialize;
    use std::fmt;

    impl<const N: usize> Serialize for StringFix<N> {
        fn serialize<S>(&self, _ser: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            // ser.serialize_str()
            todo!("StringFix Serialize")
        }
    }

    impl<'de, const N: usize> Deserialize<'de> for StringFix<N> {
        fn deserialize<D>(_de: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            // de.deserialize_unit(Vis::<N>)
            todo!("StringFix Deserialize")
        }
    }

    #[allow(dead_code)]
    struct Vis<const N: usize>;

    impl<'de, const N: usize> Visitor<'de> for Vis<N> {
        type Value = StringFix<N>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "deserialize enum error")
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            todo!("StringFix Visitor")
            // Ok(Self::Value::new())
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct EnumVariant {
    ix: u16,
    name: String,
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub struct EnumVariantRef<'a> {
    pub ix: u16,
    pub name: &'a str,
}

impl<'a> From<EnumVariantRef<'a>> for EnumVariant {
    fn from(value: EnumVariantRef<'a>) -> Self {
        Self {
            ix: value.ix,
            name: value.name.to_string(),
        }
    }
}

impl EnumVariant {
    pub fn new(ix: u16, name: impl Into<String>) -> Self {
        Self {
            ix,
            name: name.into(),
        }
    }

    pub fn ix(&self) -> u16 {
        self.ix
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn name_string(&self) -> String {
        self.name.clone()
    }

    pub fn into_parts(self) -> (u16, String) {
        (self.ix, self.name)
    }
}

impl Default for EnumVariant {
    fn default() -> Self {
        Self {
            ix: u16::MAX,
            name: String::new(),
        }
    }
}

impl fmt::Display for EnumVariant {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}({})", self.ix, self.name)
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct UnsupEvt(pub u32);

impl AppendToUrl for ScalarType {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("scalarType", self.to_variant_str());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SfDatabuffer {
    pub data_base_path: PathBuf,
    pub ksprefix: String,
    pub splits: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArchiverAppliance {
    pub data_base_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelArchiver {
    pub data_base_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub host: String,
    // TODO for `listen` and the ports, would be great to allow a default on Cluster level.
    pub listen: Option<String>,
    #[serde(deserialize_with = "serde_port::port_from_any")]
    pub port: u16,
    #[serde(deserialize_with = "serde_port::port_from_any", default)]
    pub port_raw: u16,
    pub sf_databuffer: Option<SfDatabuffer>,
    pub archiver_appliance: Option<ArchiverAppliance>,
    pub channel_archiver: Option<ChannelArchiver>,
    pub prometheus_api_bind: Option<SocketAddr>,
}

mod serde_port {
    use super::*;

    struct Vis;

    impl<'de> serde::de::Visitor<'de> for Vis {
        type Value = u16;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "a tcp port number, in numeric or string form.")
        }

        fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if val > u16::MAX as u64 {
                Err(serde::de::Error::invalid_type(
                    serde::de::Unexpected::Unsigned(val),
                    &self,
                ))
            } else {
                self.visit_i64(val as i64)
            }
        }

        fn visit_i64<E>(self, val: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if val < 1 || val > u16::MAX as i64 {
                Err(serde::de::Error::invalid_type(
                    serde::de::Unexpected::Signed(val),
                    &self,
                ))
            } else {
                Ok(val as u16)
            }
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            match val.parse::<u16>() {
                Err(_) => Err(serde::de::Error::invalid_type(
                    serde::de::Unexpected::Str(val),
                    &self,
                )),
                Ok(v) => Ok(v),
            }
        }
    }

    pub fn port_from_any<'de, D>(de: D) -> Result<u16, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // We expect to use json or yaml only.
        de.deserialize_any(Vis)
    }

    #[test]
    fn test_port_from_any() {
        #[derive(Deserialize)]
        struct Conf {
            #[serde(deserialize_with = "port_from_any")]
            port: u16,
        }
        let conf: Conf = serde_json::from_str(r#"{"port":"9192"}"#).unwrap();
        assert_eq!(conf.port, 9192);
        let conf: Conf = serde_json::from_str(r#"{"port":9194}"#).unwrap();
        assert_eq!(conf.port, 9194);
    }
}

impl Node {
    // TODO needed? Could `sf_databuffer` be None?
    pub fn dummy() -> Self {
        Self {
            host: "dummy".into(),
            listen: None,
            port: 4444,
            port_raw: 4444,
            sf_databuffer: Some(SfDatabuffer {
                data_base_path: PathBuf::new(),
                ksprefix: "daqlocal".into(),
                splits: None,
            }),
            archiver_appliance: None,
            channel_archiver: None,
            prometheus_api_bind: None,
        }
    }

    // TODO should a node know how to reach itself? Because, depending on network
    // topology (proxies etc.) the way to reach a node depends on the tuple `(node, client)`.
    pub fn baseurl(&self) -> Url {
        // TODO should be able to decide whether we are reachable via tls.
        // So far this does not matter because this `baseurl` is used for internal communication
        // and is always non-tls.
        format!("http://{}:{}", self.host, self.port)
            .parse()
            .unwrap()
    }

    pub fn listen(&self) -> String {
        match &self.listen {
            Some(x) => x.into(),
            None => "0.0.0.0".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Database {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScyllaConfig {
    pub hosts: Vec<String>,
    pub keyspace: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cluster {
    pub backend: String,
    pub nodes: Vec<Node>,
    pub database: Database,
    #[serde(rename = "runMapPulse", default)]
    pub run_map_pulse_task: bool,
    #[serde(rename = "isCentralStorage", default)]
    pub is_central_storage: bool,
    #[serde(rename = "fileIoBufferSize", default)]
    pub file_io_buffer_size: FileIoBufferSize,
    scylla: Option<ScyllaConfig>,
    #[serde(rename = "scylla_st")]
    scylla_st: Option<ScyllaConfig>,
    #[serde(rename = "scylla_mt")]
    scylla_mt: Option<ScyllaConfig>,
    #[serde(rename = "scylla_lt")]
    scylla_lt: Option<ScyllaConfig>,
    cache_scylla: Option<ScyllaConfig>,
    pub announce_backends: Option<Vec<String>>,
}

impl Cluster {
    pub fn decompress_default(&self) -> bool {
        if self.is_central_storage {
            false
        } else {
            true
        }
    }

    pub fn scylla_st(&self) -> Option<&ScyllaConfig> {
        self.scylla_st
            .as_ref()
            .map_or_else(|| self.scylla.as_ref(), Some)
    }

    pub fn scylla_mt(&self) -> Option<&ScyllaConfig> {
        self.scylla_mt.as_ref()
    }

    pub fn scylla_lt(&self) -> Option<&ScyllaConfig> {
        self.scylla_lt.as_ref()
    }

    pub fn test_00() -> Self {
        Self {
            backend: "testbackend-00".into(),
            nodes: Vec::new(),
            database: Database {
                name: "".into(),
                host: "".into(),
                port: 5432,
                user: "".into(),
                pass: "".into(),
            },
            run_map_pulse_task: false,
            is_central_storage: false,
            file_io_buffer_size: FileIoBufferSize(1024 * 8),
            scylla: None,
            scylla_st: None,
            scylla_mt: None,
            scylla_lt: None,
            cache_scylla: None,
            announce_backends: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub name: String,
    pub cluster: Cluster,
}

impl NodeConfig {
    pub fn get_node(&self) -> Option<(&Node, usize)> {
        if self.name.contains(":") {
            let mut i1 = 0;
            for n in &self.cluster.nodes {
                if self.name == format!("{}:{}", n.host, n.port) {
                    return Some((n, i1));
                }
                i1 += 1;
            }
        } else {
            let mut i1 = 0;
            for n in &self.cluster.nodes {
                if self.name == format!("{}", n.host) {
                    return Some((n, i1));
                }
                i1 += 1;
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
pub struct ServiceVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub pre: Option<String>,
}

impl fmt::Display for ServiceVersion {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match &self.pre {
            Some(pre) => write!(fmt, "{}.{}.{}-{}", self.major, self.minor, self.patch, pre),
            None => write!(fmt, "{}.{}.{}", self.major, self.minor, self.patch),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeConfigCached {
    pub node_config: NodeConfig,
    pub node: Node,
    pub ix: usize,
}

impl From<NodeConfig> for Result<NodeConfigCached, Error> {
    fn from(k: NodeConfig) -> Self {
        match k.get_node() {
            Some((node, ix)) => {
                let ret = NodeConfigCached {
                    node: node.clone(),
                    node_config: k,
                    ix,
                };
                Ok(ret)
            }
            None => Err(Error::NodeMissing),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatusArchiverAppliance {
    pub readable: Vec<(PathBuf, bool)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSizes {
    pub sizes: Vec<(String, String)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeStatusSubError {
    Msg(String),
}

impl fmt::Display for NodeStatusSubError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)
    }
}

impl std::error::Error for NodeStatusSubError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatusSub {
    pub url: String,
    pub status: Result<NodeStatus, NodeStatusSubError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_sf_databuffer: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_archiver_engine: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_archiver_appliance: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_size: Option<Result<u64, String>>,
    //#[serde(default, skip_serializing_if = "Option::is_none")]
    //pub table_sizes: Option<Result<TableSizes, Error>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archiver_appliance_status: Option<NodeStatusArchiverAppliance>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    pub subs: VecDeque<NodeStatusSub>,
}

// Describes a swissfel-databuffer style "channel" which is a time-series with a unique name within a "backend".
// Also the concept of "backend" could be split into "facility" and some optional other identifier
// for cases like e.g. post-mortem, or to differentiate between channel-access and bsread for cases where
// the same channel-name is delivered via different methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SfDbChannel {
    series: Option<u64>,
    // "backend" is currently used in the existing systems for multiple purposes:
    // it can indicate the facility (eg. sf-databuffer, hipa, ...) but also
    // some special subsystem (eg. sf-rf-databuffer).
    backend: String,
    name: String,
    kind: SeriesKind,
}

impl SfDbChannel {
    pub fn from_full<T: Into<String>, U: Into<String>>(
        backend: T,
        series: Option<u64>,
        name: U,
        kind: SeriesKind,
    ) -> Self {
        Self {
            backend: backend.into(),
            series,
            name: name.into(),
            kind,
        }
    }

    pub fn from_name<T: Into<String>, U: Into<String>>(backend: T, name: U) -> Self {
        Self {
            backend: backend.into(),
            series: None,
            name: name.into(),
            kind: SeriesKind::default(),
        }
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn series(&self) -> Option<u64> {
        self.series
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> SeriesKind {
        self.kind.clone()
    }

    pub fn set_series(&mut self, series: u64) {
        self.series = Some(series);
    }
}

impl fmt::Display for SfDbChannel {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "SfDbChannel {{ series: {:?}, backend: {:?}, name: {:?}, kind: {:?} }}",
            self.series, self.backend, self.name, self.kind
        )
    }
}

impl FromUrl for SfDbChannel {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let ret = SfDbChannel {
            backend: pairs
                .get("backend")
                .ok_or_else(|| Error::MissingBackend)?
                .into(),
            name: pairs
                .get("channelName")
                .map(String::from)
                .unwrap_or(String::new())
                .into(),
            series: pairs
                .get("seriesId")
                .and_then(|x| x.parse::<u64>().map_or(None, |x| Some(x))),
            kind: SeriesKind::from_pairs(pairs)?,
        };
        if ret.name.is_empty() && ret.series.is_none() {
            return Err(Error::NoSeriesNoName);
        }
        Ok(ret)
    }
}

impl AppendToUrl for SfDbChannel {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("backend", &self.backend);
        if self.name().len() > 0 {
            g.append_pair("channelName", &self.name);
        }
        if let Some(series) = self.series {
            g.append_pair("seriesId", &series.to_string());
        }
        drop(g);
        self.kind.append_to_url(url);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelTyped {
    pub channel: SfDbChannel,
    pub scalar_type: ScalarType,
    pub shape: Shape,
}

impl ChannelTyped {
    pub fn channel(&self) -> &SfDbChannel {
        &self.channel
    }
}

// Describes a Scylla-based "daqbuffer" style time series.
// The tuple `(backend, series)` is supposed to be unique.
// Contains also the name because it is so useful.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaqbufSeries {
    pub series: u64,
    // "backend" is currently used in the existing systems for multiple purposes:
    // it can indicate the facility (eg. sf-databuffer, hipa, ...) but also
    // some special subsystem (eg. sf-rf-databuffer).
    pub backend: String,
    // This name is only for better user-facing messages. The (backend, series-id) is the identifier.
    pub name: String,
}

impl DaqbufSeries {
    pub fn series(&self) -> u64 {
        self.series
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl FromUrl for DaqbufSeries {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let ret = DaqbufSeries {
            series: pairs
                .get("seriesId")
                .ok_or_else(|| Error::MissingSeries)
                .map(|x| x.parse::<u64>())??,
            backend: pairs
                .get("backend")
                .ok_or_else(|| Error::MissingBackend)?
                .into(),
            name: pairs
                .get("channelName")
                .map(String::from)
                .unwrap_or(String::new())
                .into(),
        };
        Ok(ret)
    }
}

impl AppendToUrl for DaqbufSeries {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("backend", &self.backend);
        if self.name().len() > 0 {
            g.append_pair("channelName", &self.name);
        }
        g.append_pair("seriesId", &self.series.to_string());
    }
}

pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl HostPort {
    pub fn new<S: Into<String>>(host: S, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn from_node(node: &Node) -> Self {
        Self {
            host: node.host.clone(),
            port: node.port,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct FilePos {
    pub pos: u64,
}

impl From<FilePos> for u64 {
    fn from(k: FilePos) -> Self {
        k.pos
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub enum ByteOrder {
    Little,
    Big,
}

impl ByteOrder {
    pub fn from_dtype_flags(flags: u8) -> Self {
        if flags & 0x20 == 0 {
            Self::Little
        } else {
            Self::Big
        }
    }

    pub fn from_bsread_str(s: &str) -> Result<ByteOrder, Error> {
        match s {
            "little" => Ok(ByteOrder::Little),
            "big" => Ok(ByteOrder::Big),
            _ => Err(Error::InputBad),
        }
    }

    pub fn is_le(&self) -> bool {
        if let Self::Little = self {
            true
        } else {
            false
        }
    }

    pub fn is_be(&self) -> bool {
        if let Self::Big = self {
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GenVar {
    Default,
    TimeWeight,
    ConstRegular,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum ShapeOld {
    Scalar,
    Wave(u32),
    Image(u32, u32),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Shape {
    Scalar,
    Wave(u32),
    Image(u32, u32),
}

impl fmt::Display for Shape {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

mod serde_shape {
    use super::*;

    impl Serialize for Shape {
        fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S::Error: serde::ser::Error,
        {
            use Shape::*;
            match self {
                Scalar => ser.collect_seq(std::iter::empty::<u32>()),
                Wave(a) => ser.collect_seq([*a].iter()),
                Image(a, b) => ser.collect_seq([*a, *b].iter()),
            }
        }
    }

    struct ShapeVis;

    impl<'de> serde::de::Visitor<'de> for ShapeVis {
        type Value = Shape;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            fmt.write_str("a vector describing the shape")
        }

        // TODO unused, do not support deser from any for Shape
        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if v == "Scalar" {
                Ok(Shape::Scalar)
            } else {
                Err(E::custom(format!("unexpected value: {v:?}")))
            }
        }

        // TODO unused, do not support deser from any for Shape
        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            use serde::de::Error;
            if let Some(key) = map.next_key::<String>()? {
                if key == "Wave" {
                    let n: u32 = map.next_value()?;
                    Ok(Shape::Wave(n))
                } else if key == "Image" {
                    let a = map.next_value::<[u32; 2]>()?;
                    Ok(Shape::Image(a[0], a[1]))
                } else {
                    Err(A::Error::custom(format!("unexpected key {key:?}")))
                }
            } else {
                Err(A::Error::custom(format!("invalid shape format")))
            }
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut a = vec![];
            while let Some(item) = seq.next_element()? {
                let n: u32 = item;
                a.push(n);
            }
            if a.len() == 0 {
                Ok(Shape::Scalar)
            } else if a.len() == 1 {
                Ok(Shape::Wave(a[0]))
            } else if a.len() == 2 {
                Ok(Shape::Image(a[0], a[1]))
            } else {
                use serde::de::Error;
                Err(A::Error::custom(format!("bad shape")))
            }
        }
    }

    impl<'de> Deserialize<'de> for Shape {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let res = de.deserialize_seq(ShapeVis);
            res
        }
    }
}

impl Shape {
    pub fn from_sf_databuffer_raw(v: &Option<Vec<u32>>) -> Result<Self, Error> {
        let ret = match v {
            Some(a) => match a.len() {
                0 => Shape::Scalar,
                1 => Shape::Wave(a[0]),
                2 => Shape::Image(a[0], a[1]),
                _ => return Err(Error::InputBad),
            },
            None => Shape::Scalar,
        };
        Ok(ret)
    }

    pub fn from_bsread_jsval(v: &JsVal) -> Result<Self, Error> {
        match v {
            JsVal::Array(v) => match v.len() {
                0 => Ok(Shape::Scalar),
                1 => match &v[0] {
                    JsVal::Number(v) => match v.as_u64() {
                        Some(0) | Some(1) => Ok(Shape::Scalar),
                        Some(v) => Ok(Shape::Wave(v as u32)),
                        None => Err(Error::ShapeBad),
                    },
                    _ => Err(Error::ShapeBad),
                },
                _ => Err(Error::ShapeBad),
            },
            _ => Err(Error::ShapeBad),
        }
    }

    // TODO use simply a list to represent all shapes: empty, or with 1 or 2 entries.
    pub fn from_db_jsval(v: &JsVal) -> Result<Self, Error> {
        match v {
            JsVal::String(s) => {
                if s == "Scalar" {
                    Ok(Shape::Scalar)
                } else {
                    Err(Error::ShapeBad)
                }
            }
            JsVal::Object(j) => match j.get("Wave") {
                Some(JsVal::Number(j)) => Ok(Shape::Wave(
                    j.as_u64().ok_or_else(|| Error::ShapeBad)? as u32,
                )),
                _ => Err(Error::ShapeBad),
            },
            _ => Err(Error::ShapeBad),
        }
    }

    pub fn from_dims_str(s: &str) -> Result<Self, Error> {
        let a: Vec<u32> = serde_json::from_str(s)?;
        if a.len() == 0 {
            Ok(Shape::Scalar)
        } else if a.len() == 1 {
            Ok(Shape::Wave(a[0]))
        } else if a.len() == 2 {
            Ok(Shape::Image(a[0], a[1]))
        } else {
            Err(Error::ShapeBad)
        }
    }

    pub fn from_scylla_shape_dims(v: &[i32]) -> Result<Self, Error> {
        let res = if v.len() == 0 {
            Shape::Scalar
        } else if v.len() == 1 {
            Shape::Wave(v[0] as u32)
        } else if v.len() == 2 {
            Shape::Image(v[0] as u32, v[1] as u32)
        } else {
            return Err(Error::ShapeBad);
        };
        Ok(res)
    }

    pub fn from_ca_count(k: u32) -> Result<Self, Error> {
        if k == 0 {
            Err(Error::InputBad)
        } else if k == 1 {
            Ok(Shape::Scalar)
        } else if k <= 1024 * 3000 {
            Ok(Shape::Wave(k))
        } else {
            Err(Error::InputBad)
        }
    }

    pub fn to_ca_count(&self) -> Result<u32, Error> {
        use Shape::*;
        let res = match self {
            Scalar => 1,
            Wave(n) => *n as u32,
            _ => return Err(Error::InputBad),
        };
        Ok(res)
    }

    pub fn to_scylla_vec(&self) -> Vec<i32> {
        use Shape::*;
        match self {
            Scalar => Vec::new(),
            Wave(n) => vec![*n as i32],
            Image(n, m) => vec![*n as i32, *m as i32],
        }
    }

    pub fn to_u32_vec(&self) -> Vec<u32> {
        use Shape::*;
        match self {
            Scalar => Vec::new(),
            Wave(n) => vec![*n as u32],
            Image(n, m) => vec![*n as u32, *m as u32],
        }
    }

    pub fn to_json_value(&self) -> JsVal {
        use serde_json::Number;
        match self {
            Shape::Scalar => JsVal::Array(Vec::new()),
            Shape::Wave(n) => JsVal::Array(vec![JsVal::Number(Number::from(*n))]),
            Shape::Image(n, m) => JsVal::Array(vec![
                JsVal::Number(Number::from(*n)),
                JsVal::Number(Number::from(*m)),
            ]),
        }
    }

    pub fn from_url_str(s: &str) -> Result<Self, Error> {
        let ret = serde_json::from_str(s)?;
        Ok(ret)
    }

    pub fn ele_count(&self) -> u64 {
        match self {
            Shape::Scalar => 1,
            Shape::Wave(n) => *n as u64,
            Shape::Image(n, m) => *n as u64 * *m as u64,
        }
    }
}

impl AppendToUrl for Shape {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        g.append_pair("shape", &format!("{:?}", self.to_scylla_vec()));
    }
}

#[test]
fn test_shape_serde() {
    let s = serde_json::to_string(&Shape::Image(42, 43)).unwrap();
    assert_eq!(s, r#"[42,43]"#);
    let s = serde_json::to_string(&ShapeOld::Scalar).unwrap();
    assert_eq!(s, r#""Scalar""#);
    let s = serde_json::to_string(&ShapeOld::Wave(8)).unwrap();
    assert_eq!(s, r#"{"Wave":8}"#);
    let s = serde_json::to_string(&ShapeOld::Image(42, 43)).unwrap();
    assert_eq!(s, r#"{"Image":[42,43]}"#);
    let s: ShapeOld = serde_json::from_str(r#""Scalar""#).unwrap();
    assert_eq!(s, ShapeOld::Scalar);
    let s: ShapeOld = serde_json::from_str(r#"{"Wave": 123}"#).unwrap();
    assert_eq!(s, ShapeOld::Wave(123));
    let s: ShapeOld = serde_json::from_str(r#"{"Image":[77, 78]}"#).unwrap();
    assert_eq!(s, ShapeOld::Image(77, 78));
    let s: Shape = serde_json::from_str(r#"[]"#).unwrap();
    assert_eq!(s, Shape::Scalar);
    let s: Shape = serde_json::from_str(r#"[12]"#).unwrap();
    assert_eq!(s, Shape::Wave(12));
    let s: Shape = serde_json::from_str(r#"[12, 13]"#).unwrap();
    assert_eq!(s, Shape::Image(12, 13));
}

pub mod timeunits {
    pub const MU: u64 = 1000;
    pub const MS: u64 = MU * 1000;
    pub const SEC: u64 = MS * 1000;
    pub const MIN: u64 = SEC * 60;
    pub const HOUR: u64 = MIN * 60;
    pub const DAY: u64 = HOUR * 24;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Dim0Kind {
    Time,
    Pulse,
}

pub trait Dim0Index: Clone + fmt::Debug + PartialOrd {
    fn add(&self, v: &Self) -> Self;
    fn sub(&self, v: &Self) -> Self;
    fn sub_n(&self, v: u64) -> Self;
    fn times(&self, x: u64) -> Self;
    fn div_n(&self, n: u64) -> Self;
    fn div_v(&self, v: &Self) -> u64;
    fn as_u64(&self) -> u64;
    fn series_range(a: Self, b: Self) -> SeriesRange;
    fn prebin_bin_len_opts() -> Vec<Self>;
    fn prebin_patch_len_for(i: usize) -> Self;
    fn to_pre_binned_patch_range_enum(
        &self,
        bin_count: u64,
        patch_offset: u64,
        patch_count: u64,
    ) -> PreBinnedPatchRangeEnum;
    fn binned_bin_len_opts() -> Vec<Self>;
    fn to_binned_range_enum(&self, bin_off: u64, bin_cnt: u64) -> BinnedRangeEnum;
}

pub trait Dim0Range: Clone + fmt::Debug + PartialOrd {}

pub struct Dim0RangeValue<T>
where
    T: Dim0Index,
{
    pub ix: [T; 2],
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct DtNano(u64);

impl DtNano {
    pub const fn from_ns(ns: u64) -> Self {
        Self(ns)
    }

    pub const fn from_ms(ms: u64) -> Self {
        Self(1000000 * ms)
    }

    pub const fn ns(&self) -> u64 {
        self.0
    }

    pub const fn ms_u64(&self) -> u64 {
        self.0 / 1000000
    }

    pub fn to_i64(&self) -> i64 {
        self.0 as i64
    }

    pub fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }

    pub fn fraction_of(self, rhs: Self) -> f32 {
        self.0 as f32 / rhs.0 as f32
    }
}

impl fmt::Display for DtNano {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let sec = self.0 / SEC;
        let ms = (self.0 - SEC * sec) / MS;
        let ns = self.0 - SEC * sec - MS * ms;
        // fmt.debug_tuple("DtNano").field(&sec).field(&ms).field(&ns).finish()
        write!(fmt, "DtNano {{ sec {}  ms {}  ns {} }}", sec, ms, ns)
    }
}

mod dt_nano_serde {
    use super::DtNano;
    use de::Visitor;
    use serde::de;
    use serde::Deserialize;
    use serde::Serialize;
    use std::fmt;

    impl Serialize for DtNano {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            ser.serialize_u64(self.ns())
        }
    }

    struct Vis1;

    impl<'de> Visitor<'de> for Vis1 {
        type Value = DtNano;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "an integer of nanoseconds")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(DtNano::from_ns(v))
        }
    }

    impl<'de> Deserialize<'de> for DtNano {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            de.deserialize_u64(Vis1)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct DtMs(u64);

impl DtMs {
    pub const fn from_nano_u64(x: u64) -> Self {
        Self(x / 1000000)
    }

    pub const fn from_ms_u64(x: u64) -> Self {
        Self(x)
    }

    pub const fn ms(&self) -> u64 {
        self.0
    }

    pub const fn ns(&self) -> u64 {
        1000000 * self.0
    }

    pub const fn dt_ns(&self) -> DtNano {
        DtNano::from_ms(self.0)
    }

    pub const fn to_i64(&self) -> i64 {
        self.0 as i64
    }
}

impl fmt::Display for DtMs {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let dur = Duration::from_millis(self.ms());
        write!(fmt, "{}", humantime::format_duration(dur))
    }
}

#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct TsNano(u64);

mod ts_nano_ser {
    use super::TsNano;
    use crate::timeunits::SEC;
    use chrono::TimeZone;
    use chrono::Utc;
    use de::Visitor;
    use serde::de;
    use serde::Deserialize;
    use serde::Serialize;
    use std::fmt;

    impl Serialize for TsNano {
        fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            if false {
                let ts = Utc.timestamp_opt((self.0 / SEC) as i64, (self.0 % SEC) as u32);
                let value = format!("{}", ts.earliest().unwrap());
                ser.serialize_newtype_struct("TsNano", &value)
            } else {
                ser.serialize_u64(self.ns())
            }
        }
    }

    struct Vis1;

    impl<'de> Visitor<'de> for Vis1 {
        type Value = TsNano;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "integer nanoseconds since unix epoch")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(TsNano::from_ns(v))
        }
    }

    impl<'de> Deserialize<'de> for TsNano {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            de.deserialize_u64(Vis1)
        }
    }
}

impl TsNano {
    pub const fn from_ns(ns: u64) -> Self {
        Self(ns)
    }

    pub const fn from_ms(ms: u64) -> Self {
        Self(1000000 * ms)
    }

    pub const fn ns(&self) -> u64 {
        self.0
    }

    pub const fn ms(&self) -> u64 {
        self.0 / 1000000
    }

    pub const fn add_dt_nano(self, v: DtNano) -> Self {
        Self(self.0 + v.0)
    }

    pub const fn sub(self, v: DtNano) -> Self {
        Self(self.0 - v.0)
    }

    pub const fn delta(self, v: Self) -> DtNano {
        DtNano(self.0 - v.0)
    }

    pub const fn add_ns(self, v: u64) -> Self {
        Self(self.0 + v)
    }

    pub const fn mul(self, v: u64) -> Self {
        Self(self.0 * v)
    }

    pub const fn div(self, v: u64) -> Self {
        Self(self.0 / v)
    }

    pub const fn to_ts_ms(self) -> TsMs {
        TsMs::from_ms_u64(self.ms())
    }

    pub const fn to_dt_nano(self) -> DtNano {
        DtNano::from_ns(self.0)
    }

    pub const fn to_dt_ms(self) -> DtMs {
        DtMs::from_ms_u64(self.ms())
    }

    pub fn from_system_time(st: SystemTime) -> Self {
        let tsunix = st.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        let x = tsunix.as_secs() * 1_000_000_000 + tsunix.subsec_nanos() as u64;
        Self::from_ns(x)
    }

    pub fn fmt(&self) -> TsNanoFmt {
        TsNanoFmt { ts: self.clone() }
    }
}

impl fmt::Debug for TsNano {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let ts = Utc
            .timestamp_opt((self.0 / SEC) as i64, (self.0 % SEC) as u32)
            .earliest()
            .unwrap_or(Default::default());
        write!(fmt, "TsNano {{ {} }}", ts.format(DATETIME_FMT_3MS))
    }
}

impl fmt::Display for TsNano {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let ts = Utc
            .timestamp_opt((self.0 / SEC) as i64, (self.0 % SEC) as u32)
            .earliest()
            .unwrap_or(Default::default());
        ts.format(DATETIME_FMT_3MS).fmt(fmt)
    }
}

impl AsRef<TsNano> for TsNano {
    fn as_ref(&self) -> &TsNano {
        &self
    }
}

impl AsRef<TsMs> for TsMs {
    fn as_ref(&self) -> &TsMs {
        &self
    }
}

pub struct TsNanoFmt {
    ts: TsNano,
}

impl fmt::Display for TsNanoFmt {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        chrono::DateTime::from_timestamp_millis(self.ts.ms() as i64)
            .unwrap()
            .format(DATETIME_FMT_3MS)
            .fmt(fmt)
    }
}

impl fmt::Debug for TsNanoFmt {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, fmt)
    }
}

pub struct TsMsFmt {
    ts: TsMs,
}

impl fmt::Display for TsMsFmt {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        chrono::DateTime::from_timestamp_millis(self.ts.ms() as i64)
            .unwrap()
            .format(DATETIME_FMT_3MS)
            .fmt(fmt)
    }
}

impl fmt::Debug for TsMsFmt {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, fmt)
    }
}

pub struct TsNanoVecFmt<I>(pub I);

impl<I, T> fmt::Display for TsNanoVecFmt<I>
where
    I: Clone + IntoIterator<Item = T>,
    T: AsRef<TsNano>,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "[")?;
        for ts in self.0.clone().into_iter() {
            write!(fmt, "  {}", ts.as_ref().fmt())?;
        }
        write!(fmt, "  ]")?;
        Ok(())
    }
}

pub struct TsMsVecFmt<I>(pub I);

impl<I, T> fmt::Display for TsMsVecFmt<I>
where
    I: Clone + IntoIterator<Item = T>,
    T: AsRef<TsMs>,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "[")?;
        for ts in self.0.clone().into_iter() {
            write!(fmt, "  {}", ts.as_ref().fmt())?;
        }
        write!(fmt, "  ]")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct PulseId(u64);

impl PulseId {
    pub fn from_id(id: u64) -> Self {
        Self(id)
    }
}

impl Dim0Index for TsNano {
    fn add(&self, v: &Self) -> Self {
        Self(self.0 + v.0)
    }

    fn sub(&self, v: &Self) -> Self {
        Self(self.0 - v.0)
    }

    fn sub_n(&self, v: u64) -> Self {
        Self(self.0 - v)
    }

    fn times(&self, x: u64) -> Self {
        Self(self.0 * x)
    }

    fn div_n(&self, n: u64) -> Self {
        Self(self.0 / n)
    }

    fn div_v(&self, v: &Self) -> u64 {
        self.0 / v.0
    }

    fn as_u64(&self) -> u64 {
        self.0
    }

    fn series_range(a: Self, b: Self) -> SeriesRange {
        SeriesRange::TimeRange(NanoRange { beg: a.0, end: b.0 })
    }

    fn prebin_bin_len_opts() -> Vec<Self> {
        PREBIN_TIME_BIN_LEN_VAR0.iter().map(|&x| Self(x)).collect()
    }

    fn prebin_patch_len_for(i: usize) -> Self {
        let _ = i;
        todo!()
    }

    fn to_pre_binned_patch_range_enum(
        &self,
        bin_count: u64,
        patch_offset: u64,
        patch_count: u64,
    ) -> PreBinnedPatchRangeEnum {
        PreBinnedPatchRangeEnum::Time(PreBinnedPatchRange {
            first: PreBinnedPatchCoord {
                bin_len: self.clone(),
                bin_count,
                patch_offset,
            },
            patch_count,
        })
    }

    fn binned_bin_len_opts() -> Vec<Self> {
        TIME_BIN_THRESHOLDS.iter().map(|&x| Self(x)).collect()
    }

    fn to_binned_range_enum(&self, bin_off: u64, bin_cnt: u64) -> BinnedRangeEnum {
        BinnedRangeEnum::Time(BinnedRange {
            bin_len: self.clone(),
            bin_off,
            bin_cnt,
        })
    }
}

impl Dim0Index for PulseId {
    fn add(&self, v: &Self) -> Self {
        Self(self.0 + v.0)
    }

    fn sub(&self, v: &Self) -> Self {
        Self(self.0 - v.0)
    }

    fn sub_n(&self, v: u64) -> Self {
        Self(self.0 - v)
    }

    fn times(&self, x: u64) -> Self {
        Self(self.0 * x)
    }

    fn div_n(&self, n: u64) -> Self {
        Self(self.0 / n)
    }

    fn div_v(&self, v: &Self) -> u64 {
        self.0 / v.0
    }

    fn as_u64(&self) -> u64 {
        self.0
    }

    fn series_range(a: Self, b: Self) -> SeriesRange {
        SeriesRange::PulseRange(PulseRange { beg: a.0, end: b.0 })
    }

    fn prebin_bin_len_opts() -> Vec<Self> {
        PREBIN_PULSE_BIN_LEN_VAR0.iter().map(|&x| Self(x)).collect()
    }

    fn prebin_patch_len_for(i: usize) -> Self {
        let _ = i;
        todo!()
    }

    fn to_pre_binned_patch_range_enum(
        &self,
        bin_count: u64,
        patch_offset: u64,
        patch_count: u64,
    ) -> PreBinnedPatchRangeEnum {
        PreBinnedPatchRangeEnum::Pulse(PreBinnedPatchRange {
            first: PreBinnedPatchCoord {
                bin_len: self.clone(),
                bin_count,
                patch_offset,
            },
            patch_count,
        })
    }

    fn binned_bin_len_opts() -> Vec<Self> {
        PULSE_BIN_THRESHOLDS.iter().map(|&x| Self(x)).collect()
    }

    fn to_binned_range_enum(&self, bin_off: u64, bin_cnt: u64) -> BinnedRangeEnum {
        BinnedRangeEnum::Pulse(BinnedRange {
            bin_len: self.clone(),
            bin_off,
            bin_cnt,
        })
    }
}

const PREBIN_TIME_BIN_LEN_VAR0: [u64; 3] = [MIN * 1, HOUR * 1, DAY];

const PREBIN_PULSE_BIN_LEN_VAR0: [u64; 4] = [100, 10000, 1000000, 100000000];

#[allow(unused)]
const PATCH_T_LEN_OPTIONS_SCALAR: [u64; 3] = [
    //
    //MIN * 60,
    HOUR * 6,
    DAY * 16,
    DAY * 64,
];

#[allow(unused)]
const PATCH_T_LEN_OPTIONS_WAVE: [u64; 3] = [
    //
    //MIN * 10,
    HOUR * 6,
    DAY * 8,
    DAY * 32,
];

const TIME_BIN_THRESHOLDS: [u64; 26] = [
    MS * 20,
    MS * 50,
    MS * 100,
    MS * 200,
    MS * 500,
    SEC,
    SEC * 2,
    SEC * 5,
    SEC * 10,
    SEC * 20,
    MIN,
    MIN * 2,
    MIN * 5,
    MIN * 10,
    MIN * 20,
    HOUR,
    HOUR * 2,
    HOUR * 4,
    HOUR * 12,
    DAY,
    DAY * 2,
    DAY * 4,
    DAY * 8,
    DAY * 16,
    DAY * 32,
    DAY * 64,
];

const TIME_BIN_LEN_CACHE_OPTS: [DtMs; 0] = [
    //
    // DtMs(1000 * 10),
    // DtMs(1000 * 60 * 60),
];

pub fn time_bin_len_cache_opts() -> &'static [DtMs] {
    &TIME_BIN_LEN_CACHE_OPTS
}

const PULSE_BIN_THRESHOLDS: [u64; 25] = [
    10, 20, 40, 80, 100, 200, 400, 800, 1000, 2000, 4000, 8000, 10000, 20000, 40000, 80000, 100000,
    200000, 400000, 800000, 1000000, 2000000, 4000000, 8000000, 10000000,
];

#[allow(unused)]
const fn time_bin_threshold_at(i: usize) -> TsNano {
    TsNano(TIME_BIN_THRESHOLDS[i])
}

#[allow(unused)]
const fn pulse_bin_threshold_at(i: usize) -> PulseId {
    PulseId(PULSE_BIN_THRESHOLDS[i])
}

/// Identifies one patch on the binning grid at a certain resolution.
/// A patch consists of `bin_count` consecutive bins.
/// In total, a given `PreBinnedPatchCoord` spans a time range from `patch_beg` to `patch_end`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreBinnedPatchCoord<T>
where
    T: Dim0Index,
{
    bin_len: T,
    bin_count: u64,
    patch_offset: u64,
}

impl<T> PreBinnedPatchCoord<T>
where
    T: Dim0Index,
{
    pub fn new(bin_len: T, bin_count: u64, patch_offset: u64) -> Self {
        Self {
            bin_len,
            bin_count,
            patch_offset,
        }
    }
    pub fn bin_len(&self) -> T {
        self.bin_len.clone()
    }

    pub fn patch_len(&self) -> T {
        self.bin_len().times(self.bin_count)
    }

    pub fn patch_beg(&self) -> T {
        self.bin_len()
            .times(self.bin_count)
            .times(self.patch_offset)
    }

    pub fn patch_end(&self) -> T {
        self.bin_len()
            .times(self.bin_count)
            .times(1 + self.patch_offset)
    }

    pub fn series_range(&self) -> SeriesRange {
        T::series_range(self.patch_beg(), self.patch_end())
    }

    pub fn bin_count(&self) -> u64 {
        self.bin_count
    }

    pub fn patch_offset(&self) -> u64 {
        self.patch_offset
    }

    pub fn edges(&self) -> Vec<T> {
        let mut ret = Vec::new();
        let mut t = self.patch_beg();
        ret.push(t.clone());
        for _ in 0..self.bin_count() {
            t = t.add(&self.bin_len);
            ret.push(t.clone());
        }
        ret
    }

    pub fn next(&self) -> Self {
        Self::new(self.bin_len.clone(), self.bin_count, 1 + self.patch_offset)
    }
}

impl<T> AppendToUrl for PreBinnedPatchCoord<T>
where
    T: Dim0Index,
{
    fn append_to_url(&self, url: &mut Url) {
        error!("TODO AppendToUrl for PreBinnedPatchCoord");
        // TODO must also emit the type of the series index
        let mut g = url.query_pairs_mut();
        g.append_pair("patchTlen", &format!("{}", 4242));
        todo!();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreBinnedPatchCoordEnum {
    Time(PreBinnedPatchCoord<TsNano>),
    Pulse(PreBinnedPatchCoord<PulseId>),
}

impl PreBinnedPatchCoordEnum {
    pub fn bin_count(&self) -> u64 {
        todo!()
    }

    pub fn patch_range(&self) -> SeriesRange {
        match self {
            PreBinnedPatchCoordEnum::Time(k) => k.series_range(),
            PreBinnedPatchCoordEnum::Pulse(k) => k.series_range(),
        }
    }

    pub fn span_desc(&self) -> String {
        match self {
            PreBinnedPatchCoordEnum::Time(k) => {
                format!(
                    "pre-W-{}-B-{}",
                    k.bin_len.0 * k.bin_count / SEC,
                    k.patch_offset / SEC
                )
            }
            PreBinnedPatchCoordEnum::Pulse(k) => {
                format!(
                    "pre-W-{}-B-{}",
                    k.bin_len.0 * k.bin_count / SEC,
                    k.patch_offset / SEC
                )
            }
        }
    }
}

impl FromUrl for PreBinnedPatchCoordEnum {
    type Error = Error;

    fn from_url(_url: &Url) -> Result<Self, Self::Error> {
        todo!()
    }

    fn from_pairs(_pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl AppendToUrl for PreBinnedPatchCoordEnum {
    fn append_to_url(&self, _url: &mut Url) {
        todo!()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreBinnedPatchRange<T>
where
    T: Dim0Index,
{
    first: PreBinnedPatchCoord<T>,
    patch_count: u64,
}

impl<T> PreBinnedPatchRange<T>
where
    T: Dim0Index,
{
    pub fn edges(&self) -> Vec<u64> {
        // let ret = Vec::new();
        // ret
        panic!()
    }

    pub fn series_range(&self) -> SeriesRange {
        // T::series_range(todo!(), todo!())
        panic!()
    }

    pub fn patch_count(&self) -> u64 {
        self.patch_count
    }

    pub fn bin_count(&self) -> u64 {
        panic!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreBinnedPatchRangeEnum {
    Time(PreBinnedPatchRange<TsNano>),
    Pulse(PreBinnedPatchRange<PulseId>),
}

impl PreBinnedPatchRangeEnum {
    fn covering_range_ty<T>(a: T, b: T, min_bin_count: u32) -> Result<Self, Error>
    where
        T: Dim0Index + 'static,
    {
        let opts = T::prebin_bin_len_opts();
        if min_bin_count < 1 {
            Err(Error::BinCountTooSmall)?;
        }
        if min_bin_count > 20000 {
            Err(Error::BinCountTooLarge)?;
        }
        let du = b.sub(&a);
        let max_bin_len = du.div_n(min_bin_count as u64);
        for (i1, bl) in opts.iter().enumerate().rev() {
            if bl <= &max_bin_len {
                let patch_len = <T as Dim0Index>::prebin_patch_len_for(i1);
                let bin_count = patch_len.div_v(bl);
                let patch_off_1 = a.div_v(&patch_len);
                let patch_off_2 = (b.add(&patch_len).sub_n(1)).div_v(&patch_len);
                let patch_count = patch_off_2 - patch_off_1;
                let ret =
                    T::to_pre_binned_patch_range_enum(&bl, bin_count, patch_off_1, patch_count);
                return Ok(ret);
            }
        }
        Err(Error::BinnedNoGridMatch)
    }

    /// Cover at least the given range with at least as many as the requested number of bins.
    pub fn covering_range(range: SeriesRange, min_bin_count: u32) -> Result<Self, Error> {
        match range {
            SeriesRange::TimeRange(k) => {
                Self::covering_range_ty(TsNano(k.beg), TsNano(k.end), min_bin_count)
            }
            SeriesRange::PulseRange(k) => {
                Self::covering_range_ty(PulseId(k.beg), PulseId(k.end), min_bin_count)
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BinnedRange<T>
where
    T: Dim0Index,
{
    // TODO remove pub, which is currently used in tests
    pub bin_len: T,
    pub bin_off: u64,
    pub bin_cnt: u64,
}

impl fmt::Debug for BinnedRange<TsNano> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let beg = self.bin_len.times(self.bin_off);
        let end = self.bin_len.times(self.bin_off + self.bin_cnt);
        write!(
            fmt,
            "BinnedRange {{ {}, {}, {} }}",
            beg,
            end,
            self.bin_len.to_dt_ms()
        )
    }
}

impl fmt::Debug for BinnedRange<PulseId> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "BinnedRange<PulseId> {{ .. }}")
    }
}

impl fmt::Display for BinnedRange<TsNano> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl BinnedRange<TsNano> {
    pub fn to_nano_range(&self) -> NanoRange {
        let beg = self.bin_len.times(self.bin_off).as_u64();
        let end = self.bin_len.times(self.bin_off + self.bin_cnt).as_u64();
        NanoRange { beg, end }
    }

    pub fn from_nano_range(range: NanoRange, bin_len: DtMs) -> Self {
        let off1 = range.beg() / bin_len.ns();
        let off2 = (bin_len.ns() - 1 + range.end()) / bin_len.ns();
        Self {
            bin_len: TsNano::from_ns(bin_len.ns()),
            bin_off: off1,
            bin_cnt: off2 - off1,
        }
    }

    pub fn covering_range_time(range: NanoRange, bin_len_req: DtMs) -> Result<Self, Error> {
        let opts = <TsNano as Dim0Index>::binned_bin_len_opts();
        let bin_len_req = if bin_len_req.ms() < opts[0].ms() {
            DtMs::from_ms_u64(opts[0].ms())
        } else {
            bin_len_req
        };
        let bin_len_req = if bin_len_req.ms() > opts.last().unwrap().ms() {
            DtMs::from_ms_u64(opts.last().unwrap().ms())
        } else {
            bin_len_req
        };
        let pv = TsNano::from_ns(bin_len_req.ns());
        let pi = opts.partition_point(|&x| x < pv);
        let bin_len = if pi == 0 {
            DtMs::from_ms_u64(opts[0].ms())
        } else {
            let v1 = DtMs::from_ms_u64(opts[pi - 1].ms());
            if let Some(&v2) = opts.get(pi) {
                let v2 = DtMs::from_ms_u64(v2.ms());
                if v1 >= bin_len_req || v2 < bin_len_req {
                    panic!("logic covering_range_time");
                } else {
                    let f1 = (bin_len_req.ms() - v1.ms()) / bin_len_req.ms();
                    let f2 = (v2.ms() - bin_len_req.ms()) / bin_len_req.ms();
                    if f1 < f2 {
                        v1
                    } else {
                        v2
                    }
                }
            } else {
                DtMs::from_ms_u64(v1.ms())
            }
        };
        let bin_off = range.beg() / bin_len.ns();
        let off2 = (range.end() + bin_len.ns() - 1) / bin_len.ns();
        let bin_cnt = off2 - bin_off;
        let ret = Self {
            bin_len: TsNano::from_ns(bin_len.ns()),
            bin_off,
            bin_cnt,
        };
        Ok(ret)
    }

    pub fn nano_beg(&self) -> TsNano {
        self.bin_len.times(self.bin_off)
    }

    pub fn nano_end(&self) -> TsNano {
        self.bin_len.times(self.bin_off + self.bin_cnt)
    }

    pub fn one_before_bin(&self) -> Self {
        Self {
            bin_len: self.bin_len,
            bin_off: self.bin_off - 1,
            bin_cnt: self.bin_cnt + 1,
        }
    }

    pub fn bin_len_dt_ms(&self) -> DtMs {
        self.bin_len.to_dt_ms()
    }

    pub fn bin_len_dt_ns(&self) -> DtNano {
        self.bin_len.to_dt_nano()
    }
}

impl<T> BinnedRange<T>
where
    T: Dim0Index,
{
    pub fn bin_count(&self) -> u64 {
        self.bin_cnt
    }

    pub fn full_range(&self) -> NanoRange {
        let beg = self.bin_len.times(self.bin_off).as_u64();
        let end = self.bin_len.times(self.bin_off + self.bin_cnt).as_u64();
        NanoRange { beg, end }
    }

    pub fn edges_u64(&self) -> Vec<u64> {
        let mut ret = Vec::new();
        let mut t = self.bin_len.times(self.bin_off);
        let end = self.bin_len.times(self.bin_off + self.bin_cnt);
        while t <= end {
            ret.push(t.as_u64());
            t = t.add(&self.bin_len);
        }
        ret
    }

    pub fn edges(&self) -> Vec<T> {
        let mut ret = Vec::new();
        let mut t = self.bin_len.times(self.bin_off);
        let end = self.bin_len.times(self.bin_off + self.bin_cnt);
        while t <= end {
            ret.push(t.clone());
            t = t.add(&self.bin_len);
        }
        ret
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinnedRangeEnum {
    Time(BinnedRange<TsNano>),
    Pulse(BinnedRange<PulseId>),
}

impl BinnedRangeEnum {
    fn covering_range_ty<T>(a: T, b: T, min_bin_count: u32) -> Result<Self, Error>
    where
        T: Dim0Index + 'static,
    {
        let opts = T::binned_bin_len_opts();
        if min_bin_count < 1 {
            Err(Error::BinCountTooSmall)?;
        }
        let bin_count_max = i32::MAX as u32;
        if min_bin_count > bin_count_max {
            Err(Error::BinCountTooLarge)?;
        }
        let du = b.sub(&a);
        let max_bin_len = du.div_n(min_bin_count as u64);
        for (_, bl) in opts.iter().enumerate().rev() {
            if bl <= &max_bin_len {
                let off_1 = a.div_v(&bl);
                let off_2 = (b.add(&bl).sub_n(1)).div_v(&bl);
                eprintln!("off_1 {off_1:?}  off_2 {off_2:?}");
                let bin_cnt = off_2 - off_1;
                let ret = T::to_binned_range_enum(bl, off_1, bin_cnt);
                return Ok(ret);
            }
        }
        Err(Error::BinnedNoGridMatch)
    }

    /// Cover at least the given range while selecting the bin width which best fits the requested bin width.
    pub fn covering_range_time(range: SeriesRange, bin_len_req: DtMs) -> Result<Self, Error> {
        match range {
            SeriesRange::TimeRange(k) => Ok(Self::Time(BinnedRange::covering_range_time(
                k,
                bin_len_req,
            )?)),
            SeriesRange::PulseRange(_) => Err(Error::TimelikeBinWidthImpossibleForPulseRange),
        }
    }

    /// Cover at least the given range with at least as many as the requested number of bins.
    pub fn covering_range(range: SeriesRange, min_bin_count: u32) -> Result<Self, Error> {
        match range {
            SeriesRange::TimeRange(k) => {
                Self::covering_range_ty(TsNano(k.beg), TsNano(k.end), min_bin_count)
            }
            SeriesRange::PulseRange(k) => {
                Self::covering_range_ty(PulseId(k.beg), PulseId(k.end), min_bin_count)
            }
        }
    }

    pub fn bin_count(&self) -> u64 {
        match self {
            BinnedRangeEnum::Time(k) => k.bin_count(),
            BinnedRangeEnum::Pulse(k) => k.bin_count(),
        }
    }

    pub fn range_at(&self, i: usize) -> Option<SeriesRange> {
        match self {
            BinnedRangeEnum::Time(k) => {
                if (i as u64) < k.bin_cnt {
                    let beg = k.bin_len.0 * (k.bin_off + i as u64);
                    let x = SeriesRange::TimeRange(NanoRange {
                        beg,
                        end: beg + k.bin_len.0,
                    });
                    Some(x)
                } else {
                    None
                }
            }
            BinnedRangeEnum::Pulse(k) => {
                if (i as u64) < k.bin_cnt {
                    let beg = k.bin_len.0 * (k.bin_off + i as u64);
                    let x = SeriesRange::PulseRange(PulseRange {
                        beg,
                        end: beg + k.bin_len.0,
                    });
                    Some(x)
                } else {
                    None
                }
            }
        }
    }

    pub fn dim0kind(&self) -> Dim0Kind {
        match self {
            BinnedRangeEnum::Time(_) => Dim0Kind::Time,
            BinnedRangeEnum::Pulse(_) => Dim0Kind::Pulse,
        }
    }

    pub fn binned_range_time(&self) -> BinnedRange<TsNano> {
        match self {
            BinnedRangeEnum::Time(x) => x.clone(),
            BinnedRangeEnum::Pulse(_) => panic!(),
        }
    }

    // Only a helper for unit tests.
    pub fn from_custom(len: TsNano, off: u64, cnt: u64) -> BinnedRangeEnum {
        let rng = BinnedRange {
            bin_len: len,
            bin_off: off,
            bin_cnt: cnt,
        };
        BinnedRangeEnum::Time(rng)
    }
}

#[cfg(test)]
mod test_binned_range {
    use super::*;

    #[test]
    fn binned_range_00() {
        let range = NanoRange {
            beg: HOUR * 72,
            end: HOUR * 73,
        };
        let range = BinnedRangeEnum::covering_range(range.into(), 10).unwrap();
        assert_eq!(range.bin_count(), 12);
        match range {
            BinnedRangeEnum::Time(range) => {
                assert_eq!(range.edges_u64()[0], HOUR * 72);
                assert_eq!(range.edges_u64()[2], HOUR * 72 + MIN * 5 * 2);
            }
            BinnedRangeEnum::Pulse(_) => panic!(),
        }
    }

    #[test]
    fn binned_range_01() {
        let range = NanoRange {
            beg: MIN * 20 + SEC * 10,
            end: HOUR * 10 + MIN * 20 + SEC * 30,
        };
        let range = BinnedRangeEnum::covering_range(range.into(), 10).unwrap();
        assert_eq!(range.bin_count(), 11);
        match range {
            BinnedRangeEnum::Time(range) => {
                assert_eq!(range.edges_u64()[0], HOUR * 0);
                assert_eq!(range.edges_u64()[1], HOUR * 1);
                assert_eq!(range.edges_u64()[11], HOUR * 11);
            }
            BinnedRangeEnum::Pulse(_) => panic!(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum AggKind {
    EventBlobs,
    DimXBins1,
    DimXBinsN(u32),
    Plain,
    TimeWeightedScalar,
    PulseIdDiff,
}

impl AggKind {
    pub fn do_time_weighted(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
            Self::PulseIdDiff => false,
        }
    }

    pub fn need_expand(&self) -> bool {
        match self {
            Self::EventBlobs => false,
            Self::TimeWeightedScalar => true,
            Self::DimXBins1 => false,
            Self::DimXBinsN(_) => false,
            Self::Plain => false,
            Self::PulseIdDiff => false,
        }
    }
}

pub fn x_bin_count(shape: &Shape, agg_kind: &AggKind) -> usize {
    match agg_kind {
        AggKind::EventBlobs => 0,
        AggKind::TimeWeightedScalar => 0,
        AggKind::DimXBins1 => 0,
        AggKind::DimXBinsN(n) => {
            if *n == 0 {
                match shape {
                    Shape::Scalar => 0,
                    Shape::Wave(n) => *n as usize,
                    Shape::Image(j, k) => *j as usize * *k as usize,
                }
            } else {
                *n as usize
            }
        }
        AggKind::Plain => match shape {
            Shape::Scalar => 0,
            Shape::Wave(n) => *n as usize,
            Shape::Image(j, k) => *j as usize * *k as usize,
        },
        AggKind::PulseIdDiff => 0,
    }
}

impl fmt::Display for AggKind {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::EventBlobs => {
                write!(fmt, "EventBlobs")
            }
            Self::DimXBins1 => {
                write!(fmt, "DimXBins1")
            }
            Self::DimXBinsN(n) => {
                write!(fmt, "DimXBinsN{}", n)
            }
            Self::Plain => {
                write!(fmt, "Plain")
            }
            Self::TimeWeightedScalar => {
                write!(fmt, "TimeWeightedScalar")
            }
            Self::PulseIdDiff => {
                write!(fmt, "PulseIdDiff")
            }
        }
    }
}

impl fmt::Debug for AggKind {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, fmt)
    }
}

impl FromStr for AggKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let nmark = "DimXBinsN";
        if s == "EventBlobs" {
            Ok(AggKind::EventBlobs)
        } else if s == "DimXBins1" {
            Ok(AggKind::DimXBins1)
        } else if s == "TimeWeightedScalar" {
            Ok(AggKind::TimeWeightedScalar)
        } else if s.starts_with(nmark) {
            let nbins: u32 = s[nmark.len()..].parse()?;
            Ok(AggKind::DimXBinsN(nbins))
        } else if s == "PulseIdDiff" {
            Ok(AggKind::PulseIdDiff)
        } else {
            Err(Error::AggKind)
        }
    }
}

pub trait ToNanos {
    fn to_nanos(&self) -> u64;
}

impl<Tz: TimeZone> ToNanos for DateTime<Tz> {
    fn to_nanos(&self) -> u64 {
        self.timestamp() as u64 * timeunits::SEC + self.timestamp_subsec_nanos() as u64
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct TsMs(pub u64);

impl TsMs {
    pub const fn from_ms_u64(x: u64) -> Self {
        Self(x)
    }

    pub const fn from_ns_u64(x: u64) -> Self {
        Self(x / 1000000)
    }

    pub fn from_system_time(st: SystemTime) -> Self {
        let tsunix = st.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        let x = tsunix.as_secs() * 1000 + tsunix.subsec_millis() as u64;
        Self::from_ms_u64(x)
    }

    pub const fn ms(self) -> u64 {
        self.0
    }

    pub const fn ns(self) -> TsNano {
        TsNano::from_ms(self.0)
    }

    pub const fn ns_u64(self) -> u64 {
        1000000 * self.0
    }

    pub const fn sec(self) -> u64 {
        self.0 / 1000
    }

    pub const fn to_u64(self) -> u64 {
        self.0
    }

    pub const fn to_i64(self) -> i64 {
        self.0 as i64
    }

    pub const fn to_grid_02(self, grid: DtMs) -> (Self, DtMs) {
        let msp = TsMs(self.0 / grid.0 * grid.0);
        let lsp = DtMs(self.0 - msp.0);
        (msp, lsp)
    }

    pub fn bump_epsilon(&self) -> TsMs {
        Self(self.0 + 1)
    }

    pub fn fmt(&self) -> TsMsFmt {
        TsMsFmt { ts: self.clone() }
    }
}

impl fmt::Display for TsMs {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "TsMs {{ {} }}", self.0)
    }
}

impl core::ops::Sub for TsMs {
    type Output = DtMs;

    fn sub(self, rhs: Self) -> Self::Output {
        DtMs(self.0.saturating_sub(rhs.0))
    }
}

pub trait RetStreamExt: Stream {
    fn only_first_error(self) -> OnlyFirstError<Self>
    where
        Self: Sized;
}

pub struct OnlyFirstError<T> {
    inp: T,
    errored: bool,
    complete: bool,
}

impl<T> OnlyFirstError<T> {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl<T, I, E> Stream for OnlyFirstError<T>
where
    T: Stream<Item = Result<I, E>> + Unpin,
{
    type Item = <T as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if self.complete {
            panic!("{} poll_next on complete", Self::type_name())
        }
        if self.errored {
            self.complete = true;
            return Ready(None);
        }
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
            Ready(Some(Err(e))) => {
                self.errored = true;
                Ready(Some(Err(e)))
            }
            Ready(None) => {
                self.complete = true;
                Ready(None)
            }
            Pending => Pending,
        }
    }
}

impl<T> RetStreamExt for T
where
    T: Stream,
{
    fn only_first_error(self) -> OnlyFirstError<Self> {
        OnlyFirstError {
            inp: self,
            errored: false,
            complete: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventDataReadStats {
    pub parsed_bytes: u64,
}

impl EventDataReadStats {
    pub fn new() -> Self {
        Self { parsed_bytes: 0 }
    }
    pub fn trans(&mut self, k: &mut Self) {
        self.parsed_bytes += k.parsed_bytes;
        k.parsed_bytes = 0;
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeFilterStats {
    pub items_no_prune_high: u64,
    pub items_all_prune_high: u64,
    pub items_part_prune_high: u64,
}

impl RangeFilterStats {
    pub fn new() -> Self {
        Self {
            items_no_prune_high: 0,
            items_all_prune_high: 0,
            items_part_prune_high: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DiskStats {
    OpenStats(OpenStats),
    SeekStats(SeekStats),
    ReadStats(ReadStats),
    ReadExactStats(ReadExactStats),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenStats {
    pub duration: Duration,
}

impl OpenStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeekStats {
    pub duration: Duration,
}

impl SeekStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadStats {
    pub duration: Duration,
}

impl ReadStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadExactStats {
    pub duration: Duration,
}

impl ReadExactStats {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Api1WarningStats {
    pub subreq_fail: usize,
}

impl Api1WarningStats {
    pub fn new() -> Self {
        Self { subreq_fail: 0 }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ByteSize(pub u32);

impl ByteSize {
    pub fn from_bytes(b: u32) -> Self {
        Self(b)
    }

    pub fn from_kb(kb: u32) -> Self {
        Self(1024 * kb)
    }

    pub fn from_mb(mb: u32) -> Self {
        Self(1024 * 1024 * mb)
    }

    pub fn bytes(&self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileIoBufferSize(pub usize);

impl FileIoBufferSize {
    pub fn new(k: usize) -> Self {
        Self(k)
    }
    pub fn bytes(&self) -> usize {
        self.0
    }
}

impl Default for FileIoBufferSize {
    fn default() -> Self {
        Self(1024 * 4)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReadSys {
    TokioAsyncRead,
    Read2,
    Read3,
    Read4,
    Read5,
    BlockingTaskIntoChannel,
}

impl ReadSys {
    pub fn default() -> Self {
        Self::BlockingTaskIntoChannel
    }
}

impl From<&str> for ReadSys {
    fn from(k: &str) -> Self {
        if k == "TokioAsyncRead" {
            Self::TokioAsyncRead
        } else if k == "Read2" {
            Self::Read2
        } else if k == "Read3" {
            Self::Read3
        } else if k == "Read4" {
            Self::Read4
        } else if k == "Read5" {
            Self::Read5
        } else if k == "BlockingTaskIntoChannel" {
            Self::BlockingTaskIntoChannel
        } else {
            Self::default()
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiskIoTune {
    pub read_sys: ReadSys,
    pub read_buffer_len: usize,
    pub read_queue_len: usize,
}

impl DiskIoTune {
    pub fn default_for_testing() -> Self {
        Self {
            read_sys: ReadSys::default(),
            read_buffer_len: 1024 * 8,
            read_queue_len: 4,
        }
    }

    pub fn default() -> Self {
        Self {
            read_sys: ReadSys::default(),
            read_buffer_len: 1024 * 16,
            read_queue_len: 4,
        }
    }

    pub fn with_read_buffer_len(mut self, x: usize) -> Self {
        self.read_buffer_len = x;
        self
    }
}

impl Default for DiskIoTune {
    fn default() -> Self {
        Self::default()
    }
}

impl FromUrl for DiskIoTune {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        Self::from_pairs(&get_url_query_pairs(url))
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let read_sys = pairs
            .get("ReadSys")
            .map(|x| x.as_str().into())
            .unwrap_or_else(|| ReadSys::default());
        let read_buffer_len = pairs
            .get("ReadBufferLen")
            .map(|x| x.parse().map_or(None, Some))
            .unwrap_or(None)
            .unwrap_or(1024 * 4);
        let read_queue_len = pairs
            .get("ReadQueueLen")
            .map(|x| x.parse().map_or(None, Some))
            .unwrap_or(None)
            .unwrap_or(8);
        let ret = DiskIoTune {
            read_sys,
            read_buffer_len,
            read_queue_len,
        };
        Ok(ret)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchQuery {
    pub backend: Option<String>,
    pub name_regex: String,
    pub source_regex: String,
    pub description_regex: String,
    #[serde(default)]
    pub icase: bool,
    #[serde(default)]
    pub kind: SeriesKind,
}

impl ChannelSearchQuery {
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let pairs = get_url_query_pairs(url);
        let ret = Self {
            backend: pairs.get("backend").map(Into::into),
            name_regex: pairs.get("nameRegex").map_or(String::new(), |k| k.clone()),
            source_regex: pairs
                .get("sourceRegex")
                .map_or(String::new(), |k| k.clone()),
            description_regex: pairs
                .get("descriptionRegex")
                .map_or(String::new(), |k| k.clone()),
            icase: pairs
                .get("icase")
                .map_or(None, |x| x.parse().ok())
                .unwrap_or(false),
            kind: SeriesKind::from_pairs(&pairs)?,
        };
        Ok(ret)
    }

    pub fn append_to_url(&self, url: &mut Url) {
        let mut qp = url.query_pairs_mut();
        if let Some(v) = &self.backend {
            qp.append_pair("backend", v);
        }
        qp.append_pair("nameRegex", &self.name_regex);
        qp.append_pair("sourceRegex", &self.source_regex);
        qp.append_pair("descriptionRegex", &self.description_regex);
        qp.append_pair("icase", &self.icase.to_string());
        drop(qp);
        self.kind.append_to_url(url);
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_url_1() {
        let mut url = url::Url::parse("http://host/123").unwrap();
        url.query_pairs_mut().append_pair("text", "jo jo • yo");
        assert_eq!(url.to_string(), "http://host/123?text=jo+jo+%E2%80%A2+yo");
    }

    #[test]
    fn parse_url_2() {
        let url = url::Url::parse("dummy:?123").unwrap();
        assert_eq!(url.query().unwrap(), "123")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchSingleResult {
    pub backend: String,
    pub name: String,
    #[serde(rename = "seriesId")]
    pub series: u64,
    pub source: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub shape: Vec<u32>,
    pub unit: String,
    pub description: String,
    #[serde(rename = "isApi0", skip_serializing_if = "Option::is_none")]
    pub is_api_0: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelSearchResult {
    pub channels: Vec<ChannelSearchSingleResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyBackend {
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusSub {
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyConfig {
    pub name: String,
    pub listen: String,
    pub port: u16,
    pub backends: Vec<ProxyBackend>,
    pub status_subs: Vec<StatusSub>,
    pub announce_backends: Option<Vec<String>>,
}

pub trait HasBackend {
    fn backend(&self) -> &str;
}

// TODO change into Option, why do I need to set a timeout using this trait?
pub trait HasTimeout {
    fn timeout(&self) -> Option<Duration>;
}

pub trait FromUrl: Sized {
    type Error;
    fn from_url(url: &Url) -> Result<Self, Self::Error>;
    // TODO put this in separate trait, because some implementors need url path segments to construct.
    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error>;
}

pub trait AppendToUrl {
    fn append_to_url(&self, url: &mut Url);
}

pub type MapQuery = BTreeMap<String, String>;

impl AppendToUrl for MapQuery {
    fn append_to_url(&self, url: &mut Url) {
        let mut g = url.query_pairs_mut();
        for (k, v) in self {
            g.append_pair(k, v);
        }
    }
}

impl FromUrl for MapQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        Ok(pairs.clone())
    }
}

impl HasBackend for MapQuery {
    fn backend(&self) -> &str {
        self.get("backend").map_or("NOBACKEND", AsRef::as_ref)
    }
}

impl HasTimeout for MapQuery {
    fn timeout(&self) -> Option<Duration> {
        let x: Option<u32> = if let Some(v) = self.get("timeout") {
            v.parse::<u32>().ok()
        } else {
            None
        };
        x.map(|x| Duration::from_millis(x as _))
    }
}

pub fn get_url_query_pairs(url: &Url) -> BTreeMap<String, String> {
    BTreeMap::from_iter(
        url.query_pairs()
            .map(|(j, k)| (j.to_string(), k.to_string())),
    )
}

// Request type of the channel/config api.
// At least on some backends the channel configuration may change depending on the queried range.
// Therefore, the query includes the range.
// The presence of a configuration in some range does not imply that there is any data available.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfigQuery {
    pub channel: SfDbChannel,
    pub range: NanoRange,
    pub expand: bool,
}

impl HasBackend for ChannelConfigQuery {
    fn backend(&self) -> &str {
        &self.channel.backend
    }
}

impl HasTimeout for ChannelConfigQuery {
    fn timeout(&self) -> Option<Duration> {
        None
    }
}

impl FromUrl for ChannelConfigQuery {
    type Error = Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let beg_date = pairs
            .get("begDate")
            .map(String::from)
            .unwrap_or_else(|| String::from("1970-01-01T00:00:00Z"));
        let end_date = pairs
            .get("endDate")
            .map(String::from)
            .unwrap_or_else(|| String::from("3000-01-01T00:00:00Z"));
        let expand = pairs.get("expand").map(|s| s == "true").unwrap_or(false);
        let ret = Self {
            channel: SfDbChannel::from_pairs(&pairs)?,
            range: NanoRange {
                beg: beg_date.parse::<DateTime<Utc>>()?.to_nanos(),
                end: end_date.parse::<DateTime<Utc>>()?.to_nanos(),
            },
            expand,
        };
        Ok(ret)
    }
}

impl AppendToUrl for ChannelConfigQuery {
    fn append_to_url(&self, url: &mut Url) {
        self.channel.append_to_url(url);
        let mut g = url.query_pairs_mut();
        g.append_pair(
            "begDate",
            &Utc.timestamp_nanos(self.range.beg as i64)
                .format(DATETIME_FMT_3MS)
                .to_string(),
        );
        g.append_pair(
            "endDate",
            &Utc.timestamp_nanos(self.range.end as i64)
                .format(DATETIME_FMT_3MS)
                .to_string(),
        );
        if self.expand {
            g.append_pair("expand", "true");
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "SfDatabuffer")]
pub struct SfChannelConfigResponse {
    #[serde(rename = "backend")]
    pub backend: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "keyspace")]
    pub keyspace: u8,
    #[serde(rename = "timeBinSize")]
    pub timebinsize: u64,
    #[serde(rename = "scalarType")]
    pub scalar_type: ScalarType,
    #[serde(rename = "shape")]
    pub shape: Shape,
    #[serde(rename = "byteOrder")]
    pub byte_order: ByteOrder,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "Daqbuf")]
pub struct DaqbufChannelConfig {
    #[serde(rename = "backend")]
    pub backend: String,
    #[serde(rename = "seriesId")]
    pub series: u64,
    #[serde(rename = "seriesKind")]
    pub kind: SeriesKind,
    #[serde(rename = "scalarType")]
    pub scalar_type: ScalarType,
    #[serde(rename = "shape")]
    pub shape: Shape,
    #[serde(rename = "name")]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ChannelConfigResponse {
    SfDatabuffer(SfChannelConfigResponse),
    Daqbuf(DaqbufChannelConfig),
}

impl From<SfChFetchInfo> for ChannelConfigResponse {
    fn from(value: SfChFetchInfo) -> Self {
        Self::SfDatabuffer(SfChannelConfigResponse {
            backend: value.backend().into(),
            name: value.name().into(),
            keyspace: value.ks(),
            timebinsize: value.bs().ms_u64(),
            scalar_type: value.scalar_type().clone(),
            shape: value.shape().clone(),
            byte_order: value.byte_order().clone(),
        })
    }
}

impl From<ChConf> for ChannelConfigResponse {
    fn from(value: ChConf) -> Self {
        Self::Daqbuf(DaqbufChannelConfig {
            backend: value.backend().into(),
            series: value.series(),
            kind: value.kind(),
            scalar_type: value.scalar_type().clone(),
            shape: value.shape().clone(),
            name: value.name().into(),
        })
    }
}

impl From<ChannelTypeConfigGen> for ChannelConfigResponse {
    fn from(value: ChannelTypeConfigGen) -> Self {
        match value {
            ChannelTypeConfigGen::Scylla(k) => k.into(),
            ChannelTypeConfigGen::SfDatabuffer(k) => k.into(),
        }
    }
}

/**
Provide basic information about a channel, especially it's shape.
Also, byte-order is important for clients that process the raw databuffer event data (python data_api3).
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInfo {
    pub scalar_type: ScalarType,
    pub byte_order: Option<ByteOrder>,
    pub shape: Shape,
    pub msg: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ChConf {
    backend: String,
    series: u64,
    kind: SeriesKind,
    scalar_type: ScalarType,
    shape: Shape,
    name: String,
}

impl ChConf {
    pub fn new<S1, S2>(
        backend: S1,
        series: u64,
        kind: SeriesKind,
        scalar_type: ScalarType,
        shape: Shape,
        name: S2,
    ) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            backend: backend.into(),
            series,
            kind,
            scalar_type,
            shape,
            name: name.into(),
        }
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn series(&self) -> u64 {
        self.series
    }

    pub fn kind(&self) -> SeriesKind {
        self.kind.clone()
    }

    pub fn scalar_type(&self) -> &ScalarType {
        &self.scalar_type
    }

    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

// Includes the necessary information to know where to localize datafiles for sf-databuffer
// and what (approximate) types to expect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct SfChFetchInfo {
    backend: String,
    name: String,
    ks: u8,
    bs: DtNano,
    scalar_type: ScalarType,
    shape: Shape,
    compression: bool,
    byte_order: ByteOrder,
    array: bool,
}

impl SfChFetchInfo {
    pub fn new<S1, S2>(
        backend: S1,
        name: S2,
        ks: u8,
        bs: DtNano,
        byte_order: ByteOrder,
        scalar_type: ScalarType,
        shape: Shape,
    ) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            backend: backend.into(),
            name: name.into(),
            ks,
            bs,
            scalar_type,
            shape,
            byte_order,
            compression: false,
            array: false,
        }
    }

    pub fn with_compression(mut self, x: bool) -> Self {
        self.compression = x;
        self
    }

    pub fn with_array(mut self, x: bool) -> Self {
        self.array = x;
        self
    }

    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ks(&self) -> u8 {
        self.ks
    }

    pub fn bs(&self) -> DtNano {
        self.bs.clone()
    }

    pub fn scalar_type(&self) -> &ScalarType {
        &self.scalar_type
    }

    pub fn shape(&self) -> &Shape {
        &self.shape
    }

    pub fn byte_order(&self) -> &ByteOrder {
        &self.byte_order
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub enum ChannelTypeConfigGen {
    Scylla(ChConf),
    SfDatabuffer(SfChFetchInfo),
}

impl ChannelTypeConfigGen {
    pub fn to_scylla(&self) -> Result<ChConf, Error> {
        if let ChannelTypeConfigGen::Scylla(k) = self {
            Ok(k.clone())
        } else {
            Err(Error::TypeMismatch)
        }
    }

    pub fn to_sf_databuffer(&self) -> Result<SfChFetchInfo, Error> {
        if let ChannelTypeConfigGen::SfDatabuffer(k) = self {
            Ok(k.clone())
        } else {
            Err(Error::TypeMismatch)
        }
    }

    pub fn backend(&self) -> &str {
        match self {
            ChannelTypeConfigGen::Scylla(x) => x.backend(),
            ChannelTypeConfigGen::SfDatabuffer(x) => x.backend(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            ChannelTypeConfigGen::Scylla(x) => x.name(),
            ChannelTypeConfigGen::SfDatabuffer(x) => x.name(),
        }
    }

    pub fn scalar_type(&self) -> &ScalarType {
        match self {
            ChannelTypeConfigGen::Scylla(x) => &x.scalar_type,
            ChannelTypeConfigGen::SfDatabuffer(x) => x.scalar_type(),
        }
    }

    pub fn shape(&self) -> &Shape {
        match self {
            ChannelTypeConfigGen::Scylla(x) => &x.shape,
            ChannelTypeConfigGen::SfDatabuffer(x) => x.shape(),
        }
    }

    pub fn series(&self) -> Option<u64> {
        match self {
            ChannelTypeConfigGen::Scylla(x) => Some(x.series()),
            ChannelTypeConfigGen::SfDatabuffer(_) => None,
        }
    }
}

impl From<SfChFetchInfo> for ChannelTypeConfigGen {
    fn from(value: SfChFetchInfo) -> Self {
        Self::SfDatabuffer(value)
    }
}

impl From<ChConf> for ChannelTypeConfigGen {
    fn from(value: ChConf) -> Self {
        Self::Scylla(value)
    }
}

pub fn f32_close(a: f32, b: f32) -> bool {
    if (a - b).abs() < 1e-4 || (a / b > 0.999 && a / b < 1.001) {
        true
    } else {
        false
    }
}

pub fn f64_close(a: f64, b: f64) -> bool {
    if (a - b).abs() < 1e-5 || (a / b > 0.9999 && a / b < 1.0001) {
        true
    } else {
        false
    }
}

pub fn test_cluster() -> Cluster {
    let nodes = (0..3)
        .into_iter()
        .map(|id| Node {
            host: "localhost".into(),
            listen: None,
            port: 6170 + id as u16,
            port_raw: 6170 + id as u16 + 100,
            sf_databuffer: Some(SfDatabuffer {
                data_base_path: test_data_base_path_databuffer().join(format!("node{:02}", id)),
                ksprefix: "ks".into(),
                splits: None,
            }),
            archiver_appliance: None,
            channel_archiver: None,
            prometheus_api_bind: None,
        })
        .collect();
    Cluster {
        backend: TEST_BACKEND.into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            port: 5432,
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        scylla_st: None,
        scylla_mt: None,
        scylla_lt: None,
        cache_scylla: None,
        run_map_pulse_task: false,
        is_central_storage: false,
        file_io_buffer_size: Default::default(),
        announce_backends: None,
    }
}

pub fn sls_test_cluster() -> Cluster {
    let nodes = (0..1)
        .into_iter()
        .map(|id| Node {
            host: "localhost".into(),
            listen: None,
            port: 6190 + id as u16,
            port_raw: 6190 + id as u16 + 100,
            sf_databuffer: None,
            archiver_appliance: None,
            channel_archiver: Some(ChannelArchiver {
                data_base_paths: vec![test_data_base_path_channel_archiver_sls()],
            }),
            prometheus_api_bind: None,
        })
        .collect();
    Cluster {
        backend: "sls-archive".into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            port: 5432,
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        scylla_st: None,
        scylla_mt: None,
        scylla_lt: None,
        cache_scylla: None,
        run_map_pulse_task: false,
        is_central_storage: false,
        file_io_buffer_size: Default::default(),
        announce_backends: None,
    }
}

pub fn archapp_test_cluster() -> Cluster {
    let nodes = (0..1)
        .into_iter()
        .map(|id| Node {
            host: "localhost".into(),
            listen: None,
            port: 6200 + id as u16,
            port_raw: 6200 + id as u16 + 100,
            sf_databuffer: None,
            channel_archiver: None,
            archiver_appliance: Some(ArchiverAppliance {
                data_base_paths: vec![test_data_base_path_archiver_appliance()],
            }),
            prometheus_api_bind: None,
        })
        .collect();
    Cluster {
        backend: "sf-archive".into(),
        nodes,
        database: Database {
            host: "127.0.0.1".into(),
            port: 5432,
            name: "testingdaq".into(),
            user: "testingdaq".into(),
            pass: "testingdaq".into(),
        },
        scylla: None,
        scylla_st: None,
        scylla_mt: None,
        scylla_lt: None,
        cache_scylla: None,
        run_map_pulse_task: false,
        is_central_storage: false,
        file_io_buffer_size: Default::default(),
        announce_backends: None,
    }
}

pub fn test_data_base_path_databuffer() -> PathBuf {
    let homedir = std::env::var("HOME").unwrap();
    let data_base_path = PathBuf::from(homedir)
        .join("daqbuffer-testdata")
        .join("databuffer");
    data_base_path
}

pub fn test_data_base_path_channel_archiver_sls() -> PathBuf {
    let homedir = std::env::var("HOME").unwrap();
    let data_base_path = PathBuf::from(homedir)
        .join("daqbuffer-testdata")
        .join("sls")
        .join("gfa03");
    data_base_path
}

pub fn test_data_base_path_archiver_appliance() -> PathBuf {
    let homedir = std::env::var("HOME").unwrap();
    let data_base_path = PathBuf::from(homedir)
        .join("daqbuffer-testdata")
        .join("archappdata")
        .join("lts")
        .join("ArchiverStore");
    data_base_path
}

#[cfg(test)]
mod test_parse {
    use super::*;

    #[test]
    fn parse_scalar_type_shape() {
        let mut url: Url = "http://test/path".parse().unwrap();
        {
            let mut g = url.query_pairs_mut();
            g.append_pair("scalarType", &format!("{:?}", ScalarType::F32));
            g.append_pair("shape", &format!("{:?}", Shape::Image(3, 4)));
        }
        let url = url;
        let urls = format!("{}", url);
        let url: Url = urls.parse().unwrap();
        let mut a = BTreeMap::new();
        for (k, v) in url.query_pairs() {
            let k = k.to_string();
            let v = v.to_string();
            info!("k {:?}  v {:?}", k, v);
            a.insert(k, v);
        }
        assert_eq!(a.get("scalarType").unwrap(), "f32");
        assert_eq!(a.get("shape").unwrap(), "Image(3, 4)");
    }
}

pub const PSI_DAQBUFFER_SERVICE_MARK: &'static str = "PSI-Daqbuffer-Service-Mark";
pub const PSI_DAQBUFFER_SEEN_URL: &'static str = "PSI-Daqbuffer-Seen-Url";

#[derive(Debug, Clone)]
pub struct ReqCtx {
    ts_ctor: Instant,
    reqid: String,
    reqid_this: String,
    marks: Vec<String>,
    mark: String,
}

impl ReqCtx {
    pub fn new_with_node<T>(req: &Request<T>, nc: &NodeConfigCached) -> Self {
        let reqid_this = status_board().unwrap().new_status_id();
        let reqid = if let Some(reqid_parent) = req.headers().get(X_DAQBUF_REQID) {
            let parent = reqid_parent.to_str().unwrap_or("badid");
            format!("{}-{}", parent, reqid_this)
        } else {
            reqid_this.clone()
        };
        let mark = format!("{}:{}", nc.node_config.name, nc.node.port);
        let mut marks = Vec::new();
        for (n, v) in req.headers().iter() {
            if n == PSI_DAQBUFFER_SERVICE_MARK {
                marks.push(String::from_utf8_lossy(v.as_bytes()).to_string());
            }
        }
        Self {
            ts_ctor: Instant::now(),
            reqid,
            reqid_this,
            marks,
            mark,
        }
    }

    pub fn new_with_proxy<T>(req: &Request<T>, proxy: &ProxyConfig) -> Self {
        let reqid_this = status_board().unwrap().new_status_id();
        let reqid = if let Some(reqid_parent) = req.headers().get(X_DAQBUF_REQID) {
            let parent = reqid_parent.to_str().unwrap_or("badid");
            format!("{}-{}", parent, reqid_this)
        } else {
            reqid_this.clone()
        };
        let mark = format!("{}:{}", proxy.name, proxy.port);
        let mut marks = Vec::new();
        for (n, v) in req.headers().iter() {
            if n == PSI_DAQBUFFER_SERVICE_MARK {
                marks.push(String::from_utf8_lossy(v.as_bytes()).to_string());
            }
        }
        Self {
            ts_ctor: Instant::now(),
            reqid,
            reqid_this,
            marks,
            mark,
        }
    }

    pub fn new_from_single_reqid(reqid: String) -> Self {
        Self {
            ts_ctor: Instant::now(),
            reqid_this: reqid.clone(),
            reqid,
            marks: Vec::new(),
            mark: String::new(),
        }
    }

    pub fn for_test() -> Self {
        Self {
            ts_ctor: Instant::now(),
            reqid: "PARENTID-TESTID".into(),
            reqid_this: "TESTID".into(),
            marks: Vec::new(),
            mark: String::new(),
        }
    }

    pub fn ts_ctor(&self) -> Instant {
        self.ts_ctor.clone()
    }

    pub fn reqid(&self) -> &str {
        &self.reqid
    }

    pub fn reqid_this(&self) -> &str {
        &self.reqid_this
    }

    pub fn mark(&self) -> &str {
        &self.mark
    }

    pub fn marks(&self) -> &[String] {
        &self.marks
    }

    pub fn header_name(&self) -> &'static str {
        X_DAQBUF_REQID
    }

    pub fn header_value(&self) -> &str {
        &self.reqid
    }
}

pub type ReqCtxArc = std::sync::Arc<ReqCtx>;

static STATUS_BOARD: AtomicPtr<RwLock<StatusBoard>> = AtomicPtr::new(std::ptr::null_mut());

#[derive(Debug, Serialize)]
pub struct StatusBoardEntry {
    #[allow(unused)]
    #[serde(serialize_with = "instant_serde::ser")]
    ts_created: SystemTime,
    #[serde(serialize_with = "instant_serde::ser")]
    ts_updated: SystemTime,
    // #[serde(skip_serializing_if = "is_false")]
    done: bool,
    // #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
    // TODO make this a better Stats container and remove pub access.
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    error_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    warn_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    channel_not_found: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    subreq_fail: usize,
}

mod instant_serde {
    use super::DATETIME_FMT_3MS;
    use serde::Serializer;
    use std::time::SystemTime;

    pub fn ser<S: Serializer>(x: &SystemTime, ser: S) -> Result<S::Ok, S::Error> {
        use chrono::LocalResult;
        let dur = x.duration_since(std::time::UNIX_EPOCH).unwrap();
        let res =
            chrono::TimeZone::timestamp_opt(&chrono::Utc, dur.as_secs() as i64, dur.subsec_nanos());
        match res {
            LocalResult::None => Err(serde::ser::Error::custom(format!(
                "Bad local instant conversion"
            ))),
            LocalResult::Single(dt) => {
                let s = dt.format(DATETIME_FMT_3MS).to_string();
                ser.serialize_str(&s)
            }
            LocalResult::Ambiguous(dt, _dt2) => {
                let s = dt.format(DATETIME_FMT_3MS).to_string();
                ser.serialize_str(&s)
            }
        }
    }
}

impl StatusBoardEntry {
    pub fn new() -> Self {
        Self {
            ts_created: SystemTime::now(),
            ts_updated: SystemTime::now(),
            done: false,
            errors: Vec::new(),
            error_count: 0,
            warn_count: 0,
            channel_not_found: 0,
            subreq_fail: 0,
        }
    }

    pub fn warn_inc(&mut self) {
        self.warn_count += 1;
    }

    pub fn channel_not_found_inc(&mut self) {
        self.channel_not_found += 1;
    }
}

#[derive(Debug, Serialize)]
pub struct StatusBoardEntryUser {
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    error_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    warn_count: usize,
    // #[serde(default, skip_serializing_if = "CmpZero::is_zero")]
    channel_not_found: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
}

impl StatusBoardEntryUser {
    pub fn new_all_good() -> Self {
        Self {
            error_count: 0,
            warn_count: 0,
            channel_not_found: 0,
            errors: Vec::new(),
        }
    }
}

impl From<&StatusBoardEntry> for StatusBoardEntryUser {
    fn from(e: &StatusBoardEntry) -> Self {
        Self {
            error_count: e.error_count,
            warn_count: e.warn_count,
            channel_not_found: e.channel_not_found,
            errors: e.errors.iter().map(|e| e.to_string()).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatusBoard {
    entries: BTreeMap<String, StatusBoardEntry>,
}

impl StatusBoard {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn new_status_id(&mut self) -> String {
        self.clean_if_needed();
        let n: u32 = rand::random();
        let s = format!("{:08x}", n);
        self.entries.insert(s.clone(), StatusBoardEntry::new());
        s
    }

    pub fn clean_if_needed(&mut self) {
        if self.entries.len() > 15000 {
            let mut tss: Vec<_> = self.entries.values().map(|e| e.ts_updated).collect();
            tss.sort_unstable();
            let tss = tss;
            let tsm = tss[tss.len() / 3];
            let a = std::mem::replace(&mut self.entries, BTreeMap::new());
            self.entries = a
                .into_iter()
                .filter(|(_k, v)| v.ts_updated >= tsm)
                .collect();
        }
    }

    pub fn get_entry(&mut self, status_id: &str) -> Option<&mut StatusBoardEntry> {
        self.entries.get_mut(status_id)
    }

    pub fn mark_alive(&mut self, status_id: &str) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn mark_done(&mut self, status_id: &str) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
                e.done = true;
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn add_error(&mut self, status_id: &str, err: String) {
        match self.entries.get_mut(status_id) {
            Some(e) => {
                e.ts_updated = SystemTime::now();
                if e.errors.len() < 100 {
                    e.errors.push(err);
                    e.error_count += 1;
                }
            }
            None => {
                error!("can not find status id {}", status_id);
            }
        }
    }

    pub fn status_as_json(&self, status_id: &str) -> Option<StatusBoardEntryUser> {
        match self.entries.get(status_id) {
            Some(e) => Some(e.into()),
            None => None,
        }
    }
}

#[derive(Debug)]
pub enum StatusBoardError {
    CantAcquire,
}

impl fmt::Display for StatusBoardError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{self:?}")
    }
}

pub fn status_board() -> Result<RwLockWriteGuard<'static, StatusBoard>, StatusBoardError> {
    let x = unsafe { &*STATUS_BOARD.load(atomic::Ordering::SeqCst) }.write();
    match x {
        Ok(x) => Ok(x),
        Err(e) => {
            error!("{}", e);
            Err(StatusBoardError::CantAcquire)
        }
    }
}

pub fn status_board_init() {
    static STATUS_BOARD_INIT: Once = Once::new();
    STATUS_BOARD_INIT.call_once(|| {
        let b = StatusBoard::new();
        let a = RwLock::new(b);
        let x = Box::new(a);
        STATUS_BOARD.store(Box::into_raw(x), atomic::Ordering::SeqCst);
    });
}

autoerr::create_error_v1!(
    name(UriError, "UriError"),
    enum variants {
        ParseError(Uri),
    },
);

pub fn req_uri_to_url(uri: &Uri) -> Result<Url, UriError> {
    if uri.scheme().is_none() {
        format!("dummy:{uri}")
            .parse()
            .map_err(|_| UriError::ParseError(uri.clone()))
    } else {
        uri.to_string()
            .parse()
            .map_err(|_| UriError::ParseError(uri.clone()))
    }
}

pub unsafe fn extltref<'a, 'b, T>(t: &'a T) -> &'b T {
    core::mem::transmute(t)
}

pub unsafe fn extltmut<'a, 'b, T>(t: &'a mut T) -> &'b mut T {
    core::mem::transmute(t)
}
