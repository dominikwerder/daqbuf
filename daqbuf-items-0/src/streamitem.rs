use crate::log::Level;
use daqbuf_err as err;
use netpod::DiskStats;
use netpod::EventDataReadStats;
use netpod::RangeFilterStats;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

pub const TERM_FRAME_TYPE_ID: u32 = 0xaa0001;
pub const ERROR_FRAME_TYPE_ID: u32 = 0xaa0002;
pub const SITEMTY_NONSPEC_FRAME_TYPE_ID: u32 = 0xaa0004;
pub const EVENT_QUERY_JSON_STRING_FRAME: u32 = 0x10000;
pub const EVENTS_0D_FRAME_TYPE_ID: u32 = 0x50000;
pub const MIN_MAX_AVG_DIM_0_BINS_FRAME_TYPE_ID: u32 = 0x70000;
pub const MIN_MAX_AVG_DIM_1_BINS_FRAME_TYPE_ID: u32 = 0x80000;
pub const MIN_MAX_AVG_WAVE_BINS: u32 = 0xa0000;
pub const WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0xb0000;
pub const LOG_FRAME_TYPE_ID: u32 = 0xc0000;
pub const STATS_FRAME_TYPE_ID: u32 = 0xd0000;
pub const RANGE_COMPLETE_FRAME_TYPE_ID: u32 = 0xe0000;
pub const EVENT_FULL_FRAME_TYPE_ID: u32 = 0x220000;
pub const EVENTS_ITEM_FRAME_TYPE_ID: u32 = 0x230000;
pub const STATS_EVENTS_FRAME_TYPE_ID: u32 = 0x240000;
pub const ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID: u32 = 0x250000;
pub const X_BINNED_SCALAR_EVENTS_FRAME_TYPE_ID: u32 = 0x880000;
pub const X_BINNED_WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0x890000;
pub const DATABUFFER_EVENT_BLOB_FRAME_TYPE_ID: u32 = 0x8a0000;
pub const CONTAINER_EVENTS_TYPE_ID: u32 = 0xc80000;

pub fn bool_is_false(j: &bool) -> bool {
    *j == false
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RangeCompletableItem<T> {
    RangeComplete,
    Data(T),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StatsItem {
    EventDataReadStats(EventDataReadStats),
    RangeFilterStats(RangeFilterStats),
    DiskStats(DiskStats),
    Warnings(),
    Binning,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StreamItem<T> {
    DataItem(T),
    Log(LogItem),
    Stats(StatsItem),
}

impl<T> StreamItem<T> {
    pub fn into_data(self) -> Result<T, Self> {
        if let StreamItem::DataItem(x) = self {
            Ok(x)
        } else {
            Err(self)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogItem {
    #[serde(with = "timestamp")]
    ts: time::UtcDateTime,
    origin: String,
    #[serde(with = "levelserde")]
    level: Level,
    msg: String,
}

impl LogItem {
    pub fn from_node(level: Level, msg: String) -> Self {
        Self {
            ts: time::UtcDateTime::now(),
            level,
            msg,
            origin: String::new(),
        }
    }

    pub fn level_msg(level: Level, msg: String) -> Self {
        Self {
            ts: time::UtcDateTime::now(),
            level,
            msg,
            origin: String::new(),
        }
    }

    pub fn origin_level_msg(origin: String, level: Level, msg: String) -> Self {
        Self {
            ts: time::UtcDateTime::now(),
            origin,
            level,
            msg,
        }
    }

    pub fn info(msg: String) -> Self {
        Self {
            ts: time::UtcDateTime::now(),
            level: Level::INFO,
            msg,
            origin: String::new(),
        }
    }

    pub fn debug(msg: String) -> Self {
        Self {
            ts: time::UtcDateTime::now(),
            level: Level::DEBUG,
            msg,
            origin: String::new(),
        }
    }

    pub fn level(&self) -> Level {
        self.level
    }

    pub fn display_log_file(&self) -> LogItemDisplayLogfile<'_> {
        LogItemDisplayLogfile { item: self }
    }
}

pub struct LogItemDisplayLogfile<'a> {
    item: &'a LogItem,
}

impl<'a> fmt::Display for LogItemDisplayLogfile<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let me = &self.item;
        write!(fmt, "{}  {}  {}  {}", me.ts, me.level, me.origin, me.msg)
    }
}

pub type SitemErrTy = err::Error;

pub type Sitemty<T> = Result<StreamItem<RangeCompletableItem<T>>, SitemErrTy>;

pub type Sitemty2<T, E> = Result<StreamItem<RangeCompletableItem<T>>, E>;

pub type Sitemty3<T, E> = Result<StreamItem<T>, E>;

#[macro_export]
macro_rules! on_sitemty_range_complete {
    ($item:expr, $ex:expr) => {
        if let Ok($crate::StreamItem::DataItem($crate::RangeCompletableItem::RangeComplete)) = $item
        {
            $ex
        }
    };
}

#[macro_export]
macro_rules! on_sitemty_data_old {
    ($item:expr, $ex:expr) => {
        if let Ok($crate::streamitem::StreamItem::DataItem(
            $crate::streamitem::RangeCompletableItem::Data(item),
        )) = $item
        {
            $ex(item)
        } else {
            $item
        }
    };
}

#[macro_export]
macro_rules! on_sitemty_data {
    ($item:expr, $ex:expr) => {{
        use $crate::streamitem::RangeCompletableItem;
        use $crate::streamitem::StreamItem;
        match $item {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(x) => $ex(x),
                    RangeCompletableItem::RangeComplete => {
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                },
                StreamItem::Log(x) => Ok(StreamItem::Log(x)),
                StreamItem::Stats(x) => Ok(StreamItem::Stats(x)),
            },
            Err(x) => Err(x),
        }
    }};
}

#[macro_export]
macro_rules! try_map_sitemty_data {
    ($item:expr, $ex:expr) => {{
        use $crate::streamitem::RangeCompletableItem;
        use $crate::streamitem::StreamItem;
        match $item {
            Ok(x) => match x {
                StreamItem::DataItem(x) => match x {
                    RangeCompletableItem::Data(x) => match $ex(x) {
                        Ok(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
                        Err(e) => Err(e),
                    },
                    RangeCompletableItem::RangeComplete => {
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                },
                StreamItem::Log(x) => Ok(StreamItem::Log(x)),
                StreamItem::Stats(x) => Ok(StreamItem::Stats(x)),
            },
            Err(x) => Err(x),
        }
    }};
}

pub fn sitem_data<X>(x: X) -> Sitemty<X> {
    Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
}

pub fn sitem2_data<T, E>(x: T) -> Sitemty2<T, E> {
    Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
}

pub fn sitem2_log<T, E>(x: LogItem) -> Sitemty2<T, E> {
    Ok(StreamItem::Log(x))
}

pub fn sitem3_data<T, E>(x: T) -> Sitemty3<T, E> {
    Ok(StreamItem::DataItem(x))
}

pub fn sitem3_log_info<T, E>(msg: String) -> Sitemty3<T, E> {
    let item = LogItem::info(msg);
    Ok(StreamItem::Log(item))
}

pub fn sitem_err_from_string<T, D>(x: T) -> Sitemty<D>
where
    T: ToString,
{
    Err(err::Error::from_string(x))
}

pub fn sitem_err2_from_string<T>(x: T) -> err::Error
where
    T: ToString,
{
    err::Error::from_string(x)
}

mod timestamp {
    use serde::Deserializer;
    use serde::Serializer;
    use serde::de;
    use serde::de::Visitor;
    use std::fmt;

    pub fn serialize<S>(val: &time::UtcDateTime, se: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let g = val
            .format(time::macros::format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]"
            ))
            .unwrap();
        se.serialize_str(&g)
    }

    struct Visit;

    impl<'de> Visitor<'de> for Visit {
        type Value = time::UtcDateTime;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "expect utc timestamp")
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let ret = time::UtcDateTime::parse(
                val,
                time::macros::format_description!(
                    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]"
                ),
            );
            ret.map_err(|e| E::custom(format!("{e}")))
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<time::UtcDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_str(Visit)
    }
}

mod levelserde {
    use super::Level;
    use serde::de::{self, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(t: &Level, se: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let g = match *t {
            Level::ERROR => "ERROR",
            Level::WARN => "WARN",
            Level::INFO => "INFO",
            Level::DEBUG => "DEBUG",
            Level::TRACE => "TRACE",
        };
        se.serialize_str(g)
    }

    struct VisitLevel;

    impl<'de> Visitor<'de> for VisitLevel {
        type Value = Level;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "expect Level code")
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let level = match val {
                "ERROR" => Level::ERROR,
                "WARN" => Level::WARN,
                "INFO" => Level::INFO,
                "DEBUG" => Level::DEBUG,
                "TRACE" => Level::TRACE,
                _ => return Err(E::custom("unknown level value")),
            };
            Ok(level)
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_str(VisitLevel)
    }
}

#[test]
fn serde_level() {
    use Level::{DEBUG, ERROR, INFO, TRACE, WARN};
    for v1 in [ERROR, WARN, INFO, DEBUG, TRACE] {
        let s = serde_json::to_string(&v1).unwrap();
        let v2 = serde_json::from_str(&s).unwrap();
        assert_eq!(v1, v2);
    }
}

pub trait AsLogItem {
    fn as_log_item(&self) -> Option<&LogItem>;
}

impl<T, E> AsLogItem for Sitemty3<T, E> {
    fn as_log_item(&self) -> Option<&LogItem> {
        todo!()
    }
}

#[allow(unreachable_code, unused)]
fn assert_impl_as_log_item<T, E>() {
    let item: Sitemty<T> = todo!();
    AsLogItem::as_log_item(&item);
    let item: Sitemty2<T, E> = todo!();
    AsLogItem::as_log_item(&item);
    let item: Sitemty3<T, E> = todo!();
    AsLogItem::as_log_item(&item);
}

pub trait FromLogItem {
    fn from_log_item(item: LogItem) -> Self;
}

impl<T, E> FromLogItem for Result<StreamItem<T>, E> {
    fn from_log_item(item: LogItem) -> Self {
        Ok(StreamItem::Log(item))
    }
}
