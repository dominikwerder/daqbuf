use daqbuf_err as err;
use netpod::log::Level;
use netpod::DiskStats;
use netpod::EventDataReadStats;
use netpod::RangeFilterStats;
use serde::Deserialize;
use serde::Serialize;

pub const TERM_FRAME_TYPE_ID: u32 = 0xaa01;
pub const ERROR_FRAME_TYPE_ID: u32 = 0xaa02;
pub const SITEMTY_NONSPEC_FRAME_TYPE_ID: u32 = 0xaa04;
pub const EVENT_QUERY_JSON_STRING_FRAME: u32 = 0x100;
pub const EVENTS_0D_FRAME_TYPE_ID: u32 = 0x500;
pub const MIN_MAX_AVG_DIM_0_BINS_FRAME_TYPE_ID: u32 = 0x700;
pub const MIN_MAX_AVG_DIM_1_BINS_FRAME_TYPE_ID: u32 = 0x800;
pub const MIN_MAX_AVG_WAVE_BINS: u32 = 0xa00;
pub const WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0xb00;
pub const LOG_FRAME_TYPE_ID: u32 = 0xc00;
pub const STATS_FRAME_TYPE_ID: u32 = 0xd00;
pub const RANGE_COMPLETE_FRAME_TYPE_ID: u32 = 0xe00;
pub const EVENT_FULL_FRAME_TYPE_ID: u32 = 0x2200;
pub const EVENTS_ITEM_FRAME_TYPE_ID: u32 = 0x2300;
pub const STATS_EVENTS_FRAME_TYPE_ID: u32 = 0x2400;
pub const ITEMS_2_CHANNEL_EVENTS_FRAME_TYPE_ID: u32 = 0x2500;
pub const X_BINNED_SCALAR_EVENTS_FRAME_TYPE_ID: u32 = 0x8800;
pub const X_BINNED_WAVE_EVENTS_FRAME_TYPE_ID: u32 = 0x8900;
pub const DATABUFFER_EVENT_BLOB_FRAME_TYPE_ID: u32 = 0x8a00;

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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StreamItem<T> {
    DataItem(T),
    Log(LogItem),
    Stats(StatsItem),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogItem {
    pub node_ix: u32,
    #[serde(with = "levelserde")]
    pub level: Level,
    pub msg: String,
}

impl LogItem {
    pub fn from_node(node_ix: usize, level: Level, msg: String) -> Self {
        Self {
            node_ix: node_ix as _,
            level,
            msg,
        }
    }
}

pub type SitemErrTy = err::Error;

pub type Sitemty<T> = Result<StreamItem<RangeCompletableItem<T>>, SitemErrTy>;

pub type Sitemty2<T, E> = Result<StreamItem<RangeCompletableItem<T>>, E>;

#[macro_export]
macro_rules! on_sitemty_range_complete {
    ($item:expr, $ex:expr) => {
        if let Ok($crate::StreamItem::DataItem($crate::RangeCompletableItem::RangeComplete)) = $item {
            $ex
        }
    };
}

#[macro_export]
macro_rules! on_sitemty_data_old {
    ($item:expr, $ex:expr) => {
        if let Ok($crate::streamitem::StreamItem::DataItem($crate::streamitem::RangeCompletableItem::Data(item))) =
            $item
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
            Level::ERROR => 1,
            Level::WARN => 2,
            Level::INFO => 3,
            Level::DEBUG => 4,
            Level::TRACE => 5,
        };
        se.serialize_u32(g)
    }

    struct VisitLevel;

    impl VisitLevel {
        fn from_u32(x: u32) -> Level {
            match x {
                1 => Level::ERROR,
                2 => Level::WARN,
                3 => Level::INFO,
                4 => Level::DEBUG,
                5 => Level::TRACE,
                _ => Level::TRACE,
            }
        }
    }

    impl<'de> Visitor<'de> for VisitLevel {
        type Value = Level;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "expect Level code")
        }

        fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(VisitLevel::from_u32(val as _))
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_u32(VisitLevel)
    }
}
