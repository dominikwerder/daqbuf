use crate::DiskIoTune;
use crate::Error;
use crate::FileIoBufferSize;
use crate::ReadSys;
use crate::query::datetime::Datetime;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::time::Duration;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Api1Range {
    #[serde(rename = "type", default, skip_serializing_if = "String::is_empty")]
    ty: String,
    #[serde(rename = "startDate")]
    beg: Datetime,
    #[serde(rename = "endDate")]
    end: Datetime,
}

impl Api1Range {
    pub fn new(beg: Datetime, end: Datetime) -> Result<Self, Error> {
        let ret = Self {
            ty: String::new(),
            beg,
            end,
        };
        Ok(ret)
    }

    pub fn beg(&self) -> &Datetime {
        &self.beg
    }

    pub fn end(&self) -> &Datetime {
        &self.end
    }
}

#[test]
fn serde_de_range_zulu() {
    let s = r#"{"startDate": "2022-11-22T10:15:12.412Z", "endDate": "2022-11-22T10:15:12.413556Z"}"#;
    let range: Api1Range = serde_json::from_str(s).unwrap();
    assert_eq!(range.beg().offset().local_minus_utc(), 0);
    assert_eq!(range.end().offset().local_minus_utc(), 0);
    assert_eq!(range.beg().timestamp_subsec_micros(), 412000);
    assert_eq!(range.end().timestamp_subsec_micros(), 413556);
}

#[test]
fn serde_de_range_offset() {
    let s = r#"{"startDate": "2022-11-22T10:15:12.412Z", "endDate": "2022-11-22T10:15:12.413556Z"}"#;
    let range: Api1Range = serde_json::from_str(s).unwrap();
    assert_eq!(range.beg().offset().local_minus_utc(), 0);
    assert_eq!(range.end().offset().local_minus_utc(), 0);
    assert_eq!(range.beg().timestamp_subsec_micros(), 412000);
    assert_eq!(range.end().timestamp_subsec_micros(), 413556);
}

#[test]
fn serde_ser_range_offset() {
    use chrono::{FixedOffset, NaiveDate, TimeZone};
    let beg = FixedOffset::east_opt(60 * 60 * 3)
        .unwrap()
        .from_local_datetime(
            &NaiveDate::from_ymd_opt(2022, 11, 22)
                .unwrap()
                .and_hms_milli_opt(13, 14, 15, 16)
                .unwrap(),
        )
        .earliest()
        .unwrap();
    let end = FixedOffset::east_opt(-60 * 60 * 1)
        .unwrap()
        .from_local_datetime(
            &NaiveDate::from_ymd_opt(2022, 11, 22)
                .unwrap()
                .and_hms_milli_opt(13, 14, 15, 800)
                .unwrap(),
        )
        .earliest()
        .unwrap();
    let range = Api1Range::new(beg.into(), end.into()).unwrap();
    let js = serde_json::to_string(&range).unwrap();
    let exp = r#"{"startDate":"2022-11-22T13:14:15.016+03:00","endDate":"2022-11-22T13:14:15.800-01:00"}"#;
    assert_eq!(js, exp);
}

#[test]
fn serde_ser_range_01() -> Result<(), Error> {
    let beg = Datetime::try_from("2022-11-22T02:03:04Z")?;
    let end = Datetime::try_from("2022-11-22T02:03:04.123Z")?;
    let range = Api1Range::new(beg, end).unwrap();
    let js = serde_json::to_string(&range).unwrap();
    let exp = r#"{"startDate":"2022-11-22T02:03:04Z","endDate":"2022-11-22T02:03:04.123Z"}"#;
    assert_eq!(js, exp);
    Ok(())
}

#[test]
fn serde_ser_range_02() -> Result<(), Error> {
    let beg = Datetime::try_from("2022-11-22T02:03:04.987654Z")?;
    let end = Datetime::try_from("2022-11-22T02:03:04.777000Z")?;
    let range = Api1Range::new(beg, end).unwrap();
    let js = serde_json::to_string(&range).unwrap();
    let exp = r#"{"startDate":"2022-11-22T02:03:04.987654Z","endDate":"2022-11-22T02:03:04.777Z"}"#;
    assert_eq!(js, exp);
    Ok(())
}

/// In Api1, the list of channels consists of either `BACKEND/CHANNELNAME`
/// or just `CHANNELNAME`.
#[derive(Debug, PartialEq)]
pub struct ChannelTuple {
    backend: Option<String>,
    name: String,
}

impl ChannelTuple {
    pub fn new(backend: String, name: String) -> Self {
        Self {
            backend: Some(backend),
            name,
        }
    }

    pub fn from_name(name: String) -> Self {
        Self { backend: None, name }
    }

    pub fn backend(&self) -> Option<&String> {
        self.backend.as_ref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

mod serde_channel_tuple {
    use super::*;
    use serde::de::{Deserialize, Deserializer, Visitor};
    use serde::ser::{Serialize, Serializer};

    impl Serialize for ChannelTuple {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if let Some(backend) = self.backend.as_ref() {
                serializer.serialize_str(&format!("{}/{}", backend, self.name))
            } else {
                serializer.serialize_str(&self.name)
            }
        }
    }

    struct Vis;

    impl<'de> Visitor<'de> for Vis {
        type Value = ChannelTuple;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "[Backendname/]Channelname")
        }

        fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let mut it = val.split("/");
            // Even empty string splits into one element of empty string
            let s0 = it.next().unwrap();
            if let Some(s1) = it.next() {
                let ret = ChannelTuple {
                    backend: Some(s0.into()),
                    name: s1.into(),
                };
                Ok(ret)
            } else {
                let ret = ChannelTuple {
                    backend: None,
                    name: s0.into(),
                };
                Ok(ret)
            }
        }
    }

    impl<'de> Deserialize<'de> for ChannelTuple {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(Vis)
        }
    }

    #[test]
    fn ser_name() {
        let x = ChannelTuple {
            backend: None,
            name: "temperature".into(),
        };
        let js = serde_json::to_string(&x).unwrap();
        assert_eq!(js, r#""temperature""#);
    }

    #[test]
    fn ser_backend_name() {
        let x = ChannelTuple {
            backend: Some("beach".into()),
            name: "temperature".into(),
        };
        let js = serde_json::to_string(&x).unwrap();
        assert_eq!(js, r#""beach/temperature""#);
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Api1Query {
    range: Api1Range,
    channels: Vec<ChannelTuple>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timeout: Option<Duration>,
    // All following parameters are private and not to be used
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file_io_buffer_size: Option<FileIoBufferSize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    decompress: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    events_max: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    io_queue_len: Option<u32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    log_level: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    read_sys: String,
}

impl Api1Query {
    pub fn new(range: Api1Range, channels: Vec<ChannelTuple>) -> Self {
        Self {
            range,
            channels,
            timeout: None,
            decompress: None,
            events_max: None,
            file_io_buffer_size: None,
            io_queue_len: None,
            log_level: String::new(),
            read_sys: String::new(),
        }
    }

    pub fn disk_io_tune(&self) -> DiskIoTune {
        let mut k = DiskIoTune::default();
        if let Some(x) = &self.file_io_buffer_size {
            k.read_buffer_len = x.0;
        }
        if let Some(x) = self.io_queue_len {
            k.read_queue_len = x as usize;
        }
        let read_sys: ReadSys = self.read_sys.as_str().into();
        k.read_sys = read_sys;
        k
    }

    pub fn range(&self) -> &Api1Range {
        &self.range
    }

    pub fn channels(&self) -> &[ChannelTuple] {
        &self.channels
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn timeout_or_default(&self) -> Duration {
        Duration::from_secs(60 * 30)
    }

    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    pub fn decompress(&self) -> Option<bool> {
        self.decompress
    }

    pub fn events_max(&self) -> Option<u64> {
        self.events_max
    }

    pub fn set_decompress(&mut self, v: Option<bool>) {
        self.decompress = v;
    }
}

#[test]
fn serde_api1_query() -> Result<(), Error> {
    let beg = Datetime::try_from("2022-11-22T08:09:10Z")?;
    let end = Datetime::try_from("2022-11-23T08:11:05.455009+02:00")?;
    let range = Api1Range::new(beg, end).unwrap();
    let ch0 = ChannelTuple::from_name("nameonly".into());
    let ch1 = ChannelTuple::new("somebackend".into(), "somechan".into());
    let qu = Api1Query::new(range, vec![ch0, ch1]);
    let js = serde_json::to_string(&qu).unwrap();
    assert_eq!(
        js,
        r#"{"range":{"startDate":"2022-11-22T08:09:10Z","endDate":"2022-11-23T08:11:05.455009+02:00"},"channels":["nameonly","somebackend/somechan"]}"#
    );
    Ok(())
}
