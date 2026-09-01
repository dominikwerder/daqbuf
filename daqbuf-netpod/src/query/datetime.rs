use crate::Error;
use chrono::DateTime;
use chrono::FixedOffset;
use serde::Deserialize;
use serde::Serialize;
use serde::de::Visitor;
use std::fmt;
use std::ops;

#[derive(Clone, Debug, PartialEq)]
pub struct Datetime(DateTime<FixedOffset>);

impl From<DateTime<FixedOffset>> for Datetime {
    fn from(x: DateTime<FixedOffset>) -> Self {
        Datetime(x)
    }
}

impl TryFrom<&str> for Datetime {
    type Error = Error;

    fn try_from(val: &str) -> Result<Self, Self::Error> {
        let dt = DateTime::<FixedOffset>::parse_from_rfc3339(val).map_err(|_| Error::InputBad)?;
        Ok(Datetime(dt))
    }
}

impl ops::Deref for Datetime {
    type Target = DateTime<FixedOffset>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// RFC 3339 (subset of ISO 8601)

impl Serialize for Datetime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use fmt::Write;
        use serde::ser::Error;
        let val = &self.0;
        let mut s = String::with_capacity(64);
        write!(&mut s, "{}", val.format("%Y-%m-%dT%H:%M:%S")).map_err(|_| Error::custom("fmt"))?;
        let ns = val.timestamp_subsec_nanos();
        let mus = val.timestamp_subsec_micros();
        if ns % 1000 != 0 {
            write!(&mut s, "{}", val.format(".%9f")).map_err(|_| Error::custom("fmt"))?;
        } else if mus % 1000 != 0 {
            write!(&mut s, "{}", val.format(".%6f")).map_err(|_| Error::custom("fmt"))?;
        } else if mus != 0 {
            write!(&mut s, "{}", val.format(".%3f")).map_err(|_| Error::custom("fmt"))?;
        }
        if val.offset().local_minus_utc() == 0 {
            write!(&mut s, "Z").map_err(|_| Error::custom("fmt"))?;
        } else {
            write!(&mut s, "{}", val.format("%:z")).map_err(|_| Error::custom("fmt"))?;
        }
        serializer.collect_str(&s)
    }
}

mod ser_impl_2 {
    use super::Datetime;
    use crate::DATETIME_FMT_0MS;
    use crate::DATETIME_FMT_3MS;
    use crate::DATETIME_FMT_6MS;
    use crate::DATETIME_FMT_9MS;
    use fmt::Write;
    use serde::ser::Error;
    use std::fmt;

    #[allow(unused)]
    fn serialize<S>(obj: &Datetime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let val = &obj.0;
        let mut s = String::with_capacity(64);
        write!(&mut s, "{}", val.format("%Y-%m-%dT%H:%M:%S")).map_err(|_| Error::custom("fmt"))?;
        let ns = val.timestamp_subsec_nanos();
        let s = if ns % 1000 != 0 {
            val.format(DATETIME_FMT_9MS)
        } else {
            let mus = val.timestamp_subsec_micros();
            if mus % 1000 != 0 {
                val.format(DATETIME_FMT_6MS)
            } else {
                let ms = val.timestamp_subsec_millis();
                if ms != 0 {
                    val.format(DATETIME_FMT_3MS)
                } else {
                    val.format(DATETIME_FMT_0MS)
                }
            }
        };
        serializer.collect_str(&s)
    }
}

struct Vis1;

impl<'de> Visitor<'de> for Vis1 {
    type Value = Datetime;

    fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Datetime")
    }

    fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Datetime::try_from(val).map_err(|e| serde::de::Error::custom(format!("{e}")))
    }
}

impl<'de> Deserialize<'de> for Datetime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(Vis1)
    }
}

#[test]
fn ser_00() {
    use chrono::TimeZone;
    let x = FixedOffset::east_opt(0)
        .unwrap()
        .with_ymd_and_hms(2023, 2, 3, 15, 12, 40)
        .earliest()
        .unwrap();
    let x = Datetime(x);
    let s = serde_json::to_string(&x).unwrap();

    assert_eq!(s, r#""2023-02-03T15:12:40Z""#);
}

#[test]
fn ser_01() {
    use chrono::TimeZone;
    let x = FixedOffset::east_opt(0)
        .unwrap()
        .with_ymd_and_hms(2023, 2, 3, 15, 12, 40)
        .earliest()
        .unwrap()
        .checked_add_signed(chrono::Duration::milliseconds(876))
        .unwrap();
    let x = Datetime(x);
    let s = serde_json::to_string(&x).unwrap();

    assert_eq!(s, r#""2023-02-03T15:12:40.876Z""#);
}

#[test]
fn ser_02() {
    use chrono::TimeZone;
    let x = FixedOffset::east_opt(0)
        .unwrap()
        .with_ymd_and_hms(2023, 2, 3, 15, 12, 40)
        .earliest()
        .unwrap()
        .checked_add_signed(chrono::Duration::nanoseconds(543430000))
        .unwrap();
    let x = Datetime(x);
    let s = serde_json::to_string(&x).unwrap();

    assert_eq!(s, r#""2023-02-03T15:12:40.543430Z""#);
}

#[test]
fn ser_03() {
    use chrono::TimeZone;
    let x = FixedOffset::east_opt(0)
        .unwrap()
        .with_ymd_and_hms(2023, 2, 3, 15, 12, 40)
        .earliest()
        .unwrap()
        .checked_add_signed(chrono::Duration::nanoseconds(543432321))
        .unwrap();
    let x = Datetime(x);
    let s = serde_json::to_string(&x).unwrap();

    assert_eq!(s, r#""2023-02-03T15:12:40.543432321Z""#);
}
