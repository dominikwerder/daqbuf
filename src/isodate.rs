use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use netpod::DATETIME_FMT_3MS;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct IsoDateTime(DateTime<Utc>);

impl IsoDateTime {
    pub fn from_unix_millis(ms: u64) -> Self {
        // let datetime = chrono::DateTime::from_timestamp_millis(ms as i64).unwrap();
        // Self(datetime)
        IsoDateTime(
            Utc.timestamp_millis_opt(ms as i64)
                .earliest()
                .unwrap_or(Utc.timestamp_nanos(0)),
        )
    }

    pub fn from_ns_u64(ts: u64) -> Self {
        IsoDateTime(Utc.timestamp_nanos(ts as i64))
    }
}

impl Serialize for IsoDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.format(DATETIME_FMT_3MS).to_string())
    }
}

pub fn make_iso_ts(tss: &[u64]) -> Vec<IsoDateTime> {
    tss.iter()
        .map(|&k| IsoDateTime(Utc.timestamp_nanos(k as i64)))
        .collect()
}
