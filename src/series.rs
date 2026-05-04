use core::fmt;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug)]
pub enum Existence<T> {
    Created(T),
    Existing(T),
}

impl<T> Existence<T> {
    pub fn into_inner(self) -> T {
        use Existence::*;
        match self {
            Created(x) => x,
            Existing(x) => x,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct SeriesId(u64);

impl SeriesId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }

    pub fn to_i64(&self) -> i64 {
        self.0 as i64
    }
}

impl fmt::Display for SeriesId {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "SeriesId {{ {:20} }}", self.0)
    }
}

mod series_serde {
    use crate::SeriesId;
    use serde::de::Visitor;
    use serde::Deserialize;

    struct Vis;

    impl<'de> Visitor<'de> for Vis {
        type Value = SeriesId;

        fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(fmt, "expect a numeric series id")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(SeriesId::new(v))
        }
    }

    impl<'de> Deserialize<'de> for SeriesId {
        fn deserialize<D>(de: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            de.deserialize_u64(Vis)
        }
    }
}

impl From<(u32, u32)> for SeriesId {
    fn from((a, b): (u32, u32)) -> Self {
        SeriesId((a as u64) | (b as u64) << 32)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChannelStatusSeriesId(u64);

impl ChannelStatusSeriesId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u64 {
        self.0
    }

    pub fn to_i64(&self) -> i64 {
        self.0 as i64
    }
}
