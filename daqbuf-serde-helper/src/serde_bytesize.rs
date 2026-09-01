use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de::{self, Visitor};
use std::fmt;

pub struct ByteSizeU32(u32);

pub fn serialize<S>(val: &ByteSizeU32, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let value = val.0;
    let (num, unit) = if value % (1 << 20) == 0 {
        (value / (1 << 20), "MB")
    } else if value % (1 << 10) == 0 {
        (value / (1 << 10), "kB")
    } else {
        (value, "B")
    };
    ser.serialize_str(&format!("{} {}", num, unit))
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<ByteSizeU32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct ByteSizeVisitor;

    impl<'de> Visitor<'de> for ByteSizeVisitor {
        type Value = ByteSizeU32;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            fmt.write_str("a u32 or a string representing a byte size (e.g., '12kB', '4 MB')")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ByteSizeU32(value as u32))
        }

        fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ByteSizeU32(value))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let val = value.trim();
            let (num, unit) = val.split_at(val.find(|c: char| !c.is_digit(10)).unwrap_or(val.len()));
            let num: u32 = num
                .trim()
                .parse()
                .map_err(|e| E::custom(format!("------------{:?}", e)))?;
            let mul = match unit.trim() {
                "B" => 1,
                "kB" => 1 << 10,
                "MB" => 1 << 20,
                _ => return Err(E::custom(format!("unknown unit: {}", unit))),
            };
            Ok(ByteSizeU32(num * mul))
        }
    }

    deserializer.deserialize_any(ByteSizeVisitor)
}

impl Serialize for ByteSizeU32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for ByteSizeU32 {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize(de)
    }
}

#[test]
fn test_00() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct A {
        #[serde(with = "self")]
        filesize: ByteSizeU32,
    }
    let a = A {
        filesize: ByteSizeU32(1024 * 1024 * 12),
    };
    let s = serde_json::to_string(&a).unwrap();
    assert_eq!(s, r#"{"filesize":"12 MB"}"#);
}

#[test]
fn test_01() {
    let a: ByteSizeU32 = serde_json::from_str("123").unwrap();
    assert_eq!(a.0, 123);
    let a: ByteSizeU32 = serde_json::from_str("\"17kB\"").unwrap();
    assert_eq!(a.0, 1024 * 17);
    let a: ByteSizeU32 = serde_json::from_str("\"17 kB\"").unwrap();
    assert_eq!(a.0, 1024 * 17);
    let a: ByteSizeU32 = serde_json::from_str("\"17  kB \"").unwrap();
    assert_eq!(a.0, 1024 * 17);
    let a: ByteSizeU32 = serde_json::from_str("\" 83  MB\"").unwrap();
    assert_eq!(a.0, 1024 * 1024 * 83);
}
