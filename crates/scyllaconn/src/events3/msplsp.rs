use netpod::DATETIME_FMT_3MS;
use netpod::TsMs;
use netpod::TsNano;
use netpod::timeunits::MS;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MspEv(u64);

impl MspEv {
    pub fn to_i64(&self) -> i64 {
        self.0 as _
    }

    pub fn to_ms(&self) -> TsMs {
        TsMs(self.0)
    }

    pub fn lsp(&self, ts: TsNano) -> Option<LspEv> {
        let ns1 = ts.ns();
        let ns2 = self.0 * MS;
        if ns1 >= ns2 {
            let lsp = LspEv(ns1 - ns2);
            Some(lsp)
        } else {
            None
        }
    }
}

impl From<TsMs> for MspEv {
    fn from(value: TsMs) -> Self {
        Self(value.0)
    }
}

impl fmt::Display for MspEv {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let fm =
            time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z");
        let sec = self.0 / 1000;
        let ns = 1000000 * (self.0 % 1000);
        let st = time::UtcDateTime::from_unix_timestamp(sec as i64).unwrap() + Duration::nanoseconds(ns as i64);
        let mut buf = [0u8; 64];
        let n = st
            .format_into(&mut std::io::Cursor::new(buf.as_mut_slice()), fm)
            .unwrap();
        write!(fmt, "{}", str::from_utf8(&buf[..n]).unwrap())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LspEv(u64);

impl LspEv {
    pub fn from_i64(x: i64) -> Self {
        Self(x as _)
    }

    pub fn max() -> Self {
        Self(i64::MAX as _)
    }

    pub fn to_i64(&self) -> i64 {
        self.0 as _
    }
}

impl fmt::Display for LspEv {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let fm = time::macros::format_description!("[hour]:[minute]:[second].[subsecond digits:3]Z");
        let sec = self.0 / 1000_000_000;
        let ns = self.0 % 1000_000_000;
        let st = time::UtcDateTime::from_unix_timestamp(sec as i64).unwrap() + Duration::nanoseconds(ns as i64);
        let mut buf = [0u8; 64];
        let n = st
            .format_into(&mut std::io::Cursor::new(buf.as_mut_slice()), fm)
            .unwrap();
        write!(fmt, "{}", str::from_utf8(&buf[..n]).unwrap())
    }
}
