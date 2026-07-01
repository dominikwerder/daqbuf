//! Error handling and reporting.

#[macro_export]
macro_rules! err_dbg_dis {
    ($tt:ty, $nn:expr) => {
        impl ::core::fmt::Display for $tt {
            fn fmt(&self, fmt: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                write!(fmt, "{}::{:?}", $nn, self)
            }
        }
    };
}

pub use anyhow;

pub mod bt {
    pub use backtrace::Backtrace;
}

use serde::Deserialize;
use serde::Serialize;
use std::array::TryFromSliceError;
use std::convert::Infallible;
use std::fmt;
use std::net::AddrParseError;
use std::num::ParseFloatError;
use std::num::ParseIntError;
use std::string::FromUtf8Error;
use std::sync::PoisonError;

pub type Res2<T> = anyhow::Result<T>;

autoerr::create_error_v1!(
    name(ErrA, "ErrA"),
    enum variants {
        Bad,
    },
);

autoerr::create_error_v1!(
    name(ErrB, "ErrB"),
    enum variants {
        Worse,
        ErrA(#[from] ErrA),
    },
);

fn f_a() -> Result<u32, ErrA> {
    Err(ErrA::Bad)
}

fn f_b() -> Result<u32, ErrB> {
    if true {
        let res = f_a()?;
        Ok(res)
    } else {
        Err(ErrB::Worse)
    }
}

#[allow(unused)]
fn f_c() -> Result<u32, anyhow::Error> {
    return Ok(f_b()?);
}

#[test]
fn test_fc() {
    assert!(f_c().is_err());
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Reason {
    InternalError,
    BadRequest,
    IoError,
}

/// The common error type for this application.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Error {
    msg: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace_str: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    public_msg: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<Reason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<Box<Error>>,
}

impl Error {
    pub fn with_msg_no_trace<S: Into<String>>(s: S) -> Self {
        Self {
            msg: s.into(),
            trace_str: None,
            public_msg: None,
            reason: None,
            parent: None,
        }
    }

    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self::with_msg_no_trace(s).add_backtrace()
    }

    pub fn with_public_msg_no_trace<S: Into<String>>(s: S) -> Self {
        let s = s.into();
        let ret = Self::with_msg_no_trace(&s);
        let ret = ret.add_public_msg(s);
        ret
    }

    pub fn with_public_msg<S: Into<String>>(s: S) -> Self {
        let s = s.into();
        let ret = Self::with_msg_no_trace(String::new());
        let ret = ret.add_backtrace();
        let ret = ret.add_public_msg(s);
        ret
    }

    pub fn from_string<E>(e: E) -> Self
    where
        E: ToString,
    {
        Self::with_msg_no_trace(e.to_string())
    }

    pub fn add_backtrace(mut self) -> Self {
        self.msg.extend(" (add_backtrace DISABLED)".chars());
        // ret.trace_str = Some(fmt_backtrace(&backtrace::Backtrace::new()));
        self
    }

    pub fn mark_bad_request(mut self) -> Self {
        self.reason = Some(Reason::BadRequest);
        self
    }

    pub fn mark_io_error(mut self) -> Self {
        self.reason = Some(Reason::IoError);
        self
    }

    pub fn add_public_msg(mut self, msg: impl Into<String>) -> Self {
        if self.public_msg.is_none() {
            self.public_msg = Some(Vec::new());
        }
        self.public_msg.as_mut().unwrap().push(msg.into());
        self
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn public_msg(&self) -> Option<&Vec<String>> {
        self.public_msg.as_ref()
    }

    pub fn reason(&self) -> Option<Reason> {
        self.reason.clone()
    }
}

#[allow(unused)]
fn fmt_backtrace(trace: &backtrace::Backtrace) -> String {
    if true {
        return String::from("fmt_backtrace DISABLED");
    }
    use std::io::Write;
    let mut buf = Vec::new();
    let mut c1 = 0;
    'outer: for fr in trace.frames() {
        for sy in fr.symbols() {
            let is_ours = match sy.filename() {
                None => false,
                Some(s) => {
                    let s = s.to_str().unwrap();
                    s.contains("/dev/daqbuffer/") || s.contains("/build/daqbuffer/")
                }
            };
            let name = match sy.name() {
                Some(k) => k.to_string(),
                _ => "[err]".into(),
            };
            let filename = match sy.filename() {
                Some(k) => match k.to_str() {
                    Some(k) => k,
                    _ => "[err]",
                },
                _ => "[err]",
            };
            let lineno = match sy.lineno() {
                Some(k) => k,
                _ => 0,
            };
            if is_ours {
                write!(&mut buf, "\n    {name}\n      {filename}  {lineno}").unwrap();
                c1 += 1;
                if c1 >= 10 {
                    break 'outer;
                }
            }
        }
    }
    String::from_utf8(buf).unwrap()
}

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let trace_str = if let Some(s) = &self.trace_str {
            s.into()
        } else {
            String::new()
        };
        write!(fmt, "msg: {}", self.msg)?;
        if let Some(msgs) = self.public_msg() {
            for (i, msg) in msgs.iter().enumerate() {
                write!(fmt, "; pub({i}): {msg}")?;
            }
        }
        if !trace_str.is_empty() {
            write!(fmt, "\nTrace:\n{}", trace_str)?;
        }
        Ok(())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

impl std::error::Error for Error {}

pub trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T, E> ErrConv<T> for Result<T, E>
where
    E: Into<Error>,
{
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(e.into()),
        }
    }
}

pub trait ErrStr<T> {
    fn errstr(self) -> Result<T, Error>;
}

impl<T, E> ErrStr<T> for Result<T, E>
where
    E: ToString,
{
    fn errstr(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(e.to_string())),
        }
    }
}

pub trait ToErr {
    fn to_err(self) -> Error;
}

impl<T: ToErr> From<T> for Error {
    fn from(k: T) -> Self {
        k.to_err()
    }
}

impl From<PublicError> for Error {
    fn from(k: PublicError) -> Self {
        Self {
            msg: String::new(),
            trace_str: None,
            public_msg: Some(k.msg.clone()),
            reason: k.reason(),
            parent: None,
        }
    }
}

impl ToErr for Infallible {
    fn to_err(self) -> Error {
        Error::with_msg_no_trace(String::new())
    }
}

impl From<String> for Error {
    fn from(k: String) -> Self {
        Self::from_string(k)
    }
}

impl From<&str> for Error {
    fn from(k: &str) -> Self {
        Self::from_string(k)
    }
}

impl From<std::io::Error> for Error {
    fn from(k: std::io::Error) -> Self {
        Self::from_string(k)
    }
}

impl From<AddrParseError> for Error {
    fn from(k: AddrParseError) -> Self {
        Self::from_string(k)
    }
}

impl From<serde_json::Error> for Error {
    fn from(k: serde_json::Error) -> Self {
        Self::from_string(k)
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(k: async_channel::SendError<T>) -> Self {
        Self::from_string(k)
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(k: async_channel::RecvError) -> Self {
        Self::from_string(k)
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(k: chrono::format::ParseError) -> Self {
        Self::from_string(k)
    }
}

impl From<ParseIntError> for Error {
    fn from(k: ParseIntError) -> Self {
        Self::from_string(k)
    }
}

impl From<ParseFloatError> for Error {
    fn from(k: ParseFloatError) -> Self {
        Self::from_string(k)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(k: FromUtf8Error) -> Self {
        Self::from_string(k)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(k: std::str::Utf8Error) -> Self {
        Self::from_string(k)
    }
}

impl From<serde_cbor::Error> for Error {
    fn from(k: serde_cbor::Error) -> Self {
        Self::from_string(k)
    }
}

impl From<std::fmt::Error> for Error {
    fn from(k: std::fmt::Error) -> Self {
        Self::from_string(k)
    }
}

impl From<regex::Error> for Error {
    fn from(k: regex::Error) -> Self {
        Self::from_string(k)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::from_string("PoisonError")
    }
}

impl From<url::ParseError> for Error {
    fn from(k: url::ParseError) -> Self {
        Self::from_string(format!("{:?}", k))
    }
}

impl From<TryFromSliceError> for Error {
    fn from(k: TryFromSliceError) -> Self {
        Self::from_string(format!("{:?}", k))
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(k: rmp_serde::encode::Error) -> Self {
        Self::from_string(format!("{:?}", k))
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(k: rmp_serde::decode::Error) -> Self {
        Self::from_string(format!("{:?}", k))
    }
}

impl From<anyhow::Error> for Error {
    fn from(k: anyhow::Error) -> Self {
        Self::from_string(format!("{k}"))
    }
}

impl From<http::Error> for Error {
    fn from(k: http::Error) -> Self {
        Self::from_string(k)
    }
}

impl From<http::uri::InvalidUri> for Error {
    fn from(k: http::uri::InvalidUri) -> Self {
        Self::from_string(k)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublicError {
    reason: Option<Reason>,
    msg: Vec<String>,
}

impl PublicError {
    pub fn reason(&self) -> Option<Reason> {
        self.reason.clone()
    }

    pub fn msg(&self) -> &Vec<String> {
        &self.msg
    }
}

impl From<String> for PublicError {
    fn from(value: String) -> Self {
        Self {
            reason: None,
            msg: vec![value],
        }
    }
}

impl From<Error> for PublicError {
    fn from(k: Error) -> Self {
        Self {
            reason: k.reason(),
            msg: k.public_msg().map(Clone::clone).unwrap_or(Vec::new()),
        }
    }
}

impl From<&Error> for PublicError {
    fn from(k: &Error) -> Self {
        Self {
            reason: k.reason(),
            msg: vec![k.msg().into()],
        }
    }
}

impl fmt::Display for PublicError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self.msg)
    }
}

impl ToPublicError for Error {
    fn to_public_error(&self) -> PublicError {
        PublicError::from(self)
    }
}

pub fn todo() {
    let bt = backtrace::Backtrace::new();
    eprintln!("TODO\n{bt:?}");
    todo!("TODO\n{bt:?}");
}

pub fn todoval<T>() -> T {
    let bt = backtrace::Backtrace::new();
    eprintln!("TODO\n{bt:?}");
    todo!("TODO todoval\n{bt:?}")
}

pub trait ToPublicError: std::error::Error + Send {
    fn to_public_error(&self) -> PublicError;
}

#[cfg(test)]
mod test {
    #![allow(unused)]
    // use super::Serialize;

    autoerr::create_error_v1!(
        name(SomeErrorEnumA, "SomeErrorEnumA"),
        enum variants {
            BadCase,
            WithStringContent(String),
            WithStringContentFmt(String),
        },
    );

    autoerr::create_error_v1!(
        name(SomeErrorEnumB0, "SomeErrorEnumB0"),
        enum variants {
            FromA(#[from] SomeErrorEnumA),
        },
    );

    autoerr::create_error_v1!(
        name(SomeErrorEnumB1, "SomeErrorEnumB1"),
        enum variants {
            FromA(#[from] SomeErrorEnumA),
            Caffe(SomeErrorEnumA),
        },
    );

    fn failing_a_00() -> Result<(), SomeErrorEnumA> {
        Err(SomeErrorEnumA::BadCase)
    }

    fn failing_b0_00() -> Result<(), SomeErrorEnumB0> {
        let ret = failing_a_00()?;
        Ok(ret)
    }

    fn failing_b1_00() -> Result<(), SomeErrorEnumB1> {
        let ret = failing_a_00()?;
        Ok(ret)
    }

    #[test]
    fn error_handle_a_00() {
        assert_eq!(
            format!("{}", SomeErrorEnumA::BadCase),
            "SomeErrorEnumA::BadCase"
        );
    }

    #[test]
    fn error_handle_a_01() {
        assert_eq!(
            SomeErrorEnumA::WithStringContent(format!("inner")).to_string(),
            "SomeErrorEnumA::WithStringContent"
        );
    }

    #[test]
    fn error_handle_a_02() {
        assert_eq!(
            SomeErrorEnumA::WithStringContentFmt(format!("inner failure \"quoted\"")).to_string(),
            "bad: inner failure \"quoted\""
        );
    }

    #[test]
    fn error_handle_b0_00() {
        let e = failing_b0_00().unwrap_err();
        let s = e.to_string();
        assert_eq!(s, "SomeErrorEnumB0::FromA(SomeErrorEnumA::BadCase)");
    }

    #[cfg(target_abi = "x32")]
    #[test]
    fn error_handle_b0_user_00() {
        use thiserror::UserErrorClass;
        use thiserror::UserErrorInfo;
        let e = failing_b0_00().unwrap_err();
        let s = e.class();
        if let UserErrorClass::Unspecified = s {
            ()
        } else {
            panic!()
        }
    }

    #[test]
    fn error_handle_b1_00() {
        let e = failing_b1_00().unwrap_err();
        let s = e.to_string();
        assert_eq!(s, "SomeErrorEnumB1::FromA(SomeErrorEnumA::BadCase)");
    }
}
