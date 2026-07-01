use daqbuf_err as err;
use err::ToPublicError;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use taskrun::tokio;

#[derive(Serialize, Deserialize)]
pub struct Error(pub err::Error);

impl Error {
    pub fn with_msg<S: Into<String>>(s: S) -> Self {
        Self(err::Error::with_msg(s))
    }

    pub fn with_msg_no_trace<S: Into<String>>(s: S) -> Self {
        Self(err::Error::with_msg_no_trace(s))
    }

    pub fn with_public_msg<S: Into<String>>(s: S) -> Self {
        Self(err::Error::with_public_msg(s))
    }

    pub fn with_public_msg_no_trace<S: Into<String>>(s: S) -> Self {
        Self(err::Error::with_public_msg_no_trace(s))
    }

    pub fn msg(&self) -> &str {
        self.0.msg()
    }

    pub fn reason(&self) -> Option<err::Reason> {
        self.0.reason()
    }

    pub fn public_msg(&self) -> Option<&Vec<String>> {
        self.0.public_msg()
    }

    pub fn add_public_msg(self, msg: impl Into<String>) -> Self {
        Error(self.0.add_public_msg(msg))
    }

    pub fn from_to_string<E: ToString>(e: E) -> Self {
        Self::with_msg_no_trace(e.to_string())
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.0, fmt)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, fmt)
    }
}

impl ToPublicError for Error {
    fn to_public_error(&self) -> err::PublicError {
        err::PublicError::from(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<err::Error> for Error {
    fn from(x: err::Error) -> Self {
        Self(x)
    }
}

impl From<Error> for err::Error {
    fn from(x: Error) -> Self {
        x.0
    }
}

pub trait Convable {}

impl<T: Convable> From<T> for Error
where
    T: ToString,
{
    fn from(x: T) -> Self {
        Self(err::Error::from_string(x))
    }
}

impl Convable for std::net::AddrParseError {}
impl Convable for std::string::FromUtf8Error {}
impl Convable for fmt::Error {}
impl Convable for std::io::Error {}
impl Convable for std::num::ParseIntError {}
impl Convable for dbconn::pg::Error {}
impl Convable for tokio::task::JoinError {}
impl Convable for tokio::time::error::Elapsed {}
impl Convable for serde_json::Error {}
impl Convable for chrono::ParseError {}
impl Convable for url::ParseError {}
impl Convable for http::uri::InvalidUri {}
impl Convable for http::Error {}
impl Convable for http::header::ToStrError {}
impl Convable for hyper::Error {}
impl Convable for std::array::TryFromSliceError {}
impl Convable for err::anyhow::Error {}
impl Convable for crate::RetrievalError {}
impl Convable for httpclient::Error {}
impl Convable for netpod::UriError {}
impl Convable for nodenet::configquorum::Error {}
impl Convable for nodenet::channelconfig::Error {}
impl Convable for query::api4::Error {}
impl Convable for query::api4::events::Error {}
impl Convable for netpod::Error {}
impl Convable for crate::http3::Error {}
impl Convable for scyllaconn::conn::Error {}
