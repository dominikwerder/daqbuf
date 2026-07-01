use daqbuf_err as err;
use err::Error;

pub trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T, A> ErrConv<T> for Result<T, async_channel::SendError<A>> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg(e.to_string())),
        }
    }
}

impl<T> ErrConv<T> for Result<T, scylla::deserialize::TypeCheckError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{:?}", e))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, scylla::errors::PagerExecutionError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{:?}", e))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, scylla::errors::NextRowError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{:?}", e))),
        }
    }
}

impl<T> ErrConv<T> for Result<T, scylla::errors::NewSessionError> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg_no_trace(format!("{:?}", e))),
        }
    }
}
