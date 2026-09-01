use crate::err::Error;
use daqbuf_err as err;
use err::ToPublicError;
use http::Response;
use http::StatusCode;
use httpclient::StreamResponse;
use httpclient::body_empty;
use httpclient::body_string;
use netpod::APP_JSON;
use netpod::log::*;

pub fn response<T>(status: T) -> http::response::Builder
where
    http::StatusCode: std::convert::TryFrom<T>,
    <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder().status(status).header("daqbuf-api-version", "1.0")
}

pub trait ToPublicResponse {
    fn to_public_response(&self) -> StreamResponse;
}

impl ToPublicResponse for Error {
    fn to_public_response(&self) -> StreamResponse {
        self.0.to_public_response()
    }
}

impl ToPublicResponse for daqbuf_err::Error {
    fn to_public_response(&self) -> StreamResponse {
        use err::Reason;
        let e = self.to_public_error();
        let status = match e.reason() {
            Some(Reason::BadRequest) => StatusCode::BAD_REQUEST,
            Some(Reason::InternalError) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let msg = match serde_json::to_string(&e) {
            Ok(s) => s,
            Err(_) => "can not serialize error".into(),
        };
        match response(status)
            .header(http::header::CONTENT_TYPE, APP_JSON)
            .body(body_string(msg))
        {
            Ok(res) => res,
            Err(e) => {
                error!("can not generate http error response {e:?}");
                let mut res = Response::new(body_empty());
                *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                res
            }
        }
    }
}
