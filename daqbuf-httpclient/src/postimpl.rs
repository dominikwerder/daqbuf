use crate::httpclient;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http_body::Body;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::combinators::UnsyncBoxBody;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
pub enum BoxBodyError {}

impl fmt::Display for BoxBodyError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "BoxBodyError {{ .. }}")
    }
}

impl std::error::Error for BoxBodyError {}

pub type ResponseTy = Response<UnsyncBoxBody<Bytes, streams::tcprawclient::ErrorBody>>;

fn make_error_response<E>(e: E) -> ResponseTy
where
    E: std::error::Error,
{
    let body = Bytes::from(e.to_string().as_bytes().to_vec());
    let body = Full::new(body);
    let body = body.map_err(|_| streams::tcprawclient::ErrorBody::Msg(String::new()));
    let body = UnsyncBoxBody::new(body);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(body)
        .unwrap()
}

pub struct HttpSimplePostImpl1 {}

impl HttpSimplePostImpl1 {
    pub fn new() -> Self {
        Self {}
    }
}

fn is_body<B>(_: &B)
where
    B: Body,
{
}

impl streams::tcprawclient::HttpSimplePost for HttpSimplePostImpl1 {
    fn http_simple_post(&self, req: Request<Full<Bytes>>) -> Pin<Box<dyn Future<Output = ResponseTy> + Send>> {
        let fut = async move {
            let mut client = match httpclient::connect_client(req.uri()).await {
                Ok(x) => x,
                Err(e) => return make_error_response(e),
            };
            let res = match client.send_request(req).await {
                Ok(x) => x,
                Err(e) => return make_error_response(e),
            };
            let (head, body) = res.into_parts();
            use http_body_util::BodyExt;
            let stream = body
                .map_err(|e| streams::tcprawclient::ErrorBody::Msg(e.to_string()))
                .boxed_unsync();
            is_body(&stream);
            Response::from_parts(head, stream)
        };
        Box::pin(fut)
    }
}
