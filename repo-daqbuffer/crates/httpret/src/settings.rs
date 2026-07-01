use crate::err::Error;
use crate::response;
use http::header;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::Requ;
use httpclient::StreamResponse;
use netpod::log::*;
use netpod::NodeConfigCached;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;

pub struct SettingsThreadsMaxHandler {}

impl SettingsThreadsMaxHandler {
    pub fn path_prefix() -> &'static str {
        "/api/4/settings/read3/threads_max"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn put(&self, req: Requ, _node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        let (head, body) = req.into_parts();
        let accept = head
            .headers
            .get(header::ACCEPT)
            .map_or(Ok(ACCEPT_ALL), |k| k.to_str())
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))?
            .to_owned();
        if accept != APP_JSON && accept != ACCEPT_ALL {
            // TODO set the public error code and message and return Err(e).
            let e = Error::with_public_msg_no_trace(format!("unsupported accept: {:?}", accept));
            error!("{e}");
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let body = httpclient::read_body_bytes(body).await?;
        //let threads_max: usize = head.uri.path()[Self::path_prefix().len()..].parse()?;
        let threads_max: usize = String::from_utf8_lossy(&body).parse()?;
        info!("threads_max {threads_max}");
        disk::read3::Read3::get().set_threads_max(threads_max);
        let ret = response(StatusCode::NO_CONTENT).body(body_empty())?;
        Ok(ret)
    }

    pub async fn get(&self, _req: Requ, _node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        let threads_max = disk::read3::Read3::get().threads_max();
        let ret = response(StatusCode::OK).body(body_string(format!("{threads_max}")))?;
        Ok(ret)
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            self.get(req, node_config).await
        } else if req.method() == Method::PUT {
            self.put(req, node_config).await
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }
}
