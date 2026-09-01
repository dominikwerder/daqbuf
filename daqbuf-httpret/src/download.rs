use crate::Requ;
use crate::body_empty;
use crate::err::Error;
use crate::response;
use futures_util::TryStreamExt;
use http::Method;
use http::StatusCode;
use httpclient::StreamResponse;
use httpclient::body_stream;
use netpod::DiskIoTune;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::get_url_query_pairs;
use netpod::log::*;
use taskrun::tokio;
use url::Url;

#[derive(Clone, Debug)]
pub struct DownloadQuery {
    disk_io_tune: DiskIoTune,
}

impl FromUrl for DownloadQuery {
    type Error = crate::err::Error;

    fn from_url(url: &Url) -> Result<Self, Self::Error> {
        let pairs = get_url_query_pairs(url);
        Self::from_pairs(&pairs)
    }

    fn from_pairs(pairs: &std::collections::BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let read_sys = pairs
            .get("ReadSys")
            .map(|x| x.as_str().into())
            .unwrap_or_else(|| netpod::ReadSys::default());
        let read_buffer_len = pairs
            .get("ReadBufferLen")
            .map(|x| x.parse().map_or(None, Some))
            .unwrap_or(None)
            .unwrap_or(1024 * 4);
        let read_queue_len = pairs
            .get("ReadQueueLen")
            .map(|x| x.parse().map_or(None, Some))
            .unwrap_or(None)
            .unwrap_or(8);
        let disk_io_tune = DiskIoTune {
            read_sys,
            read_buffer_len,
            read_queue_len,
        };
        let ret = Self { disk_io_tune };
        Ok(ret)
    }
}

pub struct DownloadHandler {}

impl DownloadHandler {
    pub fn path_prefix() -> &'static str {
        "/api/4/test/download/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            self.get(req, node_config).await
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    pub async fn get(&self, req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        let (head, _body) = req.into_parts();
        let p2 = &head.uri.path()[Self::path_prefix().len()..];
        let base = match &ncc.node.sf_databuffer {
            Some(k) => k.data_base_path.clone(),
            None => "/UNDEFINED".into(),
        };
        let url = url::Url::parse(&format!("http://dummy{}", head.uri))?;
        let query = DownloadQuery::from_url(&url)?;
        // TODO wrap file operation to return a better error.
        let pp = base.join(p2);
        info!("Try to open {pp:?}");
        let file = tokio::fs::OpenOptions::new().read(true).open(&pp).await?;
        let stream =
            disk::file_content_stream(pp, file, query.disk_io_tune.clone(), "download").map_ok(|x| x.into_buf());
        let res = response(StatusCode::OK).body(body_stream(stream))?;
        Ok(res)
    }
}
