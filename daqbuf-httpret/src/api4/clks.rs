use crate::RetrievalError;
use crate::ServiceSharedResources;
use crate::bodystream::response;
use http::Method;
use http::StatusCode;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::body_empty;
use httpclient::body_string;
use netpod::NodeConfigCached;
use netpod::ReqCtx;

macro_rules! _error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "ClKs"),
    enum variants {
        HttpLib(#[from] http::Error),
    },
);

impl From<Error> for RetrievalError {
    fn from(value: Error) -> Self {
        RetrievalError::TextError(value.to_string())
    }
}

pub struct ClKsInfo {}

impl ClKsInfo {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path() == "/api/4/private/ClKsInfo" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        _ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        use serde_json::json;
        if req.method() != Method::GET {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        } else {
            if let Some(scyqu) = shared_res.scyqueue.as_ref() {
                let clusters = scyqu
                    .clusters()
                    .iter()
                    .map(|cl| {
                        let keyspaces = cl
                            .keyspaces()
                            .iter()
                            .map(|x| {
                                json!({
                                    "name": x.name(),
                                    "rt": x.rt(),
                                })
                            })
                            .collect::<Vec<_>>();
                        json!({
                            "tag": cl.tag(),
                            "keyspaces": keyspaces,
                        })
                    })
                    .collect::<Vec<_>>();
                let js = json!({
                    "clusters": clusters,
                });
                let s = serde_json::to_string(&js).unwrap();
                Ok(response(StatusCode::OK).body(body_string(s))?)
            } else {
                let js = json!({
                    "error": "no scylla configured",
                });
                let s = serde_json::to_string(&js).unwrap();
                Ok(response(StatusCode::OK).body(body_string(s))?)
            }
        }
    }
}
