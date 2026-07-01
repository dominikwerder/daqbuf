use crate::bodystream::response;
use crate::requests::accepts_json_or_all;
use crate::ReqCtx;
use crate::ServiceSharedResources;
use futures_util::TryStreamExt;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::req_uri_to_url;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::Shape;
use netpod::UriError;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

autoerr::create_error_v1!(
    name(Error, "DataSearch"),
    enum variants {
        NoScylla,
        Uri(#[from] UriError),
        Json(#[from] serde_json::Error),
        Http(#[from] http::Error),
        ScyllaWorker(#[from] scyllaconn::worker::Error),
        HttpBody(#[from] httpclient::BodyError),
        ScyllaType(#[from] scyllaconn::scylla::errors::TypeCheckError),
        ScyllaNextRow(#[from] scyllaconn::scylla::errors::NextRowError),
    },
);

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountedIngested {
    names: Vec<String>,
    counts: Vec<u64>,
    bytes: Vec<u64>,
    scalar_types: Vec<ScalarType>,
    shapes: Vec<Shape>,
}

#[allow(unused)]
impl AccountedIngested {
    fn new() -> Self {
        Self {
            names: Vec::new(),
            counts: Vec::new(),
            bytes: Vec::new(),
            scalar_types: Vec::new(),
            shapes: Vec::new(),
        }
    }

    fn push(&mut self, name: String, counts: u64, bytes: u64, scalar_type: ScalarType, shape: Shape) {
        self.names.push(name);
        self.counts.push(counts);
        self.bytes.push(bytes);
        self.scalar_types.push(scalar_type);
        self.shapes.push(shape);
    }

    fn truncate(&mut self, len: usize) {
        self.names.truncate(len);
        self.counts.truncate(len);
        self.bytes.truncate(len);
        self.scalar_types.truncate(len);
        self.shapes.truncate(len);
    }
}

pub struct DataSearch {}

impl DataSearch {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with("/api/4/private/search/data") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, crate::err::Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match self.handle_get(req, ctx, shared_res, ncc).await {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        let s = serde_json::to_string(&e.to_string())?;
                        Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_string(s))?)
                    }
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(body_empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(body_empty())?)
        }
    }

    async fn handle_get(
        &self,
        req: Requ,
        _ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        _ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        let url = req_uri_to_url(req.uri())?;
        let _params: BTreeMap<_, _> = url.query_pairs().collect();
        if let Some(scyqu) = &shared_res.scyqueue {
            let cql = "select ts_msp from sls_st.st_ts_msp where series = 6293882751490541488";
            let st = scyqu.prepare(cql.into()).await?;
            let qp = scyqu.execute(st).await?;
            let mut it = qp.rows_stream::<(i64,)>()?;
            let mut msps_in_table = Vec::new();
            while let Some(row) = it.try_next().await? {
                msps_in_table.push(row.0);
            }
            let body = serde_json::json!({
                "msps_in_table": msps_in_table,
            });
            let body = ToJsonBody::from(&body).into_body();
            Ok(response(StatusCode::OK).body(body)?)
        } else {
            Err(Error::NoScylla)
        }
        // let ret = serde_json::json!({
        //     "key": "value",
        // });
        // let body = ToJsonBody::from(&ret).into_body();
        // Ok(response(StatusCode::OK).body(body)?)
    }
}
