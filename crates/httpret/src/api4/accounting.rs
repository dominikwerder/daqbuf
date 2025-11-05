use crate::bodystream::response;
use crate::err::Error;
use crate::requests::accepts_json_or_all;
use crate::ReqCtx;
use crate::ServiceSharedResources;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use err::ToPublicError;
use http::header;
use http::Method;
use http::StatusCode;
use httpclient::body_empty;
use httpclient::body_string;
use httpclient::IntoBody;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::ToJsonBody;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::ttl::RetentionTime;
use netpod::FromUrl;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::APP_JSON;
use query::api4::AccountingToplistQuery;
use scyllaconn::accounting::toplist::UsageData;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountedIngested {
    names: Vec<String>,
    counts: Vec<u64>,
    bytes: Vec<u64>,
    scalar_types: Vec<ScalarType>,
    shapes: Vec<Shape>,
}

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

    #[allow(unused)]
    fn sort_by_counts(&mut self) {
        let mut tmp: Vec<_> = self
            .counts
            .iter()
            .map(|&x| x)
            .enumerate()
            .map(|(i, x)| (x, i))
            .collect();
        tmp.sort_unstable();
        let tmp: Vec<_> = tmp.into_iter().rev().map(|x| x.1).collect();
        self.reorder_by_index_list(&tmp);
    }

    #[allow(unused)]
    fn sort_by_bytes(&mut self) {
        let mut tmp: Vec<_> = self.bytes.iter().map(|&x| x).enumerate().map(|(i, x)| (x, i)).collect();
        tmp.sort_unstable();
        let tmp: Vec<_> = tmp.into_iter().rev().map(|x| x.1).collect();
        self.reorder_by_index_list(&tmp);
    }

    fn reorder_by_index_list(&mut self, tmp: &[usize]) {
        self.names = tmp.iter().map(|&x| self.names[x].clone()).collect();
        self.counts = tmp.iter().map(|&x| self.counts[x]).collect();
        self.bytes = tmp.iter().map(|&x| self.bytes[x]).collect();
        self.scalar_types = tmp.iter().map(|&x| self.scalar_types[x].clone()).collect();
        self.shapes = tmp.iter().map(|&x| self.shapes[x].clone()).collect();
    }

    #[allow(unused)]
    fn truncate(&mut self, len: usize) {
        self.names.truncate(len);
        self.counts.truncate(len);
        self.bytes.truncate(len);
        self.scalar_types.truncate(len);
        self.shapes.truncate(len);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Toplist {
    dim0: AccountedIngested,
    dim1: AccountedIngested,
    infos_count_total: usize,
    infos_missing_count: usize,
    top1_usage_len: usize,
    scalar_count: usize,
    wave_count: usize,
    found: usize,
    mismatch_count: usize,
}

impl Toplist {
    fn new() -> Self {
        Self {
            dim0: AccountedIngested::new(),
            dim1: AccountedIngested::new(),
            infos_count_total: 0,
            infos_missing_count: 0,
            top1_usage_len: 0,
            scalar_count: 0,
            wave_count: 0,
            found: 0,
            mismatch_count: 0,
        }
    }
}

pub struct AccountingIngested {}

impl AccountingIngested {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with("/api/4/accounting/ingested") {
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
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match self.handle_get(req, ctx, shared_res, ncc).await {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        error!("{e}");
                        let e2 = e.to_public_error();
                        let s = serde_json::to_string(&e2)?;
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
        ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        let url = req_uri_to_url(req.uri())?;
        let qu = AccountingToplistQuery::from_url(&url)?;
        let res = fetch_data(qu.rt(), qu.ts().to_ts_ms(), ctx, shared_res, ncc).await?;
        let mut ret = AccountedIngested::new();
        for e in res.dim0.names {
            ret.names.push(e)
        }
        for e in res.dim0.counts {
            ret.counts.push(e)
        }
        for e in res.dim0.bytes {
            ret.bytes.push(e)
        }
        for e in res.dim0.scalar_types {
            ret.scalar_types.push(e)
        }
        for e in res.dim0.shapes {
            ret.shapes.push(e)
        }
        for e in res.dim1.names {
            ret.names.push(e)
        }
        for e in res.dim1.counts {
            ret.counts.push(e)
        }
        for e in res.dim1.bytes {
            ret.bytes.push(e)
        }
        for e in res.dim1.scalar_types {
            ret.scalar_types.push(e)
        }
        for e in res.dim1.shapes {
            ret.shapes.push(e)
        }
        if let Some(sort) = qu.sort() {
            if sort == "counts" {
                // ret.sort_by_counts();
            } else if sort == "bytes" {
                // ret.sort_by_bytes();
            }
        }
        let body = ToJsonBody::from(&ret).into_body();
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON)
            .body(body)?)
    }
}

pub struct AccountingToplistCounts {}

impl AccountingToplistCounts {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with("/api/4/accounting/toplist/counts") {
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
    ) -> Result<StreamResponse, Error> {
        if req.method() == Method::GET {
            if accepts_json_or_all(req.headers()) {
                match self.handle_get(req, ctx, shared_res, ncc).await {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        error!("{e}");
                        let e2 = e.to_public_error();
                        let s = serde_json::to_string(&e2)?;
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
        ctx: &ReqCtx,
        shared_res: &ServiceSharedResources,
        ncc: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        let url = req_uri_to_url(req.uri())?;
        let qu = AccountingToplistQuery::from_url(&url)?;
        let res = fetch_data(qu.rt(), qu.ts().to_ts_ms(), ctx, shared_res, ncc).await?;
        let body = ToJsonBody::from(&res).into_body();
        Ok(response(StatusCode::OK)
            .header(header::CONTENT_TYPE, APP_JSON)
            .body(body)?)
    }
}

async fn fetch_data(
    rt: RetentionTime,
    ts: TsMs,
    _ctx: &ReqCtx,
    shared_res: &ServiceSharedResources,
    _ncc: &NodeConfigCached,
) -> Result<Toplist, Error> {
    let list_len_max = 10000000;
    let _ = list_len_max;
    if let Some(scyqu) = &shared_res.scyqueue {
        let x = scyqu
            .accounting_read_ts(rt, ts)
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        let ret = resolve_usages(x, &shared_res.pgqueue).await?;
        // ret.dim0.sort_by_bytes();
        // ret.dim1.sort_by_bytes();
        // ret.dim0.truncate(list_len_max);
        // ret.dim1.truncate(list_len_max);
        Ok(ret)
    } else {
        Err(Error::with_public_msg_no_trace("not a scylla backend"))
    }
}

async fn resolve_usages(usage: UsageData, pgqu: &PgQueue) -> Result<Toplist, Error> {
    let mut ret = Toplist::new();
    let mut series_id_it = usage.series().iter().map(|&x| x);
    let mut usage_skip = 0;
    loop {
        let mut series_ids = Vec::new();
        while let Some(u) = series_id_it.next() {
            series_ids.push(u);
            if series_ids.len() >= 1000 {
                break;
            }
        }
        if series_ids.len() == 0 {
            break;
        }
        let infos = pgqu
            .info_for_series_ids(series_ids.clone())
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?
            .recv()
            .await
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?
            .map_err(|e| Error::with_msg_no_trace(e.to_string()))?;
        if infos.len() != series_ids.len() {
            return Err(Error::with_msg_no_trace("database result len mismatch"));
        }
        let nn = series_ids.len();
        for ((series, info_res), (counts, bytes)) in series_ids.into_iter().zip(infos.into_iter()).zip(
            usage
                .counts()
                .iter()
                .skip(usage_skip)
                .map(|&x| x)
                .zip(usage.bytes().iter().skip(usage_skip).map(|&x| x)),
        ) {
            if let Some(info) = info_res {
                if series != info.series {
                    return Err(Error::with_msg_no_trace("lookup mismatch"));
                }
                ret.infos_count_total += 1;
                match &info.shape {
                    Shape::Scalar => {
                        ret.scalar_count += 1;
                        ret.dim0.push(info.name, counts, bytes, info.scalar_type, info.shape);
                    }
                    Shape::Wave(_) => {
                        ret.wave_count += 1;
                        ret.dim1.push(info.name, counts, bytes, info.scalar_type, info.shape);
                    }
                    Shape::Image(_, _) => {}
                }
            } else {
                ret.infos_missing_count += 1;
                ret.dim0.push(
                    "UNRESOLVEDSERIES".into(),
                    counts,
                    bytes,
                    ScalarType::BOOL,
                    Shape::Scalar,
                );
            }
        }
        usage_skip += nn;
    }
    Ok(ret)
}
