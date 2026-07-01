use crate::RetrievalError;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use httpclient::read_body_bytes;
use hyper::server::conn::AddrStream;
use hyper::service::make_service_fn;
use hyper::service::service_fn;
use hyper::Body;
use hyper::Server;
use netpod::log::*;
use netpod::ACCEPT_ALL;
use netpod::APP_JSON;
use serde_json::json;
use serde_json::Value;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Once;

fn response<T>(status: T) -> http::response::Builder
where
    http::StatusCode: std::convert::TryFrom<T>,
    <http::StatusCode as std::convert::TryFrom<T>>::Error: Into<http::Error>,
{
    Response::builder()
        .status(status)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "*")
}

fn accepts_json(headers: &HeaderMap<HeaderValue>) -> bool {
    match headers.get(http::header::ACCEPT) {
        Some(h) => match h.to_str() {
            Ok(hv) => {
                if hv.contains(APP_JSON) {
                    true
                } else if hv.contains(ACCEPT_ALL) {
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        },
        None => false,
    }
}

pub struct StatusBuildInfoHandler {}

impl StatusBuildInfoHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/v1/status/buildinfo" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
        info!("{} for {:?}", std::any::type_name::<Self>(), req);
        if req.method() == Method::GET {
            if accepts_json(req.headers()) {
                if true {
                    let res = json!({
                      "status": "success",
                      "data": {
                        "version": "2.37",
                        "revision": "daqingest",
                        "branch": "dev",
                        "buildUser": "dominik.werder",
                        "buildDate": "2022-07-21",
                        "goVersion": "nogo"
                      }
                    });
                    let body = Body::from(serde_json::to_vec(&res)?);
                    Ok(response(StatusCode::OK).body(body)?)
                } else {
                    Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("error")))?)
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}

pub struct SeriesHandler {}

impl SeriesHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/v1/series" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
        info!("{} for {:?}", std::any::type_name::<Self>(), req);
        if req.method() == Method::GET || req.method() == Method::POST {
            if accepts_json(req.headers()) {
                let res = json!({
                    "status": "success",
                    "data": [
                        {
                            "__name__": "series1",
                            //"job": "daqingest",
                            //"instance": "node1"
                        }
                    ]
                });
                let body = Body::from(serde_json::to_vec(&res)?);
                Ok(response(StatusCode::OK).body(body)?)
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            info!("series handler with {:?}", req);
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}

pub struct MetadataHandler {}

impl MetadataHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/v1/metadata" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
        info!("{} for {:?}", std::any::type_name::<Self>(), req);
        if req.method() == Method::GET {
            if accepts_json(req.headers()) {
                if true {
                    let res = json!({
                        "status": "success",
                        "data": {}
                    });
                    let body = Body::from(serde_json::to_vec(&res)?);
                    Ok(response(StatusCode::OK).body(body)?)
                } else {
                    Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("error")))?)
                }
            } else {
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            info!("metadata handler with {:?}", req);
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}

pub struct LabelsHandler {}

impl LabelsHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/v1/labels" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
        let self_name = std::any::type_name::<Self>();
        info!("{} for {:?}", self_name, req);
        if req.method() == Method::GET || req.method() == Method::POST {
            if accepts_json(req.headers()) {
                if true {
                    let res = json!({
                        "status": "success",
                        "data": ["__name__"]
                    });
                    info!("return labels {:?}", res);
                    let body = Body::from(serde_json::to_vec(&res)?);
                    Ok(response(StatusCode::OK).body(body)?)
                } else {
                    Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(Body::from(format!("error")))?)
                }
            } else {
                warn!("{} BAD_REQUEST {:?}", self_name, req);
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            warn!("{} METHOD_NOT_ALLOWED {:?}", self_name, req);
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}

pub struct LabelValuesHandler {
    label: String,
}

impl LabelValuesHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        use regex::Regex;
        static mut RE1: Option<Regex> = None;
        static RE1_INIT: Once = Once::new();
        let re1 = unsafe {
            RE1_INIT.call_once(|| {
                RE1 = Some(Regex::new(r#"/api/v1/label/([-:._a-zA-Z0-9]+)/values"#).unwrap());
            });
            RE1.as_mut().unwrap_unchecked()
        };
        if let Some(caps) = re1.captures(req.uri().path()) {
            if let Some(label) = caps.get(1) {
                Some(LabelValuesHandler {
                    label: label.as_str().into(),
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
        let self_name = std::any::type_name::<Self>();
        info!("{} for {:?}", self_name, req);
        info!("LABEL {:?}", self.label);
        if req.method() == Method::GET || req.method() == Method::POST {
            if accepts_json(req.headers()) {
                if self.label == "__name__" {
                    let res = json!({
                        "status": "success",
                        "data": ["series1", "series2"]
                    });
                    info!("label values {:?}", res);
                    let body = Body::from(serde_json::to_vec(&res)?);
                    Ok(response(StatusCode::OK).body(body)?)
                } else {
                    let res = json!({
                        "status": "success",
                        "data": []
                    });
                    warn!("return empty label values");
                    let body = Body::from(serde_json::to_vec(&res)?);
                    Ok(response(StatusCode::OK).body(body)?)
                }
            } else {
                warn!("{} bad accept", self_name);
                Ok(response(StatusCode::BAD_REQUEST).body(Body::empty())?)
            }
        } else {
            warn!("{} label value handler with {:?}", self_name, req);
            Ok(response(StatusCode::METHOD_NOT_ALLOWED).body(Body::empty())?)
        }
    }
}

pub struct QueryHandler {}

impl QueryHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/v1/query" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
        info!("{} for {:?}", std::any::type_name::<Self>(), req);
        let url = req_uri_to_url(req.uri())?;
        info!("/api/v1/query  parsed url: {:?}", url);
        let body = read_body_bytes(req.into_body()).await?;
        let body_str = String::from_utf8_lossy(&body);
        info!("/api/v1/query  body_str: {:?}", body_str);
        let formurl = url::Url::parse(&format!("dummy:///?{}", body_str));
        info!("/api/v1/query  formurl: {:?}", formurl);
        let res = json!({
            "status": "success",
            "data": {
                "resultType": "scalar",
                "result": [40, "2"]
            }
        });
        let body = Body::from(serde_json::to_vec(&res)?);
        Ok(response(StatusCode::OK).body(body)?)
    }
}

pub struct QueryRangeHandler {}

impl QueryRangeHandler {
    pub fn handler(req: &Request<Body>) -> Option<Self> {
        if req.uri().path() == "/api/v1/query_range" {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
        info!("{} for {:?}", std::any::type_name::<Self>(), req);
        let url = req_uri_to_url(req.uri())?;
        info!("/api/v1/query_range  parsed url: {:?}", url);
        let body = read_body_bytes(req.into_body()).await?;
        let body_str = String::from_utf8_lossy(&body);
        info!("/api/v1/query_range  body_str: {:?}", body_str);
        let formurl = url::Url::parse(&format!("dummy:///?{}", body_str));
        info!("/api/v1/query_range  formurl: {:?}", formurl);
        match formurl {
            Ok(formurl) => {
                for (k, v) in formurl.query_pairs() {
                    info!("KEY {}  VALUE {}", k, v);
                }
                let qps: BTreeMap<_, _> = formurl.query_pairs().collect();
                if let (Some(ts1), Some(ts2)) = (qps.get("start"), qps.get("end")) {
                    let ts1: f64 = ts1.parse().unwrap_or(1.);
                    let ts2: f64 = ts2.parse().unwrap_or(2.);
                    let ts1 = if !ts1.is_normal() { 1. } else { ts1 };
                    let ts2 = if !ts2.is_normal() { 2. } else { ts2 };
                    let ts1 = ts1.min(3e9);
                    let ts2 = ts2.min(3e9);
                    let ts2 = if ts2 <= ts1 { ts1 + 1. } else { ts2 };
                    let dt = (ts2 - ts1) / 20.;
                    info!("ts1 {}  ts2 {}  dt {}", ts1, ts2, dt);
                    let mut ts = ts1;
                    let mut vals = Vec::new();
                    while ts < ts2 {
                        ts += dt;
                        vals.push((ts, 1.2f32));
                    }
                    let v: Vec<_> = vals
                        .into_iter()
                        .map(|(k, v)| json!([json!(k), Value::String(v.to_string())]))
                        .collect();
                    let res = serde_json::json!({
                        "status": "success",
                        "data": {
                            "resultType": "matrix",
                            "result": [
                                {
                                    "metric": {
                                        "__name__": "series1",
                                    },
                                    "values": v
                                }
                            ]
                        }
                    });
                    info!("return {:?}", res);
                    let body = Body::from(serde_json::to_vec(&res)?);
                    return Ok(response(StatusCode::OK).body(body)?);
                } else {
                }
            }
            Err(e) => {
                warn!("can not parse {e:?}");
            }
        }
        info!("query range returning default empty");
        let res = json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "__name__": "series1",
                        },
                        "values": [
                        ]
                    }
                ]
            }
        });
        let body = Body::from(serde_json::to_vec(&res)?);
        Ok(response(StatusCode::OK).body(body)?)
    }
}

async fn http_service_inner(req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
    if let Some(h) = StatusBuildInfoHandler::handler(&req) {
        h.handle(req).await
    } else if let Some(h) = SeriesHandler::handler(&req) {
        h.handle(req).await
    } else if let Some(h) = MetadataHandler::handler(&req) {
        h.handle(req).await
    } else if let Some(h) = LabelsHandler::handler(&req) {
        h.handle(req).await
    } else if let Some(h) = LabelValuesHandler::handler(&req) {
        h.handle(req).await
    } else if let Some(h) = QueryHandler::handler(&req) {
        h.handle(req).await
    } else if let Some(h) = QueryRangeHandler::handler(&req) {
        h.handle(req).await
    } else {
        warn!("no handler found for {:?}", req);
        Ok(response(StatusCode::NOT_FOUND).body(Body::empty())?)
    }
}

async fn http_service(req: Request<Body>) -> Result<Response<Body>, RetrievalError> {
    match http_service_inner(req).await {
        Ok(k) => Ok(k),
        Err(e) => {
            error!("daqbuffer node http_service sees error: {}", e);
            Err(e)
        }
    }
}

pub async fn host(bind: SocketAddr) -> Result<(), RetrievalError> {
    let make_service = make_service_fn({
        move |conn: &AddrStream| {
            let addr = conn.remote_addr();
            async move {
                Ok::<_, RetrievalError>(service_fn({
                    move |req| {
                        info!(
                            "http-request  {:?} - {:?} - {:?} - {:?}",
                            addr,
                            req.method(),
                            req.uri(),
                            req.headers()
                        );
                        let f = http_service(req);
                        Box::pin(f)
                    }
                }))
            }
        }
    });
    Server::bind(&bind).serve(make_service).await?;
    Ok(())
}
