use crate::bodystream::response;
use crate::err::Error;
use bytes::Bytes;
use http::StatusCode;
use httpclient::Requ;
use httpclient::StreamResponse;
use httpclient::body_bytes;
use httpclient::body_empty;
use netpod::ReqCtx;
use netpod::log::*;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::Cursor;
use std::io::Write;
use std::sync::OnceLock;
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize)]
struct Contents {
    files: HashMap<String, Vec<u8>>,
    dirs: HashMap<String, Contents>,
}

impl Contents {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
            dirs: HashMap::new(),
        }
    }
}

fn all_files() -> &'static Contents {
    static CELL: OnceLock<Contents> = OnceLock::new();
    CELL.get_or_init(extract_all_files)
}

fn extract_all_files() -> Contents {
    let ts1 = Instant::now();
    let buf1 = Vec::with_capacity(1024 * 1024 * 12);
    let mut dec = brotli::DecompressorWriter::new(buf1, 1024 * 12);
    // let mut dec = flate2::write::ZlibDecoder::new(buf1);
    if let Err(e) = dec.write_all(blob()) {
        error!("can not decode {e}");
        return Contents::new();
    }
    if let Err(e) = dec.flush() {
        error!("can not decode {e}");
        return Contents::new();
    }
    let buf1 = dec.get_ref();
    // let buf1 = match dec.finish() {
    //     Ok(x) => x,
    //     Err(e) => {
    //         error!("can not decode {e}");
    //         return Contents::new();
    //     }
    // };
    match ciborium::from_reader::<Contents, _>(Cursor::new(buf1)) {
        Ok(x) => {
            let ts2 = Instant::now();
            let dt = ts2.saturating_duration_since(ts1);
            debug!("content br blob loaded in {:.0} ms", 1e3 * dt.as_secs_f32());
            x
        }
        Err(_) => {
            error!("can not read apidoc");
            Contents::new()
        }
    }
}

// TODO
fn blob() -> &'static [u8] {
    // include_bytes!(concat!("../../../../apidoc/book.cbor"))
    b""
}

pub struct DocsHandler {}

impl DocsHandler {
    pub fn path_prefix() -> &'static str {
        "/api/4/docs"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) || req.uri().path().starts_with("/api/4/documentation") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, _ctx: &ReqCtx) -> Result<StreamResponse, Error> {
        let path = req.uri().path();
        if path.starts_with("/api/4/documentation") {
            let ret = http::Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(http::header::LOCATION, "/api/4/docs/")
                .body(body_empty())?;
            return Ok(ret);
        }
        if path == "/api/4/docs" {
            let ret = http::Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(http::header::LOCATION, "/api/4/docs/")
                .body(body_empty())?;
            return Ok(ret);
        }
        if path == "/api/4/docs/" {
            let ret = http::Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(http::header::LOCATION, "/api/4/docs/index.html")
                .body(body_empty())?;
            return Ok(ret);
        }
        let mut segs: VecDeque<_> = path.split("/").collect();
        for _ in 0..4 {
            segs.pop_front();
        }
        let mut data = all_files();
        loop {
            if let Some(seg) = segs.pop_front() {
                if segs.len() == 0 {
                    if let Some(x) = data.files.get(seg) {
                        return Ok(response(StatusCode::OK).body(body_bytes(Bytes::from(x.as_slice())))?);
                    } else {
                        return Ok(response(StatusCode::NOT_FOUND).body(body_empty())?);
                    }
                } else {
                    if let Some(x) = data.dirs.get(seg) {
                        data = x;
                    } else {
                        return Ok(response(StatusCode::NOT_FOUND).body(body_empty())?);
                    }
                }
            } else {
                return Ok(response(StatusCode::NOT_FOUND).body(body_empty())?);
            }
        }
    }
}
