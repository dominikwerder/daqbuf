use crate::body_empty;
use crate::body_string;
use crate::cache::Cache;
use crate::err::Error;
use crate::response;
use crate::Requ;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use chrono::TimeZone;
use chrono::Utc;
use futures_util::stream::FuturesOrdered;
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt;
use futures_util::TryStreamExt;
use http::header;
use http::Method;
use http::StatusCode;
use http::Uri;
use httpclient::connect_client;
use httpclient::read_body_bytes;
use httpclient::StreamResponse;
use hyper::Request;
use netpod::log::*;
use netpod::req_uri_to_url;
use netpod::timeunits::SEC;
use netpod::AppendToUrl;
use netpod::FromUrl;
use netpod::HasBackend;
use netpod::HasTimeout;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::DATETIME_FMT_9MS;
use scyllaconn::scylla;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use taskrun::tokio;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use url::Url;

static USE_CACHE: bool = false;
static CACHE: Cache<u64, u64> = Cache::new();

pub struct MapPulseHisto {
    _pulse: u64,
    _tss: Vec<u64>,
    _counts: Vec<u64>,
}

const MAP_PULSE_MARK_CLOSED_URL_PREFIX: &'static str = "/api/1/map/pulse/mark/closed/";
const API_4_MAP_PULSE_URL_PREFIX: &'static str = "/api/4/map/pulse/";

const MAP_PULSE_LOCAL_TIMEOUT: Duration = Duration::from_millis(8000);
const MAP_PULSE_QUERY_TIMEOUT: Duration = Duration::from_millis(10000);

async fn make_tables(pgc: &dbconn::pg::Client) -> Result<(), Error> {
    let sql = "set client_min_messages = 'warning'";
    pgc.execute(sql, &[]).await?;
    let sql = "create table if not exists map_pulse_channels (name text, tbmax int)";
    pgc.execute(sql, &[]).await?;
    let sql = "create table if not exists map_pulse_files (channel text not null, split int not null, timebin int not null, closed int not null default 0, pulse_min int8 not null, pulse_max int8 not null)";
    pgc.execute(sql, &[]).await?;
    let sql = "create unique index if not exists map_pulse_files_ix1 on map_pulse_files (channel, split, timebin)";
    pgc.execute(sql, &[]).await?;
    let sql = "alter table map_pulse_files add if not exists upc1 int not null default 0";
    pgc.execute(sql, &[]).await?;
    let sql = "alter table map_pulse_files add if not exists hostname text not null default ''";
    pgc.execute(sql, &[]).await?;
    let sql = "alter table map_pulse_files add if not exists ks int not null default 2";
    pgc.execute(sql, &[]).await?;
    let sql = "create index if not exists map_pulse_files_ix2 on map_pulse_files (hostname)";
    pgc.execute(sql, &[]).await?;
    let sql = "set client_min_messages = 'notice'";
    pgc.execute(sql, &[]).await?;
    Ok(())
}

fn _timer_channel_names() -> Vec<String> {
    let sections = vec!["SINEG01", "SINSB01", "SINSB02", "SINSB03", "SINSB04", "SINXB01"];
    let suffixes = vec!["MASTER"];
    let mut all: Vec<_> = sections
        .iter()
        .map(|sec| {
            suffixes
                .iter()
                .map(move |suf| format!("{}-RLLE-STA:{}-EVRPULSEID", sec, suf))
        })
        .flatten()
        .collect();
    // all.push("SIN-CVME-TIFGUN-EVR0:RX-PULSEID".into());
    all.push("SAR-CVME-TIFALL4:EvtSet".into());
    all.push("SAR-CVME-TIFALL5:EvtSet".into());
    all.push("SAR-CVME-TIFALL6:EvtSet".into());
    all.push("SAT-CVME-TIFALL5:EvtSet".into());
    all.push("SAT-CVME-TIFALL6:EvtSet".into());
    all
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MapfilePath {
    Scalar(PathBuf),
    Index(PathBuf),
}

async fn datafiles_for_channel(name: &str, node_config: &NodeConfigCached) -> Result<Vec<MapfilePath>, Error> {
    let mut a = Vec::new();
    let sfc = node_config.node.sf_databuffer.as_ref().unwrap();
    let data_base_path = &sfc.data_base_path;
    let channel_path = sfc
        .data_base_path
        .join(format!("{}_2", sfc.ksprefix))
        .join("byTime")
        .join(&name);
    match tokio::fs::read_dir(&channel_path).await {
        Ok(mut rd) => {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let mut rd2 = tokio::fs::read_dir(entry.path()).await?;
                while let Ok(Some(e2)) = rd2.next_entry().await {
                    let mut rd3 = tokio::fs::read_dir(e2.path()).await?;
                    while let Ok(Some(e3)) = rd3.next_entry().await {
                        if e3.file_name().to_string_lossy().ends_with("_00000_Data") {
                            let x = MapfilePath::Scalar(e3.path());
                            a.push(x);
                        }
                    }
                }
            }
            Ok(a)
        }
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => {
                files_recursive(name, &data_base_path, &sfc.ksprefix, 3, "_00000_Data_Index").await
            }
            _ => return Err(e)?,
        },
    }
}

async fn files_recursive(
    name: &str,
    data_base_path: &Path,
    ksprefix: &str,
    ks: u32,
    data_file_suffix: &str,
) -> Result<Vec<MapfilePath>, Error> {
    let mut a = Vec::new();
    let channel_path = data_base_path
        .join(format!("{}_{}", ksprefix, ks))
        .join("byTime")
        .join(&name);
    match tokio::fs::read_dir(&channel_path).await {
        Ok(mut rd) => {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let mut rd2 = tokio::fs::read_dir(entry.path()).await?;
                while let Ok(Some(e2)) = rd2.next_entry().await {
                    let mut rd3 = tokio::fs::read_dir(e2.path()).await?;
                    while let Ok(Some(e3)) = rd3.next_entry().await {
                        if e3.file_name().to_string_lossy().ends_with(data_file_suffix) {
                            let x = MapfilePath::Index(e3.path());
                            a.push(x);
                        }
                    }
                }
            }
            Ok(a)
        }
        Err(e) => match e.kind() {
            _ => return Err(e)?,
        },
    }
}

#[derive(Debug)]
struct ChunkInfo {
    pos: u64,
    len: u64,
    ts: u64,
    pulse: u64,
}

#[derive(Debug)]
struct IndexChunkInfo {
    pos_index: u64,
    #[allow(unused)]
    pos_data: u64,
    #[allow(unused)]
    len: u64,
    ts: u64,
    pulse: u64,
}

async fn read_buf_or_eof(file: &mut File, buf: &mut BytesMut) -> Result<usize, Error> {
    let mut m = 0;
    loop {
        if buf.has_remaining_mut() {
            let n = file.read_buf(buf).await?;
            if n == 0 {
                break;
            } else {
                m += n;
            }
        } else {
            break;
        }
    }
    Ok(m)
}

async fn read_first_index_chunk(
    mut file_index: File,
    file_data: File,
) -> Result<(Option<IndexChunkInfo>, File, File), Error> {
    file_index.seek(SeekFrom::Start(0)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = read_buf_or_eof(&mut file_index, &mut buf).await?;
    if n1 < 18 {
        let msg = format!("can not even read 18 bytes from index file  n1 {}", n1);
        warn!("{msg}");
        return Ok((None, file_index, file_data));
    }
    let ver = buf.get_i16();
    if ver != 0 {
        return Err(Error::with_msg_no_trace(format!("unknown file version  ver {}", ver)));
    }
    let p2 = 2;
    let ts = buf.get_u64();
    let pos_data = buf.get_u64();
    trace!("read_first_index_chunk  ts {ts}  pos_data {pos_data}");
    let (chunk, file_data) = read_chunk_at(file_data, pos_data, None).await?;
    trace!("read_first_index_chunk successful: {chunk:?}");
    let ret = IndexChunkInfo {
        pos_index: p2,
        pos_data: chunk.pos,
        len: chunk.len,
        ts: chunk.ts,
        pulse: chunk.pulse,
    };
    Ok((Some(ret), file_index, file_data))
}

async fn read_last_index_chunk(
    mut file_index: File,
    file_data: File,
) -> Result<(Option<IndexChunkInfo>, File, File), Error> {
    let flen = file_index.seek(SeekFrom::End(0)).await?;
    let entry_len = 16;
    let c1 = (flen - 2) / entry_len;
    if c1 == 0 {
        return Ok((None, file_index, file_data));
    }
    let p2 = 2 + (c1 - 1) * entry_len;
    file_index.seek(SeekFrom::Start(p2)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = read_buf_or_eof(&mut file_index, &mut buf).await?;
    if n1 < 16 {
        let msg = format!("can not even read 16 bytes from index file  n1 {}", n1);
        warn!("{msg}");
        return Ok((None, file_index, file_data));
    }
    let ts = buf.get_u64();
    let pos_data = buf.get_u64();
    trace!("read_last_index_chunk  p2 {p2}  ts {ts}  pos_data {pos_data}");
    let (chunk, file_data) = read_chunk_at(file_data, pos_data, None).await?;
    trace!("read_last_index_chunk successful: {chunk:?}");
    let ret = IndexChunkInfo {
        pos_index: p2,
        pos_data: chunk.pos,
        len: chunk.len,
        ts: chunk.ts,
        pulse: chunk.pulse,
    };
    Ok((Some(ret), file_index, file_data))
}

async fn read_first_chunk(mut file: File) -> Result<(Option<ChunkInfo>, File), Error> {
    file.seek(SeekFrom::Start(0)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = read_buf_or_eof(&mut file, &mut buf).await?;
    if n1 < 6 {
        let msg = format!("can not even read 6 bytes from datafile  n1 {}", n1);
        warn!("{msg}");
        return Ok((None, file));
    }
    let ver = buf.get_i16();
    if ver != 0 {
        return Err(Error::with_msg_no_trace(format!("unknown file version  ver {}", ver)));
    }
    let hlen = buf.get_u32() as u64;
    if n1 < 2 + hlen as usize + 4 + 3 * 8 {
        let msg = format!("did not read enough for first event  n1 {}", n1);
        warn!("{msg}");
        return Ok((None, file));
    }
    buf.advance(hlen as usize - 4);
    let clen = buf.get_u32() as u64;
    let _ttl = buf.get_u64();
    let ts = buf.get_u64();
    let pulse = buf.get_u64();
    let ret = ChunkInfo {
        pos: 2 + hlen,
        len: clen,
        ts,
        pulse,
    };
    Ok((Some(ret), file))
}

async fn read_last_chunk(mut file: File, pos_first: u64, chunk_len: u64) -> Result<(Option<ChunkInfo>, File), Error> {
    let flen = file.seek(SeekFrom::End(0)).await?;
    let c1 = (flen - pos_first) / chunk_len;
    if c1 == 0 {
        return Ok((None, file));
    }
    let p2 = pos_first + (c1 - 1) * chunk_len;
    file.seek(SeekFrom::Start(p2)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = read_buf_or_eof(&mut file, &mut buf).await?;
    if n1 < 4 + 3 * 8 {
        return Err(Error::with_msg_no_trace(format!(
            "can not read enough from datafile  flen {}  pos_first {}  chunk_len {}  c1 {}  n1 {}",
            flen, pos_first, chunk_len, c1, n1
        )));
    }
    let clen = buf.get_u32() as u64;
    if clen != chunk_len {
        return Err(Error::with_msg_no_trace(format!(
            "read_last_chunk  mismatch  flen {}  pos_first {}  chunk_len {}  clen {}  c1 {}  n1 {}",
            flen, pos_first, chunk_len, clen, c1, n1
        )));
    }
    let _ttl = buf.get_u64();
    let ts = buf.get_u64();
    let pulse = buf.get_u64();
    let ret = ChunkInfo {
        pos: p2,
        len: clen,
        ts,
        pulse,
    };
    Ok((Some(ret), file))
}

async fn read_chunk_at(mut file: File, pos: u64, chunk_len: Option<u64>) -> Result<(ChunkInfo, File), Error> {
    file.seek(SeekFrom::Start(pos)).await?;
    let mut buf = BytesMut::with_capacity(1024);
    let n1 = file.read_buf(&mut buf).await?;
    if n1 < 4 + 3 * 8 {
        return Err(Error::with_msg_no_trace(format!(
            "read_chunk_at can not read enough from datafile  n1 {}",
            n1
        )));
    }
    let clen = buf.get_u32() as u64;
    if let Some(chunk_len) = chunk_len {
        if clen != chunk_len {
            return Err(Error::with_msg_no_trace(format!(
                "read_chunk_at  mismatch: pos {}  clen {}  chunk_len {}",
                pos, clen, chunk_len
            )));
        }
    }
    let _ttl = buf.get_u64();
    let ts = buf.get_u64();
    let pulse = buf.get_u64();
    trace!("data chunk len {}  ts {}  pulse {}", clen, ts, pulse);
    let ret = ChunkInfo {
        pos,
        len: clen,
        ts,
        pulse,
    };
    Ok((ret, file))
}

pub struct IndexChannelHttpFunction {}

impl IndexChannelHttpFunction {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().eq("/api/1/maintenance/index/pulse/channel") {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(
        &self,
        req: Requ,
        ctx: &ReqCtx,
        node_config: &NodeConfigCached,
    ) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let ret = match Self::index(req, false, node_config).await {
            Ok(msg) => {
                let dt = ctx.ts_ctor().elapsed();
                let res = response(StatusCode::OK).body(body_string(msg))?;
                info!("IndexChannelHttpFunction  response dt {:.0} ms", 1e3 * dt.as_secs_f32());
                res
            }
            Err(e) => {
                error!("IndexChannelHttpFunction {e}");
                response(StatusCode::INTERNAL_SERVER_ERROR).body(body_string(format!("{:?}", e)))?
            }
        };
        Ok(ret)
    }

    async fn index(req: Requ, do_print: bool, node_config: &NodeConfigCached) -> Result<String, Error> {
        // TODO avoid double-insert on central storage.
        // TODO
        let (pgc, _pgjh) = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
        // TODO remove update of static columns when older clients are removed.
        let sql = "insert into map_pulse_files (channel, split, timebin, pulse_min, pulse_max, hostname, ks) values ($1, $2, $3, $4, $5, $6, $7) on conflict (channel, split, timebin) do update set pulse_min = $4, pulse_max = $5, upc1 = map_pulse_files.upc1 + 1, hostname = $6";
        let insert_01 = pgc.prepare(sql).await?;
        make_tables(&pgc).await?;
        let url = Url::parse(&format!("dummy:{}", req.uri()))
            .map_err(|e| Error::with_msg_no_trace(format!("{} {}", e, req.uri())))?;
        let pairs: std::collections::HashMap<_, _> = url.query_pairs().collect();
        let channel_name = if let Some(x) = pairs.get("name") {
            x
        } else {
            return Err(Error::with_msg_no_trace(format!("no name given")));
        };
        match Self::index_channel(&channel_name, &pgc, do_print, &insert_01, node_config).await {
            Ok(m) => Ok(m),
            Err(e) => {
                error!("error while indexing {}  {:?}", channel_name, e);
                let s = format!("error from index_channel {e}");
                Ok(s)
            }
        }
    }

    async fn index_channel(
        channel_name: &str,
        pgc: &dbconn::pg::Client,
        do_print: bool,
        insert_01: &dbconn::pg::Statement,
        node_config: &NodeConfigCached,
    ) -> Result<String, Error> {
        let mut msg = format!("Index channel {}", channel_name);
        let files = datafiles_for_channel(channel_name, node_config).await?;
        let mut files = files;
        files.sort();
        let files = files;
        msg = format!("{}\n{:?}", msg, files);
        let mut latest_pair = (0, 0);
        let files_from = files.len() - files.len().min(2);
        for mp in files[files_from..].into_iter() {
            match mp {
                MapfilePath::Scalar(path) => {
                    trace!("Scalar {path:?}");
                    let splitted: Vec<_> = path.to_str().unwrap().split("/").collect();
                    let timebin: u64 = splitted[splitted.len() - 3].parse()?;
                    let split: u64 = splitted[splitted.len() - 2].parse()?;
                    if false {
                        // Not needed, we any use only the last N files.
                        // TODO the timebin unit depends on the keyspace.
                        // In worst case could depend on the current channel config, and could have changed
                        // at each config change. That would be madness. Luckily, it seems always 1d for ks 2 and 3.
                        let timebin_dt = Duration::from_secs(60 * 60 * 24 * timebin);
                        let timebin_ts = SystemTime::UNIX_EPOCH.checked_add(timebin_dt).unwrap();
                        let tsnow = SystemTime::now();
                        if timebin_ts + Duration::from_secs(60 * 60 * 24 * 2) < tsnow {
                            debug!("FILTER PAST {timebin} {path:?}");
                        } else if timebin_ts > tsnow + Duration::from_secs(60 * 60 * 24 * 2) {
                            debug!("FILTER FUTU {timebin} {path:?}");
                        } else {
                            debug!("KEEP TIMEBI {timebin} {path:?}");
                        }
                    }
                    let file = match tokio::fs::OpenOptions::new().read(true).open(&path).await {
                        Ok(x) => x,
                        Err(e) => {
                            let e = Error::with_msg_no_trace(format!("MapfilePath::Scalar {e} {path:?}"));
                            error!("{e}");
                            return Err(e);
                        }
                    };
                    let (r2, file) = read_first_chunk(file).await?;
                    msg = format!("{}\n{:?}", msg, r2);
                    if let Some(r2) = r2 {
                        let (r3, _file) = read_last_chunk(file, r2.pos, r2.len).await?;
                        msg = format!("{}\n{:?}", msg, r3);
                        if let Some(r3) = r3 {
                            if r3.pulse > latest_pair.0 {
                                latest_pair = (r3.pulse, r3.ts);
                            }
                            pgc.execute(
                                insert_01,
                                &[
                                    &channel_name,
                                    &(split as i32),
                                    &(timebin as i32),
                                    &(r2.pulse as i64),
                                    &(r3.pulse as i64),
                                    &node_config.node.host,
                                    &(2 as i32),
                                ],
                            )
                            .await?;
                        } else {
                            warn!("could not find last event chunk in {path:?}");
                        }
                    } else {
                        warn!("could not find first event chunk in {path:?}");
                    }
                }
                MapfilePath::Index(path_index) => {
                    trace!("Index {path_index:?}");
                    let path_data = {
                        let fns = path_index.file_name().unwrap().to_str().unwrap();
                        path_index.parent().unwrap().join(&fns[..fns.len() - 6])
                    };
                    let splitted: Vec<_> = path_index.to_str().unwrap().split("/").collect();
                    let timebin: u64 = splitted[splitted.len() - 3].parse()?;
                    let split: u64 = splitted[splitted.len() - 2].parse()?;
                    let file_index = match tokio::fs::OpenOptions::new().read(true).open(&path_index).await {
                        Ok(x) => x,
                        Err(e) => {
                            let e = Error::with_msg_no_trace(format!("MapfilePath::Index {e} {path_index:?}"));
                            error!("{e}");
                            return Err(e);
                        }
                    };
                    let file_data = match tokio::fs::OpenOptions::new().read(true).open(&path_data).await {
                        Ok(x) => x,
                        Err(e) => {
                            let e = Error::with_msg_no_trace(format!("MapfilePath::Index {e} {path_data:?}"));
                            error!("{e}");
                            return Err(e);
                        }
                    };
                    let (r2, file_index, file_data) = read_first_index_chunk(file_index, file_data).await?;
                    msg = format!("{}\n{:?}", msg, r2);
                    if let Some(r2) = r2 {
                        let (r3, _file_index, _file_data) = read_last_index_chunk(file_index, file_data).await?;
                        msg = format!("{}\n{:?}", msg, r3);
                        if let Some(r3) = r3 {
                            if r3.pulse > latest_pair.0 {
                                latest_pair = (r3.pulse, r3.ts);
                            }
                            pgc.execute(
                                insert_01,
                                &[
                                    &channel_name,
                                    &(split as i32),
                                    &(timebin as i32),
                                    &(r2.pulse as i64),
                                    &(r3.pulse as i64),
                                    &node_config.node.host,
                                    &(3 as i32),
                                ],
                            )
                            .await?;
                        } else {
                            warn!("could not find last index chunk in {path_index:?}");
                        }
                    } else {
                        warn!("could not find first index chunk in {path_index:?}");
                    }
                }
            }
        }
        if do_print {
            if channel_name.contains("SAT-CVME-TIFALL5:EvtSet")
                || channel_name.contains("SINSB04")
                || channel_name.contains("SINSB03")
            {
                info!("latest for {channel_name}  {latest_pair:?}");
            }
        }
        Ok(msg)
    }
}

pub struct UpdateTaskGuard {
    do_abort: Arc<AtomicUsize>,
    // TODO allow user to explicitly stop and wait with timeout.
    // Protect against double-abort on Drop, or multiple calls.
    jh: Option<JoinHandle<Result<(), Error>>>,
}

impl UpdateTaskGuard {
    pub async fn abort_wait(&mut self) -> Result<(), Error> {
        if let Some(jh) = self.jh.take() {
            info!("UpdateTaskGuard::abort_wait");
            let fut = tokio::time::timeout(Duration::from_millis(20000), async { jh.await });
            Ok(fut.await???)
        } else {
            Ok(())
        }
    }
}

impl Drop for UpdateTaskGuard {
    fn drop(&mut self) {
        info!("impl Drop for UpdateTaskGuard");
        self.do_abort.fetch_add(1, Ordering::SeqCst);
    }
}

async fn update_task(do_abort: Arc<AtomicUsize>, node_config: NodeConfigCached) -> Result<(), Error> {
    let mut print_last = Instant::now();
    loop {
        if do_abort.load(Ordering::SeqCst) != 0 {
            info!("update_task  break A");
            break;
        }
        tokio::time::sleep(Duration::from_millis(10000 + (0x3fff & commonio::tokio_rand().await?))).await;
        if do_abort.load(Ordering::SeqCst) != 0 {
            info!("update_task  break B");
            break;
        }
        let ts1 = Instant::now();
        let do_print = if ts1.duration_since(print_last) >= Duration::from_millis(119000) {
            print_last = ts1;
            true
        } else {
            false
        };
        CACHE.housekeeping();

        // TODO do the actual work here
        let _ = node_config;
        match tokio::time::sleep(Duration::from_millis(100))
            .map(|_| Ok::<_, Error>(123u32))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("issue during last update task: {:?}", e);
                tokio::time::sleep(Duration::from_millis(20000)).await;
            }
        }

        let ts2 = Instant::now();
        let dt = ts2.duration_since(ts1);
        if do_print || dt >= Duration::from_millis(4000) {
            info!("Done update task  {:.0}ms", dt.as_secs_f32() * 1e3);
        }
    }
    Ok(())
}

pub struct UpdateTask {
    do_abort: Arc<AtomicUsize>,
    fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
    complete: bool,
}

impl Future for UpdateTask {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        loop {
            break if self.complete {
                panic!("poll on complete")
            } else if self.do_abort.load(Ordering::SeqCst) != 0 {
                self.complete = true;
                Ready(Ok(()))
            } else {
                match self.fut.poll_unpin(cx) {
                    Ready(res) => {
                        self.complete = true;
                        Ready(res)
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl UpdateTask {
    /// Returns a guard which must be kept alive as long as the service should run.
    /// Should instead of this use a system-timer and call the rest api.
    #[allow(unused)]
    fn _new(node_config: NodeConfigCached) -> UpdateTaskGuard {
        let do_abort = Arc::new(AtomicUsize::default());
        let task = Self {
            do_abort: do_abort.clone(),
            fut: Box::pin(update_task(do_abort.clone(), node_config)),
            complete: false,
        };
        let jh = tokio::spawn(task);
        let ret = UpdateTaskGuard { do_abort, jh: Some(jh) };
        ret
    }
}

async fn search_pulse(pulse: u64, path: &Path) -> Result<Option<u64>, Error> {
    let f1 = tokio::fs::OpenOptions::new().read(true).open(path).await?;
    let (ck1, f1) = read_first_chunk(f1).await?;
    if let Some(ck1) = ck1 {
        if ck1.pulse == pulse {
            return Ok(Some(ck1.ts));
        }
        if ck1.pulse > pulse {
            trace!("search_pulse  {}  lower than first {:?}", pulse, path);
            return Ok(None);
        }
        let (ck2, mut f1) = read_last_chunk(f1, ck1.pos, ck1.len).await?;
        if let Some(ck2) = ck2 {
            if ck2.pulse == pulse {
                return Ok(Some(ck2.ts));
            }
            if ck2.pulse < pulse {
                trace!("search_pulse  {}  higher than last {:?}", pulse, path);
                return Ok(None);
            }
            let chunk_len = ck1.len;
            let mut p1 = ck1.pos;
            let mut p2 = ck2.pos;
            loop {
                let d = p2 - p1;
                if 0 != d % chunk_len {
                    return Err(Error::with_msg_no_trace(format!("search_pulse  ")));
                }
                if d <= chunk_len {
                    trace!("search_pulse  {}  not in {:?}", pulse, path);
                    return Ok(None);
                }
                let m = p1 + d / chunk_len / 2 * chunk_len;
                let (z, f2) = read_chunk_at(f1, m, Some(chunk_len)).await?;
                f1 = f2;
                if z.pulse == pulse {
                    return Ok(Some(z.ts));
                } else if z.pulse > pulse {
                    p2 = m;
                } else {
                    p1 = m;
                }
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

async fn search_index_pulse(pulse: u64, path: &Path) -> Result<Option<u64>, Error> {
    let fn_index = format!("{}_Index", path.file_name().unwrap().to_str().unwrap());
    let path_index = path.parent().unwrap().join(fn_index);
    let f1 = tokio::fs::OpenOptions::new().read(true).open(path_index).await?;
    let f2 = tokio::fs::OpenOptions::new().read(true).open(path).await?;
    let (ck1, f1, f2) = read_first_index_chunk(f1, f2).await?;
    if let Some(ck1) = ck1 {
        if ck1.pulse == pulse {
            return Ok(Some(ck1.ts));
        }
        if ck1.pulse > pulse {
            return Ok(None);
        }
        let (ck2, mut f1, mut f2) = read_last_index_chunk(f1, f2).await?;
        if let Some(ck2) = ck2 {
            if ck2.pulse == pulse {
                return Ok(Some(ck2.ts));
            }
            if ck2.pulse < pulse {
                return Ok(None);
            }
            let index_entry_len = 16;
            let chunk_len = index_entry_len;
            let mut p1 = ck1.pos_index;
            let mut p2 = ck2.pos_index;
            loop {
                let d = p2 - p1;
                if 0 != d % index_entry_len {
                    return Err(Error::with_msg_no_trace(format!("search_pulse  ")));
                }
                if d <= chunk_len {
                    return Ok(None);
                }
                let m = p1 + d / chunk_len / 2 * chunk_len;
                let (z, f1b, f2b) = async {
                    f1.seek(SeekFrom::Start(m)).await?;
                    let mut buf3 = [0; 16];
                    f1.read_exact(&mut buf3).await?;
                    let ts = u64::from_be_bytes(buf3[0..8].try_into()?);
                    let pos_data = u64::from_be_bytes(buf3[8..16].try_into()?);
                    trace!("search loop in index  m {m}  ts {ts}  pos_data {pos_data}");
                    let (chunk, f2) = read_chunk_at(f2, pos_data, None).await?;
                    trace!("search loop read_chunk_at successful: {chunk:?}");
                    let ret = IndexChunkInfo {
                        pos_index: p2,
                        pos_data: chunk.pos,
                        len: chunk.len,
                        ts: chunk.ts,
                        pulse: chunk.pulse,
                    };
                    Ok::<_, Error>((ret, f1, f2))
                }
                .await?;
                f1 = f1b;
                f2 = f2b;
                if z.pulse == pulse {
                    return Ok(Some(z.ts));
                } else if z.pulse > pulse {
                    p2 = m;
                } else {
                    p1 = m;
                }
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapPulseQuery {
    pub backend: String,
    pub pulse: u64,
}

impl HasBackend for MapPulseQuery {
    fn backend(&self) -> &str {
        &self.backend
    }
}

impl HasTimeout for MapPulseQuery {
    fn timeout(&self) -> Option<Duration> {
        Some(MAP_PULSE_QUERY_TIMEOUT)
    }
}

impl FromUrl for MapPulseQuery {
    type Error = Error;

    fn from_url(url: &url::Url) -> Result<Self, Self::Error> {
        let mut pit = url
            .path_segments()
            .ok_or_else(|| Error::with_msg_no_trace(format!("no path in url  {url}")))?
            .rev();
        let pulsestr = pit
            .next()
            .ok_or_else(|| Error::with_msg_no_trace(format!("no pulse in url path  {pit:?}")))?;
        let backend = pit.next().unwrap_or("sf-databuffer").into();
        // TODO legacy: use a default backend if not specified.
        let backend = if backend == "pulse" {
            String::from("sf-databuffer")
        } else {
            backend
        };
        let pulse: u64 = pulsestr.parse()?;
        let ret = Self { backend, pulse };
        trace!("FromUrl {ret:?}");
        Ok(ret)
    }

    fn from_pairs(_pairs: &BTreeMap<String, String>) -> Result<Self, Self::Error> {
        Err(Error::with_msg_no_trace(format!("can not only construct from pairs")))
    }
}

impl AppendToUrl for MapPulseQuery {
    fn append_to_url(&self, _url: &mut Url) {}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalMap {
    pulse: u64,
    tss: Vec<u64>,
    channels: Vec<String>,
}

pub trait ErrConv<T> {
    fn err_conv(self) -> Result<T, daqbuf_err::Error>;
}

impl<T> ErrConv<T> for Result<T, scylla::errors::NewSessionError> {
    fn err_conv(self) -> Result<T, daqbuf_err::Error> {
        self.map_err(|e| daqbuf_err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

impl<T> ErrConv<T> for Result<T, scylla::deserialize::TypeCheckError> {
    fn err_conv(self) -> Result<T, daqbuf_err::Error> {
        self.map_err(|e| daqbuf_err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

impl<T> ErrConv<T> for Result<T, scylla::errors::PagerExecutionError> {
    fn err_conv(self) -> Result<T, daqbuf_err::Error> {
        self.map_err(|e| daqbuf_err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

impl<T> ErrConv<T> for Result<T, scylla::errors::NextRowError> {
    fn err_conv(self) -> Result<T, daqbuf_err::Error> {
        self.map_err(|e| daqbuf_err::Error::with_msg_no_trace(format!("{e:?}")))
    }
}

pub struct MapPulseScyllaHandler {}

impl MapPulseScyllaHandler {
    pub fn prefix() -> &'static str {
        "/api/4/scylla/map/pulse/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let url = req_uri_to_url(req.uri())?;
        let query = MapPulseQuery::from_url(&url)?;
        let pulse = query.pulse;
        let scyconf = if let Some(x) = node_config.node_config.cluster.scylla_st() {
            x
        } else {
            return Err(Error::with_public_msg_no_trace("no scylla configured"));
        };
        let scy = scyllaconn::conn::create_scy_session(&scyconf).await?;
        let pulse_a = (pulse >> 14) as i64;
        let pulse_b = (pulse & 0x3fff) as i32;
        let mut it = scy
            .query_iter(
                "select ts_a, ts_b from pulse where pulse_a = ? and pulse_b = ?",
                (pulse_a, pulse_b),
            )
            .await
            .err_conv()?
            .rows_stream::<(i64, i32)>()
            .err_conv()?;
        let ch = "pulsemaptable";
        let mut tss = Vec::new();
        let mut channels = Vec::new();
        while let Some(row) = it.try_next().await.err_conv()? {
            let ts_a = row.0 as u64;
            let ts_b = row.1 as u64;
            tss.push(ts_a * netpod::timeunits::SEC + ts_b);
            channels.push(ch.into());
        }
        let ret = LocalMap { pulse, tss, channels };
        Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&ret)?))?)
    }
}

fn extract_path_number_after_prefix(req: &Requ, prefix: &str) -> Result<u64, Error> {
    let v: Vec<_> = req.uri().path().split(prefix).collect();
    if v.len() < 2 {
        return Err(Error::with_msg_no_trace(format!(
            "extract_path_number_after_prefix can not understand url {}  prefix {}",
            req.uri(),
            prefix
        )));
    }
    v[1].parse().map_err(|_| {
        Error::with_public_msg_no_trace(format!(
            "extract_path_number_after_prefix can not understand url {}  prefix {}",
            req.uri(),
            prefix
        ))
    })
}

pub struct MapPulseLocalHttpFunction {}

impl MapPulseLocalHttpFunction {
    pub fn prefix() -> &'static str {
        "/api/1/map/pulse/local/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let pulse = extract_path_number_after_prefix(&req, Self::prefix())?;
        let req_from = req
            .headers()
            .get("x-req-from")
            .map(|x| {
                x.to_str()
                    .map(|x| x.to_string())
                    .unwrap_or_else(|_| format!("bad string in x-req-from"))
            })
            .unwrap_or_else(|| String::from("missing x-req-from"));
        let ts1 = Instant::now();
        // TODO
        let (conn, _pgjh) = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
        let sql = "select channel, hostname, timebin, split, ks from map_pulse_files where hostname = $1 and pulse_min <= $2 and (pulse_max >= $2 or closed = 0)";
        let rows = conn.query(sql, &[&node_config.node.host, &(pulse as i64)]).await?;
        let cands: Vec<_> = rows
            .iter()
            .map(|r| {
                let channel: String = r.try_get(0).unwrap_or("nochannel".into());
                let hostname: String = r.try_get(1).unwrap_or("nohost".into());
                let timebin: i32 = r.try_get(2).unwrap_or(0);
                let split: i32 = r.try_get(3).unwrap_or(0);
                let ks: i32 = r.try_get(4).unwrap_or(0);
                (channel, hostname, timebin as u32, split as u32, ks as u32)
            })
            .collect();
        let dt = Instant::now().duration_since(ts1);
        if dt >= Duration::from_millis(300) {
            info!(
                "map pulse local  req-from {:?}  candidate list in {:.0}ms",
                req_from,
                dt.as_secs_f32() * 1e3
            );
        }
        let mut futs = FuturesUnordered::new();
        for (ch, hostname, tb, sp, ks) in cands {
            futs.push(Self::search(pulse, ch, hostname, tb, sp, ks, node_config));
        }
        let mut tss = Vec::new();
        let mut channels = Vec::new();
        use futures_util::StreamExt;
        while let Some(k) = futs.next().await {
            match k {
                Ok(item) => match item {
                    Some((ts, ch)) => {
                        tss.push(ts);
                        channels.push(ch);
                    }
                    None => {}
                },
                Err(e) => {
                    error!("{e}");
                }
            }
        }
        let ret = LocalMap { pulse, tss, channels };
        Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&ret)?))?)
    }

    async fn search(
        pulse: u64,
        ch: String,
        hostname: String,
        tb: u32,
        sp: u32,
        ks: u32,
        node_config: &NodeConfigCached,
    ) -> Result<Option<(u64, String)>, Error> {
        trace!(
            "search in  ks {}  sp {}  tb {}  host {}  ch {}",
            ks,
            sp,
            tb,
            hostname,
            ch
        );
        if ks == 2 {
            match disk::paths::data_path_tb(ks, &ch, tb, 1000 * 60 * 60 * 24, sp, &node_config.node) {
                Ok(path) => match search_pulse(pulse, &path).await {
                    Ok(ts) => {
                        if let Some(ts) = ts {
                            info!("pulse {pulse}  found in  ks {ks}  sp {sp}  tb {tb}  ch {ch}  ts {ts}");
                            Ok(Some((ts, ch)))
                        } else {
                            Ok(None)
                        }
                    }
                    Err(e) => {
                        let e = Error::with_msg_no_trace(format!(
                            "pulse {pulse}  can not map  ks {ks}  sp {sp}  tb {tb}  ch {ch}  {e}"
                        ));
                        error!("{e}");
                        return Err(e);
                    }
                },
                Err(e) => {
                    let e = Error::with_msg_no_trace(format!(
                        "pulse {pulse}  no path  ks {ks}  sp {sp}  tb {tb}  ch {ch}  {e}"
                    ));
                    error!("{e}");
                    return Err(e);
                }
            }
        } else if ks == 3 {
            match disk::paths::data_path_tb(ks, &ch, tb, 1000 * 60 * 60 * 24, sp, &node_config.node) {
                Ok(path) => match search_index_pulse(pulse, &path).await {
                    Ok(ts) => {
                        if let Some(ts) = ts {
                            info!(
                                "pulse {}  found in  ks {}  sp {}  tb {}  ch {}  ts {}",
                                pulse, ks, sp, tb, ch, ts
                            );
                            Ok(Some((ts, ch)))
                        } else {
                            Ok(None)
                        }
                    }
                    Err(e) => {
                        let e = Error::with_msg_no_trace(format!(
                            "pulse {pulse}  can not map  ks {ks}  sp {sp}  tb {tb}  ch {ch}  {e}"
                        ));
                        error!("{e}");
                        return Err(e);
                    }
                },
                Err(e) => {
                    let e = Error::with_msg_no_trace(format!(
                        "pulse {pulse}  no path  ks {ks}  sp {sp}  tb {tb}  ch {ch}  {e}"
                    ));
                    error!("{e}");
                    return Err(e);
                }
            }
        } else {
            let e = Error::with_msg_no_trace(format!(
                "pulse {pulse}  bad keyspace  ks {ks}  sp {sp}  tb {tb}  ch {ch}"
            ));
            error!("{e}");
            return Err(e);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsHisto {
    pulse: u64,
    tss: Vec<u64>,
    counts: Vec<u64>,
}

pub struct MapPulseHistoHttpFunction {}

impl MapPulseHistoHttpFunction {
    pub fn prefix() -> &'static str {
        "/api/1/map/pulse/histo/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let pulse = extract_path_number_after_prefix(&req, Self::prefix())?;
        let ret = Self::histo(pulse, node_config).await?;
        Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&ret)?))?)
    }

    pub async fn histo(pulse: u64, node_config: &NodeConfigCached) -> Result<TsHisto, Error> {
        let mut futs = FuturesOrdered::new();
        for node in &node_config.node_config.cluster.nodes {
            let s = format!(
                "http://{}:{}{}{}",
                node.host,
                node.port,
                MapPulseLocalHttpFunction::prefix(),
                pulse
            );
            let uri: Uri = s.parse()?;
            let req = Request::get(&uri)
                .header(header::HOST, uri.host().unwrap())
                .header("x-req-from", &node_config.node.host)
                .body(body_empty())?;
            let fut = async move {
                match connect_client(req.uri()).await {
                    Ok(mut client) => {
                        let fut = client.send_request(req);
                        tokio::time::timeout(MAP_PULSE_LOCAL_TIMEOUT, fut)
                            .await
                            .map(|x| x.map_err(Error::from_to_string))
                    }
                    Err(e) => Ok(Err(Error::from_to_string(e))),
                }
            };
            futs.push_back(fut);
        }
        use futures_util::stream::StreamExt;
        let mut map = BTreeMap::new();
        while let Some(futres) = futs.next().await {
            match futres {
                Ok(res) => match res {
                    Ok(res) => match read_body_bytes(res.into_body()).await {
                        Ok(body) => match serde_json::from_slice::<LocalMap>(&body) {
                            Ok(lm) => {
                                for ts in lm.tss {
                                    let a = map.get(&ts);
                                    if let Some(&j) = a {
                                        map.insert(ts, j + 1);
                                    } else {
                                        map.insert(ts, 1);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("pulse map sub request  pulse {pulse}  serde error {e}");
                            }
                        },
                        Err(e) => {
                            error!("pulse map sub request  pulse {pulse}  body error {e}");
                        }
                    },
                    Err(e) => {
                        error!("pulse map sub request  pulse {pulse}  error {e}");
                    }
                },
                Err(e) => {
                    let _: Elapsed = e;
                    error!("pulse map sub request timed out  pulse {pulse}");
                }
            }
        }
        let ret = TsHisto {
            pulse,
            tss: map.keys().map(|j| *j).collect(),
            counts: map.values().map(|j| *j).collect(),
        };
        info!("pulse {pulse}  histo {ret:?}");
        Ok(ret)
    }
}

pub struct MapPulseHttpFunction {}

impl MapPulseHttpFunction {
    pub fn prefix() -> &'static str {
        "/api/1/map/pulse/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        use crate::cache::CachePortal;
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        trace!("MapPulseHttpFunction  handle  uri: {:?}", req.uri());
        let pulse = extract_path_number_after_prefix(&req, Self::prefix())?;
        if USE_CACHE {
            match CACHE.portal(pulse) {
                CachePortal::Fresh => {
                    let histo = MapPulseHistoHttpFunction::histo(pulse, node_config).await?;
                    let mut i1 = 0;
                    let mut max = 0;
                    for i2 in 0..histo.tss.len() {
                        if histo.counts[i2] > max {
                            max = histo.counts[i2];
                            i1 = i2;
                        }
                    }
                    if max > 0 {
                        let val = histo.tss[i1];
                        CACHE.set_value(pulse, val);
                        Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&val)?))?)
                    } else {
                        Ok(response(StatusCode::NO_CONTENT).body(body_empty())?)
                    }
                }
                CachePortal::Existing(rx) => {
                    trace!("waiting for already running pulse map  pulse {pulse}");
                    match rx.recv().await {
                        Ok(_) => {
                            error!("should never recv from existing operation  pulse {pulse}");
                            Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
                        }
                        Err(_e) => match CACHE.portal(pulse) {
                            CachePortal::Known(ts) => {
                                info!("pulse {pulse}  known from cache  ts {ts}");
                                Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&ts)?))?)
                            }
                            CachePortal::Fresh => {
                                error!("pulse {pulse}  woken up, but fresh");
                                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
                            }
                            CachePortal::Existing(..) => {
                                error!("pulse {pulse}  woken up, but existing");
                                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
                            }
                        },
                    }
                }
                CachePortal::Known(ts) => {
                    info!("pulse {pulse}  in cache  ts {ts}");
                    Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&ts)?))?)
                }
            }
        } else {
            let histo = MapPulseHistoHttpFunction::histo(pulse, node_config).await?;
            let mut i1 = 0;
            let mut max = 0;
            for i2 in 0..histo.tss.len() {
                if histo.counts[i2] > max {
                    max = histo.counts[i2];
                    i1 = i2;
                }
            }
            if max > 0 {
                let val = histo.tss[i1];
                Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&val)?))?)
            } else {
                Ok(response(StatusCode::NO_CONTENT).body(body_empty())?)
            }
        }
    }
}

pub struct Api4MapPulseHttpFunction {}

impl Api4MapPulseHttpFunction {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(API_4_MAP_PULSE_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub fn path_matches(path: &str) -> bool {
        path.starts_with(API_4_MAP_PULSE_URL_PREFIX)
    }

    pub async fn find_timestamp(q: MapPulseQuery, ncc: &NodeConfigCached) -> Result<Option<u64>, Error> {
        use crate::cache::CachePortal;
        let pulse = q.pulse;
        let res = if USE_CACHE {
            match CACHE.portal(pulse) {
                CachePortal::Fresh => {
                    trace!("value not yet in cache  pulse {pulse}");
                    let histo = MapPulseHistoHttpFunction::histo(pulse, ncc).await?;
                    let mut i1 = 0;
                    let mut max = 0;
                    for i2 in 0..histo.tss.len() {
                        if histo.counts[i2] > max {
                            max = histo.counts[i2];
                            i1 = i2;
                        }
                    }
                    if histo.tss.len() > 1 {
                        warn!("Ambigious pulse map  pulse {}  histo {:?}", pulse, histo);
                    }
                    if max > 0 {
                        let val = histo.tss[i1];
                        CACHE.set_value(pulse, val);
                        Ok(Some(val))
                    } else {
                        Ok(None)
                    }
                }
                CachePortal::Existing(rx) => {
                    trace!("waiting for already running pulse map  pulse {pulse}");
                    match rx.recv().await {
                        Ok(_) => {
                            error!("should never recv from existing operation  pulse {pulse}");
                            Err(Error::with_msg_no_trace("map pulse error"))
                        }
                        Err(_e) => {
                            trace!("woken up while value wait  pulse {pulse}");
                            match CACHE.portal(pulse) {
                                CachePortal::Known(val) => {
                                    trace!("good, value after wakeup  pulse {pulse}");
                                    Ok(Some(val))
                                }
                                CachePortal::Fresh => {
                                    error!("woken up, but portal fresh  pulse {pulse}");
                                    Err(Error::with_msg_no_trace("map pulse error"))
                                }
                                CachePortal::Existing(..) => {
                                    error!("woken up, but portal existing  pulse {pulse}");
                                    Err(Error::with_msg_no_trace("map pulse error"))
                                }
                            }
                        }
                    }
                }
                CachePortal::Known(val) => {
                    trace!("value already in cache  pulse {pulse}  ts {val}");
                    Ok(Some(val))
                }
            }
        } else {
            trace!("value not yet in cache  pulse {pulse}");
            let histo = MapPulseHistoHttpFunction::histo(pulse, ncc).await?;
            let mut i1 = 0;
            let mut max = 0;
            for i2 in 0..histo.tss.len() {
                if histo.counts[i2] > max {
                    max = histo.counts[i2];
                    i1 = i2;
                }
            }
            if histo.tss.len() > 1 {
                warn!("Ambigious pulse map  pulse {}  histo {:?}", pulse, histo);
            }
            if max > 0 {
                let val = histo.tss[i1];
                Ok(Some(val))
            } else {
                Ok(None)
            }
        };
        res
    }

    pub async fn handle(&self, req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let ts1 = Instant::now();
        debug!("Api4MapPulseHttpFunction  handle  uri: {:?}", req.uri());
        let url = req_uri_to_url(req.uri())?;
        let q = MapPulseQuery::from_url(&url)?;
        let ret = match Self::find_timestamp(q, ncc).await {
            Ok(Some(val)) => Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&val)?))?),
            Ok(None) => Ok(response(StatusCode::NO_CONTENT).body(body_empty())?),
            Err(e) => {
                error!("find_timestamp {e}");
                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
            }
        };
        let ts2 = Instant::now();
        let dt = ts2.duration_since(ts1);
        if dt > Duration::from_millis(1500) {
            warn!("Api4MapPulseHttpFunction  took {:.2}s", dt.as_secs_f32());
        }
        ret
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Api4MapPulse2Response {
    sec: u64,
    ns: u64,
    datetime: String,
}

pub struct Api4MapPulse2HttpFunction {}

impl Api4MapPulse2HttpFunction {
    pub fn path_prefix() -> &'static str {
        "/api/4/map/pulse-v2/"
    }

    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(Self::path_prefix()) {
            Some(Self {})
        } else {
            None
        }
    }

    pub fn path_matches(path: &str) -> bool {
        path.starts_with(Self::path_prefix())
    }

    pub async fn handle(&self, req: Requ, ncc: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        let ts1 = Instant::now();
        debug!("Api4MapPulse2HttpFunction  handle  uri: {:?}", req.uri());
        let url = req_uri_to_url(req.uri())?;
        let q = MapPulseQuery::from_url(&url)?;
        let ret = match Api4MapPulseHttpFunction::find_timestamp(q, ncc).await {
            Ok(Some(val)) => {
                let sec = val / SEC;
                let ns = val % SEC;
                let datetime = Utc
                    .timestamp_opt(sec as i64, ns as u32)
                    .earliest()
                    .ok_or_else(|| Error::with_msg_no_trace("DateTime earliest fail"))?
                    .format(DATETIME_FMT_9MS)
                    .to_string();
                let res = Api4MapPulse2Response { sec, ns, datetime };
                Ok(response(StatusCode::OK).body(body_string(serde_json::to_string(&res)?))?)
            }
            Ok(None) => Ok(response(StatusCode::NO_CONTENT).body(body_empty())?),
            Err(e) => {
                error!("find_timestamp {e}");
                Ok(response(StatusCode::INTERNAL_SERVER_ERROR).body(body_empty())?)
            }
        };
        let ts2 = Instant::now();
        let dt = ts2.duration_since(ts1);
        if dt > Duration::from_millis(1500) {
            warn!("Api4MapPulseHttpFunction  took {:.2}s", dt.as_secs_f32());
        }
        ret
    }
}

pub struct MarkClosedHttpFunction {}

impl MarkClosedHttpFunction {
    pub fn handler(req: &Requ) -> Option<Self> {
        if req.uri().path().starts_with(MAP_PULSE_MARK_CLOSED_URL_PREFIX) {
            Some(Self {})
        } else {
            None
        }
    }

    pub async fn handle(&self, req: Requ, node_config: &NodeConfigCached) -> Result<StreamResponse, Error> {
        if req.method() != Method::GET {
            return Ok(response(StatusCode::NOT_ACCEPTABLE).body(body_empty())?);
        }
        info!("MarkClosedHttpFunction  handle  uri: {:?}", req.uri());
        match MarkClosedHttpFunction::mark_closed(node_config).await {
            Ok(_) => {
                let ret = response(StatusCode::OK).body(body_empty())?;
                Ok(ret)
            }
            Err(e) => {
                let msg = format!("{:?}", e);
                let ret = response(StatusCode::INTERNAL_SERVER_ERROR).body(body_string(msg))?;
                Ok(ret)
            }
        }
    }

    pub async fn mark_closed(node_config: &NodeConfigCached) -> Result<(), Error> {
        let (conn, pgjh) = dbconn::create_connection(&node_config.node_config.cluster.database).await?;
        // TODO
        let _ = &pgjh;
        let sql = "select distinct channel from map_pulse_files order by channel";
        let rows = conn.query(sql, &[]).await?;
        let chns: Vec<_> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        for chn in &chns {
            let sql = concat!(
                "with q1 as (select channel, split, timebin from map_pulse_files",
                " where channel = $1 and hostname = $2",
                " order by timebin desc offset 2)",
                " update map_pulse_files t2 set closed = 1 from q1",
                " where t2.channel = q1.channel",
                " and t2.closed = 0",
                " and t2.split = q1.split",
                " and t2.timebin = q1.timebin",
            );
            let nmod = conn.execute(sql, &[&chn, &node_config.node.host]).await?;
            info!(
                "mark files  mod {}  chn {:?}  host {:?}",
                nmod, chn, node_config.node.host
            );
        }
        Ok(())
    }
}
