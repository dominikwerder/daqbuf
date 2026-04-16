use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::range::ScyllaSeriesRange;
use futures_util::Future;
use futures_util::Stream;
use futures_util::TryStreamExt;
use netpod::TsMs;
use netpod::log;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;

macro_rules! trace_msp { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }
macro_rules! log_fetch_result { ($($arg:tt)*) => { if true { log::trace!("fetch  {}", format_args!($($arg)*)); } }; }
macro_rules! debug_scy6 { ($($arg:tt)*) => { if false { log::debug!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "EventsMsp"),
    enum variants {
        Logic,
        Worker(Box<crate::worker::Error>),
        ScyllaRow(#[from] scylla::errors::NextRowError),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        TooManyRows,
    },
);

impl From<crate::worker::Error> for Error {
    fn from(value: crate::worker::Error) -> Self {
        Self::Worker(Box::new(value))
    }
}

enum Resolvable<F>
where
    F: Future,
{
    Future(F),
    Output(<F as Future>::Output),
    Taken,
}

impl<F> Resolvable<F>
where
    F: Future,
{
    fn take(&mut self) -> Option<<F as Future>::Output> {
        let x = std::mem::replace(self, Resolvable::Taken);
        match x {
            Resolvable::Future(_) => None,
            Resolvable::Output(x) => Some(x),
            Resolvable::Taken => None,
        }
    }

    fn is_taken(&self) -> bool {
        if let Self::Taken = self { true } else { false }
    }
}

fn trait_assert<T>(_: T)
where
    T: Stream + Unpin + Send,
{
}

#[allow(unused)]
fn trait_assert_try() {
    // let x: MspStreamRt = phantomval();
    // trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}

async fn _find_ts_msp(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    limit: Option<u32>,
    bck: bool,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    stmts: &StmtsEventsQueryOpts,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let selfname = "find_ts_msp";
    trace_msp!("{selfname}  bck {bck}  series  {:?}  {:?}  {}", rt, series, range.fmt());
    if bck {
        if daqbuf_series::dbg::dbg_check_scy6(daqbuf_series::SeriesId::new(series)) {
            log::info!("{selfname}  bck {bck}  with scy6 check");
            let res1 = find_ts_msp_bck(rt, series, range.clone(), limit, stmts, scy).await?;
            let res2 = find_ts_msp_bck_workaround(rt, series, range, stmts, scy).await?;
            if res1 != res2 {
                log::error!(
                    "{selfname}  bck {bck}  workaround and normal differ  workaround {:?}  normal {:?}",
                    res2,
                    res1
                );
            } else {
                log::info!("{selfname}  bck {bck}  workaround and normal agree");
            }
            Ok(res1)
        } else {
            if true {
                find_ts_msp_bck_workaround(rt, series, range, stmts, scy).await
            } else {
                find_ts_msp_bck(rt, series, range, limit, stmts, scy).await
            }
        }
    } else {
        find_ts_msp_fwd(rt, series, range, limit, stmts, scy).await
    }
}

async fn find_ts_msp_fwd(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    limit: Option<u32>,
    stmts: &StmtsEventsQueryOpts,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let selfname = "find_ts_msp_fwd";
    let _ = rt;
    let mut ret = VecDeque::new();
    // TODO time range truncation can be handled better
    let stmt = stmts.ts_msp_fwd().clone();
    let limit = limit.unwrap_or(40);
    let params = (
        series as i64,
        range.beg().ms() as i64,
        1 + range.end().ms() as i64,
        limit as i32,
    );
    debug_scy6!("{selfname}  EXECUTE {cql}  {params:?}", cql = stmt.get_statement());
    log_fetch_result!("{selfname}  {:?}", params);
    let mut res = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
    while let Some(row) = res.try_next().await? {
        let ts = TsMs::from_ms_u64(row.0 as u64);
        log_fetch_result!("{selfname}  {params:?}  {ts}");
        ret.push_back(ts);
    }
    Ok(ret)
}

async fn find_ts_msp_bck(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    limit: Option<u32>,
    stmts: &StmtsEventsQueryOpts,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let selfname = "find_ts_msp_bck";
    let _ = rt;
    let mut ret = VecDeque::new();
    let stmt = stmts.ts_msp_bck().clone();
    let limit = limit.unwrap_or(2);
    let params = (series as i64, range.beg().ms() as i64, limit as i32);
    debug_scy6!("{selfname}  EXECUTE {cql}  {params:?}", cql = stmt.get_statement());
    log_fetch_result!("{selfname}  {:?}", params);
    let mut res = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
    while let Some(row) = res.try_next().await? {
        let ts = TsMs::from_ms_u64(row.0 as u64);
        log_fetch_result!("{selfname}  {params:?}  {ts}");
        ret.push_front(ts);
    }
    Ok(ret)
}

// Workaround because scylla's order by desc is broken at the moment.
async fn find_ts_msp_bck_workaround(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    stmts: &StmtsEventsQueryOpts,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let selfname = "find_ts_msp_bck_workaround";
    let _ = rt;
    let mut ret = Vec::new();
    let stmt = stmts.ts_msp_bck_workaround().clone();
    let params = (series as i64, 0 as i64, i64::MAX);
    debug_scy6!("{selfname}  EXECUTE {cql}  {params:?}", cql = stmt.get_statement());
    log_fetch_result!("{selfname}  {:?}", params);
    let mut res = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
    let mut c = 0;
    while let Some(row) = res.try_next().await? {
        c += 1;
        let ts = TsMs::from_ms_u64(row.0 as u64);
        if ts >= range.beg().to_ts_ms() {
            log_fetch_result!("{selfname}  {params:?}  {ts}  DISCARD AFTER RANGE");
        } else if ret.len() > 1024 * 1024 {
            return Err(Error::TooManyRows);
        } else {
            log_fetch_result!("{selfname}  {params:?}  {ts}  USE");
            ret.push(ts);
        }
    }
    log_fetch_result!("{selfname}  {params:?}  considered msp {c}");
    if ret.len() > 1024 * 10 {
        log::info!("quite many ts_msp values in reverse lookup  len {}", ret.len());
    }
    if ret.len() > 1024 * 160 {
        log::warn!("quite many ts_msp values in reverse lookup  len {}", ret.len());
    }
    let m = ret.len().max(2) - 2;
    log_fetch_result!("{selfname}  {params:?}  {ret:?}  {m}");
    let ret = ret.into_iter().skip(m).collect();
    log_fetch_result!("{selfname}  {params:?}  {ret:?}");
    Ok(ret)
}
