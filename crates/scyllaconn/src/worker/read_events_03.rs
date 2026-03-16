const READ_NEXT_TIMEOUT: Duration = Duration::from_millis(5000);
const QUERY_PAGE_SIZE: i32 = 512;

mod datatypes;

use crate::events2::prepare::StmtsEvents;
use crate::events2::prepare::StmtsEventsClusterKeyspace;
use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::events3::jobtrace::ReadEventKind;
use crate::events3::jobtrace::ReadJobTrace;
use crate::range::ScyllaSeriesRange;
use crate::worker::ReadEvents03FwdParams;
use daqbuf_series::SeriesId;
use datatypes::ValTyDyn;
use futures_util::TryFutureExt;
use items_0::timebin::BinningggContainerEventsDyn;
use netpod::DtNano;
use netpod::TsMs;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }
macro_rules! trace_fetch { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }
macro_rules! trace4 { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "ScyllaEvents"),
    enum variants {
        Worker(Box<crate::worker::Error>),
        Msp(#[from] crate::events2::msp::Error),
        Datatypes(#[from] datatypes::Error),
        Timeout,
        Unordered,
        OutOfRange,
        BadBatch,
        ReadQueueEmptyBck,
        ReadQueueEmptyFwd,
        Logic,
        TruncateLogic,
        AlreadyTaken,
        DrainFailure,
        RangeEndOverflow,
        NotTokenAware,
        Prepare(#[from] crate::events2::prepare::Error),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaWorker(Box<crate::worker::Error>),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        UserTriggered,
    },
);

struct ScySessionRef<'a>(&'a scylla::client::session::Session);

impl<'a> Deref for ScySessionRef<'a> {
    type Target = scylla::client::session::Session;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a> Clone for ScySessionRef<'a> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

#[derive(Debug)]
pub(super) struct ReadNextValuesOpts {
    rt: RetentionTime,
    series: SeriesId,
    ts_msp: TsMs,
    range: ScyllaSeriesRange,
    with_values: bool,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    val_ty_dyn: Box<dyn ValTyDyn>,
}

async fn read_next_values_fwd(
    opts: ReadNextValuesOpts,
    stmts: &StmtsEventsQueryOpts,
    scy: ScySessionRef<'_>,
    jobtrace: &mut ReadJobTrace,
) -> Result<Box<dyn BinningggContainerEventsDyn>, Error> {
    let selfname = "read_next_values_fwd";
    let bck = false;
    let val_ty_dyn = &opts.val_ty_dyn;
    trace_fetch!("{selfname}  {:?}  st_name {}", opts, val_ty_dyn.st_name());
    let series = opts.series;
    let ts_msp = opts.ts_msp;
    let range = opts.range;
    let table_name = val_ty_dyn.table_name();
    let with_values = opts.with_values;
    if taskrun::trigger_error_worker_scylla_events(None) {
        return Err(Error::UserTriggered);
    }
    if range.end() > TsNano::from_ns(i64::MAX as u64) {
        return Err(Error::RangeEndOverflow);
    }
    let ts_lsp_min = if range.beg() > ts_msp.ns() {
        range.beg().delta(ts_msp.ns())
    } else {
        DtNano::from_ns(0)
    };
    let ts_lsp_max = if range.end() > ts_msp.ns() {
        range.end().delta(ts_msp.ns())
    } else {
        DtNano::from_ns(0)
    };
    trace_fetch!(
        "{selfname}  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  {}",
        ts_msp.fmt(),
        ts_lsp_min,
        ts_lsp_max,
        table_name
    );
    let qu = stmts
        .lsp(bck, with_values)
        .shape(val_ty_dyn.is_valueblob())
        .st(val_ty_dyn.st_name())?;
    let qu = {
        let mut qu = qu.clone();
        if qu.is_token_aware() == false {
            return Err(Error::NotTokenAware);
        }
        qu.set_page_size(QUERY_PAGE_SIZE);
        qu
    };
    let params = (
        series.to_i64(),
        ts_msp.ms() as i64,
        ts_lsp_min.ns() as i64,
        ts_lsp_max.ns() as i64,
    );
    trace4!("{selfname}  EXECUTE  {cql}  {params:?}", cql = qu.get_statement());
    trace_fetch!("{selfname} event search  params {:?}", params);
    jobtrace.add_event_now(ReadEventKind::CallExecuteIter);
    let res = scy.execute_iter(qu.clone(), params).await?;
    let ret = opts.val_ty_dyn.read_into_container(res, ts_msp, with_values).await?;
    let byte_est = ret.byte_estimate();
    trace_fetch!(
        "{selfname}  read  ts_msp {}  len {}  byte_est {}",
        ts_msp.fmt(),
        ret.len(),
        byte_est
    );
    Ok(ret)
}

async fn read_fwd_inner(
    params: ReadEvents03FwdParams,
    stmts: &StmtsEventsQueryOpts,
    scy: ScySessionRef<'_>,
    jobtrace: &mut ReadJobTrace,
) -> Result<Box<dyn BinningggContainerEventsDyn>, Error> {
    use crate::worker::Timeoutable;
    let selfname = "read_fwd_inner";
    let val_ty_dyn = datatypes::val_ty_dyn_from_type(params.shape.clone(), params.scalar_type.clone());
    let params_dbg = params.clone();
    let res = if daqbuf_series::dbg::dbg_check_scy6(params.series) {
        debug!("{selfname}  DEBUG COMPARISON RUN  series id {}", params.series.id());
        let res1 = {
            let opts = ReadNextValuesOpts {
                rt: params.rt.clone(),
                series: params.series,
                ts_msp: params.ts_msp,
                range: params.range.clone(),
                with_values: params.with_values,
                scylla_opts: params.scylla_opts.clone(),
                val_ty_dyn: val_ty_dyn.clone_dyn(),
            };
            // TODO run same query with workaround on and off.
            // opts.readopts.use_scylla6_workarounds = UseScylla6Workarounds::with_workarounds();
            let res = read_next_values_fwd(opts, stmts, scy.clone(), jobtrace)
                .with_timeout(READ_NEXT_TIMEOUT)
                .map_err(|_| {
                    warn!("{selfname}  timeout  {:?}", params_dbg);
                    Error::Timeout
                })
                .await??;
            res
        };
        let res2 = {
            let opts = ReadNextValuesOpts {
                rt: params.rt.clone(),
                series: params.series,
                ts_msp: params.ts_msp,
                range: params.range,
                with_values: params.with_values,
                scylla_opts: params.scylla_opts,
                val_ty_dyn,
            };
            // opts.readopts.use_scylla6_workarounds = UseScylla6Workarounds::with_workarounds();
            let res = read_next_values_fwd(opts, stmts, scy.clone(), jobtrace)
                .with_timeout(READ_NEXT_TIMEOUT)
                .map_err(|_| {
                    warn!("{selfname}  timeout  {:?}", params_dbg);
                    Error::Timeout
                })
                .await??;
            res
        };
        let tss1 = res1.dbg_to_tss();
        let tss2 = res2.dbg_to_tss();
        if tss1 != tss2 {
            log::info!("{selfname}  DIFFERENCE  len {n} vs {m}", n = tss1.len(), m = tss2.len());
        } else {
        }
        res1
    } else {
        //
        //
        // TODO check how many of these are actually needed:
        //
        let opts = ReadNextValuesOpts {
            rt: params.rt.clone(),
            series: params.series,
            ts_msp: params.ts_msp,
            range: params.range,
            with_values: params.with_values,
            scylla_opts: params.scylla_opts,
            val_ty_dyn,
        };
        let res = read_next_values_fwd(opts, stmts, scy, jobtrace)
            .with_timeout(READ_NEXT_TIMEOUT)
            .map_err(|_| {
                warn!("{selfname}  timeout  {:?}", params_dbg);
                Error::Timeout
            })
            .await??;
        res
    };
    Ok(res)
}

pub async fn read_fwd(
    params: ReadEvents03FwdParams,
    stmts: &StmtsEventsQueryOpts,
    scy: Arc<Session>,
) -> Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error> {
    let mut jobtrace = ReadJobTrace::new();
    let x = read_fwd_inner(params, stmts, ScySessionRef(&scy), &mut jobtrace).await?;
    Ok((x, jobtrace))
}
