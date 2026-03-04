mod datatypes;

use super::Error;
use crate::events2::prepare::StmtsEvents;
use crate::events3::jobtrace::ReadJobTrace;
use crate::range::ScyllaSeriesRange;
use crate::worker::EventReadOpts;
use crate::worker::ReadEvents03FwdParams;
use async_channel::Sender;
use daqbuf_series::SeriesId;
use datatypes::ValTyDyn;
use items_0::timebin::BinningggContainerEventsDyn;
use netpod::TsMs;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::sync::Arc;
use std::time::Duration;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

#[derive(Debug)]
pub(super) struct ReadNextValuesOpts {
    rt: RetentionTime,
    series: SeriesId,
    ts_msp: TsMs,
    range: ScyllaSeriesRange,
    bck: bool,
    readopts: EventReadOpts,
    val_ty_dyn: Box<dyn ValTyDyn>,
}

async fn read_fwd_inner(
    params: ReadEvents03FwdParams,
    stmts: Arc<StmtsEvents>,
    scy: Arc<scylla::client::session::Session>,
) -> Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), crate::worker::Error> {
    use crate::worker::TimelimitedJobResult;
    use crate::worker::Timeoutable;
    let selfname = "read_fwd_inner";
    let val_ty_dyn = datatypes::ValTyDynTesting::boxed_from_type(params.shape.clone(), params.scalar_type.clone());
    let params_dbg = params.clone();
    let res = if daqbuf_series::dbg::dbg_check_scy6(params.series) {
        log::info!(
            "read_events_v02_inner  DEBUG COMPARISON RUN  series id {}",
            params.series.id()
        );
        let res1 = {
            //
            //
            // TODO check how many of these are actually needed:
            //
            let mut opts = ReadNextValuesOpts {
                rt: params.rt.clone(),
                series: params.series,
                ts_msp: params.ts_msp,
                range: params.range.clone(),
                bck: !params.fwd,
                readopts: params.readopts.clone(),
                val_ty_dyn: val_ty_dyn.clone_dyn(),
            };
            // TODO run same query with workaround on and off.
            // opts.readopts.use_scylla6_workarounds = UseScylla6Workarounds::with_workarounds();
            let fut = read_next_values_3(opts, scy.clone(), stmts.clone()).with_timeout(Duration::from_millis(5000));
            let res = fut.await.map_err_timeout_job().inspect_err(|e| match e {
                crate::worker::Error::TimeoutJob => {
                    warn!("read_events_v02_inner  TimeoutJob  params {:?}", params_dbg);
                }
                _ => {}
            })??;
            res
        };
        let res2 = {
            let mut opts = ReadNextValuesOpts {
                rt: params.rt.clone(),
                series: params.series,
                ts_msp: params.ts_msp,
                range: params.range,
                bck: !params.fwd,
                readopts: params.readopts,
                val_ty_dyn,
            };
            // opts.readopts.use_scylla6_workarounds = UseScylla6Workarounds::with_workarounds();
            let fut = read_next_values_3(opts, scy, stmts).with_timeout(Duration::from_millis(5000));
            let res = fut.await.map_err_timeout_job().inspect_err(|e| match e {
                crate::worker::Error::TimeoutJob => {
                    warn!("read_events_v02_inner  TimeoutJob  params {:?}", params_dbg);
                }
                _ => {}
            })??;
            res
        };
        let tss1 = res1.0.dbg_to_tss();
        let tss2 = res2.0.dbg_to_tss();
        if tss1 != tss2 {
            log::info!(
                "read_events_v02_inner  DIFFERENCE  len {n} vs {m}",
                n = tss1.len(),
                m = tss2.len()
            );
        } else {
        }
        res1
    } else {
        let opts = ReadNextValuesOpts {
            rt: params.rt.clone(),
            series: params.series,
            ts_msp: params.ts_msp,
            range: params.range,
            bck: !params.fwd,
            readopts: params.readopts,
            val_ty_dyn,
        };
        let fut = read_next_values_3(opts, scy, stmts).with_timeout(Duration::from_millis(5000));
        let res = fut.await.map_err_timeout_job().inspect_err(|e| match e {
            crate::worker::Error::TimeoutJob => {
                warn!("read_events_v02_inner  TimeoutJob  params {:?}", params_dbg);
            }
            _ => {}
        })??;
        res
    };
    Ok(res)
}

pub async fn read_fwd(
    params: ReadEvents03FwdParams,
    tx: Sender<Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>>,
    stmts: Arc<StmtsEvents>,
    scy: Arc<Session>,
) -> () {
    let selfname = "read_fwd";
    // TODO impl a separate query execution code.
    // The type conversions are actually part of the worker.
    // Therefore, should also move the type conversion code there.
    let x = read_fwd_inner(params, stmts, scy).await;
    let _ = tx.send(x);
    todo!("{selfname}  TODO")
}
