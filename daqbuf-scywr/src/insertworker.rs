use crate::config::ScyllaIngestConfig;
use crate::iteminsertqueue::Accounting;
use crate::iteminsertqueue::AccountingRecv;
use crate::iteminsertqueue::BinWriteIndexV04;
use crate::iteminsertqueue::InsertFut;
use crate::iteminsertqueue::InsertFutKindDbgTag;
use crate::iteminsertqueue::InsertItem;
use crate::iteminsertqueue::MspItem;
use crate::iteminsertqueue::QueryItem;
use crate::iteminsertqueue::TimeBinSimpleF32V02;
use crate::iteminsertqueue::insert_item_fut;
use crate::iteminsertqueue::insert_msp_fut;
use crate::store::DataStore;
use async_channel::Receiver;
use async_channel::Sender;
use atomic::AtomicU64;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use log;
use netpod::ttl::RetentionTime;
use smallvec::SmallVec;
use smallvec::smallvec;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! debug_setup { ($($arg:tt)*) => ( if false { log::debug!($($arg)*); } ); }
macro_rules! trace2 { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ); }
macro_rules! trace_transform { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ); }
macro_rules! trace_inspect { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ); }
macro_rules! trace_item_execute { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "ScyllaInsertWorker"),
    enum variants {
        Store(#[from] crate::store::Error),
    },
);

fn stats_inc_for_err(err: &crate::iteminsertqueue::InsertFutError, mett: &mut stats::mett::ScyllaInsertWorker) {
    use crate::iteminsertqueue::InsertFutError;
    match err {
        InsertFutError::Execution(e) => match e {
            scylla::errors::ExecutionError::RequestTimeout(_) => {
                mett.db_timeout().inc();
            }
            _ => {
                if true {
                    warn!("db error {}", err);
                }
                mett.db_error().inc();
            }
        },
        InsertFutError::NoFuture => {
            mett.db_no_future().inc();
        }
    }
}

#[allow(unused)]
fn back_off_next(backoff_dt: &mut Duration) {
    *backoff_dt = *backoff_dt + (*backoff_dt) * 3 / 2;
    let dtmax = Duration::from_millis(4000);
    if *backoff_dt > dtmax {
        *backoff_dt = dtmax;
    }
}

#[allow(unused)]
async fn back_off_sleep(backoff_dt: &mut Duration) {
    back_off_next(backoff_dt);
    tokio::time::sleep(*backoff_dt).await;
}

#[derive(Debug)]
pub enum InsertWorkerOutputItem {
    Metrics(stats::mett::ScyllaInsertWorker),
}

/// Shares the input metrics between the stream which prepares the database
/// futures and the worker loop which emits the metrics. Both are driven by the
/// same task, the lock is taken once per batch.
#[derive(Clone)]
struct InputMett {
    inner: Arc<Mutex<stats::mett::ScyllaWorkerInput>>,
}

impl InputMett {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(stats::mett::ScyllaWorkerInput::new())),
        }
    }

    fn ingest(&self, inp: stats::mett::ScyllaWorkerInput) {
        match self.inner.lock() {
            Ok(mut g) => g.ingest(inp),
            Err(_) => {
                error!("InputMett  poisoned");
            }
        }
    }

    fn take_and_reset(&self) -> stats::mett::ScyllaWorkerInput {
        match self.inner.lock() {
            Ok(mut g) => g.take_and_reset(),
            Err(_) => {
                error!("InputMett  poisoned");
                stats::mett::ScyllaWorkerInput::new()
            }
        }
    }
}

pub struct InsertWorkerOpts {
    pub store_workers_rate: Arc<AtomicU64>,
    pub insert_workers_running: Arc<AtomicU64>,
    pub insert_frac: Arc<AtomicU64>,
    pub array_truncate: Arc<AtomicU64>,
}

pub async fn spawn_scylla_insert_workers(
    rett: RetentionTime,
    scyconf: ScyllaIngestConfig,
    insert_scylla_sessions: usize,
    insert_worker_count: usize,
    insert_worker_concurrency: usize,
    item_inp: Receiver<VecDeque<QueryItem>>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    use_rate_limit_queue: bool,
    ignore_writes: bool,
    tx: Sender<InsertWorkerOutputItem>,
) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
    let item_inp = if use_rate_limit_queue {
        crate::ratelimit::rate_limiter(insert_worker_opts.store_workers_rate.clone(), item_inp)
    } else {
        item_inp
    };
    let mut jhs = Vec::new();
    if scyconf.keyspace() == "none" {
        let jh = tokio::spawn(async move {
            let _tx = tx;
            while let Ok(_) = item_inp.recv().await {}
            Ok(())
        });
        jhs.push(jh);
    } else {
        let mut data_stores = Vec::new();
        for _ in 0..insert_scylla_sessions {
            let data_store = Arc::new(DataStore::new(&scyconf, rett.clone()).await?);
            data_stores.push(data_store);
        }
        for worker_ix in 0..insert_worker_count {
            let data_store = data_stores[worker_ix * data_stores.len() / insert_worker_count].clone();
            let wid = InsertWorkerId::new(
                rett.clone(),
                worker_ix,
                format!("worker-{}-{}", scyconf.keyspace(), worker_ix),
            );
            let jh = tokio::spawn(worker_streamed(
                worker_ix,
                insert_worker_concurrency,
                item_inp.clone(),
                insert_worker_opts.clone(),
                Some(data_store),
                ignore_writes,
                tx.clone(),
                wid,
            ));
            jhs.push(jh);
        }
    }
    Ok(jhs)
}

pub async fn spawn_scylla_insert_workers_dummy(
    insert_worker_count: usize,
    insert_worker_concurrency: usize,
    item_inp: Receiver<VecDeque<QueryItem>>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    tx: Sender<InsertWorkerOutputItem>,
) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
    let mut jhs = Vec::new();
    for worker_ix in 0..insert_worker_count {
        let data_store = None;
        let wid = InsertWorkerId::new(RetentionTime::Short, worker_ix, format!("dummy-{worker_ix}"));
        let jh = tokio::spawn(worker_streamed(
            worker_ix,
            insert_worker_concurrency,
            item_inp.clone(),
            insert_worker_opts.clone(),
            data_store,
            true,
            tx.clone(),
            wid,
        ));
        jhs.push(jh);
    }
    Ok(jhs)
}

struct FutTrackDt<F> {
    ts1: Instant,
    ts2: Instant,
    ts_net: Instant,
    npoll: u16,
    fut: F,
    jobkind: FutJobKind,
}

impl FutTrackDt<InsertFut> {
    fn from_fut_job(job: FutJob) -> Self {
        let tsnow = Instant::now();
        Self {
            ts1: tsnow,
            ts2: tsnow,
            ts_net: job.ts_net,
            npoll: 0,
            fut: job.fut,
            jobkind: job.jobkind,
        }
    }
}

impl<F> Future for FutTrackDt<F>
where
    F: Future + Unpin,
{
    type Output = (Instant, Instant, Instant, F::Output, FutJobKind, u16);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        if self.npoll == 0 {
            self.ts2 = Instant::now();
        }
        self.npoll = self.npoll.saturating_add(1);
        match self.as_mut().fut.poll_unpin(cx) {
            Ready(x) => Ready((self.ts_net, self.ts1, self.ts2, x, self.jobkind.clone(), self.npoll)),
            Pending => Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InsertWorkerId {
    rt: RetentionTime,
    id: usize,
    short_name: String,
}

impl InsertWorkerId {
    pub fn new(rt: RetentionTime, id: usize, worker_name: String) -> Self {
        let short_name = format!("{} {} {}", worker_name, rt.debug_tag(), id);
        Self { rt, id, short_name }
    }

    pub fn short_name(&self) -> &str {
        &self.short_name
    }

    #[allow(unused)]
    fn _dummy(&self) {
        let _ = &self.rt;
        let _ = &self.id;
    }
}

async fn worker_streamed(
    worker_ix: usize,
    concurrency: usize,
    item_inp: Receiver<VecDeque<QueryItem>>,
    insert_worker_opts: Arc<InsertWorkerOpts>,
    data_store: Option<Arc<DataStore>>,
    ignore_writes: bool,
    tx: Sender<InsertWorkerOutputItem>,
    wid: InsertWorkerId,
) -> Result<(), Error> {
    debug_setup!("worker_streamed  begin");
    let tsnow = Instant::now();
    let mut mett = stats::mett::ScyllaInsertWorker::new();
    let mett_input = InputMett::new();
    let mut mett_emit_last = tsnow;
    let metrics_ivl = Duration::from_millis(500);
    insert_worker_opts
        .insert_workers_running
        .fetch_add(1, atomic::Ordering::AcqRel);
    let stream = item_inp;
    let worker_name = data_store
        .as_ref()
        .map_or_else(|| format!("dummy"), |x| x.rett.debug_tag().to_string());
    let stream = inspect_items(stream, worker_name.clone());
    let wid = Arc::new(wid);
    if let Some(data_store) = data_store {
        mett.worker_start().inc();
        let stream = transform_to_db_futures(stream, data_store, ignore_writes, wid.clone(), mett_input.clone());
        // let stream = stream.map(|_| Vec::new());
        let stream = stream
            .map(|x| futures_util::stream::iter(x))
            .flatten_unordered(Some(1))
            .map(|x| FutTrackDt::from_fut_job(x))
            .buffer_unordered(concurrency);
        let mut stream = Box::pin(stream);
        debug_setup!("waiting for item");
        while let Some((ts_net, ts1, ts2, item, jobkind, npoll)) = stream.next().await {
            if false {
                debug!("see scylla result item  {ts_net:?}  {ts1:?}  {ts2:?}  {item:?}  {jobkind:?}");
                continue;
            }
            trace_item_execute!("see scylla result item  {ts_net:?}  {ts1:?}  {ts2:?}  {item:?}  {jobkind:?}");
            let tsnow = Instant::now();
            match jobkind {
                FutJobKind::SeriesData => {
                    mett.jobtrans().SeriesData().inc();
                }
                FutJobKind::SeriesMsp => {
                    mett.jobtrans().SeriesMsp().inc();
                }
                FutJobKind::TimeBinSimpleF32V02 => {
                    mett.jobtrans().TimeBinSimpleF32V02().inc();
                }
                FutJobKind::BinWriteIndexV04 => {
                    mett.jobtrans().BinWriteIndexV04().inc();
                }
                FutJobKind::Accounting => {
                    mett.jobtrans().Accounting().inc();
                }
                FutJobKind::AccountingRecv => {
                    mett.jobtrans().AccountingRecv().inc();
                }
            }
            match item {
                Ok(_) => {
                    mett.job_ok().inc();
                    let dt1 = tsnow.saturating_duration_since(ts1);
                    let dt2 = tsnow.saturating_duration_since(ts2);
                    let dt_net = tsnow.saturating_duration_since(ts_net);
                    mett.job_dt1().push_dur_100us(dt1);
                    mett.job_dt2().push_dur_100us(dt2);
                    mett.job_dt_net().push_dur_100us(dt_net);
                    mett.job_npoll().push_val(npoll as u32);
                }
                Err(e) => {
                    mett.job_err().inc();
                    mett.job_npoll().push_val(npoll as u32);
                    stats_inc_for_err(&e, &mut mett);
                }
            }
            if mett_emit_last + metrics_ivl <= tsnow {
                mett_emit_last = tsnow;
                mett.input().ingest(mett_input.take_and_reset());
                mett.metrics_emit().inc();
                let m = mett.take_and_reset();
                let item = InsertWorkerOutputItem::Metrics(m);
                match tx.send(item).await {
                    Ok(()) => {}
                    Err(_) => {
                        error!("insert worker can not emit metrics");
                        break;
                    }
                }
            }
        }
        mett.worker_finish().inc();
    } else {
        mett.worker_dummy_start().inc();
        let mut stream = Box::pin(stream);
        while let Some(item) = stream.next().await {
            drop(item);
        }
        mett.worker_dummy_finish().inc();
    };
    // Hand out whatever accumulated since the last emit, so that the counters
    // of a worker which stops are not lost.
    {
        mett.input().ingest(mett_input.take_and_reset());
        mett.metrics_emit().inc();
        let m = mett.take_and_reset();
        if tx.send(InsertWorkerOutputItem::Metrics(m)).await.is_err() {
            error!("insert worker can not emit final metrics");
        }
    }
    insert_worker_opts
        .insert_workers_running
        .fetch_sub(1, atomic::Ordering::AcqRel);
    debug_setup!("insert worker {} done", worker_ix);
    Ok(())
}

#[derive(Debug, Clone)]
enum FutJobKind {
    SeriesData,
    SeriesMsp,
    TimeBinSimpleF32V02,
    BinWriteIndexV04,
    Accounting,
    AccountingRecv,
}

struct FutJob {
    fut: InsertFut,
    ts_net: Instant,
    jobkind: FutJobKind,
}

fn transform_to_db_futures<S>(
    item_inp: S,
    data_store: Arc<DataStore>,
    ignore_writes: bool,
    wid: Arc<InsertWorkerId>,
    mett_input: InputMett,
) -> impl Stream<Item = Vec<FutJob>>
where
    S: Stream<Item = VecDeque<QueryItem>>,
{
    trace_transform!("transform_to_db_futures  begin");
    item_inp.map(move |batch| {
        trace_transform!("transform_to_db_futures  have batch  len {}", batch.len());
        let tsnow = Instant::now();
        let mut res = Vec::with_capacity(32);
        // One metrics update per batch, not per item.
        let mut mett = stats::mett::ScyllaWorkerInput::new();
        mett.batch_recv().inc();
        mett.batch_len().push_val(batch.len() as u32);
        mett.item_recv().add(batch.len() as u32);
        for item in batch {
            let wid = wid.clone();
            if ignore_writes {
                mett.item_ignored().inc();
            }
            let futs = match item {
                QueryItem::Insert(item) => {
                    mett.item_insert().inc();
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_query_insert_futs(item, &data_store, wid)
                    }
                }
                QueryItem::Msp(item) => {
                    mett.item_msp().inc();
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_msp_insert_futs(item, &data_store, wid)
                    }
                }
                QueryItem::TimeBinSimpleF32V02(item) => {
                    mett.item_timebin_simple_f32_v02().inc();
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_timebin_v02_insert_futs(item, &data_store, tsnow, wid)
                    }
                }
                QueryItem::BinWriteIndexV04(item) => {
                    mett.item_bin_write_index_v04().inc();
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_bin_write_index_v04_insert_futs(item, &data_store, tsnow, wid)
                    }
                }
                QueryItem::Accounting(item) => {
                    mett.item_accounting().inc();
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_accounting_insert_futs(item, &data_store, tsnow, wid)
                    }
                }
                QueryItem::AccountingRecv(item) => {
                    mett.item_accounting_recv().inc();
                    if ignore_writes {
                        SmallVec::new()
                    } else {
                        prepare_accounting_recv_insert_futs(item, &data_store, tsnow, wid)
                    }
                }
            };
            trace_transform!("prepared futs  len {}", futs.len());
            mett.fut_prepared().add(futs.len() as u32);
            res.extend(futs.into_iter());
        }
        mett.futs_per_batch().push_val(res.len() as u32);
        mett_input.ingest(mett);
        res
    })
}

fn inspect_items(
    item_inp: Receiver<VecDeque<QueryItem>>,
    worker_name: String,
) -> impl Stream<Item = VecDeque<QueryItem>> {
    trace_inspect!("transform_to_db_futures  begin");
    // TODO possible without box?
    // let item_inp = Box::pin(item_inp);
    item_inp.inspect(move |batch| {
        for item in batch {
            match &item {
                QueryItem::Insert(item) => {
                    trace_item_execute!("execute  {}  Insert  {}", worker_name, item.string_short());
                }
                QueryItem::Msp(item) => {
                    trace_item_execute!("execute  {}  Msp  {}", worker_name, item.string_short());
                }
                QueryItem::TimeBinSimpleF32V02(_) => {
                    trace_item_execute!("execute  {}  TimeBinSimpleF32V02", worker_name);
                }
                QueryItem::BinWriteIndexV04(_) => {
                    trace_item_execute!("execute  {}  BinWriteIndexV04", worker_name);
                }
                QueryItem::Accounting(_) => {
                    trace_item_execute!("execute  {}  Accounting  {:?}", worker_name, item);
                }
                QueryItem::AccountingRecv(_) => {
                    trace_item_execute!("execute  {}  Accounting  {:?}", worker_name, item);
                }
            }
        }
    })
}

fn prepare_msp_insert_futs(
    item: MspItem,
    data_store: &Arc<DataStore>,
    wid: Arc<InsertWorkerId>,
) -> SmallVec<[FutJob; 4]> {
    trace2!("execute  MSP bump");
    let fut = insert_msp_fut(
        item.series(),
        item.ts_msp(),
        data_store.scy.clone(),
        data_store.qu_insert_ts_msp.clone(),
        wid,
    );
    let fut = FutJob {
        fut,
        ts_net: item.ts_net(),
        jobkind: FutJobKind::SeriesMsp,
    };
    let futs = smallvec![fut];
    futs
}

fn prepare_query_insert_futs(
    item: InsertItem,
    data_store: &Arc<DataStore>,
    wid: Arc<InsertWorkerId>,
) -> SmallVec<[FutJob; 4]> {
    let item_ts_net = item.ts_net;
    let do_insert = true;
    let fut = insert_item_fut(item, &data_store, do_insert, wid);
    let fut = FutJob {
        fut,
        ts_net: item_ts_net,
        jobkind: FutJobKind::SeriesData,
    };
    let futs = smallvec![fut];
    futs
}

fn prepare_timebin_v02_insert_futs(
    item: TimeBinSimpleF32V02,
    data_store: &Arc<DataStore>,
    tsnow: Instant,
    wid: Arc<InsertWorkerId>,
) -> SmallVec<[FutJob; 4]> {
    let params = (
        item.series.id() as i64,
        item.binlen,
        item.msp,
        item.off,
        item.cnt,
        item.min,
        item.max,
        item.avg,
        item.dev,
        item.lst,
    );
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_binned_scalar_f32_v02.clone(),
        params,
        wid,
        InsertFutKindDbgTag::Other,
    );
    let fut = FutJob {
        fut,
        ts_net: tsnow,
        jobkind: FutJobKind::TimeBinSimpleF32V02,
    };
    let futs = smallvec![fut];

    // TODO match on the query result:
    // match qres {
    //     Ok(_) => {
    //         backoff = backoff_0;
    //     }
    //     Err(e) => {
    //         stats_inc_for_err(&stats, &crate::iteminsertqueue::Error::QueryError(e));
    //         back_off_sleep(&mut backoff).await;
    //     }
    // }

    futs
}

fn prepare_bin_write_index_v04_insert_futs(
    item: BinWriteIndexV04,
    data_store: &Arc<DataStore>,
    tsnow: Instant,
    wid: Arc<InsertWorkerId>,
) -> SmallVec<[FutJob; 4]> {
    let params = (item.series, item.pbp, item.msp, item.lsp, item.binlen);
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_insert_bin_write_index_v04.clone(),
        params,
        wid,
        InsertFutKindDbgTag::Other,
    );
    let fut = FutJob {
        fut,
        ts_net: tsnow,
        jobkind: FutJobKind::BinWriteIndexV04,
    };
    let futs = smallvec![fut];

    // TODO match on the query result:
    // match qres {
    //     Ok(_) => {
    //         backoff = backoff_0;
    //     }
    //     Err(e) => {
    //         stats_inc_for_err(&stats, &crate::iteminsertqueue::Error::QueryError(e));
    //         back_off_sleep(&mut backoff).await;
    //     }
    // }

    futs
}

fn prepare_accounting_insert_futs(
    item: Accounting,
    data_store: &Arc<DataStore>,
    tsnow: Instant,
    wid: Arc<InsertWorkerId>,
) -> SmallVec<[FutJob; 4]> {
    let params = (
        item.part,
        item.ts.sec() as i64,
        item.series.id() as i64,
        item.count,
        item.bytes,
    );
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_account_00.clone(),
        params,
        wid,
        InsertFutKindDbgTag::Other,
    );
    let fut = FutJob {
        fut,
        ts_net: tsnow,
        jobkind: FutJobKind::Accounting,
    };
    let futs = smallvec![fut];
    futs
}

fn prepare_accounting_recv_insert_futs(
    item: AccountingRecv,
    data_store: &Arc<DataStore>,
    tsnow: Instant,
    wid: Arc<InsertWorkerId>,
) -> SmallVec<[FutJob; 4]> {
    let params = (
        item.part,
        item.ts.sec() as i64,
        item.series.id() as i64,
        item.count,
        item.bytes,
    );
    let fut = InsertFut::new(
        data_store.scy.clone(),
        data_store.qu_account_recv_00.clone(),
        params,
        wid,
        InsertFutKindDbgTag::Other,
    );
    let fut = FutJob {
        fut,
        ts_net: tsnow,
        jobkind: FutJobKind::AccountingRecv,
    };
    let futs = smallvec![fut];
    futs
}
