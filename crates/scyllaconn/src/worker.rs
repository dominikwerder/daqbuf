use crate::binwriteindex::BinWriteIndexEntry;
use crate::conn::create_scy_session_no_ks;
use crate::events2::events::ReadEventsJobParams;
use crate::events2::events::ReadJobTrace;
use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use async_channel::Receiver;
use async_channel::Sender;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::BinlenU32;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_bins::ContainerBins;
use netpod::DtMs;
use netpod::ScyllaConfig;
use netpod::TsMs;
use netpod::UseScylla6Workarounds;
use netpod::log;
use netpod::ttl::RetentionTime;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }

const CONCURRENT_QUERIES_PER_WORKER: usize = 80;
const SCYLLA_WORKER_QUEUE_LEN: usize = 200;

autoerr::create_error_v1!(
    name(Error, "ScyllaWorker"),
    enum variants {
        ScyllaConnection(#[from] crate::conn::Error),
        Prepare(#[from] crate::events2::prepare::Error),
        Events(#[from] crate::events2::events::Error),
        Msp(#[from] crate::events2::msp::Error),
        // Reserved for failure of sending jobs to the worker. Not for results back to client.
        JobChannelSend,
        ChannelRecv,
        Join,
        Toplist(#[from] crate::accounting::toplist::Error),
        MissingKeyspaceConfig,
        CacheWriteF32(#[from] streams::timebin::cached::reader::Error),
        ScyllaPrepare(#[from] scylla::errors::PrepareError),
        ScyllaType(#[from] scylla::deserialize::TypeCheckError),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        TimeoutJob,
    },
);

pub trait TimelimitedJobResult<T> {
    fn map_err_timeout_job(self) -> Result<T, Error>;
}

impl<T> TimelimitedJobResult<T> for Result<T, tokio::time::error::Elapsed> {
    fn map_err_timeout_job(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(_) => Err(Error::TimeoutJob),
        }
    }
}

type ScySessTy = scylla::client::session::Session;

pub trait Timeoutable {
    fn with_timeout(self, dur: Duration) -> tokio::time::Timeout<Self>
    where
        Self: Sized;
}

impl<F> Timeoutable for F
where
    F: futures_util::Future,
{
    fn with_timeout(self, dur: Duration) -> tokio::time::Timeout<Self>
    where
        // Self: Sized,
        Self: futures_util::Future,
    {
        tokio::time::timeout(dur, self)
    }
}

#[derive(Debug)]
struct FindTsMsp {
    rt: RetentionTime,
    series: SeriesId,
    range: ScyllaSeriesRange,
    bck: bool,
    use_scylla6_workarounds: UseScylla6Workarounds,
    tx: Sender<Result<VecDeque<TsMs>, Error>>,
}

impl FindTsMsp {
    async fn execute_inner(self, stmts: &StmtsEvents, scy: &ScySessTy) -> Result<VecDeque<TsMs>, Error> {
        let res = crate::events2::msp::find_ts_msp(
            &self.rt,
            self.series.id(),
            self.range.clone(),
            self.bck.clone(),
            self.use_scylla6_workarounds,
            &stmts,
            &scy,
        )
        .with_timeout(Duration::from_millis(5000))
        .await
        .map_err_timeout_job()
        .inspect_err(|_| {
            warn!(
                "FindTsMsp: job timeout {:?}",
                (&self.rt, &self.series, &self.range, &self.bck)
            );
        })??;
        Ok(res)
    }

    async fn execute(self, stmts: &StmtsEvents, scy: &ScySessTy) {
        // TODO avoid the extra clone
        let tx = self.tx.clone();
        let params_dbg = (
            self.rt.clone(),
            self.series.clone(),
            self.range.clone(),
            self.bck.clone(),
        );
        let x = self.execute_inner(stmts, scy).await;
        if tx.try_send(x).is_err() {
            warn!("FindTsMsp: failed to send result back to caller {:?}", params_dbg);
        }
    }
}

#[derive(Debug)]
struct ReadPrebinnedF32 {
    rt: RetentionTime,
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: core::ops::Range<u32>,
    use_scylla6_workarounds: UseScylla6Workarounds,
    tx: Sender<Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error>>,
}

#[derive(Debug)]
struct BinWriteIndexRead {
    rt: RetentionTime,
    series: SeriesId,
    pbp: PrebinnedPartitioning,
    msp: MspU32,
    lsp_min: LspU32,
    lsp_max: LspU32,
    use_scylla6_workarounds: UseScylla6Workarounds,
    tx: Sender<Result<VecDeque<BinWriteIndexEntry>, Error>>,
}

impl BinWriteIndexRead {
    async fn execute_inner(self, stmts: &StmtsEvents, scy: &ScySessTy) -> Result<(), Error> {
        let params = (
            self.series.id() as i64,
            self.pbp.db_ix() as i16,
            self.msp.to_db_i32(),
            self.lsp_min.to_db_i32(),
            self.lsp_max.to_db_i32(),
        );
        info!("BinWriteIndexRead execute {:?}", params);
        let res = scy
            .execute_iter(
                stmts
                    .cache_bypass(*self.use_scylla6_workarounds)
                    .rt(&self.rt)
                    .bin_write_index_read()
                    .clone(),
                params,
            )
            .await?;
        let mut it = res.rows_stream::<(i32, i32)>()?;
        let mut all = VecDeque::new();
        while let Some((lsp, binlen)) = it.try_next().await? {
            let v = BinWriteIndexEntry {
                lsp: LspU32::from_db_i32(lsp),
                binlen: BinlenU32(binlen as u32),
            };
            all.push_back(v);
        }
        if self.tx.try_send(Ok(all)).is_err() {
            // TODO count for stats
            warn!("BinWriteIndexRead: failed to send result back to caller");
        }
        Ok(())
    }

    async fn execute(self, stmts: &StmtsEvents, scy: &ScySessTy) {
        // TODO avoid the extra clone
        let tx = self.tx.clone();
        match self.execute_inner(stmts, scy).await {
            Ok(()) => {}
            Err(e) => {
                if tx.try_send(Err(e)).is_err() {
                    // TODO count for stats
                    warn!("BinWriteIndexRead: failed to send result back to caller");
                }
            }
        }
    }
}

#[derive(Debug)]
struct PrepareV1 {
    cql: String,
    tx: Sender<Result<scylla::statement::prepared::PreparedStatement, Error>>,
}

struct ExecuteV1 {
    st: scylla::statement::prepared::PreparedStatement,
    params: Box<dyn scylla::serialize::row::SerializeRow + Send>,
    tx: Sender<Result<scylla::client::pager::QueryPager, Error>>,
}

impl fmt::Debug for ExecuteV1 {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ExecuteV1")
            .field("st", &self.st)
            .field("params", &"...")
            .field("tx", &"...")
            .finish()
    }
}

#[derive(Debug)]
enum Job {
    FindTsMsp(FindTsMsp),
    AccountingReadTs(
        RetentionTime,
        TsMs,
        Sender<Result<crate::accounting::toplist::UsageData, crate::accounting::toplist::Error>>,
    ),
    WriteCacheF32(
        u64,
        ContainerBins<f32, f32>,
        Sender<Result<(), streams::timebin::cached::reader::Error>>,
    ),
    ReadPrebinnedF32(ReadPrebinnedF32),
    BinWriteIndexRead(BinWriteIndexRead),
    PrepareV1(PrepareV1),
    ExecuteV1(ExecuteV1),
    ReadEvents02(
        ReadEventsJobParams,
        Sender<Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error>>,
    ),
}

#[derive(Debug, Clone)]
pub struct ScyllaQueue {
    tx: Sender<Job>,
}

impl ScyllaQueue {
    pub async fn find_ts_msp(
        &self,
        rt: RetentionTime,
        series: SeriesId,
        range: ScyllaSeriesRange,
        bck: bool,
        use_scylla6_workarounds: UseScylla6Workarounds,
    ) -> Result<VecDeque<TsMs>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = FindTsMsp {
            rt,
            series,
            range,
            bck,
            use_scylla6_workarounds,
            tx,
        };
        let job = Job::FindTsMsp(job);
        self.tx.send(job).await.map_err(|_| Error::JobChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn read_events_v02(
        &self,
        params: ReadEventsJobParams,
    ) -> Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadEvents02(params, tx);
        self.tx.send(job).await.map_err(|_| Error::JobChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn accounting_read_ts(
        &self,
        rt: RetentionTime,
        ts: TsMs,
    ) -> Result<crate::accounting::toplist::UsageData, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::AccountingReadTs(rt, ts, tx);
        self.tx.send(job).await.map_err(|_| Error::JobChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn write_cache_f32(
        &self,
        series: u64,
        bins: ContainerBins<f32, f32>,
    ) -> Result<(), streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::WriteCacheF32(series, bins, tx);
        self.tx
            .send(job)
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn read_prebinned_f32(
        &self,
        rt: RetentionTime,
        series: u64,
        bin_len: DtMs,
        msp: u64,
        offs: core::ops::Range<u32>,
        use_scylla6_workarounds: UseScylla6Workarounds,
    ) -> Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadPrebinnedF32(ReadPrebinnedF32 {
            rt,
            series,
            bin_len,
            msp,
            offs,
            use_scylla6_workarounds,
            tx,
        });
        self.tx
            .send(job)
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn bin_write_index_read(
        &self,
        rt: RetentionTime,
        series: SeriesId,
        pbp: PrebinnedPartitioning,
        msp: MspU32,
        lsp_min: LspU32,
        lsp_max: LspU32,
        use_scylla6_workarounds: UseScylla6Workarounds,
    ) -> Result<VecDeque<BinWriteIndexEntry>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = BinWriteIndexRead {
            rt,
            series,
            pbp,
            msp,
            lsp_min,
            lsp_max,
            use_scylla6_workarounds,
            tx,
        };
        let job = Job::BinWriteIndexRead(job);
        self.tx
            .send(job)
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn prepare(&self, cql: String) -> Result<scylla::statement::prepared::PreparedStatement, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::PrepareV1(PrepareV1 { cql, tx });
        self.tx
            .send(job)
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn execute(
        &self,
        st: scylla::statement::prepared::PreparedStatement,
    ) -> Result<scylla::client::pager::QueryPager, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let params = Box::new(());
        let job = Job::ExecuteV1(ExecuteV1 { st, params, tx });
        self.tx
            .send(job)
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
        Ok(res)
    }
}

struct FutCatchUnwind<F> {
    fut: F,
}

impl<F> FutCatchUnwind<F> {
    fn new(fut: F) -> Self {
        Self { fut }
    }
}

impl<F> Future for FutCatchUnwind<F>
where
    F: futures_util::Future,
{
    type Output = std::thread::Result<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        use Poll::*;
        use std::panic::AssertUnwindSafe;
        let fut = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.fut) };
        let lmb = AssertUnwindSafe(|| fut.poll(cx));
        let res = std::panic::catch_unwind(lmb);
        match res {
            Ok(Ready(v)) => Ready(Ok(v)),
            Ok(Pending) => Pending,
            Err(e) => Ready(Err(e)),
        }
    }
}

#[derive(Debug)]
pub struct ScyllaWorker {
    rx: Receiver<Job>,
    scyconf_st: ScyllaConfig,
    scyconf_mt: ScyllaConfig,
    scyconf_lt: ScyllaConfig,
}

impl ScyllaWorker {
    pub async fn new(
        scyconf_st: ScyllaConfig,
        scyconf_mt: ScyllaConfig,
        scyconf_lt: ScyllaConfig,
    ) -> Result<(ScyllaQueue, Self), Error> {
        let (tx, rx) = async_channel::bounded(SCYLLA_WORKER_QUEUE_LEN);
        let queue = ScyllaQueue { tx };
        let worker = Self {
            rx,
            scyconf_st,
            scyconf_mt,
            scyconf_lt,
        };
        Ok((queue, worker))
    }

    async fn work_inner(&self) -> Result<(), Error> {
        let scy = create_scy_session_no_ks(&self.scyconf_st).await?;
        let scy = Arc::new(scy);
        let kss = [
            self.scyconf_st.keyspace.as_str(),
            self.scyconf_mt.keyspace.as_str(),
            self.scyconf_lt.keyspace.as_str(),
        ];
        debug!("scylla worker  prepare start");
        let stmts = StmtsEvents::new(kss.try_into().map_err(|_| Error::MissingKeyspaceConfig)?, &scy).await?;
        let stmts = Arc::new(stmts);
        debug!("scylla worker  prepare done");
        self.rx
            .clone()
            .map(|job| async {
                match job {
                    Job::FindTsMsp(job) => job.execute(&stmts, &scy).await,
                    Job::AccountingReadTs(rt, ts, tx) => {
                        let ks = match &rt {
                            RetentionTime::Short => &self.scyconf_st.keyspace,
                            RetentionTime::Medium => &self.scyconf_mt.keyspace,
                            RetentionTime::Long => &self.scyconf_lt.keyspace,
                        };
                        let res = crate::accounting::toplist::read_ts(&ks, rt, ts, &scy).await;
                        if tx.send(res.map_err(Into::into)).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::WriteCacheF32(a, b, tx) => {
                        let _ = a;
                        let _ = b;
                        // let res = super::bincache::worker_write(series, bins, &stmts_cache, &scy).await;
                        let res = Err(streams::timebin::cached::reader::Error::TodoImpl);
                        if tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::ReadPrebinnedF32(job) => {
                        // TODO remove I guess?
                        let res = super::bincache::worker_read(
                            job.rt,
                            job.series,
                            job.bin_len,
                            job.msp,
                            job.offs,
                            job.use_scylla6_workarounds,
                            &stmts,
                            &scy,
                        )
                        .await;
                        if job.tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::BinWriteIndexRead(job) => job.execute(&stmts, &scy).await,
                    Job::PrepareV1(job) => {
                        let res = scy.prepare(job.cql).await.map_err(|e| e.into());
                        // TODO log?
                        let _ = job.tx.send(res).await;
                    }
                    Job::ExecuteV1(job) => {
                        let res = scy.execute_iter(job.st, job.params).await.map_err(|e| e.into());
                        // TODO log?
                        let _ = job.tx.send(res).await;
                    }
                    Job::ReadEvents02(params, tx) => {
                        crate::events2::events::read_events_v02(params, tx, stmts.clone(), scy.clone()).await
                    }
                }
            })
            .buffer_unordered(CONCURRENT_QUERIES_PER_WORKER)
            .for_each(|_| futures_util::future::ready(()))
            .await;
        Ok(())
    }

    pub async fn work(self) -> Result<(), Error> {
        loop {
            info!("scylla worker start");
            let res = FutCatchUnwind::new(self.work_inner()).await;
            let res = match res {
                Ok(x) => x,
                Err(e) => {
                    error!("panic in scylla worker: {:?}", e);
                    let x = 10;
                    info!("scylla worker sleep {x} sec before reconnect");
                    taskrun::tokio::time::sleep(std::time::Duration::from_secs(x)).await;
                    continue;
                }
            };
            match res {
                Ok(()) => {
                    info!("scylla worker finished");
                    break Ok(());
                }
                Err(e) => {
                    error!("scylla worker error: {}", e);
                    let x = 5;
                    error!("scylla worker sleep {x} sec before reconnect");
                    taskrun::tokio::time::sleep(std::time::Duration::from_secs(x)).await;
                }
            }
        }
    }
}
