use crate::binwriteindex::BinWriteIndexEntry;
use crate::conn::create_scy_session_no_ks;
use crate::events2::events::ReadEventsJobParams;
use crate::events2::events::ReadJobTrace;
use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use async_channel::Receiver;
use async_channel::Sender;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_bins::ContainerBins;
use netpod::DtMs;
use netpod::ScyllaConfig;
use netpod::TsMs;
use netpod::log;
use netpod::ttl::RetentionTime;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

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
        ChannelSend,
        ChannelRecv,
        Join,
        Toplist(#[from] crate::accounting::toplist::Error),
        MissingKeyspaceConfig,
        CacheWriteF32(#[from] streams::timebin::cached::reader::Error),
        ScyllaPrepare(#[from] scylla::errors::PrepareError),
        ScyllaType(#[from] scylla::deserialize::TypeCheckError),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
    },
);

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_: async_channel::SendError<T>) -> Self {
        Self::ChannelSend
    }
}

type ScySessTy = scylla::client::session::Session;

#[derive(Debug)]
struct ReadPrebinnedF32 {
    rt: RetentionTime,
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: core::ops::Range<u32>,
    tx: Sender<Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error>>,
}

#[derive(Debug)]
struct BinWriteIndexRead {
    rt1: RetentionTime,
    series: SeriesId,
    pbp: PrebinnedPartitioning,
    msp: MspU32,
    lsp_min: u32,
    lsp_max: u32,
    tx: Sender<Result<VecDeque<BinWriteIndexEntry>, Error>>,
}

impl BinWriteIndexRead {
    async fn execute(self, stmts: &StmtsEvents, scy: &ScySessTy) {
        // TODO avoid the extra clone
        let tx = self.tx.clone();
        match self.execute_inner(stmts, scy).await {
            Ok(()) => {}
            Err(e) => {
                if tx.send(Err(e)).await.is_err() {
                    // TODO count for stats
                }
            }
        }
    }

    async fn execute_inner(self, stmts: &StmtsEvents, scy: &ScySessTy) -> Result<(), Error> {
        let params = (
            self.series.id() as i64,
            self.pbp.db_ix() as i16,
            self.msp.0 as i32,
            self.lsp_min as i32,
            self.lsp_max as i32,
        );
        log::info!("execute {:?}", params);
        let res = scy
            .execute_iter(stmts.rt(&self.rt1).bin_write_index_read().clone(), params)
            .await?;
        let mut it = res.rows_stream::<(i32, i32)>()?;
        let mut all = VecDeque::new();
        while let Some((lsp, binlen)) = it.try_next().await? {
            let v = BinWriteIndexEntry {
                lsp: lsp as u32,
                binlen: binlen as u32,
            };
            all.push_back(v);
        }
        self.tx.send(Ok(all)).await?;
        Ok(())
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
    FindTsMsp(
        RetentionTime,
        // series-id
        u64,
        ScyllaSeriesRange,
        bool,
        Sender<Result<VecDeque<TsMs>, Error>>,
    ),
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
        series: u64,
        range: ScyllaSeriesRange,
        bck: bool,
    ) -> Result<VecDeque<TsMs>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::FindTsMsp(rt, series, range, bck, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn read_events_v02(
        &self,
        params: ReadEventsJobParams,
    ) -> Result<(Box<dyn BinningggContainerEventsDyn>, ReadJobTrace), Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadEvents02(params, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
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
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
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
    ) -> Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadPrebinnedF32(ReadPrebinnedF32 {
            rt,
            series,
            bin_len,
            msp,
            offs,
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
        rt1: RetentionTime,
        series: SeriesId,
        pbp: PrebinnedPartitioning,
        msp: MspU32,
        lsp_min: u32,
        lsp_max: u32,
    ) -> Result<VecDeque<BinWriteIndexEntry>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = BinWriteIndexRead {
            rt1,
            series,
            pbp,
            msp,
            lsp_min,
            lsp_max,
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

    pub async fn work(self) -> Result<(), Error> {
        let scy = create_scy_session_no_ks(&self.scyconf_st).await?;
        let scy = Arc::new(scy);
        let kss = [
            self.scyconf_st.keyspace.as_str(),
            self.scyconf_mt.keyspace.as_str(),
            self.scyconf_lt.keyspace.as_str(),
        ];
        debug!("scylla worker  prepare start");
        let stmts = StmtsEvents::new(
            kss.try_into().map_err(|_| Error::MissingKeyspaceConfig)?,
            self.scyconf_st.bypass_cache,
            &scy,
        )
        .await?;
        let stmts = Arc::new(stmts);
        // let stmts_cache = StmtsCache::new(kss[0], &scy).await?;
        // let stmts_cache = Arc::new(stmts_cache);
        debug!("scylla worker  prepare done");
        self.rx
            .map(|job| async {
                match job {
                    Job::FindTsMsp(rt, series, range, bck, tx) => {
                        let res = crate::events2::msp::find_ts_msp(&rt, series, range, bck, &stmts, &scy).await;
                        if tx.send(res.map_err(Into::into)).await.is_err() {
                            // TODO count for stats
                        }
                    }
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
                        let res = super::bincache::worker_read(
                            job.rt,
                            job.series,
                            job.bin_len,
                            job.msp,
                            job.offs,
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
                        let res = crate::events2::events::read_events_v02(params, scy.clone(), stmts.clone())
                            .await
                            .map_err(From::from);
                        if tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                }
            })
            .buffer_unordered(CONCURRENT_QUERIES_PER_WORKER)
            .for_each(|_| futures_util::future::ready(()))
            .await;
        info!("scylla worker finished");
        Ok(())
    }
}
