mod read_events_03;

use crate::binwriteindex::BinWriteIndexEntry;
use crate::conn::create_scy_session_no_ks;
use crate::events2::events::ReadJobTrace;
use crate::events2::prepare::StmtsEvents;
use crate::events2::prepare::StmtsEventsClusterKeyspace;
use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::events3::SeriesInfo;
use crate::events3::msplsp::LspEv;
use crate::events3::msplsp::MspEv;
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
use netpod::RangeExcl;
use netpod::ScalarType;
use netpod::ScyllaConfig;
use netpod::ScyllaConfigMultiKeyspace;
use netpod::Shape;
use netpod::TsMs;
use netpod::ttl::RetentionTime;
use serde::Serialize;
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
        ReadEvents03(#[from] read_events_03::Error),
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
        ClusterKeyspaceNoMatch,
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
    limit: Option<u32>,
    bck: bool,
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    tx: Sender<Result<VecDeque<TsMs>, Error>>,
}

impl FindTsMsp {
    async fn execute_inner(self, stmts: &StmtsEventsQueryOpts, scy: &ScySessTy) -> Result<VecDeque<TsMs>, Error> {
        let res = crate::events2::msp::find_ts_msp(
            &self.rt,
            self.series.id(),
            self.range.clone(),
            self.limit,
            self.bck.clone(),
            self.scylla_opts.clone(),
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

    async fn execute(self, stmts: &StmtsEventsQueryOpts, scy: &ScySessTy) {
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
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
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
    scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
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
                    .cache_bypass(self.scylla_opts.bins_fwd_cache_bypass())
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
    ReadMsp03Fwd(crate::events3::mspfwd::ReadMsp03Fwd),
    ReadMsp03Bck(crate::events3::mspbck::ReadMsp03Bck),
    ReadEvents03Fwd(
        ReadEvents03FwdParams,
        Sender<
            Result<
                (
                    Box<dyn BinningggContainerEventsDyn>,
                    crate::events3::jobtrace::ReadJobTrace,
                ),
                Error,
            >,
        >,
    ),
    Read03LspLst(crate::events3::lsplst::Read03LspLst),
    Read03LspFwd(crate::events3::lspfwd::Read03LspFwd),
}

#[derive(Debug, Clone)]
pub struct EventReadOpts {
    pub with_values: bool,
    pub one_before: bool,
    pub qucap: u32,
    pub scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
}

impl EventReadOpts {
    pub fn new(
        one_before: bool,
        with_values: bool,
        qucap: Option<u32>,
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Self {
        Self {
            one_before,
            with_values,
            qucap: qucap.unwrap_or(6),
            scylla_opts,
        }
    }

    pub fn with_values(&self) -> bool {
        self.with_values
    }
}

#[derive(Debug, Clone)]
pub struct ReadEventsJobParams {
    pub series: SeriesId,
    pub rt: RetentionTime,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub ts_msp: TsMs,
    pub range: ScyllaSeriesRange,
    pub fwd: bool,
    pub readopts: EventReadOpts,
}

#[derive(Debug, Clone)]
pub struct ReadEvents03FwdParams {
    pub series: SeriesId,
    pub rt: RetentionTime,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub ts_msp: TsMs,
    pub range: ScyllaSeriesRange,
    pub with_values: bool,
    pub scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct KeyspaceId {
    name: String,
    rt: RetentionTime,
}

impl KeyspaceId {
    pub fn new(name: String, rt: RetentionTime) -> Self {
        Self { name, rt }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rt(&self) -> RetentionTime {
        self.rt.clone()
    }
}

#[derive(Debug, Clone)]
pub struct ScyllaQueueCluster {
    tag: String,
    keyspaces: Vec<KeyspaceId>,
    tx: Sender<(KeyspaceId, Job)>,
    // TODO the last Self should await jh:
    #[allow(unused)]
    jh: Arc<tokio::task::JoinHandle<Result<(), Error>>>,
}

impl ScyllaQueueCluster {
    async fn new(scyconf: &ScyllaConfigMultiKeyspace) -> Result<Self, Error> {
        let tag = scyconf.tag.clone();
        let (tx, rx) = async_channel::bounded(128);
        let task = Self::worker(rx, scyconf.clone());
        let jh = tokio::task::spawn(task);
        let ret = Self {
            tag,
            keyspaces: scyconf
                .keyspaces
                .iter()
                .map(|x| KeyspaceId::new(x.0.clone(), x.1.clone()))
                .collect(),
            tx,
            jh: Arc::new(jh),
        };
        Ok(ret)
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn keyspaces(&self) -> &[KeyspaceId] {
        &self.keyspaces
    }

    pub async fn prepare(
        &self,
        ks: KeyspaceId,
        cql: String,
    ) -> Result<scylla::statement::prepared::PreparedStatement, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::PrepareV1(PrepareV1 { cql, tx });
        self.tx
            .send((ks, job))
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
        ks: KeyspaceId,
        st: scylla::statement::prepared::PreparedStatement,
        params: Box<dyn scylla::serialize::row::SerializeRow + Send>,
    ) -> Result<scylla::client::pager::QueryPager, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ExecuteV1(ExecuteV1 { st, params, tx });
        self.tx
            .send((ks, job))
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelSend)?;
        let res = rx
            .recv()
            .await
            .map_err(|_| streams::timebin::cached::reader::Error::ChannelRecv)??;
        Ok(res)
    }

    async fn find_ts_msp(
        &self,
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        limit: Option<u32>,
        bck: bool,
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Result<VecDeque<TsMs>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = FindTsMsp {
            rt: ks.rt.clone(),
            series,
            range,
            limit,
            bck,
            scylla_opts,
            tx,
        };
        let job = Job::FindTsMsp(job);
        self.tx.send((ks, job)).await.map_err(|_| Error::JobChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn find_ts_msp_fwd(
        &self,
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        limit: Option<u32>,
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Result<VecDeque<TsMs>, Error> {
        self.find_ts_msp(ks, series, range, limit, false, scylla_opts).await
    }

    pub async fn find_ts_msp_bck(
        &self,
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        limit: Option<u32>,
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Result<VecDeque<TsMs>, Error> {
        self.find_ts_msp(ks, series, range, limit, true, scylla_opts).await
    }

    pub async fn read_msp_03_fwd(
        &self,
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        begexcl: RangeExcl,
        limit: Option<u32>,
    ) -> crate::events3::mspfwd::Item {
        debug!("read_msp_03_fwd  {ks:?}  {series:?}  {range:?}  {begexcl:?}  {limit:?}");
        let limit = limit.unwrap_or(40);
        let (job, rx) = crate::events3::mspfwd::ReadMsp03Fwd::new(ks.clone(), series, range, begexcl, limit);
        let job = Job::ReadMsp03Fwd(job);
        self.tx.send((ks, job)).await?;
        let res = rx.recv().await.inspect_err(|e| {
            eprintln!("GOT RECV ERROR {e}");
        })?;
        res
    }

    pub async fn read_msp_03_bck(
        &self,
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
    ) -> crate::events3::mspbck::Item {
        let (job, rx) = crate::events3::mspbck::ReadMsp03Bck::new(ks.clone(), series, range);
        let job = Job::ReadMsp03Bck(job);
        self.tx.send((ks, job)).await?;
        let res = rx.recv().await??;
        Ok(res)
    }

    pub async fn read_03_lsp_lst(
        &self,
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        end: Option<LspEv>,
    ) -> crate::events3::lsplst::Item {
        let (job, rx) = crate::events3::lsplst::Read03LspLst::new(ks.clone(), series_info, msp, end);
        let job = Job::Read03LspLst(job);
        self.tx.send((ks, job)).await?;
        let res = rx.recv().await??;
        Ok(res)
    }

    pub async fn read_03_lsp_fwd(
        &self,
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        range: ScyllaSeriesRange,
        limit: u32,
    ) -> crate::events3::lspfwd::Item {
        let (job, rx) = crate::events3::lspfwd::Read03LspFwd::new(ks.clone(), series_info, msp, range, limit);
        let job = Job::Read03LspFwd(job);
        self.tx.send((ks, job)).await?;
        let res = rx.recv().await??;
        Ok(res)
    }

    async fn worker(rx: Receiver<(KeyspaceId, Job)>, scyconf: ScyllaConfigMultiKeyspace) -> Result<(), Error> {
        let scy = create_scy_session_no_ks(&scyconf).await?;
        let scy = Arc::new(scy);
        let mut stmtsa = Vec::new();
        for (ks, rt) in &scyconf.keyspaces {
            debug!("scylla worker  prepare start");
            let stmts = StmtsEventsClusterKeyspace::new(ks, &rt, &scy).await?;
            stmtsa.push(stmts);
        }
        let stmtsa = Arc::new(stmtsa);
        let scyconf = Arc::new(scyconf);
        debug!("scylla worker  prepare done");
        rx.map(|(ksjob, job)| {
            let mut stmtsix = None;
            for (i, ((ks2, _rt), _stmts)) in scyconf.keyspaces.iter().zip(stmtsa.iter()).enumerate() {
                if ks2 == &ksjob.name {
                    stmtsix = Some(i);
                    break;
                }
            }
            (ksjob, job, stmtsix)
        })
        .filter_map(|(ksjob, job, stmtsix)| {
            if let Some(i) = stmtsix {
                futures_util::future::ready(Some((ksjob, job, i)))
            } else {
                futures_util::future::ready(None)
            }
        })
        .map(|(ksjob, job, stmtsix)| {
            let scy = scy.clone();
            let stmtsa = stmtsa.clone();
            let scyconf = scyconf.clone();
            async move {
                match job {
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
                    Job::FindTsMsp(job) => {
                        let stmts = &stmtsa[stmtsix];
                        let stmts = stmts.cache_bypass(job.scylla_opts.msp_cache_bypass());
                        job.execute(stmts, &scy).await;
                    }
                    Job::AccountingReadTs(rt, ts, tx) => {
                        let _ = rt;
                        for ((ks, rt), _stmts) in scyconf.keyspaces.iter().zip(stmtsa.iter()) {
                            let res = crate::accounting::toplist::read_ts(ks, rt.clone(), ts, &scy).await;
                            error!("TODO return accounting data for dynamic configured list of keyspaces");
                            if tx.send(res.map_err(Into::into)).await.is_err() {
                                // TODO count for stats
                            }
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
                    Job::ReadPrebinnedF32(_job) => {
                        // TODO where is it used?
                        error!("ReadPrebinnedF32 adapt to StmtsEventsClusterKeyspace");
                        // let res = super::bincache::worker_read(
                        //     job.rt,
                        //     job.series,
                        //     job.bin_len,
                        //     job.msp,
                        //     job.offs,
                        //     job.scylla_opts,
                        //     &stmts,
                        //     &scy,
                        // )
                        // .await;
                        // if job.tx.send(res).await.is_err() {
                        //     // TODO count for stats
                        // }
                    }
                    Job::BinWriteIndexRead(_job) => {
                        error!("BinWriteIndexRead adapt to StmtsEventsClusterKeyspace");
                        // job.execute(&stmts, &scy).await
                    }
                    Job::ReadEvents02(_params, _tx) => {
                        error!("ReadEvents02 adapt to StmtsEventsClusterKeyspace");
                        // crate::events2::events::read_events_v02(params, tx, stmts.clone(), scy.clone()).await
                    }
                    Job::ReadMsp03Fwd(job) => {
                        if let Some(((_ks, _rt), stmts)) = scyconf
                            .keyspaces
                            .iter()
                            .zip(stmtsa.iter())
                            .filter(|((ks, rt), _)| *ks == ksjob.name && *rt == ksjob.rt)
                            .next()
                        {
                            let stmts = stmts.cache_bypass(true);
                            job.exec(stmts, &scy).await;
                        } else {
                            // TODO use a nested Result instead.
                            error!("ks not found for job");
                        }
                    }
                    Job::ReadMsp03Bck(job) => {
                        if let Some(((_ks, _rt), stmts)) = scyconf
                            .keyspaces
                            .iter()
                            .zip(stmtsa.iter())
                            .filter(|((ks, rt), _)| *ks == ksjob.name && *rt == ksjob.rt)
                            .next()
                        {
                            let stmts = stmts.cache_bypass(true);
                            job.exec(stmts, &scy).await;
                        } else {
                            // TODO use a nested Result instead.
                            error!("ks not found for job");
                        }
                    }
                    Job::ReadEvents03Fwd(params, tx) => {
                        let mut ret = None;
                        for ((_, _), stmts) in scyconf.keyspaces.iter().zip(stmtsa.iter()) {
                            // TODO this whole worker is dedicated to one cluster.
                            // The job must get routed to the correct worker.
                            // Here, the job must indicate the keyspace to use.
                            warn!("TODO  ReadEvents03Fwd  using first available keyspace");
                            let stmts = stmts.cache_bypass(true);
                            ret = Some(read_events_03::read_fwd(params, stmts, scy.clone()).await);
                            break;
                        }
                        if let Some(x) = ret {
                            let _ = tx.send(x.map_err(Into::into));
                        } else {
                            let _ = tx.send(Err(Error::ClusterKeyspaceNoMatch));
                        }
                    }
                    Job::Read03LspLst(job) => {
                        if let Some(((_ks, _rt), stmts)) = scyconf
                            .keyspaces
                            .iter()
                            .zip(stmtsa.iter())
                            .filter(|((ks, rt), _)| *ks == ksjob.name && *rt == ksjob.rt)
                            .next()
                        {
                            let stmts = stmts.cache_bypass(true);
                            job.exec(stmts, &scy).await;
                        } else {
                            // TODO use a nested Result instead.
                            error!("ks not found for job");
                        }
                    }
                    Job::Read03LspFwd(job) => {
                        if let Some(((_ks, _rt), stmts)) = scyconf
                            .keyspaces
                            .iter()
                            .zip(stmtsa.iter())
                            .filter(|((ks, rt), _)| *ks == ksjob.name && *rt == ksjob.rt)
                            .next()
                        {
                            let stmts = stmts.cache_bypass(true);
                            job.exec(stmts, &scy).await;
                        } else {
                            // TODO use a nested Result instead.
                            error!("ks not found for job");
                        }
                    }
                }
            }
        })
        .buffer_unordered(CONCURRENT_QUERIES_PER_WORKER)
        .for_each(|_| futures_util::future::ready(()))
        .await;
        Ok(())
    }

    pub fn new_mock(tag: String, keyspaces: Vec<KeyspaceId>) -> Result<Self, Error> {
        let (tx, rx) = async_channel::bounded(128);
        let task = Self::worker_mock(rx, tag.clone());
        let jh = tokio::task::spawn(task);
        let ret = Self {
            tag,
            keyspaces,
            tx,
            jh: Arc::new(jh),
        };
        Ok(ret)
    }

    async fn worker_mock(rx: Receiver<(KeyspaceId, Job)>, tag: String) -> Result<(), Error> {
        let tag = &tag;
        rx.map(|(ksjob, job)| async move {
            match job {
                Job::PrepareV1(..) => {
                    debug!("can not execute Job::PrepareV1 in mock");
                }
                Job::ExecuteV1(..) => {
                    debug!("can not execute Job::ExecuteV1 in mock");
                }
                Job::FindTsMsp(..) => {
                    debug!("can not execute Job::FindTsMsp in mock");
                }
                Job::AccountingReadTs(..) => {
                    debug!("can not execute Job::AccountingReadTs in mock");
                }
                Job::WriteCacheF32(..) => {
                    debug!("can not execute Job::WriteCacheF32 in mock");
                }
                Job::ReadPrebinnedF32(..) => {
                    debug!("can not execute Job::ReadPrebinnedF32 in mock");
                }
                Job::BinWriteIndexRead(..) => {
                    debug!("can not execute Job::BinWriteIndexRead in mock");
                }
                Job::ReadEvents02(..) => {
                    debug!("can not execute Job::ReadEvents02 in mock");
                }
                Job::ReadMsp03Fwd(job) => {
                    job.exec_mock(&tag, ksjob).await;
                }
                Job::ReadMsp03Bck(job) => {
                    job.exec_mock(&tag, ksjob).await;
                }
                Job::ReadEvents03Fwd(..) => {
                    debug!("can not execute Job::ReadEvents03Fwd in mock");
                }
                Job::Read03LspLst(job) => {
                    job.exec_mock(&tag, ksjob).await;
                }
                Job::Read03LspFwd(job) => {
                    job.exec_mock(&tag, ksjob).await;
                }
            }
        })
        .buffer_unordered(CONCURRENT_QUERIES_PER_WORKER)
        .for_each(|_| futures_util::future::ready(()))
        .await;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ScyllaQueue {
    tx: Sender<Job>,
    clusters: Vec<Arc<ScyllaQueueCluster>>,
}

impl ScyllaQueue {
    pub fn clusters(&self) -> &Vec<Arc<ScyllaQueueCluster>> {
        &self.clusters
    }

    pub fn into_clusters(self) -> impl Iterator<Item = Arc<ScyllaQueueCluster>> {
        self.clusters.into_iter()
    }

    pub async fn find_ts_msp(
        &self,
        rt: RetentionTime,
        series: SeriesId,
        range: ScyllaSeriesRange,
        bck: bool,
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Result<VecDeque<TsMs>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = FindTsMsp {
            rt,
            series,
            range,
            limit: None,
            bck,
            scylla_opts,
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

    pub async fn read_events_03_fwd(
        &self,
        params: ReadEvents03FwdParams,
    ) -> Result<
        (
            Box<dyn BinningggContainerEventsDyn>,
            crate::events3::jobtrace::ReadJobTrace,
        ),
        Error,
    > {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadEvents03Fwd(params, tx);
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
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadPrebinnedF32(ReadPrebinnedF32 {
            rt,
            series,
            bin_len,
            msp,
            offs,
            scylla_opts,
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
        scylla_opts: query::api4::scyllaopts::ScyllaOptsQuery,
    ) -> Result<VecDeque<BinWriteIndexEntry>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = BinWriteIndexRead {
            rt,
            series,
            pbp,
            msp,
            lsp_min,
            lsp_max,
            scylla_opts,
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
        scyconf_multi: &[ScyllaConfigMultiKeyspace],
    ) -> Result<(ScyllaQueue, tokio::task::JoinHandle<()>), Error> {
        let clusters = {
            let mut a = Vec::new();
            for e in scyconf_multi.iter() {
                let x = ScyllaQueueCluster::new(e).await?;
                a.push(Arc::new(x));
            }
            a
        };
        let (tx, rx) = async_channel::bounded(SCYLLA_WORKER_QUEUE_LEN);
        let queue = ScyllaQueue { tx, clusters };
        let worker = Self {
            rx,
            scyconf_st,
            scyconf_mt,
            scyconf_lt,
        };
        let jh = taskrun::spawn(async move {
            match worker.work().await {
                Ok(()) => {}
                Err(e) => {
                    error!("received error from ScyllaWorker: {}", e);
                }
            }
        });
        Ok((queue, jh))
    }

    async fn work_inner(&self) -> Result<(), Error> {
        let scy = create_scy_session_no_ks(&self.scyconf_st).await?;
        let scy = Arc::new(scy);
        debug!("scylla worker  prepare start");
        let kss = [
            self.scyconf_st.keyspace.as_str(),
            self.scyconf_mt.keyspace.as_str(),
            self.scyconf_lt.keyspace.as_str(),
        ];
        let stmts_st = StmtsEventsClusterKeyspace::new(&self.scyconf_st.keyspace, &RetentionTime::Short, &scy).await?;
        let stmts_mt = StmtsEventsClusterKeyspace::new(&self.scyconf_mt.keyspace, &RetentionTime::Medium, &scy).await?;
        let stmts_lt = StmtsEventsClusterKeyspace::new(&self.scyconf_lt.keyspace, &RetentionTime::Long, &scy).await?;
        let stmts = StmtsEvents::new(kss.try_into().map_err(|_| Error::MissingKeyspaceConfig)?, &scy).await?;
        let stmts = Arc::new(stmts);
        debug!("scylla worker  prepare done");
        self.rx
            .clone()
            .map(|job| async {
                match job {
                    Job::FindTsMsp(job) => {
                        let stmts = match job.rt {
                            RetentionTime::Short => &stmts_st,
                            RetentionTime::Medium => &stmts_mt,
                            RetentionTime::Long => &stmts_lt,
                        };
                        let stmts = stmts.cache_bypass(job.scylla_opts.msp_cache_bypass());
                        job.execute(stmts, &scy).await
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
                        // TODO remove I guess?
                        let res = super::bincache::worker_read(
                            job.rt,
                            job.series,
                            job.bin_len,
                            job.msp,
                            job.offs,
                            job.scylla_opts,
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
                    Job::ReadMsp03Fwd(..) => {
                        error!("TODO  Job::ReadMsp03Fwd  only on cluster aware worker");
                    }
                    Job::ReadMsp03Bck(..) => {
                        error!("TODO  Job::ReadMsp03Bck  only on cluster aware worker");
                    }
                    Job::ReadEvents03Fwd(..) => {
                        error!("TODO  Job::ReadEvents03Fwd  only on cluster aware worker");
                    }
                    Job::Read03LspLst(..) => {
                        error!("TODO  Job::Read03LspLst  only on cluster aware worker");
                    }
                    Job::Read03LspFwd(..) => {
                        error!("TODO  Job::Read03LspFwd  only on cluster aware worker");
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
