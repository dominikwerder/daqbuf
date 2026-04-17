pub(super) mod read_events_03;

use crate::binwriteindex::BinWriteIndexEntry;
use crate::conn::create_scy_session_no_ks;
use crate::events2::prepare::StmtsEvents;
use crate::events2::prepare::StmtsEventsClusterKeyspace;
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
use items_2::binning::container_bins::ContainerBins;
use netpod::CacheBypass;
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
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }

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

#[derive(Debug, Clone)]
pub struct ScyllaOptsDefault {
    msp_cache_bypass: CacheBypass,
    lsp_asc_cache_bypass: CacheBypass,
    lsp_desc_cache_bypass: CacheBypass,
    bins_fwd_cache_bypass: CacheBypass,
    avoid_order_desc: bool,
}

impl ScyllaOptsDefault {
    pub fn from_config_v0(conf: &ScyllaConfig) -> Self {
        Self {
            msp_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_asc),
            lsp_asc_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_asc),
            lsp_desc_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_desc),
            bins_fwd_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_asc),
            avoid_order_desc: conf.avoid_order_desc,
        }
    }

    pub fn from_config_v1(conf: &ScyllaConfigMultiKeyspace) -> Self {
        Self {
            msp_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_asc),
            lsp_asc_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_asc),
            lsp_desc_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_desc),
            bins_fwd_cache_bypass: CacheBypass::from_bool(conf.cache_bypass_asc),
            avoid_order_desc: conf.avoid_order_desc,
        }
    }

    pub fn for_mock() -> Self {
        Self {
            msp_cache_bypass: CacheBypass::Cache,
            lsp_asc_cache_bypass: CacheBypass::Cache,
            lsp_desc_cache_bypass: CacheBypass::Cache,
            bins_fwd_cache_bypass: CacheBypass::Cache,
            avoid_order_desc: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScyllaOptsSubmit {
    pub msp_cache_bypass: Option<CacheBypass>,
    pub lsp_asc_cache_bypass: Option<CacheBypass>,
    pub lsp_desc_cache_bypass: Option<CacheBypass>,
    pub bins_fwd_cache_bypass: Option<CacheBypass>,
    pub avoid_order_desc: Option<bool>,
}

impl ScyllaOptsSubmit {
    pub fn no_choice() -> Self {
        Self {
            msp_cache_bypass: None,
            lsp_asc_cache_bypass: None,
            lsp_desc_cache_bypass: None,
            bins_fwd_cache_bypass: None,
            avoid_order_desc: None,
        }
    }

    pub fn resolve(&self, oth: &ScyllaOptsDefault) -> ScyllaOptsJob {
        ScyllaOptsJob {
            msp_cache_bypass: self.msp_cache_bypass.clone().unwrap_or(oth.msp_cache_bypass.clone()),
            lsp_asc_cache_bypass: self
                .lsp_asc_cache_bypass
                .clone()
                .unwrap_or(oth.lsp_asc_cache_bypass.clone()),
            lsp_desc_cache_bypass: self
                .lsp_desc_cache_bypass
                .clone()
                .unwrap_or(oth.lsp_desc_cache_bypass.clone()),
            bins_fwd_cache_bypass: self
                .bins_fwd_cache_bypass
                .clone()
                .unwrap_or(oth.bins_fwd_cache_bypass.clone()),
            avoid_order_desc: self.avoid_order_desc.clone().unwrap_or(oth.avoid_order_desc.clone()),
        }
    }
}

#[derive(Debug)]
pub struct ScyllaOptsJob {
    pub msp_cache_bypass: CacheBypass,
    pub lsp_asc_cache_bypass: CacheBypass,
    pub lsp_desc_cache_bypass: CacheBypass,
    pub bins_fwd_cache_bypass: CacheBypass,
    pub avoid_order_desc: bool,
}

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
struct ReadPrebinnedF32 {
    rt: RetentionTime,
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: core::ops::Range<u32>,
    pub scyopts: ScyllaOptsSubmit,
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
    scylla_opts: ScyllaOptsSubmit,
    tx: Sender<Result<VecDeque<BinWriteIndexEntry>, Error>>,
}

impl BinWriteIndexRead {
    async fn execute_inner(self, stmts: &StmtsEvents, scy: &ScySessTy, scyopts: ScyllaOptsJob) -> Result<(), Error> {
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
                    .cache_bypass(scyopts.bins_fwd_cache_bypass.clone().into())
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

    async fn execute(self, stmts: &StmtsEvents, scy: &ScySessTy, scyopts: ScyllaOptsJob) {
        // TODO avoid the extra clone
        let tx = self.tx.clone();
        match self.execute_inner(stmts, scy, scyopts).await {
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
    AccountingReadTs(
        KeyspaceId,
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
    Read03MspFwd(crate::events3::mspfwd::Read03MspFwd),
    Read03LspOnly(crate::events3::lsplst::Read03LspOnly),
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
    scyopts: Arc<ScyllaOptsDefault>,
    tx: Sender<(KeyspaceId, Job)>,
    // TODO the last Self should await jh:
    #[allow(unused)]
    jh: Arc<tokio::task::JoinHandle<Result<(), Error>>>,
}

impl ScyllaQueueCluster {
    async fn new(scyconf: &ScyllaConfigMultiKeyspace) -> Result<Self, Error> {
        let tag = scyconf.tag.clone();
        let scyopts = ScyllaOptsDefault::from_config_v1(scyconf);
        let (tx, rx) = async_channel::bounded(128);
        let task = Self::worker(rx, scyconf.clone(), scyopts.clone());
        let jh = tokio::task::spawn(task);
        let ret = Self {
            tag,
            keyspaces: scyconf
                .keyspaces
                .iter()
                .map(|x| KeyspaceId::new(x.0.clone(), x.1.clone()))
                .collect(),
            scyopts: Arc::new(scyopts),
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

    pub fn scyopts(&self) -> &ScyllaOptsDefault {
        &self.scyopts
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

    pub async fn accounting_read_ts(
        &self,
        ks: KeyspaceId,
        ts: TsMs,
    ) -> Result<crate::accounting::toplist::UsageData, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::AccountingReadTs(ks.clone(), ts, tx);
        self.tx.send((ks, job)).await.map_err(|_| Error::JobChannelSend)?;
        let res = rx.recv().await.map_err(|_| Error::ChannelRecv)??;
        Ok(res)
    }

    pub async fn read_03_msp_fwd(
        &self,
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        begexcl: RangeExcl,
        limit: u32,
        scyopts: ScyllaOptsSubmit,
    ) -> crate::events3::mspfwd::Item {
        trace!("read_msp_03_fwd  {ks:?}  {series:?}  {range:?}  {begexcl:?}  {limit:?}");
        let (job, rx) = crate::events3::mspfwd::Read03MspFwd::new(ks.clone(), series, range, begexcl, limit, scyopts);
        let job = Job::Read03MspFwd(job);
        self.tx.send((ks, job)).await?;
        let res = rx.recv().await.inspect_err(|e| {
            error!("GOT RECV ERROR {e}");
        })?;
        res
    }

    pub async fn read_03_lsp_only(
        &self,
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        scyopts: ScyllaOptsSubmit,
    ) -> crate::events3::lsplst::ItemLspOnly {
        let (job, rx) = crate::events3::lsplst::Read03LspOnly::new(ks.clone(), series_info, msp, scyopts);
        let job = Job::Read03LspOnly(job);
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
        scyopts: ScyllaOptsSubmit,
    ) -> crate::events3::lsplst::Item {
        let (job, rx) = crate::events3::lsplst::Read03LspLst::new(ks.clone(), series_info, msp, end, scyopts);
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
        begexcl: RangeExcl,
        limit: u32,
        scyopts: ScyllaOptsSubmit,
    ) -> crate::events3::lspfwd::Item {
        let (job, rx) =
            crate::events3::lspfwd::Read03LspFwd::new(ks.clone(), series_info, msp, range, begexcl, limit, scyopts);
        let job = Job::Read03LspFwd(job);
        self.tx.send((ks, job)).await?;
        let res = rx.recv().await??;
        Ok(res)
    }

    async fn worker(
        rx: Receiver<(KeyspaceId, Job)>,
        scyconf: ScyllaConfigMultiKeyspace,
        scyopts: ScyllaOptsDefault,
    ) -> Result<(), Error> {
        let scy = create_scy_session_no_ks(&scyconf).await?;
        let scy = Arc::new(scy);
        let mut stmtsa = Vec::new();
        for (ks, rt) in &scyconf.keyspaces {
            debug!("scylla worker  prepare start");
            let stmts = StmtsEventsClusterKeyspace::new(&scyconf.tag, ks, &rt, &scy).await?;
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
        .map(|(ksjob, job, _stmtsix)| {
            let scy = scy.clone();
            let stmtsa = stmtsa.clone();
            let scyconf = scyconf.clone();
            let scyopts = scyopts.clone();
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
                    Job::AccountingReadTs(ks, ts, tx) => {
                        for ((ks2, rt2), _stmts) in scyconf.keyspaces.iter().zip(stmtsa.iter()) {
                            if ks2 == ks.name() && *rt2 == ks.rt() {
                                let res = crate::accounting::toplist::read_ts(ks.name(), ks.rt(), ts, &scy).await;
                                if tx.send(res.map_err(Into::into)).await.is_err() {
                                    // TODO count for stats
                                }
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
                    Job::Read03MspFwd(job) => {
                        if let Some(((_ks, _rt), stmts)) = scyconf
                            .keyspaces
                            .iter()
                            .zip(stmtsa.iter())
                            .filter(|((ks, rt), _)| *ks == ksjob.name && *rt == ksjob.rt)
                            .next()
                        {
                            job.exec(stmts, &scy, &scyopts).await;
                        } else {
                            // TODO use a nested Result instead.
                            error!("ks not found for job");
                        }
                    }
                    Job::Read03LspOnly(job) => {
                        if let Some(((_ks, _rt), stmts)) = scyconf
                            .keyspaces
                            .iter()
                            .zip(stmtsa.iter())
                            .filter(|((ks, rt), _)| *ks == ksjob.name && *rt == ksjob.rt)
                            .next()
                        {
                            job.exec(stmts, &scy, &scyopts).await;
                        } else {
                            // TODO use a nested Result instead.
                            error!("ks not found for job");
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
                            job.exec(stmts, &scy, &scyopts).await;
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
                            job.exec(stmts, &scy, &scyopts).await;
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
            scyopts: Arc::new(ScyllaOptsDefault::for_mock()),
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
                Job::Read03MspFwd(job) => {
                    job.exec_mock(&tag, ksjob).await;
                }
                Job::Read03LspOnly(job) => {
                    job.exec_mock(&tag, ksjob).await;
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
        scyopts: ScyllaOptsSubmit,
    ) -> Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ReadPrebinnedF32(ReadPrebinnedF32 {
            rt,
            series,
            bin_len,
            msp,
            offs,
            scyopts,
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
        scylla_opts: ScyllaOptsSubmit,
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
    scyopts: ScyllaOptsDefault,
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
            scyopts: ScyllaOptsDefault::from_config_v0(&scyconf_st),
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
        let stmts = StmtsEvents::new(kss.try_into().map_err(|_| Error::MissingKeyspaceConfig)?, &scy).await?;
        let stmts = Arc::new(stmts);
        debug!("scylla worker  prepare done");
        self.rx
            .clone()
            .map(|job| async {
                match job {
                    Job::AccountingReadTs(ks, ts, tx) => {
                        let res = crate::accounting::toplist::read_ts(ks.name(), ks.rt(), ts, &scy).await;
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
                        let scyopts = job.scyopts.resolve(&self.scyopts);
                        let res = super::bincache::worker_read(
                            job.rt,
                            job.series,
                            job.bin_len,
                            job.msp,
                            job.offs,
                            scyopts,
                            &stmts,
                            &scy,
                        )
                        .await;
                        if job.tx.send(res).await.is_err() {
                            // TODO count for stats
                        }
                    }
                    Job::BinWriteIndexRead(job) => {
                        let scyopts = job.scylla_opts.resolve(&self.scyopts);
                        job.execute(&stmts, &scy, scyopts).await
                    }
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
                    Job::Read03MspFwd(..) => {
                        error!("TODO  Job::ReadMsp03Fwd  only on cluster aware worker");
                    }
                    Job::Read03LspOnly(..) => {
                        error!("TODO  Job::Read03LspOnly  only on cluster aware worker");
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
