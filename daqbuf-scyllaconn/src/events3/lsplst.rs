use crate::events2::prepare::StmtsEventsClusterKeyspace;
use crate::events3::SERIES_ID_A;
use crate::events3::SeriesInfo;
use crate::events3::msplsp::LspEv;
use crate::events3::msplsp::MspEv;
use crate::events3::test_data;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaOptsDefault;
use crate::worker::ScyllaOptsSubmit;
use futures_util::TryStreamExt;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;

macro_rules! warn { ($($arg:tt)*) => { log::warn!($($arg)*); }; }
macro_rules! debug { ($($arg:tt)*) => { log::debug!($($arg)*); }; }
macro_rules! trace { ($($arg:tt)*) => { log::trace!($($arg)*); }; }

fn _keep() {
    warn!("");
    debug!("");
    trace!("");
}

autoerr::create_error_v1!(
    name(Error, "Read03LspLst"),
    enum variants {
        Send,
        Recv,
        NoKs,
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        Prepare(#[from] crate::events2::prepare::Error),
    },
);

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_: async_channel::SendError<T>) -> Self {
        Error::Send
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(_: async_channel::RecvError) -> Self {
        Error::Recv
    }
}

pub type Item = Result<Option<LspEv>, Error>;

pub struct Read03LspLst {
    #[allow(unused)]
    ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    end: Option<LspEv>,
    scyopts: ScyllaOptsSubmit,
    tx: async_channel::Sender<Item>,
}

impl Read03LspLst {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        end: Option<LspEv>,
        scyopts: ScyllaOptsSubmit,
    ) -> (Self, async_channel::Receiver<Item>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self {
            ks,
            series_info,
            msp,
            end,
            scyopts,
            tx,
        };
        (ret, rx)
    }

    pub async fn exec(self, stmts: &StmtsEventsClusterKeyspace, scy: &Session, scyopts: &ScyllaOptsDefault) {
        let res = self.exec_inner(stmts, scy, scyopts).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_inner(&self, stmts: &StmtsEventsClusterKeyspace, scy: &Session, scyopts: &ScyllaOptsDefault) -> Item {
        let scyopts = self.scyopts.resolve(scyopts);
        let cby = scyopts.lsp_desc_cache_bypass.to_bool();
        debug!("Read03LspLst  cache_bypass {cby}  E1");
        let stmt = stmts
            .cache_bypass(cby)
            .lsp_lst()
            .shape(self.series_info.shape())
            .st(self.series_info.scalar_type().to_scylla_table_name_id())?
            .clone();
        let lsp_max = self.end.clone().map_or(i64::MAX, |x| x.to_i64());
        let params = (self.series_info.id().to_i64(), self.msp.to_i64(), lsp_max);
        let mut rows = scy
            .execute_iter(stmt, params)
            .await
            .map_err(|e| {
                let ks = &self.ks;
                warn!("error Read03LspLst  execute  {ks:?}  {e}");
                e
            })?
            .rows_stream::<(i64,)>()
            .map_err(|e| {
                let ks = &self.ks;
                warn!("error Read03LspLst  typed  {ks:?}  {e}");
                e
            })?;
        while let Some((v,)) = rows.try_next().await.map_err(|e| {
            let ks = &self.ks;
            warn!("error Read03LspLst  rows  {ks:?}  {e}");
            e
        })? {
            return Ok(Some(LspEv::from_i64(v)));
        }
        Ok(None)
    }

    pub async fn exec_mock(self, cltag: &str, ks: KeyspaceId) {
        let res = self.exec_mock_inner(cltag, ks).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_mock_inner(&self, cltag: &str, ks: KeyspaceId) -> Item {
        let lsp_max = self.end.unwrap_or(LspEv::max());
        if self.series_info.id() == SERIES_ID_A {
            if cltag == "mock1" {
                match ks.rt() {
                    RetentionTime::Short => {
                        let ret = test_data::produce_full_event_set()
                            .by_msp
                            .get(&self.msp)
                            .map_or(None, |x| {
                                x.iter()
                                    .rev()
                                    .map(|x| *x)
                                    .filter(|(_, lsp, ..)| *lsp < lsp_max)
                                    .map(|x| x.1)
                                    .next()
                            });
                        Ok(ret)
                    }
                    RetentionTime::Medium => todo!(),
                    RetentionTime::Long => todo!(),
                }
            } else {
                Err(Error::NoKs)
            }
        } else {
            Ok(None)
        }
    }
}

impl fmt::Debug for Read03LspLst {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Read03LspLst").finish()
    }
}

pub type ItemLspOnly = Result<VecDeque<LspEv>, Error>;

pub struct Read03LspOnly {
    #[allow(unused)]
    ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    scyopts: ScyllaOptsSubmit,
    tx: async_channel::Sender<ItemLspOnly>,
}

impl Read03LspOnly {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        scyopts: ScyllaOptsSubmit,
    ) -> (Self, async_channel::Receiver<ItemLspOnly>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self {
            ks,
            series_info,
            msp,
            scyopts,
            tx,
        };
        (ret, rx)
    }

    pub async fn exec(self, stmts: &StmtsEventsClusterKeyspace, scy: &Session, scyopts: &ScyllaOptsDefault) {
        let res = self.exec_inner(stmts, scy, scyopts).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_inner(
        &self,
        stmts: &StmtsEventsClusterKeyspace,
        scy: &Session,
        scyopts: &ScyllaOptsDefault,
    ) -> ItemLspOnly {
        let scyopts = self.scyopts.resolve(scyopts);
        let cby = scyopts.lsp_desc_cache_bypass.to_bool();
        // TODO actually pass Shape to the query selector
        let is_array = match self.series_info.shape() {
            netpod::Shape::Scalar => false,
            netpod::Shape::Wave(_) => true,
            netpod::Shape::Image(_, _) => true,
        };
        let cltag = stmts.cltag();
        let stmt = stmts
            .cache_bypass(cby)
            .lsp(false, false)
            .shape(is_array)
            .st(self.series_info.scalar_type().to_scylla_table_name_id())?
            .clone();
        let lsp_min = 0i64;
        let lsp_max = i64::MAX;
        let limit = 812412;
        let params = (
            self.series_info.id().to_i64(),
            self.msp.to_i64(),
            lsp_min,
            lsp_max,
            limit,
        );
        let mut rows = scy
            .execute_iter(stmt, params)
            .await
            .map_err(|e| {
                let ks = &self.ks;
                warn!("error Read03LspOnly  execute  {cltag}  {ks:?}  {e}");
                e
            })?
            .rows_stream::<(i64,)>()
            .map_err(|e| {
                let ks = &self.ks;
                warn!("error Read03LspOnly  typed  {cltag}  {ks:?}  {e}");
                e
            })?;
        let mut res = VecDeque::new();
        while let Some((v,)) = rows.try_next().await.map_err(|e| {
            let ks = &self.ks;
            warn!("error Read03LspOnly  rows  {cltag}  {ks:?}  {e}");
            e
        })? {
            res.push_back(LspEv::from_i64(v));
        }
        Ok(res)
    }

    pub async fn exec_mock(self, cltag: &str, ks: KeyspaceId) {
        let res = self.exec_mock_inner(cltag, ks).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_mock_inner(&self, cltag: &str, ks: KeyspaceId) -> ItemLspOnly {
        let lsp_max = netpod::todoval();
        if self.series_info.id() == SERIES_ID_A {
            if cltag == "mock1" {
                match ks.rt() {
                    RetentionTime::Short => {
                        let _ret = test_data::produce_full_event_set()
                            .by_msp
                            .get(&self.msp)
                            .map_or(None, |x| {
                                x.iter()
                                    .rev()
                                    .map(|x| *x)
                                    .filter(|(_, lsp, ..)| *lsp < lsp_max)
                                    .map(|x| x.1)
                                    .next()
                            });
                        todo!("TODO impl")
                    }
                    RetentionTime::Medium => todo!(),
                    RetentionTime::Long => todo!(),
                }
            } else {
                Err(Error::NoKs)
            }
        } else {
            todo!("TODO impl")
        }
    }
}

impl fmt::Debug for Read03LspOnly {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Read03LspOnly").finish()
    }
}
