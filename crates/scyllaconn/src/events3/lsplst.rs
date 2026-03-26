use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::events3::SERIES_ID_A;
use crate::events3::SeriesInfo;
use crate::events3::msplsp::LspEv;
use crate::events3::msplsp::MspEv;
use crate::events3::test_data;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use daqbuf_series::SeriesId;
use futures_util::TryStreamExt;
use netpod::DtNano;
use netpod::TsMs;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;

macro_rules! error { ($($arg:tt)*) => { log::error!($($arg)*); }; }

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
    ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    end: Option<LspEv>,
    tx: async_channel::Sender<Item>,
}

impl Read03LspLst {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        end: Option<LspEv>,
    ) -> (Self, async_channel::Receiver<Item>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self {
            ks,
            series_info,
            msp,
            end,
            tx,
        };
        (ret, rx)
    }

    pub async fn exec(self, stmts: &StmtsEventsQueryOpts, scy: &Session) {
        let res = self.exec_inner(stmts, scy).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_inner(&self, stmts: &StmtsEventsQueryOpts, scy: &Session) -> Item {
        let stmt = stmts
            .lsp_lst()
            .shape(self.series_info.shape())
            .st(self.series_info.scalar_type().to_scylla_table_name_id())?
            .clone();
        let lsp_max = self.end.clone().map_or(i64::MAX, |x| x.to_i64());
        let params = (self.series_info.id().to_i64(), self.msp.to_i64(), lsp_max);
        let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
        while let Some((v,)) = rows.try_next().await? {
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
