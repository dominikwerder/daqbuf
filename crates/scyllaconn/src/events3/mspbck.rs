use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::events3::MSP_A_00;
use crate::events3::SERIES_ID_A;
use crate::events3::test_data;
use crate::events3::test_data::series_a_msps;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use daqbuf_series::SeriesId;
use futures_util::TryStreamExt;
use netpod::DtNano;
use netpod::TsMs;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;

autoerr::create_error_v1!(
    name(Error, "ReadMsp03Bck"),
    enum variants {
        Send,
        Recv,
        NoKs,
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
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

pub type Item = Result<VecDeque<TsMs>, Error>;

pub struct ReadMsp03Bck {
    ks: KeyspaceId,
    series: SeriesId,
    range: ScyllaSeriesRange,
    tx: async_channel::Sender<Item>,
}

impl ReadMsp03Bck {
    pub fn new(ks: KeyspaceId, series: SeriesId, range: ScyllaSeriesRange) -> (Self, async_channel::Receiver<Item>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self { ks, series, range, tx };
        (ret, rx)
    }

    pub async fn exec(self, stmts: &StmtsEventsQueryOpts, scy: &Session) {
        let res = self.exec_inner(stmts, scy).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_inner(&self, stmts: &StmtsEventsQueryOpts, scy: &Session) -> Item {
        let stmt = stmts.ts_msp_bck_win().clone();
        let win = self.ks.rt().msp_rollover_ivl_on_read();
        let beg = self.range.beg().sub(DtNano::from_ms(1000 * win.as_secs()));
        let end = self.range.beg();
        let params = (self.series.to_i64(), beg.ms() as i64, end.ms() as i64);
        let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
        let mut ret = VecDeque::new();
        while let Some((v,)) = rows.try_next().await? {
            ret.push_back(TsMs::from_ms_u64(v as _));
        }
        Ok(ret)
    }

    pub async fn exec_mock(self, cltag: &str, ks: KeyspaceId) {
        let res = self.exec_mock_inner(cltag, ks).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_mock_inner(&self, cltag: &str, ks: KeyspaceId) -> Item {
        if self.series == SERIES_ID_A {
            if cltag == "mock1" {
                match ks.rt() {
                    RetentionTime::Short => {
                        let ret = series_a_msps()
                            .into_iter()
                            .filter(test_data::pred_ms_range(self.range.clone()))
                            .collect();
                        Ok(ret)
                    }
                    RetentionTime::Medium => todo!(),
                    RetentionTime::Long => todo!(),
                }
            } else {
                Err(Error::NoKs)
            }
        } else {
            Ok(VecDeque::new())
        }
    }
}

impl fmt::Debug for ReadMsp03Bck {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ReadMsp03Bck").finish()
    }
}
