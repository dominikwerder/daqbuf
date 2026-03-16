use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use daqbuf_series::SeriesId;
use futures_util::TryStreamExt;
use netpod::TsMs;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;

autoerr::create_error_v1!(
    name(Error, "ReadMsp03Fwd"),
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

pub struct ReadMsp03Fwd {
    ks: KeyspaceId,
    series: SeriesId,
    range: ScyllaSeriesRange,
    limit: u32,
    tx: async_channel::Sender<Item>,
}

impl ReadMsp03Fwd {
    pub fn new(
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        limit: u32,
    ) -> (Self, async_channel::Receiver<Item>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self {
            ks,
            series,
            range,
            limit,
            tx,
        };
        (ret, rx)
    }

    pub async fn exec(self, stmts: &StmtsEventsQueryOpts, scy: &Session) {
        let res = self.exec_inner(stmts, scy).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_inner(&self, stmts: &StmtsEventsQueryOpts, scy: &Session) -> Result<VecDeque<TsMs>, Error> {
        let stmt = stmts.ts_msp_fwd().clone();
        let limit = self.limit;
        let params = (
            self.series.to_i64(),
            self.range.beg().ms() as i64,
            self.range.end().ms() as i64,
            limit as i32,
        );
        let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
        let mut ret = VecDeque::new();
        while let Some((v,)) = rows.try_next().await? {
            ret.push_back(TsMs::from_ms_u64(v as _));
        }
        Ok(ret)
    }
}

impl fmt::Debug for ReadMsp03Fwd {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ReadMsp03Fwd").finish()
    }
}
