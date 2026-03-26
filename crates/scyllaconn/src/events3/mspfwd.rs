use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::events3::SERIES_ID_A;
use crate::events3::test_data;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use daqbuf_series::SeriesId;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::TryStreamExt;
use netpod::RangeExcl;
use netpod::TsMs;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }

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
    begexcl: RangeExcl,
    tx: async_channel::Sender<Item>,
}

impl ReadMsp03Fwd {
    pub fn new(
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        begexcl: RangeExcl,
        limit: u32,
    ) -> (Self, async_channel::Receiver<Item>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self {
            ks,
            series,
            range,
            begexcl,
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
        let begj = if self.begexcl.excl_beg() {
            i64::MIN
        } else {
            self.range.beg().to_dt_ms().to_i64()
        };
        let begk = self.range.beg().to_dt_ms().to_i64();
        let end = self.range.end().to_dt_ms().to_i64();
        let limit = self.limit;
        let params = (self.series.to_i64(), begj, begk, end, limit as i32);
        let stmt = stmts.ts_msp_fwd2().clone();
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
        debug!("exec_mock_inner  {cltag}  {ks:?}  {self:?}");
        if self.series == SERIES_ID_A {
            if cltag == "mock1" {
                match ks.rt() {
                    RetentionTime::Short => {
                        let begj = if self.begexcl.excl_beg() {
                            TsNano::from_ns(u64::MAX)
                        } else {
                            self.range.beg()
                        };
                        let begk = self.range.beg();
                        let end = self.range.end();
                        let ret = test_data::produce_full_event_set()
                            .by_msp
                            .keys()
                            .map(|x| *x)
                            .filter(test_data::pred_ms_range(begj, begk, end))
                            .map(|x| x.to_ms())
                            .take(self.limit as usize)
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

impl fmt::Debug for ReadMsp03Fwd {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ReadMsp03Fwd").finish()
    }
}

pub struct ReadMspFwdStream {
    ks: KeyspaceId,
    series: SeriesId,
    range: ScyllaSeriesRange,
    begexcl: RangeExcl,
    limit: u32,
    fut: Option<Pin<Box<dyn Future<Output = Item> + Send>>>,
    scyqu: ScyllaQueueCluster,
}

impl ReadMspFwdStream {
    pub fn new(
        ks: KeyspaceId,
        series: SeriesId,
        range: ScyllaSeriesRange,
        begexcl: RangeExcl,
        limit: u32,
        scyqu: ScyllaQueueCluster,
    ) -> Self {
        let limit = limit.max(1).min(40);
        Self {
            ks,
            series,
            range,
            begexcl,
            limit,
            fut: None,
            scyqu,
        }
    }

    fn trigger_done(&mut self) {
        self.range = ScyllaSeriesRange::new(self.range.end(), self.range.end());
    }
}

impl Stream for ReadMspFwdStream {
    type Item = Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            match &mut self.fut {
                Some(fut) => match fut.poll_unpin(cx) {
                    Ready(x) => {
                        self.fut = None;
                        match x {
                            Ok(v) => {
                                if v.len() < self.limit as _ {
                                    self.trigger_done();
                                } else {
                                    self.begexcl = RangeExcl::Beg;
                                    let beg = v.back().unwrap().ns();
                                    self.range = ScyllaSeriesRange::new(beg, self.range.end());
                                }
                                break Ready(Some(Ok(v)));
                            }
                            Err(e) => {
                                self.trigger_done();
                                break Ready(Some(Err(e)));
                            }
                        }
                    }
                    Pending => break Pending,
                },
                None => {
                    if self.range.beg() < self.range.end() {
                        let scyqu = self.scyqu.clone();
                        let ks = self.ks.clone();
                        let series = self.series.clone();
                        let range = self.range.clone();
                        let begexcl = self.begexcl;
                        let limit = Some(self.limit.clone());
                        let fut = async move { scyqu.read_msp_03_fwd(ks, series, range, begexcl, limit).await };
                        self.fut = Some(Box::pin(fut));
                    } else {
                        break Ready(None);
                    }
                }
            }
        }
    }
}
