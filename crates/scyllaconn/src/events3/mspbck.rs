use crate::events2::prepare::StmtsEventsQueryOpts;
use crate::events3::SeriesInfo;
use crate::events3::mspfwd;
use crate::events3::mspfwd::ReadMspFwdStream;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use daqbuf_series::SeriesId;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use netpod::DtNano;
use netpod::RangeExcl;
use netpod::TsMs;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::fmt;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ReadMsp03Bck"),
    enum variants {
        Send,
        Recv,
        NoKs,
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        MspFwd(#[from] mspfwd::Error),
        MspBckTooMany,
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
        error!("ReadMsp03Bck exec_inner not supported");
        // let stmt = stmts.ts_msp_bck_win().clone();
        // let win = self.ks.rt().msp_rollover_ivl_on_read();
        // let beg = self.range.beg().sub(DtNano::from_ms(1000 * win.as_secs()));
        // let end = self.range.beg();
        // let params = (self.series.to_i64(), beg.ms() as i64, end.ms() as i64);
        // let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
        // let mut ret = VecDeque::new();
        // while let Some((v,)) = rows.try_next().await? {
        //     ret.push_back(TsMs::from_ms_u64(v as _));
        // }
        Ok(VecDeque::new())
    }

    pub async fn exec_mock(self, cltag: &str, ks: KeyspaceId) {
        let res = self.exec_mock_inner(cltag, ks).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_mock_inner(&self, cltag: &str, ks: KeyspaceId) -> Item {
        error!("ReadMsp03Bck exec_inner not supported");
        Ok(VecDeque::new())
    }
}

impl fmt::Debug for ReadMsp03Bck {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ReadMsp03Bck").finish()
    }
}

pub async fn msp_bck(
    ks: KeyspaceId,
    series_info: SeriesInfo,
    beg: TsNano,
    scyqu: ScyllaQueueCluster,
) -> Result<VecDeque<TsMs>, Error> {
    let ks = ks.clone();
    let series = series_info.id();
    let win = DtNano::from_sec(ks.rt().msp_rollover_ivl_on_read().as_secs());
    debug!("backward window {win} h", win = win.sec_u64() / 60 / 60);
    let range = ScyllaSeriesRange::new(beg.sub(win), beg);
    // TODO change the limit to larger for non-test-data
    let mut stream = ReadMspFwdStream::new(ks, series, range, RangeExcl::None, 1, scyqu);
    let mut msps = VecDeque::new();
    while let Some(x) = stream.next().await {
        msps.extend(x?);
        let n = msps.len();
        if n > 20 {
            warn!("many msp in backward window {n}");
        } else if n > 200 {
            error!("too many msp in backward window {n}");
            return Err(Error::MspBckTooMany);
        }
    }
    for e in &msps {
        debug!("got backward msp {e}");
    }
    Ok(msps)
}
