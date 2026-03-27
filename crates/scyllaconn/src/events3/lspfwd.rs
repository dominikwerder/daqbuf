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
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::DtNano;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::fmt;

macro_rules! error { ($($arg:tt)*) => { log::error!($($arg)*); }; }

autoerr::create_error_v1!(
    name(Error, "Read03LspFwd"),
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

pub type Item = Result<Box<dyn BinningggContainerEventsDyn>, Error>;

#[derive(Debug)]
pub struct Read03LspFwd {
    ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    range: ScyllaSeriesRange,
    limit: u32,
    tx: async_channel::Sender<Item>,
}

impl Read03LspFwd {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        range: ScyllaSeriesRange,
        limit: u32,
    ) -> (Self, async_channel::Receiver<Item>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self {
            ks,
            series_info,
            msp,
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

    async fn exec_inner(&self, stmts: &StmtsEventsQueryOpts, scy: &Session) -> Item {
        // TODO string is also scalar, but maybe stored as valueblob?
        let shape = self.series_info.shape();
        let array = match shape {
            Shape::Scalar => false,
            Shape::Wave(_) => true,
            Shape::Image(_, _) => {
                error!("unexpected shape for lsp fwd query for {shape:?}");
                true
            }
        };
        let stmt = stmts
            .lsp(false, false)
            .shape(array)
            .st(self.series_info.scalar_type().to_scylla_table_name_id())?
            .clone();
        let lsp_beg = self.msp.lsp(self.range.beg()).map_or(0i64, |x| x.to_i64());
        let lsp_end = self.msp.lsp(self.range.end()).map_or(0i64, |x| x.to_i64());
        let lim = self.limit as i64;
        let params = (self.series_info.id().to_i64(), self.msp.to_i64(), lsp_beg, lsp_end, lim);
        let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?;
        // TODO branch on type
        let mut evs = ContainerEvents::<u32>::new();
        while let Some((lsp,)) = rows.try_next().await? {
            let lsp = LspEv::from_i64(lsp);
            let ts = self.msp.to_ts(lsp);
            let val = lsp.to_i64() as _;
            evs.push_back(ts, val);
        }
        Ok(Box::new(evs))
    }

    pub async fn exec_mock(self, cltag: &str, ks: KeyspaceId) {
        let res = self.exec_mock_inner(cltag, ks).await;
        let _ = self.tx.send(res).await;
    }

    async fn exec_mock_inner(&self, cltag: &str, ks: KeyspaceId) -> Item {
        if self.series_info.id() == SERIES_ID_A {
            if cltag == "mock1" {
                match ks.rt() {
                    RetentionTime::Short => {
                        let mut evs = ContainerEvents::<u32>::new();
                        let beg = self.range.beg();
                        let end = self.range.end();
                        if let Some(events) = test_data::produce_full_event_set().by_msp.get(&self.msp) {
                            for (ts, _lsp, val, _nb) in events {
                                if beg <= *ts && *ts < end {
                                    evs.push_back(*ts, *val);
                                    if evs.len() >= self.limit as _ {
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(Box::new(evs) as Box<dyn BinningggContainerEventsDyn>)
                    }
                    RetentionTime::Medium => todo!(),
                    RetentionTime::Long => todo!(),
                }
            } else {
                Err(Error::NoKs)
            }
        } else {
            let c = items_2::empty::empty_events_dyn_ev(&self.series_info.scalar_type(), &self.series_info.shape())
                .unwrap();
            Ok(c)
        }
    }
}
