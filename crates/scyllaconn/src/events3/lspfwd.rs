use crate::events2::prepare::StmtsEventsClusterKeyspace;
use crate::events3::SERIES_ID_A;
use crate::events3::SeriesInfo;
use crate::events3::msplsp::LspEv;
use crate::events3::msplsp::MspEv;
use crate::events3::test_data;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaOptsDefault;
use crate::worker::ScyllaOptsSubmit;
use futures_util::TryStreamExt;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_events::ContainerEvents;
use netpod::EnumVariant;
use netpod::RangeExcl;
use netpod::ScalarType;
use netpod::Shape;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;

macro_rules! error { ($($arg:tt)*) => { log::error!($($arg)*); }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

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
        Msg(String),
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
    _ks: KeyspaceId,
    series_info: SeriesInfo,
    msp: MspEv,
    range: ScyllaSeriesRange,
    begexcl: RangeExcl,
    limit: u32,
    scyopts: ScyllaOptsSubmit,
    tx: async_channel::Sender<Item>,
}

impl Read03LspFwd {
    pub fn new(
        ks: KeyspaceId,
        series_info: SeriesInfo,
        msp: MspEv,
        range: ScyllaSeriesRange,
        begexcl: RangeExcl,
        limit: u32,
        scyopts: ScyllaOptsSubmit,
    ) -> (Self, async_channel::Receiver<Item>) {
        let (tx, rx) = async_channel::bounded(1);
        let ret = Self {
            _ks: ks,
            series_info,
            msp,
            range,
            begexcl,
            limit,
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
        let cby = scyopts.lsp_asc_cache_bypass.to_bool();
        trace!("Read03LspFwd  cache_bypass {cby}");
        let stmt = stmts
            .cache_bypass(cby)
            .lsp(false, true)
            .shape(array)
            .st(self.series_info.scalar_type().to_scylla_table_name_id())?
            .clone();
        let lsp_beg = self.msp.lsp(self.range.beg()).map_or(0i64, |x| x.to_i64());
        let lsp_end = self.msp.lsp(self.range.end()).map_or(0i64, |x| x.to_i64());
        let lsp_beg = if let RangeExcl::Beg = self.begexcl {
            lsp_beg + 1
        } else {
            lsp_beg
        };
        let lim = self.limit as i32;
        let params = (self.series_info.id().to_i64(), self.msp.to_i64(), lsp_beg, lsp_end, lim);
        let ret = match self.series_info.shape() {
            Shape::Scalar => match self.series_info.scalar_type() {
                ScalarType::U8 => {
                    type ST = u8;
                    type SC = i8;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::U16 => {
                    type ST = u16;
                    type SC = i16;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::U32 => {
                    type ST = u32;
                    type SC = i32;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::U64 => {
                    type ST = u64;
                    type SC = i64;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I8 => {
                    type ST = i8;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I16 => {
                    type ST = i16;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I32 => {
                    type ST = i32;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I64 => {
                    type ST = i64;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::F32 => {
                    type ST = f32;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs)
                }
                ScalarType::F64 => {
                    type ST = f64;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs)
                }
                ScalarType::BOOL => {
                    type ST = bool;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs)
                }
                ScalarType::STRING => {
                    type ST = String;
                    type SC = ST;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, val as ST);
                    }
                    Box::new(evs)
                }
                ScalarType::Enum => {
                    type ST = EnumVariant;
                    let mut rows = scy
                        .execute_iter(stmt, params)
                        .await?
                        .rows_stream::<(i64, i16, String)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, ix, name)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        evs.push_back(ts, EnumVariant::new(ix, name));
                    }
                    Box::new(evs)
                }
            },
            Shape::Wave(_) => match self.series_info.scalar_type() {
                ScalarType::U8 => {
                    type ST = Vec<u8>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::U16 => {
                    type ST = Vec<u16>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::U32 => {
                    type ST = Vec<u32>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::U64 => {
                    type ST = Vec<u64>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I8 => {
                    type ST = Vec<i8>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I16 => {
                    type ST = Vec<i16>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I32 => {
                    type ST = Vec<i32>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::I64 => {
                    type ST = Vec<i64>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::F32 => {
                    type ST = Vec<f32>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::F64 => {
                    type ST = Vec<f64>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::BOOL => {
                    type ST = Vec<bool>;
                    type SC = Vec<u8>;
                    use crate::worker::read_events_03::datatypes::ValTy;
                    let mut rows = scy.execute_iter(stmt, params).await?.rows_stream::<(i64, SC)>()?;
                    let mut evs = ContainerEvents::<ST>::new();
                    while let Some((lsp, val)) = rows.try_next().await? {
                        let lsp = LspEv::from_i64(lsp);
                        let ts = self.msp.to_ts(lsp);
                        let val = ST::from_valueblob(val);
                        evs.push_back(ts, val);
                    }
                    Box::new(evs) as Box<dyn BinningggContainerEventsDyn>
                }
                ScalarType::Enum | ScalarType::STRING => {
                    let st = self.series_info.scalar_type();
                    let sh = self.series_info.shape();
                    let e = Error::Msg(format!("{st:?}  {sh:?}  currently not supported"));
                    return Err(e);
                }
            },
            Shape::Image(_, _) => {
                let e = Error::Msg(format!("Shape::Image currently not supported"));
                return Err(e);
            }
        };
        Ok(ret)
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
