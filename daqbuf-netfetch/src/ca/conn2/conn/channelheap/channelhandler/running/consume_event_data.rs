use super::Error;
use super::Running;
use crate::ca::conn2::ca_writer_value::CaWriterValue;
use ca_proto::ca::proto::CaDataScalarValue;
use ca_proto::ca::proto::CaDataValue;
use ca_proto::ca::proto::CaEventValue;
use netpod::ScalarType;
use netpod::TsNano;
use scywr::insertqueues::InsertDeques;
use scywr::iteminsertqueue::QueryItem;
use stats::IntervalEma;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }

#[derive(Debug)]
pub(super) struct ChannelConsumeState {
    pub(super) ts_alive_last: Instant,
    pub(super) ts_activity_last: Instant,
    pub(super) st_activity_last: SystemTime,
    item_recv_ivl_ema: IntervalEma,
    insert_item_ivl_ema: IntervalEma,
    #[allow(unused)]
    insert_recv_ivl_last: Instant,
    muted_before: u32,
    recv_count: u64,
    recv_bytes: u64,
    dw_st_last: SystemTime,
    dw_mt_last: SystemTime,
    dw_lt_last: SystemTime,
    val_lst_st: serde_json::Value,
    val_lst_mt: serde_json::Value,
    val_lst_lt: serde_json::Value,
}

impl ChannelConsumeState {
    pub(super) fn new() -> Self {
        let tsnow = Instant::now();
        Self {
            ts_alive_last: tsnow,
            ts_activity_last: tsnow,
            st_activity_last: SystemTime::now(),
            item_recv_ivl_ema: IntervalEma::new(),
            insert_item_ivl_ema: IntervalEma::new(),
            insert_recv_ivl_last: tsnow,
            muted_before: 0,
            recv_count: 0,
            recv_bytes: 0,
            dw_st_last: SystemTime::UNIX_EPOCH,
            dw_mt_last: SystemTime::UNIX_EPOCH,
            dw_lt_last: SystemTime::UNIX_EPOCH,
            val_lst_st: serde_json::Value::Null,
            val_lst_mt: serde_json::Value::Null,
            val_lst_lt: serde_json::Value::Null,
        }
    }
}

fn check_ev_value_data(data: &CaDataValue, scalar_type: &ScalarType) -> Result<(), Error> {
    match data {
        CaDataValue::Scalar(x) => match x {
            CaDataScalarValue::F32(..) => match scalar_type {
                ScalarType::F32 => {}
                _ => error!("MISMATCH  got f32  exp {:?}", scalar_type),
            },
            CaDataScalarValue::F64(..) => match scalar_type {
                ScalarType::F64 => {}
                _ => error!("MISMATCH  got f64  exp {:?}", scalar_type),
            },
            CaDataScalarValue::I16(..) => match scalar_type {
                ScalarType::I16 => {}
                ScalarType::Enum => {}
                _ => error!("MISMATCH  got i16  exp {:?}", scalar_type),
            },
            CaDataScalarValue::I32(..) => match scalar_type {
                ScalarType::I32 => {}
                _ => error!("MISMATCH  got i32  exp {:?}", scalar_type),
            },
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

impl Running {
    /// Applies one decoded Channel Access value to this channel's real-time writer and bin
    /// writer, and returns whatever scylla write items that produced. Shared by both the
    /// monitoring and the polling read path, since both feed the same series.
    pub(super) fn ingest_event(
        &mut self,
        value: CaEventValue,
        payload_len: u32,
        tsnow: Instant,
        stnow: SystemTime,
        tscaproto: Instant,
    ) -> Result<Vec<QueryItem>, Error> {
        self.crst.ts_alive_last = tsnow;
        self.crst.ts_activity_last = tsnow;
        self.crst.st_activity_last = stnow;
        self.crst.item_recv_ivl_ema.tick(tsnow);
        self.crst.recv_count += 1;
        self.crst.recv_bytes += payload_len as u64;
        let tsev_local = TsNano::from_system_time(stnow);
        let ts = value.ts().ok_or(Error::MissingTimestamp)?;
        let ts_diff = ts.abs_diff(tsev_local.ns());
        self.mett.ca_ts_off().push_dur_100us(Duration::from_nanos(ts_diff));
        let tsev = if self.use_ioc_time {
            value.ts().map(TsNano::from_ns).unwrap_or(tsev_local)
        } else {
            tsev_local
        };
        check_ev_value_data(&value.data, &self.rtwriter.scalar_type())?;
        self.crst.muted_before = 0;
        self.crst.insert_item_ivl_ema.tick(tsnow);
        let val_for_agg = value.f32_for_binning();
        let mut scratch = InsertDeques::new();
        let value_cloned = value.clone();
        let enum_str_table: BTreeMap<i32, String> = BTreeMap::new();
        let wres = self.rtwriter.write(
            CaWriterValue::new(value_cloned, &enum_str_table),
            tscaproto,
            tsev,
            &mut scratch,
        )?;
        if wres.st.accept {
            self.crst.dw_st_last = tsev.to_system_time();
            self.crst.val_lst_st = value.to_json_value();
        }
        if wres.mt.accept {
            self.crst.dw_mt_last = tsev.to_system_time();
            self.crst.val_lst_mt = value.to_json_value();
        }
        if wres.lt.accept {
            self.crst.dw_lt_last = tsev.to_system_time();
            self.crst.val_lst_lt = value.to_json_value();
        }
        if let Some(binwriter) = self.binwriter.as_mut() {
            binwriter.ingest(tsev, val_for_agg, &mut scratch)?;
        }
        self.mett.ts_msp_reput_onevent().add(wres.msp_rewrite() as _);
        self.mett
            .writer_ignore_rewind_time()
            .add(wres.ignore_rewind_time() as _);
        self.mett.writer_ignore_same_time().add(wres.ignore_same_time() as _);
        self.mett.writer_ignore_same_value().add(wres.ignore_same_value() as _);
        self.mett
            .writer_ignore_monitor_not_min_quiet()
            .add(wres.ignore_monitor_not_min_quiet() as _);
        self.mett
            .writer_ignore_poll_not_min_quiet()
            .add(wres.ignore_poll_not_min_quiet() as _);
        self.mett.writer_ignore_rate_cap().add(wres.ignore_rate_cap() as _);
        let mut items = Vec::new();
        for q in [
            scratch.st_rf1_qu,
            scratch.st_rf3_qu,
            scratch.mt_rf3_qu,
            scratch.lt_rf3_qu,
            scratch.lt_rf3_lat5_qu,
        ] {
            items.extend(q);
        }
        Ok(items)
    }
}
