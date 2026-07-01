use ca_proto::ca::proto::CaDataValue;
use ca_proto::ca::proto::CaEventValue;
use netpod::ByteSize;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use serde::Serialize;
use series::SeriesId;
use serieswriter::rtwriter::RtWriter;
use serieswriter::writer::EmittableType;
use std::collections::BTreeMap;
use std::time::Instant;

macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }

#[derive(Debug, Serialize)]
struct CaWriterValueState {
    series_data: SeriesId,
    series_status: SeriesId,
    last_accepted_ts: TsNano,
    last_accepted_val: Option<CaWriterValue>,
}

impl CaWriterValueState {
    fn new(series_status: SeriesId, series_data: SeriesId, rt: RetentionTime) -> Self {
        Self {
            series_data,
            series_status,
            last_accepted_ts: TsNano::from_ns(0),
            last_accepted_val: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct CaWriterValue(CaEventValue, Option<String>);

impl CaWriterValue {
    fn new(val: CaEventValue, enum_str_table: &BTreeMap<i32, String>) -> Self {
        let valstr = match &val.data {
            CaDataValue::Scalar(val) => {
                use ca_proto::ca::proto::CaDataScalarValue;
                match val {
                    CaDataScalarValue::Enum(x) => {
                        let x = *x as i32;
                        let conv = enum_str_table
                            .get(&x)
                            .map_or_else(|| String::from("undefined"), String::from);
                        Some(conv)
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        Self(val, valstr)
    }
}

impl EmittableType for CaWriterValue {
    type State = CaWriterValueState;

    fn ts(&self) -> TsNano {
        TsNano::from_ns(self.0.ts().unwrap_or(0))
    }

    fn has_change(&self, k: &Self) -> bool {
        if self.0.data != k.0.data {
            true
        } else if self.0.meta != k.0.meta {
            true
        } else {
            false
        }
    }

    fn byte_size(&self) -> u32 {
        self.0.data.byte_size()
    }

    fn into_query_item(
        mut self,
        ts_net: Instant,
        tsev: TsNano,
        state: &mut <Self as EmittableType>::State,
    ) -> serieswriter::writer::EmitRes {
        let byte_size = self.byte_size();
        let data_item = {
            use ca_proto::ca::proto::CaDataValue;
            use scywr::iteminsertqueue::DataValue;
            match self.0.data {
                CaDataValue::Scalar(val) => DataValue::Scalar({
                    use ca_proto::ca::proto::CaDataScalarValue;
                    use scywr::iteminsertqueue::ScalarValue;
                    match val {
                        CaDataScalarValue::I8(x) => ScalarValue::I8(x),
                        CaDataScalarValue::I16(x) => ScalarValue::I16(x),
                        CaDataScalarValue::I32(x) => ScalarValue::I32(x),
                        CaDataScalarValue::F32(x) => ScalarValue::F32(x),
                        CaDataScalarValue::F64(x) => ScalarValue::F64(x),
                        CaDataScalarValue::Enum(x) => ScalarValue::Enum(
                            x,
                            self.1.take().unwrap_or_else(|| {
                                warn!("NoEnumStr");
                                String::from("NoEnumStr")
                            }),
                        ),
                        CaDataScalarValue::String(x) => ScalarValue::String(x),
                        CaDataScalarValue::Bool(x) => ScalarValue::Bool(x),
                    }
                }),
                CaDataValue::Array(val) => DataValue::Array({
                    use ca_proto::ca::proto::CaDataArrayValue;
                    use scywr::iteminsertqueue::ArrayValue;
                    match val {
                        CaDataArrayValue::I8(x) => ArrayValue::I8(x),
                        CaDataArrayValue::I16(x) => ArrayValue::I16(x),
                        CaDataArrayValue::I32(x) => ArrayValue::I32(x),
                        CaDataArrayValue::F32(x) => ArrayValue::F32(x),
                        CaDataArrayValue::F64(x) => ArrayValue::F64(x),
                        CaDataArrayValue::Bool(x) => ArrayValue::Bool(x),
                    }
                }),
            }
        };

        // TODO move to separate impl
        // let diff_status = match state.last_accepted_val.as_ref() {
        //     Some(last) => match &last.0.meta {
        //         proto::CaMetaValue::CaMetaTime(last_meta) => match &self.0.meta {
        //             proto::CaMetaValue::CaMetaTime(meta) => meta.status != last_meta.status,
        //             _ => false,
        //         },
        //         _ => false,
        //     },
        //     None => true,
        // };
        // let mut n_status = 0;
        // if diff_status {
        //     use scywr::iteminsertqueue::DataValue;
        //     use scywr::iteminsertqueue::ScalarValue;
        //     match self.0.meta {
        //         proto::CaMetaValue::CaMetaTime(meta) => {
        //             let (ts_msp, ts_lsp, ts_msp_chg) = state.msp_split_status.split(ts, 2);
        //             if ts_msp_chg {
        //                 items.push(QueryItem::Msp(MspItem::new(
        //                     state.series_status.clone(),
        //                     ts_msp.to_ts_ms(),
        //                     ts_net,
        //                 )));
        //             }
        //             let data_value = DataValue::Scalar(ScalarValue::I16(meta.status as i16));
        //             let item = scywriiq::InsertItem {
        //                 series: state.series_status.clone(),
        //                 ts_msp: ts_msp.to_ts_ms(),
        //                 ts_lsp,
        //                 ts_net,
        //                 val: data_value,
        //             };
        //             items.push(QueryItem::Insert(item));
        //             n_status += 1;
        //             // info!("diff_status  emit {:?}", state.series_status);
        //         }
        //         _ => {
        //             // TODO must be able to return error here
        //             warn!("diff_status logic error");
        //         }
        //     };
        // }
        let ret = serieswriter::writer::EmitRes {
            data_item,
            bytes: ByteSize(byte_size),
        };
        ret
    }
}

pub type CaRtWriter = RtWriter<CaWriterValue>;
