use crate::binning::container::bins::BinAggedType;
use crate::binning::container_events::EventValueType;
use crate::offsets::ts_offs_from_abs;
use crate::offsets::ts_offs_from_abs_with_anchor;
use items_0::apitypes::UserApiType;
use items_0::collect_s::ToCborValue;
use items_0::collect_s::ToJsonValue;
use netpod::TsNano;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt;

#[derive(Serialize)]
pub struct ContainerEventsApi<EVT>
where
    EVT: EventValueType,
{
    pub tss: VecDeque<u64>,
    pub values: EVT::Container,
    #[serde(skip_serializing_if = "netpod::is_false")]
    pub range_final: bool,
    #[serde(skip_serializing_if = "netpod::is_false")]
    pub timed_out: bool,
}

impl<EVT> fmt::Debug for ContainerEventsApi<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ContainerEventsApi")
            // .field("tss", &self.tss)
            // .field("values", &self.values)
            .finish()
    }
}

impl<EVT> ToCborValue for ContainerEventsApi<EVT>
where
    EVT: EventValueType,
{
    fn to_cbor_value(&self) -> Result<ciborium::Value, ciborium::value::Error> {
        let val = ciborium::value::Value::serialized(self).unwrap();
        Ok(val)
    }
}

impl<EVT> ToJsonValue for ContainerEventsApi<EVT>
where
    EVT: EventValueType,
{
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        let ret = serde_json::to_value(self);
        ret
    }
}

impl<EVT> UserApiType for ContainerEventsApi<EVT> where EVT: EventValueType {}

#[derive(Serialize)]
pub struct ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub ts1s: VecDeque<TsNano>,
    pub ts2s: VecDeque<TsNano>,
    pub cnts: VecDeque<u64>,
    pub mins: VecDeque<EVT>,
    pub maxs: VecDeque<EVT>,
    pub aggs: VecDeque<BVT>,
    pub fnls: VecDeque<bool>,
}

impl<EVT, BVT> fmt::Debug for ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ContainerBinsApi")
            // .field("tss", &self.tss)
            // .field("values", &self.values)
            .finish()
    }
}

impl<EVT, BVT> ToCborValue for ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn to_cbor_value(&self) -> Result<ciborium::Value, ciborium::value::Error> {
        // let val = ciborium::value::Value::serialized(self).unwrap();
        // Ok(val)
        let e = ciborium::value::Error::Custom("binned data as cbor is not yet available".into());
        Err(e)
    }
}

impl<EVT, BVT> ToJsonValue for ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        use serde_json::json;
        use serde_json::Value;
        // let ret = serde_json::to_value(self);
        // ret
        let (ts_anch, ts1ms, ts1ns) = ts_offs_from_abs(&self.ts1s);
        let (ts2ms, ts2ns) = ts_offs_from_abs_with_anchor(ts_anch, &self.ts2s);

        let ret = json!({
            "tsAnchor": ts_anch,
            "ts1Ms": ts1ms,
            "ts2Ms": ts2ms,
            "ts1Ns": ts1ns,
            "ts2Ns": ts2ns,
            "counts": self.cnts,
            "mins": self.mins,
            "maxs": self.maxs,
            "avgs": self.aggs,
        });
        Ok(ret)
    }
}

impl<EVT, BVT> UserApiType for ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
}
