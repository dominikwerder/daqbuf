use crate::binning::container::bins::BinAggedType;
use crate::binning::container_events::Container;
use crate::binning::container_events::EventValueType;
use crate::offsets::ts_offs_from_abs;
use crate::offsets::ts_offs_from_abs_with_anchor;
use items_0::apitypes::UserApiType;
use items_0::collect_s::ToCborValue;
use items_0::collect_s::ToJsonValue;
use netpod::TsNano;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt;

#[derive(Serialize)]
pub struct ContainerEventsApi<EVT>
where
    EVT: EventValueType,
{
    pub tss: VecDeque<TsNano>,
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
    fn into_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        let tss: Vec<_> = self.tss.into_iter().map(|x| x.ns()).collect();
        let mut ret = self.values.into_user_facing_fields();
        ret.push(("tss".into(), Box::new(tss)));
        // Clients rely on `scalar_type` to create correctly typed HDF5 datasets.
        ret.push((
            "scalar_type".into(),
            Box::new(EVT::scalar_type_name_string()),
        ));
        if self.range_final {
            ret.push(("rangeFinal".into(), Box::new(true)));
        }
        if self.timed_out {
            ret.push(("timedOut".into(), Box::new(true)));
        }
        ret
    }

    fn into_fields_box(self: Box<Self>) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        ToCborValue::into_fields(*self)
    }
}

impl<EVT> ToJsonValue for ContainerEventsApi<EVT>
where
    EVT: EventValueType,
{
    fn into_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        let mut ret = self.values.into_user_facing_fields_json();
        let (ts_anch, ts_ms, ts_ns) = ts_offs_from_abs(&self.tss);
        ret.push(("tsAnchor".into(), Box::new(ts_anch)));
        ret.push(("tsMs".into(), Box::new(ts_ms)));
        ret.push(("tsNs".into(), Box::new(ts_ns)));
        if self.range_final {
            ret.push(("rangeFinal".into(), Box::new(true)));
        }
        if self.timed_out {
            ret.push(("timedOut".into(), Box::new(true)));
        }
        ret
    }

    fn into_fields_box(self: Box<Self>) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        ToJsonValue::into_fields(*self)
    }
}

impl<EVT> UserApiType for ContainerEventsApi<EVT>
where
    EVT: EventValueType,
{
    fn into_serializable_normal(self: Box<Self>) -> Box<dyn erased_serde::Serialize> {
        let mut map = BTreeMap::new();
        for (k, v) in ToCborValue::into_fields_box(self) {
            map.insert(k, v);
        }
        Box::new(map)
    }

    fn into_serializable_json(self: Box<Self>) -> Box<dyn erased_serde::Serialize> {
        let mut map = BTreeMap::new();
        for (k, v) in ToJsonValue::into_fields_box(self) {
            map.insert(k, v);
        }
        Box::new(map)
    }
}

#[derive(Serialize)]
pub struct ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    pub ts1s: VecDeque<TsNano>,
    pub ts2s: VecDeque<TsNano>,
    pub cnts: VecDeque<u64>,
    pub mins: <EVT as EventValueType>::Container,
    pub maxs: <EVT as EventValueType>::Container,
    pub aggs: <BVT as BinAggedType>::Container,
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
    fn into_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        let mut ret = Vec::<(String, Box<dyn erased_serde::Serialize>)>::new();
        // let mut ret = self.aggs.into_user_facing_fields_json();
        ret.push(("ts1s".into(), Box::new(self.ts1s)));
        ret.push(("ts2s".into(), Box::new(self.ts2s)));
        ret.push(("counts".into(), Box::new(self.cnts)));
        {
            let fields = self.mins.into_user_facing_fields();
            for (k, v) in fields {
                let k = if k == "values" {
                    "mins".to_string()
                } else {
                    format!("mins_{}", k)
                };
                ret.push((k, v));
            }
        }
        {
            let fields = self.maxs.into_user_facing_fields();
            for (k, v) in fields {
                let k = if k == "values" {
                    "maxs".to_string()
                } else {
                    format!("maxs_{}", k)
                };
                ret.push((k, v));
            }
        }
        ret.push(("avgs".into(), Box::new(self.aggs)));
        ret
    }

    fn into_fields_box(self: Box<Self>) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        ToCborValue::into_fields(*self)
    }
}

impl<EVT, BVT> ToJsonValue for ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn into_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        let mut ret = Vec::<(String, Box<dyn erased_serde::Serialize>)>::new();
        // let mut ret = self.aggs.into_user_facing_fields_json();
        let (ts_anch, ts1_ms, ts1_ns) = ts_offs_from_abs(&self.ts1s);
        let (ts2_ms, ts2_ns) = ts_offs_from_abs_with_anchor(ts_anch, &self.ts2s);
        ret.push(("tsAnchor".into(), Box::new(ts_anch)));
        ret.push(("ts1Ms".into(), Box::new(ts1_ms)));
        ret.push(("ts1Ns".into(), Box::new(ts1_ns)));
        ret.push(("ts2Ms".into(), Box::new(ts2_ms)));
        ret.push(("ts2Ns".into(), Box::new(ts2_ns)));
        ret.push(("counts".into(), Box::new(self.cnts)));
        {
            let fields = self.mins.into_user_facing_fields();
            for (k, v) in fields {
                let k = if k == "values" {
                    "mins".to_string()
                } else {
                    format!("mins_{}", k)
                };
                ret.push((k, v));
            }
        }
        {
            let fields = self.maxs.into_user_facing_fields();
            for (k, v) in fields {
                let k = if k == "values" {
                    "maxs".to_string()
                } else {
                    format!("maxs_{}", k)
                };
                ret.push((k, v));
            }
        }
        ret.push(("avgs".into(), Box::new(self.aggs)));
        ret
    }

    fn into_fields_box(self: Box<Self>) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        ToJsonValue::into_fields(*self)
    }
}

impl<EVT, BVT> UserApiType for ContainerBinsApi<EVT, BVT>
where
    EVT: EventValueType,
    BVT: BinAggedType,
{
    fn into_serializable_normal(self: Box<Self>) -> Box<dyn erased_serde::Serialize> {
        let mut map = BTreeMap::new();
        for (k, v) in ToCborValue::into_fields_box(self) {
            map.insert(k, v);
        }
        Box::new(map)
    }

    fn into_serializable_json(self: Box<Self>) -> Box<dyn erased_serde::Serialize> {
        let mut map = BTreeMap::new();
        for (k, v) in ToJsonValue::into_fields_box(self) {
            map.insert(k, v);
        }
        Box::new(map)
    }
}
