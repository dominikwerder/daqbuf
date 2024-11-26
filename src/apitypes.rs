use crate::collect_s::ToCborValue;
use core::fmt;
use serde::Serialize;
use std::collections::VecDeque;

pub trait ContPayload: fmt::Debug + Serialize + Send {}

impl<T> ContPayload for T where T: fmt::Debug + Serialize + Send {}

pub trait UserApiType: ToCborValue {}

pub trait ToUserFacingApiType {
    fn to_user_facing_api_type(self) -> Box<dyn UserApiType>;
}

#[derive(Serialize)]
pub struct ContainerEventsApi<EVT>
where
    EVT: ContPayload,
{
    pub tss: VecDeque<u64>,
    pub values: VecDeque<EVT>,
}

impl<EVT> fmt::Debug for ContainerEventsApi<EVT>
where
    EVT: ContPayload,
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
    EVT: ContPayload,
{
    fn to_cbor_value(&self) -> Result<ciborium::Value, ciborium::value::Error> {
        // let mut out = Vec::new();
        // ciborium::ser::into_writer(self, &mut out).unwrap();
        let val = ciborium::value::Value::serialized(self).unwrap();
        Ok(val)
    }
}

impl<EVT> UserApiType for ContainerEventsApi<EVT> where EVT: ContPayload {}
