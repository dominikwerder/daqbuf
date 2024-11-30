use crate::collect_s::ToCborValue;
use crate::collect_s::ToJsonValue;
use core::fmt;
use serde::Serialize;

pub trait UserApiType: ToCborValue + ToJsonValue {}

pub trait ToUserFacingApiType {
    fn to_user_facing_api_type(self) -> Box<dyn UserApiType>;
    fn to_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType>;
}

#[derive(Debug, Serialize)]
pub struct EmptyStruct {}

impl EmptyStruct {
    pub fn new() -> Self {
        Self {}
    }
}

impl ToCborValue for EmptyStruct {
    fn to_cbor_value(&self) -> Result<ciborium::Value, ciborium::value::Error> {
        let ret = ciborium::Value::Map(Vec::new());
        Ok(ret)
    }
}

impl ToJsonValue for EmptyStruct {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        let ret = serde_json::to_value(self);
        ret
    }
}

impl UserApiType for EmptyStruct {}
