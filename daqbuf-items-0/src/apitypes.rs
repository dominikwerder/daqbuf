use crate::collect_s::ToCborValue;
use serde::Serialize;
use std::collections::BTreeMap;

pub trait UserApiType {
    fn into_serializable_normal(self: Box<Self>) -> Box<dyn erased_serde::Serialize>;
    fn into_serializable_json(self: Box<Self>) -> Box<dyn erased_serde::Serialize>;
}

pub trait ToUserFacingApiType {
    fn into_user_facing_api_type(self) -> Box<dyn UserApiType>;
    fn into_user_facing_api_type_box(self: Box<Self>) -> Box<dyn UserApiType>;
}

#[derive(Debug, Serialize)]
pub struct EmptyStruct {}

impl EmptyStruct {
    pub fn new() -> Self {
        Self {}
    }
}

impl ToCborValue for EmptyStruct {
    fn into_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        Vec::new()
    }

    fn into_fields_box(self: Box<Self>) -> Vec<(String, Box<dyn erased_serde::Serialize>)> {
        self.into_fields()
    }
}

impl UserApiType for EmptyStruct {
    fn into_serializable_normal(self: Box<Self>) -> Box<dyn erased_serde::Serialize> {
        Box::new(BTreeMap::<String, u32>::new())
    }

    fn into_serializable_json(self: Box<Self>) -> Box<dyn erased_serde::Serialize> {
        Box::new(BTreeMap::<String, u32>::new())
    }
}
