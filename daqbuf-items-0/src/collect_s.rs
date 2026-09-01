use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::TypeName;
use crate::WithLen;
use crate::apitypes::ToUserFacingApiType;
use crate::container::ByteEstimate;
use crate::log::*;
use daqbuf_err as err;
use err::Error;
use std::any;
use std::any::Any;
use std::fmt;

pub trait ToJsonValue: fmt::Debug + Send {
    fn into_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)>;
    fn into_fields_box(self: Box<Self>) -> Vec<(String, Box<dyn erased_serde::Serialize>)>;
}

pub trait ToCborValue: fmt::Debug + Send {
    fn into_fields(self) -> Vec<(String, Box<dyn erased_serde::Serialize>)>;
    fn into_fields_box(self: Box<Self>) -> Vec<(String, Box<dyn erased_serde::Serialize>)>;
}

impl AsAnyRef for serde_json::Value {
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl AsAnyMut for serde_json::Value {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub trait CollectedDyn: fmt::Debug + TypeName + Send + WithLen + ToUserFacingApiType {}

impl TypeName for Box<dyn CollectedDyn> {
    fn type_name(&self) -> String {
        self.as_ref().type_name()
    }
}

impl WithLen for Box<dyn CollectedDyn> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

pub trait CollectorTy: fmt::Debug + Send + Unpin + WithLen + ByteEstimate {
    type Input: CollectableDyn;
    type Output: CollectedDyn;
    fn ingest(&mut self, src: &mut Self::Input);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    // TODO use this crate's Error instead:
    fn result(&mut self) -> Result<Self::Output, Error>;
}

pub trait CollectorDyn: fmt::Debug + Send + WithLen + ByteEstimate {
    fn ingest(&mut self, src: &mut dyn CollectableDyn);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    // TODO factor the required parameters into new struct? Generic over events or binned?
    fn result(&mut self) -> Result<Box<dyn CollectedDyn>, Error>;
}

impl<T> CollectorDyn for T
where
    T: fmt::Debug + CollectorTy + 'static,
{
    fn ingest(&mut self, src: &mut dyn CollectableDyn) {
        if let Some(src) = src.as_any_mut().downcast_mut::<<T as CollectorTy>::Input>() {
            let s1 = any::type_name::<T>();
            let s2 = any::type_name::<<T as CollectorTy>::Input>();
            trace!("sees incoming &mut ref  len {}  t1 {}  t2 {}", src.len(), s1, s2);
            T::ingest(self, src)
        } else if let Some(src) = src.as_any_mut().downcast_mut::<Box<<T as CollectorTy>::Input>>() {
            trace!("sees incoming &mut Box");
            T::ingest(self, src)
        } else {
            error!(
                "No idea what this is. Expect: {}  input {}  got: {} {:?}",
                any::type_name::<T>(),
                any::type_name::<<T as CollectorTy>::Input>(),
                src.type_name(),
                src
            );
        }
    }

    fn set_range_complete(&mut self) {
        T::set_range_complete(self)
    }

    fn set_timed_out(&mut self) {
        T::set_timed_out(self)
    }

    fn result(&mut self) -> Result<Box<dyn CollectedDyn>, Error> {
        let ret = T::result(self)?;
        Ok(Box::new(ret))
    }
}

// TODO rename to `Typed`
pub trait CollectableType: fmt::Debug + WithLen + AsAnyRef + AsAnyMut + TypeName + Send {
    type Collector: CollectorTy<Input = Self>;
    fn new_collector() -> Self::Collector;
}

pub trait CollectableDyn: fmt::Debug + WithLen + AsAnyRef + AsAnyMut + Send + TypeName {
    fn new_collector(&self) -> Box<dyn CollectorDyn>;
}

impl<T> CollectableDyn for T
where
    T: CollectableType + 'static,
{
    fn new_collector(&self) -> Box<dyn CollectorDyn> {
        Box::new(T::new_collector())
    }
}

impl TypeName for Box<dyn CollectableDyn> {
    fn type_name(&self) -> String {
        self.as_ref().type_name()
    }
}

// TODO do this with some blanket impl:
impl WithLen for Box<dyn CollectableDyn> {
    fn len(&self) -> usize {
        WithLen::len(self.as_ref())
    }
}

// TODO do this with some blanket impl:
impl CollectableDyn for Box<dyn CollectableDyn> {
    fn new_collector(&self) -> Box<dyn CollectorDyn> {
        CollectableDyn::new_collector(self.as_ref())
    }
}
