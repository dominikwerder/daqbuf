use crate::container::ByteEstimate;
use crate::timebin::BinningggContainerBinsDyn;
use crate::AsAnyMut;
use crate::AsAnyRef;
use crate::Events;
use crate::TypeName;
use crate::WithLen;
use daqbuf_err as err;
use err::Error;
use netpod::log::*;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRangeEnum;
use serde::Serialize;
use std::any;
use std::any::Any;
use std::fmt;

// TODO check usage of this trait
pub trait ToJsonBytes {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error>;
}

pub trait ToJsonResult: fmt::Debug + AsAnyRef + AsAnyMut + Send {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error>;
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

impl ToJsonResult for serde_json::Value {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        Ok(self.clone())
    }
}

impl ToJsonBytes for serde_json::Value {
    fn to_json_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
}

pub trait CollectedDyn: fmt::Debug + TypeName + Send + AsAnyRef + WithLen + ToJsonResult {}

impl ToJsonResult for Box<dyn CollectedDyn> {
    fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        ToJsonResult::to_json_value(self.as_ref())
    }
}

impl WithLen for Box<dyn CollectedDyn> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl TypeName for Box<dyn CollectedDyn> {
    fn type_name(&self) -> String {
        self.as_ref().type_name()
    }
}

impl CollectedDyn for Box<dyn CollectedDyn> {}

pub trait CollectorTy: fmt::Debug + Send + Unpin + WithLen + ByteEstimate {
    type Input: CollectableDyn;
    type Output: CollectedDyn + ToJsonResult + Serialize;

    fn ingest(&mut self, src: &mut Self::Input);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn set_continue_at_here(&mut self);

    // TODO use this crate's Error instead:
    fn result(&mut self, range: Option<SeriesRange>, binrange: Option<BinnedRangeEnum>) -> Result<Self::Output, Error>;
}

pub trait CollectorDyn: fmt::Debug + Send + WithLen + ByteEstimate {
    fn ingest(&mut self, src: &mut dyn CollectableDyn);
    fn set_range_complete(&mut self);
    fn set_timed_out(&mut self);
    fn set_continue_at_here(&mut self);
    // TODO factor the required parameters into new struct? Generic over events or binned?
    fn result(
        &mut self,
        range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Box<dyn CollectedDyn>, Error>;
}

impl<T> CollectorDyn for T
where
    T: fmt::Debug + CollectorTy + 'static,
{
    fn ingest(&mut self, src: &mut dyn CollectableDyn) {
        if let Some(src) = src.as_any_mut().downcast_mut::<<T as CollectorTy>::Input>() {
            trace!("sees incoming &mut ref");
            T::ingest(self, src)
        } else {
            if let Some(src) = src.as_any_mut().downcast_mut::<Box<<T as CollectorTy>::Input>>() {
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
    }

    fn set_range_complete(&mut self) {
        T::set_range_complete(self)
    }

    fn set_timed_out(&mut self) {
        T::set_timed_out(self)
    }

    fn set_continue_at_here(&mut self) {
        T::set_continue_at_here(self)
    }

    fn result(
        &mut self,
        range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Box<dyn CollectedDyn>, Error> {
        let ret = T::result(self, range, binrange)?;
        Ok(Box::new(ret))
    }
}

// TODO rename to `Typed`
pub trait CollectableType: fmt::Debug + WithLen + AsAnyRef + AsAnyMut + TypeName + Send {
    type Collector: CollectorTy<Input = Self>;
    fn new_collector() -> Self::Collector;
}

#[derive(Debug)]
pub struct CollectorForDyn {
    inner: Box<dyn CollectorDyn>,
}

impl WithLen for CollectorForDyn {
    fn len(&self) -> usize {
        todo!()
    }
}

impl ByteEstimate for CollectorForDyn {
    fn byte_estimate(&self) -> u64 {
        todo!()
    }
}

impl CollectorDyn for CollectorForDyn {
    fn ingest(&mut self, src: &mut dyn CollectableDyn) {
        todo!()
    }

    fn set_range_complete(&mut self) {
        todo!()
    }

    fn set_timed_out(&mut self) {
        todo!()
    }

    fn set_continue_at_here(&mut self) {
        todo!()
    }

    fn result(
        &mut self,
        range: Option<SeriesRange>,
        binrange: Option<BinnedRangeEnum>,
    ) -> Result<Box<dyn crate::collect_s::CollectedDyn>, Error> {
        todo!()
    }
}

pub trait CollectableDyn: fmt::Debug + WithLen + AsAnyRef + AsAnyMut + TypeName + Send {
    fn new_collector(&self) -> Box<dyn CollectorDyn>;
}

impl TypeName for Box<dyn BinningggContainerBinsDyn> {
    fn type_name(&self) -> String {
        BinningggContainerBinsDyn::type_name(self.as_ref()).into()
    }
}

impl WithLen for Box<dyn BinningggContainerBinsDyn> {
    fn len(&self) -> usize {
        WithLen::len(self.as_ref())
    }
}

impl CollectableDyn for Box<dyn BinningggContainerBinsDyn> {
    fn new_collector(&self) -> Box<dyn CollectorDyn> {
        self.as_ref().new_collector()
    }
}

impl TypeName for Box<dyn Events> {
    fn type_name(&self) -> String {
        self.as_ref().type_name()
    }
}

impl CollectableDyn for Box<dyn Events> {
    fn new_collector(&self) -> Box<dyn CollectorDyn> {
        self.as_ref().new_collector()
    }
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
