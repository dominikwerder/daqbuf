pub use futures_util;

use netpod::TsNano;
use std::any::Any;

pub trait WithLen {
    fn len(&self) -> usize;
}

impl<T> WithLen for Box<T>
where
    T: WithLen,
{
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl WithLen for bytes::Bytes {
    fn len(&self) -> usize {
        self.len()
    }
}

pub trait Empty {
    fn empty() -> Self;
}

pub trait Resettable {
    fn reset(&mut self);
}

pub trait Appendable<STY>: WithLen {
    fn push(&mut self, ts: TsNano, value: STY);
}

pub trait Extendable: Empty + WithLen {
    fn extend_from(&mut self, src: &mut Self);
}

pub trait TypeName {
    fn type_name(&self) -> String;
}

pub trait AsAnyRef {
    fn as_any_ref(&self) -> &dyn Any;
}

pub trait AsAnyMut {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T> AsAnyRef for Box<T>
where
    T: AsAnyRef + ?Sized,
{
    fn as_any_ref(&self) -> &dyn Any {
        self.as_ref().as_any_ref()
    }
}

impl<T> AsAnyMut for Box<T>
where
    T: AsAnyMut + ?Sized,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self.as_mut().as_any_mut()
    }
}
