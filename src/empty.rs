use crate::eventsdim0::EventsDim0;
use crate::eventsdim1::EventsDim1;
use crate::Error;
use daqbuf_err as err;
use items_0::Empty;
use items_0::Events;
use netpod::log::*;
use netpod::EnumVariant;
use netpod::ScalarType;
use netpod::Shape;

pub fn empty_events_dyn_ev(scalar_type: &ScalarType, shape: &Shape) -> Result<Box<dyn Events>, Error> {
    let ret: Box<dyn Events> = match shape {
        Shape::Scalar => {
            use ScalarType::*;
            type K<T> = EventsDim0<T>;
            match scalar_type {
                U8 => Box::new(K::<u8>::empty()),
                U16 => Box::new(K::<u16>::empty()),
                U32 => Box::new(K::<u32>::empty()),
                U64 => Box::new(K::<u64>::empty()),
                I8 => Box::new(K::<i8>::empty()),
                I16 => Box::new(K::<i16>::empty()),
                I32 => Box::new(K::<i32>::empty()),
                I64 => Box::new(K::<i64>::empty()),
                F32 => Box::new(K::<f32>::empty()),
                F64 => Box::new(K::<f64>::empty()),
                BOOL => Box::new(K::<bool>::empty()),
                STRING => Box::new(K::<String>::empty()),
                Enum => Box::new(K::<EnumVariant>::empty()),
            }
        }
        Shape::Wave(..) => {
            use ScalarType::*;
            type K<T> = EventsDim1<T>;
            match scalar_type {
                U8 => Box::new(K::<u8>::empty()),
                U16 => Box::new(K::<u16>::empty()),
                U32 => Box::new(K::<u32>::empty()),
                U64 => Box::new(K::<u64>::empty()),
                I8 => Box::new(K::<i8>::empty()),
                I16 => Box::new(K::<i16>::empty()),
                I32 => Box::new(K::<i32>::empty()),
                I64 => Box::new(K::<i64>::empty()),
                F32 => Box::new(K::<f32>::empty()),
                F64 => Box::new(K::<f64>::empty()),
                BOOL => Box::new(K::<bool>::empty()),
                STRING => Box::new(K::<String>::empty()),
                Enum => Box::new(K::<EnumVariant>::empty()),
            }
        }
        Shape::Image(..) => {
            error!("TODO empty_events_dyn_ev  {scalar_type:?}  {shape:?}");
            err::todoval()
        }
    };
    Ok(ret)
}
