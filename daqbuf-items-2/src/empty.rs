use crate::binning::container_events::ContainerEvents;
use crate::binning::container_events::PulsedVal;
use crate::log::*;
use crate::Error;
use daqbuf_err as err;
use items_0::timebin::BinningggContainerEventsDyn;
use netpod::EnumVariant;
use netpod::ScalarType;
use netpod::Shape;

pub fn empty_events_dyn_ev(
    scalar_type: &ScalarType,
    shape: &Shape,
) -> Result<Box<dyn BinningggContainerEventsDyn>, Error> {
    let ret: Box<dyn BinningggContainerEventsDyn> = match shape {
        Shape::Scalar => {
            use ScalarType::*;
            type K<T> = ContainerEvents<T>;
            match scalar_type {
                U8 => Box::new(K::<u8>::new()),
                U16 => Box::new(K::<u16>::new()),
                U32 => Box::new(K::<u32>::new()),
                U64 => Box::new(K::<u64>::new()),
                I8 => Box::new(K::<i8>::new()),
                I16 => Box::new(K::<i16>::new()),
                I32 => Box::new(K::<i32>::new()),
                I64 => Box::new(K::<i64>::new()),
                F32 => Box::new(K::<f32>::new()),
                F64 => Box::new(K::<f64>::new()),
                BOOL => Box::new(K::<bool>::new()),
                STRING => Box::new(K::<String>::new()),
                Enum => Box::new(K::<EnumVariant>::new()),
            }
        }
        Shape::Wave(..) => {
            use ScalarType::*;
            type K<T> = ContainerEvents<Vec<T>>;
            match scalar_type {
                U8 => Box::new(K::<u8>::new()),
                U16 => Box::new(K::<u16>::new()),
                U32 => Box::new(K::<u32>::new()),
                U64 => Box::new(K::<u64>::new()),
                I8 => Box::new(K::<i8>::new()),
                I16 => Box::new(K::<i16>::new()),
                I32 => Box::new(K::<i32>::new()),
                I64 => Box::new(K::<i64>::new()),
                F32 => Box::new(K::<f32>::new()),
                F64 => Box::new(K::<f64>::new()),
                BOOL => Box::new(K::<bool>::new()),
                STRING => Box::new(K::<String>::new()),
                Enum => Box::new(K::<EnumVariant>::new()),
            }
        }
        Shape::Image(..) => {
            error!("TODO empty_events_dyn_ev  {:?}  {:?}", scalar_type, shape);
            err::todoval()
        }
    };
    Ok(ret)
}

pub fn empty_events_pulsed_dyn_ev(
    scalar_type: &ScalarType,
    shape: &Shape,
) -> Result<Box<dyn BinningggContainerEventsDyn>, Error> {
    let ret: Box<dyn BinningggContainerEventsDyn> = match shape {
        Shape::Scalar => {
            use ScalarType::*;
            type K<T> = ContainerEvents<PulsedVal<T>>;
            match scalar_type {
                U8 => Box::new(K::<u8>::new()),
                U16 => Box::new(K::<u16>::new()),
                U32 => Box::new(K::<u32>::new()),
                U64 => Box::new(K::<u64>::new()),
                I8 => Box::new(K::<i8>::new()),
                I16 => Box::new(K::<i16>::new()),
                I32 => Box::new(K::<i32>::new()),
                I64 => Box::new(K::<i64>::new()),
                F32 => Box::new(K::<f32>::new()),
                F64 => Box::new(K::<f64>::new()),
                BOOL => Box::new(K::<bool>::new()),
                STRING => Box::new(K::<String>::new()),
                Enum => Box::new(K::<EnumVariant>::new()),
            }
        }
        Shape::Wave(..) => {
            use ScalarType::*;
            type K<T> = ContainerEvents<PulsedVal<Vec<T>>>;
            match scalar_type {
                U8 => Box::new(K::<u8>::new()),
                U16 => Box::new(K::<u16>::new()),
                U32 => Box::new(K::<u32>::new()),
                U64 => Box::new(K::<u64>::new()),
                I8 => Box::new(K::<i8>::new()),
                I16 => Box::new(K::<i16>::new()),
                I32 => Box::new(K::<i32>::new()),
                I64 => Box::new(K::<i64>::new()),
                F32 => Box::new(K::<f32>::new()),
                F64 => Box::new(K::<f64>::new()),
                BOOL => Box::new(K::<bool>::new()),
                STRING => Box::new(K::<String>::new()),
                Enum => Box::new(K::<EnumVariant>::new()),
            }
        }
        Shape::Image(..) => {
            error!("TODO empty_events_dyn_ev  {:?}  {:?}", scalar_type, shape);
            err::todoval()
        }
    };
    Ok(ret)
}
