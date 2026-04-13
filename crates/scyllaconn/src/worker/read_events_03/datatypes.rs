use crate::events3::jobtrace::ReadEventKind;
use crate::events3::jobtrace::ReadJobTrace;
use futures_util::TryStreamExt;
use items_0::Appendable;
use items_0::Empty;
use items_0::scalar_ops::ScalarOps;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_events::ContainerEvents;
use netpod::EnumVariant;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsMs;
use netpod::TsNano;
use scylla::client::pager::QueryPager;
use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }
macro_rules! trace2 { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "EventsDatatypes"),
    enum variants {
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        Logic,
    },
);

pub trait ValTyDyn: fmt::Debug + Send {
    fn table_name(&self) -> &str;
    fn st_name(&self) -> &str;
    fn is_valueblob(&self) -> bool;
    fn read_into_container(
        &self,
        scyres: QueryPager,
        ts_msp: TsMs,
        with_vaues: bool,
        // TODO
        // jobtrace: &mut ReadJobTrace,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn BinningggContainerEventsDyn>, Error>> + Send>>;
    fn empty_container_for_test(&self) -> Box<dyn BinningggContainerEventsDyn>;
    fn clone_dyn(&self) -> Box<dyn ValTyDyn>;
}

#[derive(Debug, Clone)]
struct ValTyDynTesting<ST>
where
    ST: ValTy,
{
    _t1: PhantomData<ST>,
}

impl<ST> ValTyDynTesting<ST>
where
    ST: ValTy,
{
    fn new() -> Self {
        Self { _t1: PhantomData }
    }

    fn boxed() -> Box<dyn ValTyDyn> {
        Box::new(Self::new())
    }

    async fn read_into_container_impl(
        scyres: QueryPager,
        ts_msp: TsMs,
        with_values: bool,
        // TODO
        // jobtrace: &mut ReadJobTrace,
    ) -> Result<Box<dyn BinningggContainerEventsDyn>, Error> {
        let selfname = "read_into_container_branch_00";
        let mut jobtrace = ReadJobTrace::new();
        // TODO must branch already here depending on what input columns we expect
        let mut ret = <ST as ValTy>::Container::empty();
        let ret = if with_values {
            if ST::is_valueblob() {
                let mut it = scyres.rows_stream::<(i64, Vec<u8>)>()?;
                while let Some(row) = it.try_next().await? {
                    let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                    let value = ST::from_valueblob(row.1);
                    ret.push(ts, value);
                }
                ret
            } else {
                let mut i = 0;
                let mut it = scyres.rows_stream::<<ST as ValTy>::ScyRowTy>()?;
                while let Some(row) = it.try_next().await? {
                    let (ts, value) = <ST as ValTy>::scy_row_to_ts_val(ts_msp, row);
                    trace2!("{selfname}  {ts}");
                    // let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                    // let value = <ST as ValTy>::from_scyty(row.1);
                    ret.push(ts, value);
                    i += 1;
                    if i % 2000 == 0 {
                        jobtrace.add_event_now(ReadEventKind::ScyllaReadRow(i));
                    }
                }
                {
                    jobtrace.add_event_now(ReadEventKind::ScyllaReadRowDone(i));
                }
                ret
            }
        } else {
            let mut it = scyres.rows_stream::<(i64,)>()?;
            while let Some(row) = it.try_next().await? {
                let ts = TsNano::from_ns(ts_msp.ns_u64() + row.0 as u64);
                let value = <ST as ValTy>::default();
                ret.push(ts, value);
            }
            ret
        };
        let ret = Box::new(ret);
        Ok(ret)
    }
}

impl<ST> ValTyDyn for ValTyDynTesting<ST>
where
    ST: ValTy,
{
    fn table_name(&self) -> &str {
        ST::table_name()
    }

    fn st_name(&self) -> &str {
        ST::st_name()
    }

    fn is_valueblob(&self) -> bool {
        ST::is_valueblob()
    }

    fn read_into_container(
        &self,
        scyres: QueryPager,
        ts_msp: TsMs,
        with_values: bool,
        // jobtrace: &mut ReadJobTrace,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn BinningggContainerEventsDyn>, Error>> + Send>> {
        Box::pin(Self::read_into_container_impl(
            scyres,
            ts_msp,
            with_values,
            // TODO
            // jobtrace,
        ))
    }

    fn empty_container_for_test(&self) -> Box<dyn BinningggContainerEventsDyn> {
        Box::new(<ST::Container as Empty>::empty())
    }

    fn clone_dyn(&self) -> Box<dyn ValTyDyn> {
        let ret = Self { _t1: PhantomData };
        Box::new(ret)
    }
}

pub fn val_ty_dyn_from_type(shape: Shape, scalar_type: ScalarType) -> Box<dyn ValTyDyn> {
    match shape {
        Shape::Scalar => {
            use ScalarType::*;
            match scalar_type {
                U8 => ValTyDynTesting::<u8>::boxed(),
                U16 => ValTyDynTesting::<u16>::boxed(),
                U32 => ValTyDynTesting::<u32>::boxed(),
                U64 => ValTyDynTesting::<u64>::boxed(),
                I8 => ValTyDynTesting::<i8>::boxed(),
                I16 => ValTyDynTesting::<i16>::boxed(),
                I32 => ValTyDynTesting::<i32>::boxed(),
                I64 => ValTyDynTesting::<i64>::boxed(),
                F32 => ValTyDynTesting::<f32>::boxed(),
                F64 => ValTyDynTesting::<f64>::boxed(),
                BOOL => ValTyDynTesting::<bool>::boxed(),
                STRING => ValTyDynTesting::<String>::boxed(),
                Enum => ValTyDynTesting::<EnumVariant>::boxed(),
            }
        }
        Shape::Wave(_) => {
            use ScalarType::*;
            match scalar_type {
                U8 => ValTyDynTesting::<Vec<u8>>::boxed(),
                U16 => ValTyDynTesting::<Vec<u16>>::boxed(),
                U32 => ValTyDynTesting::<Vec<u32>>::boxed(),
                U64 => ValTyDynTesting::<Vec<u64>>::boxed(),
                I8 => ValTyDynTesting::<Vec<i8>>::boxed(),
                I16 => ValTyDynTesting::<Vec<i16>>::boxed(),
                I32 => ValTyDynTesting::<Vec<i32>>::boxed(),
                I64 => ValTyDynTesting::<Vec<i64>>::boxed(),
                F32 => ValTyDynTesting::<Vec<f32>>::boxed(),
                F64 => ValTyDynTesting::<Vec<f64>>::boxed(),
                BOOL => ValTyDynTesting::<Vec<bool>>::boxed(),
                STRING => {
                    warn!("read not yet supported  {:?}  {:?}", shape, scalar_type);
                    ValTyDynTesting::<Vec<String>>::boxed()
                }
                Enum => {
                    warn!("read not yet supported  {:?}  {:?}", shape, scalar_type);
                    ValTyDynTesting::<Vec<EnumVariant>>::boxed()
                }
            }
        }
        Shape::Image(_, _) => {
            error!("read not yet supported  {:?}  {:?}", shape, scalar_type);
            ValTyDynTesting::<u8>::boxed()
        }
    }
}

trait ValTy: fmt::Debug + Send + Sized + 'static {
    type ScaTy: ScalarOps + std::default::Default;
    type ScyTy: for<'a, 'b> scylla::deserialize::value::DeserializeValue<'a, 'b> + Send;
    type ScyRowTy: for<'a, 'b> scylla::deserialize::row::DeserializeRow<'a, 'b> + Send;
    type Container: BinningggContainerEventsDyn + Empty + Appendable<Self>;
    fn from_valueblob(inp: Vec<u8>) -> Self;
    fn table_name() -> &'static str;
    fn default() -> Self;
    fn is_valueblob() -> bool;
    fn st_name() -> &'static str;
    fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self);
}

macro_rules! impl_scaty_scalar {
    ($st:ty, $st_scy:ty, $st_name:expr, $table_name:expr) => {
        impl ValTy for $st {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type ScyRowTy = (i64, $st_scy);
            type Container = ContainerEvents<Self::ScaTy>;

            fn from_valueblob(_inp: Vec<u8>) -> Self {
                panic!("unused")
            }

            fn table_name() -> &'static str {
                concat!("scalar_", $table_name)
            }

            fn default() -> Self {
                <Self as std::default::Default>::default()
            }

            fn is_valueblob() -> bool {
                false
            }

            fn st_name() -> &'static str {
                $st_name
            }

            fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
                let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
                (ts, inp.1 as Self::ScaTy)
            }
        }
    };
}

macro_rules! impl_scaty_array {
    ($vt:ty, $st:ty, $st_scy:ty, $st_name:expr, $table_name:expr) => {
        impl ValTy for $vt {
            type ScaTy = $st;
            type ScyTy = $st_scy;
            type ScyRowTy = (i64, $st_scy);
            type Container = ContainerEvents<Vec<Self::ScaTy>>;

            fn from_valueblob(inp: Vec<u8>) -> Self {
                if inp.len() < 32 {
                    <Self as ValTy>::default()
                } else {
                    let en = std::mem::size_of::<Self::ScaTy>();
                    let n = (inp.len().max(32) - 32) / en;
                    let mut c = Vec::with_capacity(n);
                    for i in 0..n {
                        let r1 = &inp[32 + en * (0 + i)..32 + en * (1 + i)];
                        let p1 = r1 as *const _ as *const $st;
                        let v1 = unsafe { p1.read_unaligned() };
                        c.push(v1);
                    }
                    c
                }
            }

            fn table_name() -> &'static str {
                concat!("array_", $table_name)
            }

            fn default() -> Self {
                Vec::new()
            }

            fn is_valueblob() -> bool {
                true
            }

            fn st_name() -> &'static str {
                $st_name
            }

            fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
                let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
                (ts, inp.1.into_iter().map(|x| x as _).collect())
            }
        }
    };
}

impl ValTy for EnumVariant {
    type ScaTy = EnumVariant;
    type ScyTy = i16;
    type ScyRowTy = (i64, i16, String);
    type Container = ContainerEvents<EnumVariant>;

    fn from_valueblob(_inp: Vec<u8>) -> Self {
        panic!("unused")
    }

    fn table_name() -> &'static str {
        "array_string"
    }

    fn default() -> Self {
        <Self as Default>::default()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "enum"
    }

    fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
        let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
        (ts, EnumVariant::new(inp.1 as i16, inp.2))
    }
}

impl_scaty_scalar!(u8, i8, "u8", "u8");
impl_scaty_scalar!(u16, i16, "u16", "u16");
impl_scaty_scalar!(u32, i32, "u32", "u32");
impl_scaty_scalar!(u64, i64, "u64", "u64");
impl_scaty_scalar!(i8, i8, "i8", "i8");
impl_scaty_scalar!(i16, i16, "i16", "i16");
impl_scaty_scalar!(i32, i32, "i32", "i32");
impl_scaty_scalar!(i64, i64, "i64", "i64");
impl_scaty_scalar!(f32, f32, "f32", "f32");
impl_scaty_scalar!(f64, f64, "f64", "f64");
impl_scaty_scalar!(bool, bool, "bool", "bool");
impl_scaty_scalar!(String, String, "string", "string");

impl_scaty_array!(Vec<u8>, u8, Vec<i8>, "u8", "u8");
impl_scaty_array!(Vec<u16>, u16, Vec<i16>, "u16", "u16");
impl_scaty_array!(Vec<u32>, u32, Vec<i32>, "u32", "u32");
impl_scaty_array!(Vec<u64>, u64, Vec<i64>, "u64", "u64");
impl_scaty_array!(Vec<i8>, i8, Vec<i8>, "i8", "i8");
impl_scaty_array!(Vec<i16>, i16, Vec<i16>, "i16", "i16");
impl_scaty_array!(Vec<i32>, i32, Vec<i32>, "i32", "i32");
impl_scaty_array!(Vec<i64>, i64, Vec<i64>, "i64", "i64");
impl_scaty_array!(Vec<f32>, f32, Vec<f32>, "f32", "f32");
impl_scaty_array!(Vec<f64>, f64, Vec<f64>, "f64", "f64");
impl_scaty_array!(Vec<bool>, bool, Vec<bool>, "bool", "bool");

impl ValTy for Vec<String> {
    type ScaTy = String;
    type ScyTy = Vec<String>;
    type ScyRowTy = (i64, Vec<String>);
    type Container = ContainerEvents<Vec<String>>;

    fn from_valueblob(_inp: Vec<u8>) -> Self {
        warn!("array string not yet supported");
        // TODO
        Vec::new()
    }

    fn table_name() -> &'static str {
        "array_string"
    }

    fn default() -> Self {
        Vec::new()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "string"
    }

    fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
        warn!("array string not yet supported");
        let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
        // TODO
        (ts, Vec::new())
    }
}

impl ValTy for Vec<EnumVariant> {
    type ScaTy = EnumVariant;
    // TODO
    type ScyTy = i16;
    type ScyRowTy = (i64, i16, String);
    type Container = ContainerEvents<Vec<EnumVariant>>;

    fn from_valueblob(_inp: Vec<u8>) -> Self {
        warn!("enum waveform not yet supported");
        Vec::new()
    }

    fn table_name() -> &'static str {
        "array_enum"
    }

    fn default() -> Self {
        Vec::new()
    }

    fn is_valueblob() -> bool {
        false
    }

    fn st_name() -> &'static str {
        "enum"
    }

    fn scy_row_to_ts_val(msp: TsMs, inp: Self::ScyRowTy) -> (TsNano, Self) {
        let ts = TsNano::from_ns(msp.ns_u64() + inp.0 as u64);
        (ts, Vec::new())
    }
}
