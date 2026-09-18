use super::Error;
use super::field::IntroRegistry;
use super::field::PvaArrayKind;
use super::field::PvaField;
use super::field::PvaScalarType;
use super::field::PvaStruct;
use super::field::PvaUnion;
use super::pvdata::BitSet;
use super::pvdata::PVA_ARRAY_LEN_MAX;
use super::pvdata::Reader;
use netpod::timeunits::SEC;
use serde::Serialize;
use serde::ser::SerializeMap;
use std::sync::Arc;

pub const EPICS_EPOCH_OFFSET: u64 = 631152000;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum PvaValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    BoolArray(Vec<bool>),
    I8Array(Vec<i8>),
    I16Array(Vec<i16>),
    I32Array(Vec<i32>),
    I64Array(Vec<i64>),
    U8Array(Vec<u8>),
    U16Array(Vec<u16>),
    U32Array(Vec<u32>),
    U64Array(Vec<u64>),
    F32Array(Vec<f32>),
    F64Array(Vec<f64>),
    StringArray(Vec<String>),
    Struct(PvaStructValue),
    StructArray(Vec<Option<PvaStructValue>>),
    Union(PvaUnionValue),
    UnionArray(Vec<Option<PvaUnionValue>>),
    VariantUnion(Option<Box<PvaValue>>),
    VariantUnionArray(Vec<Option<PvaValue>>),
}

macro_rules! decode_array {
    ($r:expr, $n:expr, $at:expr, $m:ident, $var:ident) => {{
        let mut a = Vec::with_capacity($n.min($at).min(4096));
        for i in 0..$n {
            let v = $r.$m()?;
            if i < $at {
                a.push(v);
            }
        }
        PvaValue::$var(a)
    }};
}

impl PvaValue {
    pub fn decode(
        field: &PvaField,
        r: &mut Reader,
        reg: &mut IntroRegistry,
        array_truncate: usize,
    ) -> Result<Self, Error> {
        match field {
            PvaField::Scalar(st) => Self::decode_scalar(*st, r),
            PvaField::ScalarArray(st, kind) => {
                let n = match kind {
                    PvaArrayKind::Fixed(n) => *n as usize,
                    _ => r.size_req()?,
                };
                if n > PVA_ARRAY_LEN_MAX {
                    return Err(Error::ArrayTooLong(n));
                }
                Self::decode_scalar_array(*st, n, r, array_truncate)
            }
            PvaField::Struct(ty) => Ok(PvaValue::Struct(PvaStructValue::decode_full(
                ty,
                r,
                reg,
                array_truncate,
            )?)),
            PvaField::StructArray(ty) => {
                let n = r.size_req()?;
                if n > PVA_ARRAY_LEN_MAX {
                    return Err(Error::ArrayTooLong(n));
                }
                let mut a = Vec::with_capacity(n.min(array_truncate).min(4096));
                for i in 0..n {
                    let v = if r.u8()? != 0 {
                        Some(PvaStructValue::decode_full(ty, r, reg, array_truncate)?)
                    } else {
                        None
                    };
                    if i < array_truncate {
                        a.push(v);
                    }
                }
                Ok(PvaValue::StructArray(a))
            }
            PvaField::Union(ty) => Ok(PvaValue::Union(PvaUnionValue::decode(ty, r, reg, array_truncate)?)),
            PvaField::UnionArray(ty) => {
                let n = r.size_req()?;
                if n > PVA_ARRAY_LEN_MAX {
                    return Err(Error::ArrayTooLong(n));
                }
                let mut a = Vec::with_capacity(n.min(array_truncate).min(4096));
                for i in 0..n {
                    let v = if r.u8()? != 0 {
                        Some(PvaUnionValue::decode(ty, r, reg, array_truncate)?)
                    } else {
                        None
                    };
                    if i < array_truncate {
                        a.push(v);
                    }
                }
                Ok(PvaValue::UnionArray(a))
            }
            PvaField::VariantUnion => Ok(PvaValue::VariantUnion(Self::decode_variant(r, reg, array_truncate)?)),
            PvaField::VariantUnionArray => {
                let n = r.size_req()?;
                if n > PVA_ARRAY_LEN_MAX {
                    return Err(Error::ArrayTooLong(n));
                }
                let mut a = Vec::with_capacity(n.min(array_truncate).min(4096));
                for i in 0..n {
                    let v = if r.u8()? != 0 {
                        Self::decode_variant(r, reg, array_truncate)?.map(|x| *x)
                    } else {
                        None
                    };
                    if i < array_truncate {
                        a.push(v);
                    }
                }
                Ok(PvaValue::VariantUnionArray(a))
            }
        }
    }

    fn decode_variant(
        r: &mut Reader,
        reg: &mut IntroRegistry,
        array_truncate: usize,
    ) -> Result<Option<Box<PvaValue>>, Error> {
        match reg.parse_field(r)? {
            None => Ok(None),
            Some(f) => Ok(Some(Box::new(Self::decode(&f, r, reg, array_truncate)?))),
        }
    }

    fn decode_scalar(st: PvaScalarType, r: &mut Reader) -> Result<Self, Error> {
        use PvaScalarType::*;
        let ret = match st {
            Bool => PvaValue::Bool(r.boolean()?),
            I8 => PvaValue::I8(r.i8()?),
            I16 => PvaValue::I16(r.i16()?),
            I32 => PvaValue::I32(r.i32()?),
            I64 => PvaValue::I64(r.i64()?),
            U8 => PvaValue::U8(r.u8()?),
            U16 => PvaValue::U16(r.u16()?),
            U32 => PvaValue::U32(r.u32()?),
            U64 => PvaValue::U64(r.u64()?),
            F32 => PvaValue::F32(r.f32()?),
            F64 => PvaValue::F64(r.f64()?),
            String => PvaValue::String(r.string()?),
        };
        Ok(ret)
    }

    fn decode_scalar_array(st: PvaScalarType, n: usize, r: &mut Reader, at: usize) -> Result<Self, Error> {
        use PvaScalarType::*;
        let ret = match st {
            Bool => decode_array!(r, n, at, boolean, BoolArray),
            I8 => decode_array!(r, n, at, i8, I8Array),
            I16 => decode_array!(r, n, at, i16, I16Array),
            I32 => decode_array!(r, n, at, i32, I32Array),
            I64 => decode_array!(r, n, at, i64, I64Array),
            U8 => decode_array!(r, n, at, u8, U8Array),
            U16 => decode_array!(r, n, at, u16, U16Array),
            U32 => decode_array!(r, n, at, u32, U32Array),
            U64 => decode_array!(r, n, at, u64, U64Array),
            F32 => decode_array!(r, n, at, f32, F32Array),
            F64 => decode_array!(r, n, at, f64, F64Array),
            String => decode_array!(r, n, at, string, StringArray),
        };
        Ok(ret)
    }

    pub fn zeroed(field: &PvaField) -> Self {
        match field {
            PvaField::Scalar(st) => Self::zeroed_scalar(*st),
            PvaField::ScalarArray(st, _) => Self::zeroed_scalar_array(*st),
            PvaField::Struct(ty) => PvaValue::Struct(PvaStructValue::zeroed(ty)),
            PvaField::StructArray(_) => PvaValue::StructArray(Vec::new()),
            PvaField::Union(ty) => PvaValue::Union(PvaUnionValue {
                ty: ty.clone(),
                sel: None,
                val: None,
            }),
            PvaField::UnionArray(_) => PvaValue::UnionArray(Vec::new()),
            PvaField::VariantUnion => PvaValue::VariantUnion(None),
            PvaField::VariantUnionArray => PvaValue::VariantUnionArray(Vec::new()),
        }
    }

    fn zeroed_scalar(st: PvaScalarType) -> Self {
        use PvaScalarType::*;
        match st {
            Bool => PvaValue::Bool(false),
            I8 => PvaValue::I8(0),
            I16 => PvaValue::I16(0),
            I32 => PvaValue::I32(0),
            I64 => PvaValue::I64(0),
            U8 => PvaValue::U8(0),
            U16 => PvaValue::U16(0),
            U32 => PvaValue::U32(0),
            U64 => PvaValue::U64(0),
            F32 => PvaValue::F32(0.),
            F64 => PvaValue::F64(0.),
            String => PvaValue::String(std::string::String::new()),
        }
    }

    fn zeroed_scalar_array(st: PvaScalarType) -> Self {
        use PvaScalarType::*;
        match st {
            Bool => PvaValue::BoolArray(Vec::new()),
            I8 => PvaValue::I8Array(Vec::new()),
            I16 => PvaValue::I16Array(Vec::new()),
            I32 => PvaValue::I32Array(Vec::new()),
            I64 => PvaValue::I64Array(Vec::new()),
            U8 => PvaValue::U8Array(Vec::new()),
            U16 => PvaValue::U16Array(Vec::new()),
            U32 => PvaValue::U32Array(Vec::new()),
            U64 => PvaValue::U64Array(Vec::new()),
            F32 => PvaValue::F32Array(Vec::new()),
            F64 => PvaValue::F64Array(Vec::new()),
            String => PvaValue::StringArray(Vec::new()),
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        use PvaValue::*;
        let ret = match self {
            Bool(x) => f64::from(*x),
            I8(x) => *x as f64,
            I16(x) => *x as f64,
            I32(x) => *x as f64,
            I64(x) => *x as f64,
            U8(x) => *x as f64,
            U16(x) => *x as f64,
            U32(x) => *x as f64,
            U64(x) => *x as f64,
            F32(x) => *x as f64,
            F64(x) => *x,
            _ => return None,
        };
        Some(ret)
    }

    pub fn as_i64(&self) -> Option<i64> {
        use PvaValue::*;
        let ret = match self {
            Bool(x) => i64::from(*x),
            I8(x) => *x as i64,
            I16(x) => *x as i64,
            I32(x) => *x as i64,
            I64(x) => *x,
            U8(x) => *x as i64,
            U16(x) => *x as i64,
            U32(x) => *x as i64,
            U64(x) => *x as i64,
            F32(x) => *x as i64,
            F64(x) => *x as i64,
            _ => return None,
        };
        Some(ret)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            PvaValue::String(x) => Some(x),
            _ => None,
        }
    }

    pub fn as_struct(&self) -> Option<&PvaStructValue> {
        match self {
            PvaValue::Struct(x) => Some(x),
            _ => None,
        }
    }

    pub fn f32_for_binning(&self) -> f32 {
        use PvaValue::*;
        match self {
            String(x) => x.len() as f32,
            BoolArray(x) => x.iter().fold(0., |a, x| a + f32::from(*x)),
            I8Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            I16Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            I32Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            I64Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            U8Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            U16Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            U32Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            U64Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            F32Array(x) => x.iter().fold(0., |a, x| a + *x),
            F64Array(x) => x.iter().fold(0., |a, x| a + *x as f32),
            StringArray(x) => x.len() as f32,
            Struct(x) => x.nt_value().map_or(0., |x| x.f32_for_binning()),
            StructArray(x) => x.len() as f32,
            Union(x) => x.val.as_ref().map_or(0., |x| x.f32_for_binning()),
            UnionArray(x) => x.len() as f32,
            VariantUnion(x) => x.as_ref().map_or(0., |x| x.f32_for_binning()),
            VariantUnionArray(x) => x.len() as f32,
            _ => self.as_f64().unwrap_or(0.) as f32,
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        use PvaValue::*;
        use serde_json::json;
        match self {
            Bool(x) => json!(*x),
            I8(x) => json!(*x),
            I16(x) => json!(*x),
            I32(x) => json!(*x),
            I64(x) => json!(*x),
            U8(x) => json!(*x),
            U16(x) => json!(*x),
            U32(x) => json!(*x),
            U64(x) => json!(*x),
            F32(x) => json!(*x),
            F64(x) => json!(*x),
            String(x) => json!(x),
            BoolArray(x) => json!(x),
            I8Array(x) => json!(x),
            I16Array(x) => json!(x),
            I32Array(x) => json!(x),
            I64Array(x) => json!(x),
            U8Array(x) => json!(x),
            U16Array(x) => json!(x),
            U32Array(x) => json!(x),
            U64Array(x) => json!(x),
            F32Array(x) => json!(x),
            F64Array(x) => json!(x),
            StringArray(x) => json!(x),
            Struct(x) => x.to_json_value(),
            StructArray(x) => {
                json!(
                    x.iter()
                        .map(|x| x.as_ref().map_or(json!(null), |x| x.to_json_value()))
                        .collect::<Vec<_>>()
                )
            }
            Union(x) => x.val.as_ref().map_or(json!(null), |x| x.to_json_value()),
            UnionArray(x) => json!(
                x.iter()
                    .map(|x| x
                        .as_ref()
                        .and_then(|x| x.val.as_ref())
                        .map_or(json!(null), |x| x.to_json_value()))
                    .collect::<Vec<_>>()
            ),
            VariantUnion(x) => x.as_ref().map_or(json!(null), |x| x.to_json_value()),
            VariantUnionArray(x) => json!(
                x.iter()
                    .map(|x| x.as_ref().map_or(json!(null), |x| x.to_json_value()))
                    .collect::<Vec<_>>()
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PvaStructValue {
    ty: Arc<PvaStruct>,
    vals: Vec<PvaValue>,
}

impl PvaStructValue {
    pub fn new(ty: Arc<PvaStruct>, vals: Vec<PvaValue>) -> Self {
        Self { ty, vals }
    }

    pub fn ty(&self) -> &Arc<PvaStruct> {
        &self.ty
    }

    pub fn vals(&self) -> &[PvaValue] {
        &self.vals
    }

    pub fn type_id(&self) -> &str {
        self.ty.id()
    }

    pub fn decode_full(
        ty: &Arc<PvaStruct>,
        r: &mut Reader,
        reg: &mut IntroRegistry,
        array_truncate: usize,
    ) -> Result<Self, Error> {
        let mut vals = Vec::with_capacity(ty.len());
        for f in ty.fields() {
            vals.push(PvaValue::decode(f, r, reg, array_truncate)?);
        }
        Ok(Self { ty: ty.clone(), vals })
    }

    pub fn decode_partial(
        ty: &Arc<PvaStruct>,
        changed: &BitSet,
        r: &mut Reader,
        reg: &mut IntroRegistry,
        array_truncate: usize,
    ) -> Result<PvaDelta, Error> {
        let mut entries = Vec::new();
        Self::collect_partial(ty, 0, changed, r, reg, array_truncate, &mut entries)?;
        Ok(PvaDelta {
            ty: ty.clone(),
            changed: changed.clone(),
            entries,
        })
    }

    fn collect_partial(
        ty: &Arc<PvaStruct>,
        base: u32,
        changed: &BitSet,
        r: &mut Reader,
        reg: &mut IntroRegistry,
        array_truncate: usize,
        out: &mut Vec<(u32, PvaValue)>,
    ) -> Result<(), Error> {
        if changed.get(base) {
            let v = Self::decode_full(ty, r, reg, array_truncate)?;
            out.push((base, PvaValue::Struct(v)));
            return Ok(());
        }
        for i in 0..ty.len() {
            let off = base + ty.offsets()[i];
            match &ty.fields()[i] {
                PvaField::Struct(s) => {
                    Self::collect_partial(s, off, changed, r, reg, array_truncate, out)?;
                }
                f => {
                    if changed.get(off) {
                        out.push((off, PvaValue::decode(f, r, reg, array_truncate)?));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn zeroed(ty: &Arc<PvaStruct>) -> Self {
        let vals = ty.fields().iter().map(PvaValue::zeroed).collect();
        Self { ty: ty.clone(), vals }
    }

    pub fn get(&self, path: &str) -> Option<&PvaValue> {
        let mut cur = self;
        let mut it = path.split('.').peekable();
        while let Some(seg) = it.next() {
            let i = cur.ty.index_of(seg)?;
            let v = cur.vals.get(i)?;
            if it.peek().is_none() {
                return Some(v);
            }
            match v {
                PvaValue::Struct(x) => cur = x,
                _ => return None,
            }
        }
        None
    }

    pub fn offset_of_path(&self, path: &str) -> Option<u32> {
        self.ty.offset_of_path(path)
    }

    pub fn node_mut(&mut self, offset: u32) -> Option<&mut PvaValue> {
        if offset == 0 {
            return None;
        }
        let ty = self.ty.clone();
        node_mut_inner(&ty, &mut self.vals, 0, offset)
    }

    pub fn nt_value(&self) -> Option<&PvaValue> {
        self.get("value")
    }

    pub fn nt_timestamp_unix_ns(&self) -> Option<u64> {
        let s = self.get("timeStamp.secondsPastEpoch")?.as_i64()?;
        let n = self.get("timeStamp.nanoseconds")?.as_i64()?;
        if s < 0 || n < 0 {
            return None;
        }
        Some(SEC * (s as u64 + EPICS_EPOCH_OFFSET) + n as u64)
    }

    pub fn nt_alarm(&self) -> Option<(i64, i64, &str)> {
        let sev = self.get("alarm.severity")?.as_i64()?;
        let sta = self.get("alarm.status")?.as_i64()?;
        let msg = self.get("alarm.message").and_then(|x| x.as_str()).unwrap_or("");
        Some((sev, sta, msg))
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for (name, v) in self.ty.names().iter().zip(self.vals.iter()) {
            m.insert(name.clone(), v.to_json_value());
        }
        serde_json::Value::Object(m)
    }
}

fn node_mut_inner<'a>(ty: &PvaStruct, vals: &'a mut [PvaValue], base: u32, target: u32) -> Option<&'a mut PvaValue> {
    let mut found = None;
    for i in 0..ty.len() {
        let off = base + ty.offsets()[i];
        let n = ty.fields()[i].node_count();
        if target >= off && target < off + n {
            found = Some((i, off));
            break;
        }
    }
    let (i, off) = found?;
    if target == off {
        return vals.get_mut(i);
    }
    match (&ty.fields()[i], vals.get_mut(i)?) {
        (PvaField::Struct(s), PvaValue::Struct(sv)) => node_mut_inner(s, &mut sv.vals, off, target),
        _ => None,
    }
}

impl Serialize for PvaStructValue {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let mut m = ser.serialize_map(Some(self.vals.len()))?;
        for (name, v) in self.ty.names().iter().zip(self.vals.iter()) {
            m.serialize_entry(name, v)?;
        }
        m.end()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PvaUnionValue {
    ty: Arc<PvaUnion>,
    sel: Option<usize>,
    val: Option<Box<PvaValue>>,
}

impl PvaUnionValue {
    pub fn ty(&self) -> &Arc<PvaUnion> {
        &self.ty
    }

    pub fn selected_name(&self) -> Option<&str> {
        self.sel.and_then(|i| self.ty.names().get(i)).map(|x| x.as_str())
    }

    pub fn value(&self) -> Option<&PvaValue> {
        self.val.as_deref()
    }

    fn decode(
        ty: &Arc<PvaUnion>,
        r: &mut Reader,
        reg: &mut IntroRegistry,
        array_truncate: usize,
    ) -> Result<Self, Error> {
        match r.size()? {
            None => Ok(Self {
                ty: ty.clone(),
                sel: None,
                val: None,
            }),
            Some(i) => {
                let f = ty.fields().get(i).ok_or(Error::BadUnionSelector(i))?.clone();
                let v = PvaValue::decode(&f, r, reg, array_truncate)?;
                Ok(Self {
                    ty: ty.clone(),
                    sel: Some(i),
                    val: Some(Box::new(v)),
                })
            }
        }
    }
}

impl Serialize for PvaUnionValue {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let mut m = ser.serialize_map(Some(1))?;
        m.serialize_entry(self.selected_name().unwrap_or(""), &self.val)?;
        m.end()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PvaDelta {
    ty: Arc<PvaStruct>,
    changed: BitSet,
    entries: Vec<(u32, PvaValue)>,
}

impl PvaDelta {
    pub fn ty(&self) -> &Arc<PvaStruct> {
        &self.ty
    }

    pub fn changed(&self) -> &BitSet {
        &self.changed
    }

    pub fn entries(&self) -> &[(u32, PvaValue)] {
        &self.entries
    }

    pub fn is_full(&self) -> bool {
        self.changed.get(0)
    }

    pub fn touches(&self, path: &str) -> bool {
        match self.ty.offset_of_path(path) {
            Some(off) => self.is_full() || self.changed.get(off),
            None => false,
        }
    }

    pub fn get(&self, path: &str) -> Option<&PvaValue> {
        let off = self.ty.offset_of_path(path)?;
        for (o, v) in &self.entries {
            if *o == off {
                return Some(v);
            }
            if off > *o
                && let PvaValue::Struct(sv) = v
            {
                if *o == 0 {
                    return sv.get(path);
                }
                if let Some(rest) = strip_prefix_path(&self.ty, *o, path)
                    && let Some(x) = sv.get(rest)
                {
                    return Some(x);
                }
            }
        }
        None
    }

    pub fn into_full(self) -> Option<PvaStructValue> {
        if !self.is_full() {
            return None;
        }
        for (o, v) in self.entries {
            if o == 0
                && let PvaValue::Struct(x) = v
            {
                return Some(x);
            }
        }
        None
    }
}

fn strip_prefix_path<'a>(ty: &PvaStruct, off: u32, path: &'a str) -> Option<&'a str> {
    let mut cur = ty;
    let mut acc = 0;
    let mut rest = path;
    loop {
        let (seg, tail) = match rest.split_once('.') {
            Some((a, b)) => (a, Some(b)),
            None => (rest, None),
        };
        let i = cur.index_of(seg)?;
        acc += cur.offsets()[i];
        if acc == off {
            return tail;
        }
        match (&cur.fields()[i], tail) {
            (PvaField::Struct(s), Some(t)) => {
                cur = s;
                rest = t;
            }
            _ => return None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pva::field::test::composite_pv;
    use crate::pva::field::test::nt_scalar_f64;
    use crate::pva::pvdata::Endian;
    use crate::pva::pvdata::Writer;

    fn enc(f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf, Endian::Little);
        f(&mut w);
        buf
    }

    fn full_nt_scalar_bytes(v: f64, secs: i64, nanos: i32) -> Vec<u8> {
        enc(|w| {
            w.f64(v);
            w.i32(0);
            w.i32(0);
            w.string("");
            w.i64(secs);
            w.i32(nanos);
            w.i32(0);
        })
    }

    #[test]
    fn decode_full_nt_scalar() {
        let ty = nt_scalar_f64();
        let b = full_nt_scalar_bytes(1.5, 1000, 250);
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let v = PvaStructValue::decode_full(&ty, &mut r, &mut reg, usize::MAX).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(v.type_id(), "epics:nt/NTScalar:1.0");
        assert_eq!(v.get("value"), Some(&PvaValue::F64(1.5)));
        assert_eq!(v.get("timeStamp.secondsPastEpoch"), Some(&PvaValue::I64(1000)));
        assert_eq!(v.get("timeStamp.nanoseconds"), Some(&PvaValue::I32(250)));
        assert_eq!(v.get("alarm.message"), Some(&PvaValue::String(String::new())));
        assert_eq!(v.get("nope"), None);
        assert_eq!(v.nt_timestamp_unix_ns(), Some(SEC * (1000 + EPICS_EPOCH_OFFSET) + 250));
        assert_eq!(v.nt_alarm(), Some((0, 0, "")));
        assert_eq!(v.to_json_value()["value"], serde_json::json!(1.5));
    }

    #[test]
    fn decode_partial_value_and_timestamp() {
        let ty = nt_scalar_f64();
        let mut changed = BitSet::new();
        changed.set(ty.offset_of_path("value").unwrap());
        changed.set(ty.offset_of_path("timeStamp").unwrap());
        let b = enc(|w| {
            w.f64(2.5);
            w.i64(77);
            w.i32(88);
            w.i32(0);
        });
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let d = PvaStructValue::decode_partial(&ty, &changed, &mut r, &mut reg, usize::MAX).unwrap();
        assert_eq!(r.remaining(), 0);
        assert!(!d.is_full());
        assert_eq!(d.entries().len(), 2);
        assert_eq!(d.entries()[0].0, 1);
        assert_eq!(d.entries()[0].1, PvaValue::F64(2.5));
        assert_eq!(d.entries()[1].0, 6);
        assert!(d.touches("value"));
        assert!(d.touches("timeStamp"));
        assert!(!d.touches("alarm"));
        assert_eq!(d.get("value"), Some(&PvaValue::F64(2.5)));
        assert_eq!(d.get("timeStamp.secondsPastEpoch"), Some(&PvaValue::I64(77)));
        assert_eq!(d.get("alarm.severity"), None);
    }

    #[test]
    fn decode_partial_single_leaf_inside_substruct() {
        let ty = nt_scalar_f64();
        let mut changed = BitSet::new();
        changed.set(ty.offset_of_path("alarm.severity").unwrap());
        let b = enc(|w| w.i32(3));
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let d = PvaStructValue::decode_partial(&ty, &changed, &mut r, &mut reg, usize::MAX).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(d.entries(), &[(3, PvaValue::I32(3))]);
        assert_eq!(d.get("alarm.severity"), Some(&PvaValue::I32(3)));
    }

    #[test]
    fn decode_partial_whole_struct() {
        let ty = nt_scalar_f64();
        let mut changed = BitSet::new();
        changed.set(0);
        let b = full_nt_scalar_bytes(9.5, 4, 5);
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let d = PvaStructValue::decode_partial(&ty, &changed, &mut r, &mut reg, usize::MAX).unwrap();
        assert_eq!(r.remaining(), 0);
        assert!(d.is_full());
        assert!(d.touches("alarm.message"));
        let v = d.into_full().unwrap();
        assert_eq!(v.get("value"), Some(&PvaValue::F64(9.5)));
    }

    #[test]
    fn composite_five_fields() {
        let ty = composite_pv();
        let b = enc(|w| {
            w.i64(100);
            w.i32(200);
            w.i64(4711);
            w.f64(1.0);
            w.f64(2.0);
            w.size(3);
            w.f32(1.);
            w.f32(2.);
            w.f32(3.);
        });
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let v = PvaStructValue::decode_full(&ty, &mut r, &mut reg, usize::MAX).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(v.get("pulseId"), Some(&PvaValue::I64(4711)));
        assert_eq!(v.get("value1"), Some(&PvaValue::F64(1.0)));
        assert_eq!(v.get("value3"), Some(&PvaValue::F32Array(vec![1., 2., 3.])));
        assert_eq!(v.get("timeStamp.nanoseconds"), Some(&PvaValue::I32(200)));
        assert_eq!(v.nt_timestamp_unix_ns(), Some(SEC * (100 + EPICS_EPOCH_OFFSET) + 200));
    }

    #[test]
    fn array_truncate_still_consumes_all() {
        let ty = composite_pv();
        let b = enc(|w| {
            w.i64(0);
            w.i32(0);
            w.i64(0);
            w.f64(0.);
            w.f64(0.);
            w.size(5);
            for i in 0..5 {
                w.f32(i as f32);
            }
        });
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let v = PvaStructValue::decode_full(&ty, &mut r, &mut reg, 2).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(v.get("value3"), Some(&PvaValue::F32Array(vec![0., 1.])));
    }

    #[test]
    fn node_mut_navigates() {
        let ty = nt_scalar_f64();
        let mut v = PvaStructValue::zeroed(&ty);
        assert_eq!(v.node_mut(0), None);
        *v.node_mut(1).unwrap() = PvaValue::F64(7.);
        *v.node_mut(8).unwrap() = PvaValue::I32(42);
        assert_eq!(v.get("value"), Some(&PvaValue::F64(7.)));
        assert_eq!(v.get("timeStamp.nanoseconds"), Some(&PvaValue::I32(42)));
        assert!(v.node_mut(99).is_none());
        let n = v.node_mut(6).unwrap();
        assert!(matches!(n, PvaValue::Struct(_)));
    }

    #[test]
    fn serialize_as_map() {
        let ty = nt_scalar_f64();
        let b = full_nt_scalar_bytes(1.5, 1, 2);
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let v = PvaStructValue::decode_full(&ty, &mut r, &mut reg, usize::MAX).unwrap();
        let j: serde_json::Value = serde_json::to_value(&v).unwrap();
        assert_eq!(j["value"], serde_json::json!({"F64": 1.5}));
        assert_eq!(j["timeStamp"]["Struct"]["nanoseconds"], serde_json::json!({"I32": 2}));
    }
}
