use super::Error;
use super::pvdata::PVA_INTRO_REGISTRY_MAX;
use super::pvdata::PVA_STRUCT_DEPTH_MAX;
use super::pvdata::PVA_STRUCT_FIELDS_MAX;
use super::pvdata::Reader;
use super::pvdata::Writer;
use netpod::ScalarType;
use netpod::Shape;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

pub const TYPE_CODE_NULL: u8 = 0xff;
pub const TYPE_CODE_ONLY_ID: u8 = 0xfe;
pub const TYPE_CODE_FULL_WITH_ID: u8 = 0xfd;
pub const TYPE_CODE_FULL_TAGGED_ID: u8 = 0xfc;

const ARRAY_MASK: u8 = 0x18;
const ARRAY_NONE: u8 = 0x00;
const ARRAY_VARIABLE: u8 = 0x08;
const ARRAY_BOUNDED: u8 = 0x10;
const ARRAY_FIXED: u8 = 0x18;
const BASE_MASK: u8 = 0xe7;

const CODE_BOOL: u8 = 0x00;
const CODE_I8: u8 = 0x20;
const CODE_I16: u8 = 0x21;
const CODE_I32: u8 = 0x22;
const CODE_I64: u8 = 0x23;
const CODE_U8: u8 = 0x24;
const CODE_U16: u8 = 0x25;
const CODE_U32: u8 = 0x26;
const CODE_U64: u8 = 0x27;
const CODE_F32: u8 = 0x42;
const CODE_F64: u8 = 0x43;
const CODE_STRING: u8 = 0x60;
const CODE_STRUCT: u8 = 0x80;
const CODE_UNION: u8 = 0x81;
const CODE_VARIANT_UNION: u8 = 0x82;
const CODE_BOUNDED_STRING_A: u8 = 0x83;
const CODE_BOUNDED_STRING_B: u8 = 0x86;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum PvaScalarType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    String,
}

impl PvaScalarType {
    pub fn from_type_code(x: u8) -> Result<Self, Error> {
        use PvaScalarType::*;
        let ret = match x {
            CODE_BOOL => Bool,
            CODE_I8 => I8,
            CODE_I16 => I16,
            CODE_I32 => I32,
            CODE_I64 => I64,
            CODE_U8 => U8,
            CODE_U16 => U16,
            CODE_U32 => U32,
            CODE_U64 => U64,
            CODE_F32 => F32,
            CODE_F64 => F64,
            CODE_STRING => String,
            _ => return Err(Error::BadScalarTypeCode(x)),
        };
        Ok(ret)
    }

    pub fn to_type_code(&self) -> u8 {
        use PvaScalarType::*;
        match self {
            Bool => CODE_BOOL,
            I8 => CODE_I8,
            I16 => CODE_I16,
            I32 => CODE_I32,
            I64 => CODE_I64,
            U8 => CODE_U8,
            U16 => CODE_U16,
            U32 => CODE_U32,
            U64 => CODE_U64,
            F32 => CODE_F32,
            F64 => CODE_F64,
            String => CODE_STRING,
        }
    }

    pub fn wire_size(&self) -> Option<usize> {
        use PvaScalarType::*;
        let ret = match self {
            Bool => 1,
            I8 => 1,
            I16 => 2,
            I32 => 4,
            I64 => 8,
            U8 => 1,
            U16 => 2,
            U32 => 4,
            U64 => 8,
            F32 => 4,
            F64 => 8,
            String => return None,
        };
        Some(ret)
    }

    pub fn to_netpod_scalar_type(&self) -> ScalarType {
        use PvaScalarType::*;
        match self {
            Bool => ScalarType::BOOL,
            I8 => ScalarType::I8,
            I16 => ScalarType::I16,
            I32 => ScalarType::I32,
            I64 => ScalarType::I64,
            U8 => ScalarType::U8,
            U16 => ScalarType::U16,
            U32 => ScalarType::U32,
            U64 => ScalarType::U64,
            F32 => ScalarType::F32,
            F64 => ScalarType::F64,
            String => ScalarType::STRING,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum PvaArrayKind {
    Variable,
    Bounded(u32),
    Fixed(u32),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PvaField {
    Scalar(PvaScalarType),
    ScalarArray(PvaScalarType, PvaArrayKind),
    Struct(Arc<PvaStruct>),
    StructArray(Arc<PvaStruct>),
    Union(Arc<PvaUnion>),
    UnionArray(Arc<PvaUnion>),
    VariantUnion,
    VariantUnionArray,
}

impl PvaField {
    pub fn node_count(&self) -> u32 {
        match self {
            PvaField::Struct(x) => x.node_count(),
            _ => 1,
        }
    }

    pub fn to_netpod(&self) -> Option<(ScalarType, Shape)> {
        match self {
            PvaField::Scalar(x) => Some((x.to_netpod_scalar_type(), Shape::Scalar)),
            PvaField::ScalarArray(x, kind) => {
                let n = match kind {
                    PvaArrayKind::Variable => 0,
                    PvaArrayKind::Bounded(n) => *n,
                    PvaArrayKind::Fixed(n) => *n,
                };
                Some((x.to_netpod_scalar_type(), Shape::Wave(n)))
            }
            _ => None,
        }
    }

    pub fn write(&self, w: &mut Writer) {
        match self {
            PvaField::Scalar(x) => w.u8(x.to_type_code()),
            PvaField::ScalarArray(x, kind) => match kind {
                PvaArrayKind::Variable => w.u8(x.to_type_code() | ARRAY_VARIABLE),
                PvaArrayKind::Bounded(n) => {
                    w.u8(x.to_type_code() | ARRAY_BOUNDED);
                    w.size(*n as usize);
                }
                PvaArrayKind::Fixed(n) => {
                    w.u8(x.to_type_code() | ARRAY_FIXED);
                    w.size(*n as usize);
                }
            },
            PvaField::Struct(x) => {
                w.u8(CODE_STRUCT);
                x.write_body(w);
            }
            PvaField::StructArray(x) => {
                w.u8(CODE_STRUCT | ARRAY_VARIABLE);
                w.u8(CODE_STRUCT);
                x.write_body(w);
            }
            PvaField::Union(x) => {
                w.u8(CODE_UNION);
                x.write_body(w);
            }
            PvaField::UnionArray(x) => {
                w.u8(CODE_UNION | ARRAY_VARIABLE);
                w.u8(CODE_UNION);
                x.write_body(w);
            }
            PvaField::VariantUnion => w.u8(CODE_VARIANT_UNION),
            PvaField::VariantUnionArray => w.u8(CODE_VARIANT_UNION | ARRAY_VARIABLE),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PvaStruct {
    id: String,
    names: Vec<String>,
    fields: Vec<PvaField>,
    offsets: Vec<u32>,
    node_count: u32,
}

impl PvaStruct {
    pub fn new(id: String, names: Vec<String>, fields: Vec<PvaField>) -> Self {
        let mut offsets = Vec::with_capacity(fields.len());
        let mut acc = 1;
        for f in &fields {
            offsets.push(acc);
            acc += f.node_count();
        }
        Self {
            id,
            names,
            fields,
            offsets,
            node_count: acc,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn fields(&self) -> &[PvaField] {
        &self.fields
    }

    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    pub fn node_count(&self) -> u32 {
        self.node_count
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|x| x == name)
    }

    pub fn field_of(&self, name: &str) -> Option<&PvaField> {
        self.index_of(name).map(|i| &self.fields[i])
    }

    pub fn offset_of_path(&self, path: &str) -> Option<u32> {
        let mut cur = self;
        let mut acc = 0;
        let mut it = path.split('.').peekable();
        while let Some(seg) = it.next() {
            let i = cur.index_of(seg)?;
            acc += cur.offsets[i];
            if it.peek().is_none() {
                return Some(acc);
            }
            match &cur.fields[i] {
                PvaField::Struct(x) => cur = x,
                _ => return None,
            }
        }
        None
    }

    pub fn field_of_path(&self, path: &str) -> Option<&PvaField> {
        let mut cur = self;
        let mut it = path.split('.').peekable();
        while let Some(seg) = it.next() {
            let i = cur.index_of(seg)?;
            if it.peek().is_none() {
                return Some(&cur.fields[i]);
            }
            match &cur.fields[i] {
                PvaField::Struct(x) => cur = x,
                _ => return None,
            }
        }
        None
    }

    fn write_body(&self, w: &mut Writer) {
        w.string(&self.id);
        w.size(self.fields.len());
        for (name, f) in self.names.iter().zip(self.fields.iter()) {
            w.string(name);
            f.write(w);
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PvaUnion {
    id: String,
    names: Vec<String>,
    fields: Vec<PvaField>,
}

impl PvaUnion {
    pub fn new(id: String, names: Vec<String>, fields: Vec<PvaField>) -> Self {
        Self { id, names, fields }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn fields(&self) -> &[PvaField] {
        &self.fields
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|x| x == name)
    }

    fn write_body(&self, w: &mut Writer) {
        w.string(&self.id);
        w.size(self.fields.len());
        for (name, f) in self.names.iter().zip(self.fields.iter()) {
            w.string(name);
            f.write(w);
        }
    }
}

#[derive(Debug, Default)]
pub struct IntroRegistry {
    by_id: HashMap<u16, PvaField>,
}

impl IntroRegistry {
    pub fn new() -> Self {
        Self { by_id: HashMap::new() }
    }

    pub fn clear(&mut self) {
        self.by_id.clear();
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn insert(&mut self, id: u16, field: PvaField) -> Result<(), Error> {
        if self.by_id.len() >= PVA_INTRO_REGISTRY_MAX && !self.by_id.contains_key(&id) {
            return Err(Error::IntroRegistryFull);
        }
        self.by_id.insert(id, field);
        Ok(())
    }

    pub fn parse_field(&mut self, r: &mut Reader) -> Result<Option<PvaField>, Error> {
        self.parse_field_depth(r, 0)
    }

    pub fn parse_struct(&mut self, r: &mut Reader) -> Result<Arc<PvaStruct>, Error> {
        match self.parse_field(r)? {
            Some(PvaField::Struct(x)) => Ok(x),
            _ => Err(Error::ExpectedStructIntrospection),
        }
    }

    fn parse_field_depth(&mut self, r: &mut Reader, depth: u32) -> Result<Option<PvaField>, Error> {
        if depth > PVA_STRUCT_DEPTH_MAX {
            return Err(Error::StructTooDeep);
        }
        let code = r.u8()?;
        match code {
            TYPE_CODE_NULL => Ok(None),
            TYPE_CODE_ONLY_ID => {
                let id = r.u16()?;
                match self.by_id.get(&id) {
                    Some(x) => Ok(Some(x.clone())),
                    None => Err(Error::UnknownIntroId(id)),
                }
            }
            TYPE_CODE_FULL_WITH_ID => {
                let id = r.u16()?;
                let f = self.parse_field_depth(r, depth)?.ok_or(Error::NullFieldDescWithId)?;
                self.insert(id, f.clone())?;
                Ok(Some(f))
            }
            TYPE_CODE_FULL_TAGGED_ID => {
                let id = r.u16()?;
                let _tag = r.size()?;
                let f = self.parse_field_depth(r, depth)?.ok_or(Error::NullFieldDescWithId)?;
                self.insert(id, f.clone())?;
                Ok(Some(f))
            }
            _ => Ok(Some(self.parse_body(r, code, depth)?)),
        }
    }

    fn parse_body(&mut self, r: &mut Reader, code: u8, depth: u32) -> Result<PvaField, Error> {
        let base = code & BASE_MASK;
        let arr = code & ARRAY_MASK;
        match base {
            CODE_STRUCT | CODE_UNION | CODE_VARIANT_UNION => {
                if arr != ARRAY_NONE && arr != ARRAY_VARIABLE {
                    return Err(Error::BadArrayKindForComplexType(code));
                }
                let is_array = arr == ARRAY_VARIABLE;
                if base == CODE_VARIANT_UNION {
                    return Ok(if is_array {
                        PvaField::VariantUnionArray
                    } else {
                        PvaField::VariantUnion
                    });
                }
                if is_array {
                    let el = self.parse_field_depth(r, depth)?.ok_or(Error::NullFieldDescWithId)?;
                    return match (base, el) {
                        (CODE_STRUCT, PvaField::Struct(x)) => Ok(PvaField::StructArray(x)),
                        (CODE_UNION, PvaField::Union(x)) => Ok(PvaField::UnionArray(x)),
                        _ => Err(Error::BadTypeCode(code)),
                    };
                }
                let id = r.string()?;
                let n = r.size_req()?;
                if n > PVA_STRUCT_FIELDS_MAX {
                    return Err(Error::StructTooManyFields(n));
                }
                let mut names = Vec::with_capacity(n.min(1024));
                let mut fields = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    let name = r.string()?;
                    let f = self
                        .parse_field_depth(r, depth + 1)?
                        .ok_or(Error::NullFieldDescWithId)?;
                    names.push(name);
                    fields.push(f);
                }
                if base == CODE_STRUCT {
                    Ok(PvaField::Struct(Arc::new(PvaStruct::new(id, names, fields))))
                } else {
                    Ok(PvaField::Union(Arc::new(PvaUnion::new(id, names, fields))))
                }
            }
            CODE_BOUNDED_STRING_A | CODE_BOUNDED_STRING_B => Err(Error::BoundedStringUnsupported),
            _ => {
                let st = PvaScalarType::from_type_code(base)?;
                match arr {
                    ARRAY_NONE => Ok(PvaField::Scalar(st)),
                    ARRAY_VARIABLE => Ok(PvaField::ScalarArray(st, PvaArrayKind::Variable)),
                    ARRAY_BOUNDED => {
                        let n = r.size_req()?;
                        Ok(PvaField::ScalarArray(st, PvaArrayKind::Bounded(n as u32)))
                    }
                    _ => {
                        let n = r.size_req()?;
                        Ok(PvaField::ScalarArray(st, PvaArrayKind::Fixed(n as u32)))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::pva::pvdata::Endian;

    pub fn nt_scalar_f64() -> Arc<PvaStruct> {
        let alarm = Arc::new(PvaStruct::new(
            "alarm_t".into(),
            vec!["severity".into(), "status".into(), "message".into()],
            vec![
                PvaField::Scalar(PvaScalarType::I32),
                PvaField::Scalar(PvaScalarType::I32),
                PvaField::Scalar(PvaScalarType::String),
            ],
        ));
        let ts = Arc::new(PvaStruct::new(
            "time_t".into(),
            vec!["secondsPastEpoch".into(), "nanoseconds".into(), "userTag".into()],
            vec![
                PvaField::Scalar(PvaScalarType::I64),
                PvaField::Scalar(PvaScalarType::I32),
                PvaField::Scalar(PvaScalarType::I32),
            ],
        ));
        Arc::new(PvaStruct::new(
            "epics:nt/NTScalar:1.0".into(),
            vec!["value".into(), "alarm".into(), "timeStamp".into()],
            vec![
                PvaField::Scalar(PvaScalarType::F64),
                PvaField::Struct(alarm),
                PvaField::Struct(ts),
            ],
        ))
    }

    pub fn composite_pv() -> Arc<PvaStruct> {
        let ts = Arc::new(PvaStruct::new(
            "time_t".into(),
            vec!["secondsPastEpoch".into(), "nanoseconds".into()],
            vec![
                PvaField::Scalar(PvaScalarType::I64),
                PvaField::Scalar(PvaScalarType::I32),
            ],
        ));
        Arc::new(PvaStruct::new(
            "my:composite:1.0".into(),
            vec![
                "timeStamp".into(),
                "pulseId".into(),
                "value1".into(),
                "value2".into(),
                "value3".into(),
            ],
            vec![
                PvaField::Struct(ts),
                PvaField::Scalar(PvaScalarType::I64),
                PvaField::Scalar(PvaScalarType::F64),
                PvaField::Scalar(PvaScalarType::F64),
                PvaField::ScalarArray(PvaScalarType::F32, PvaArrayKind::Variable),
            ],
        ))
    }

    fn roundtrip(f: &PvaField) -> PvaField {
        let mut buf = Vec::new();
        f.write(&mut Writer::new(&mut buf, Endian::Little));
        let mut r = Reader::new(&buf, Endian::Little);
        let back = IntroRegistry::new().parse_field(&mut r).unwrap().unwrap();
        assert_eq!(r.remaining(), 0);
        back
    }

    #[test]
    fn node_offsets_nt_scalar() {
        let s = nt_scalar_f64();
        assert_eq!(s.node_count(), 10);
        assert_eq!(s.offsets(), &[1, 2, 6]);
        assert_eq!(s.offset_of_path("value"), Some(1));
        assert_eq!(s.offset_of_path("alarm"), Some(2));
        assert_eq!(s.offset_of_path("alarm.severity"), Some(3));
        assert_eq!(s.offset_of_path("alarm.message"), Some(5));
        assert_eq!(s.offset_of_path("timeStamp"), Some(6));
        assert_eq!(s.offset_of_path("timeStamp.secondsPastEpoch"), Some(7));
        assert_eq!(s.offset_of_path("timeStamp.nanoseconds"), Some(8));
        assert_eq!(s.offset_of_path("timeStamp.nope"), None);
        assert_eq!(s.offset_of_path("value.nope"), None);
    }

    #[test]
    fn node_offsets_composite() {
        let s = composite_pv();
        assert_eq!(s.node_count(), 8);
        assert_eq!(s.offsets(), &[1, 4, 5, 6, 7]);
        assert_eq!(s.offset_of_path("timeStamp.nanoseconds"), Some(3));
        assert_eq!(s.offset_of_path("pulseId"), Some(4));
        assert_eq!(s.offset_of_path("value3"), Some(7));
    }

    #[test]
    fn struct_array_counts_as_one_node() {
        let inner = nt_scalar_f64();
        let s = PvaStruct::new(
            "outer".into(),
            vec!["a".into(), "b".into()],
            vec![PvaField::StructArray(inner), PvaField::Scalar(PvaScalarType::I32)],
        );
        assert_eq!(s.node_count(), 3);
        assert_eq!(s.offsets(), &[1, 2]);
    }

    #[test]
    fn field_desc_roundtrip() {
        let s = PvaField::Struct(nt_scalar_f64());
        assert_eq!(roundtrip(&s), s);
        let s = PvaField::Struct(composite_pv());
        assert_eq!(roundtrip(&s), s);
        for f in [
            PvaField::Scalar(PvaScalarType::Bool),
            PvaField::Scalar(PvaScalarType::U64),
            PvaField::Scalar(PvaScalarType::String),
            PvaField::ScalarArray(PvaScalarType::F32, PvaArrayKind::Variable),
            PvaField::ScalarArray(PvaScalarType::I16, PvaArrayKind::Bounded(7)),
            PvaField::ScalarArray(PvaScalarType::I16, PvaArrayKind::Fixed(300)),
            PvaField::StructArray(nt_scalar_f64()),
            PvaField::VariantUnion,
            PvaField::VariantUnionArray,
        ] {
            assert_eq!(roundtrip(&f), f);
        }
    }

    #[test]
    fn scalar_type_codes() {
        let cases = [
            (0x00u8, PvaScalarType::Bool),
            (0x20, PvaScalarType::I8),
            (0x21, PvaScalarType::I16),
            (0x22, PvaScalarType::I32),
            (0x23, PvaScalarType::I64),
            (0x24, PvaScalarType::U8),
            (0x25, PvaScalarType::U16),
            (0x26, PvaScalarType::U32),
            (0x27, PvaScalarType::U64),
            (0x42, PvaScalarType::F32),
            (0x43, PvaScalarType::F64),
            (0x60, PvaScalarType::String),
        ];
        for (code, st) in cases {
            assert_eq!(PvaScalarType::from_type_code(code).unwrap(), st);
            assert_eq!(st.to_type_code(), code);
        }
        assert!(PvaScalarType::from_type_code(0x41).is_err());
    }

    #[test]
    fn spec_example_timestamp_with_id() {
        let mut buf = vec![TYPE_CODE_FULL_WITH_ID, 0x01, 0x00, CODE_STRUCT];
        {
            let mut w = Writer::new(&mut buf, Endian::Little);
            w.string("timeStamp_t");
            w.size(3);
            w.string("secondsPastEpoch");
            w.u8(CODE_I64);
            w.string("nanoseconds");
            w.u8(CODE_I32);
            w.string("userTag");
            w.u8(CODE_I32);
        }
        let mut reg = IntroRegistry::new();
        let mut r = Reader::new(&buf, Endian::Little);
        let f = reg.parse_field(&mut r).unwrap().unwrap();
        assert_eq!(r.remaining(), 0);
        let s = match &f {
            PvaField::Struct(x) => x.clone(),
            _ => panic!("expected struct"),
        };
        assert_eq!(s.id(), "timeStamp_t");
        assert_eq!(s.names(), &["secondsPastEpoch", "nanoseconds", "userTag"]);
        assert_eq!(s.node_count(), 4);
        assert_eq!(reg.len(), 1);
        let byid = [TYPE_CODE_ONLY_ID, 0x01, 0x00];
        let mut r = Reader::new(&byid, Endian::Little);
        assert_eq!(reg.parse_field(&mut r).unwrap().unwrap(), f);
    }

    #[test]
    fn only_id_unknown_errors() {
        let b = [TYPE_CODE_ONLY_ID, 0x07, 0x00];
        let mut r = Reader::new(&b, Endian::Little);
        let e = IntroRegistry::new().parse_field(&mut r).unwrap_err();
        assert!(format!("{e:?}").contains("UnknownIntroId"));
    }

    #[test]
    fn null_type_code() {
        let b = [TYPE_CODE_NULL];
        let mut r = Reader::new(&b, Endian::Little);
        assert_eq!(IntroRegistry::new().parse_field(&mut r).unwrap(), None);
    }

    #[test]
    fn bounded_string_rejected() {
        for code in [CODE_BOUNDED_STRING_A, CODE_BOUNDED_STRING_B] {
            let b = [code];
            let mut r = Reader::new(&b, Endian::Little);
            assert!(IntroRegistry::new().parse_field(&mut r).is_err());
        }
    }
}
