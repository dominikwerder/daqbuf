use crate::channelconfig::CompressionMethod;
use crate::nom;
use netpod::log::*;
use netpod::ScalarType;
use netpod::Shape;
use nom::bytes::complete::take;
use nom::error::context;
use nom::error::ContextError;
use nom::error::ErrorKind;
use nom::error::ParseError;
use nom::multi::many0;
use nom::number::complete::be_u32;
use nom::number::complete::be_u64;
use nom::number::complete::be_u8;
use nom::Err;
use nom::IResult;
use nom::Needed;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::num::NonZeroUsize;

#[cfg(test)]
use daqbuf_err as err;

type Nres<'a, O, E = nom::error::Error<&'a [u8]>> = Result<(&'a [u8], O), nom::Err<E>>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Api1ByteOrder {
    #[serde(rename = "LITTLE_ENDIAN")]
    Little,
    #[serde(rename = "BIG_ENDIAN")]
    Big,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Api1ScalarType {
    #[serde(rename = "uint8")]
    U8,
    #[serde(rename = "uint16")]
    U16,
    #[serde(rename = "uint32")]
    U32,
    #[serde(rename = "uint64")]
    U64,
    #[serde(rename = "int8")]
    I8,
    #[serde(rename = "int16")]
    I16,
    #[serde(rename = "int32")]
    I32,
    #[serde(rename = "int64")]
    I64,
    #[serde(rename = "float32")]
    F32,
    #[serde(rename = "float64")]
    F64,
    #[serde(rename = "bool")]
    BOOL,
    #[serde(rename = "string")]
    STRING,
}

impl Api1ScalarType {
    pub fn to_str(&self) -> &'static str {
        use Api1ScalarType as A;
        match self {
            A::U8 => "uint8",
            A::U16 => "uint16",
            A::U32 => "uint32",
            A::U64 => "uint64",
            A::I8 => "int8",
            A::I16 => "int16",
            A::I32 => "int32",
            A::I64 => "int64",
            A::F32 => "float32",
            A::F64 => "float64",
            A::BOOL => "bool",
            A::STRING => "string",
        }
    }
}

#[test]
fn test_custom_variant_name() {
    let val = Api1ScalarType::F32;
    assert_eq!(format!("{val:?}"), "F32");
    assert_eq!(format!("{val}"), "float32");
    let s = serde_json::to_string(&val).unwrap();
    assert_eq!(s, "\"float32\"");
}

impl fmt::Display for Api1ScalarType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.to_str())
    }
}

impl From<&ScalarType> for Api1ScalarType {
    fn from(k: &ScalarType) -> Self {
        use Api1ScalarType as B;
        use ScalarType as A;
        match k {
            A::U8 => B::U8,
            A::U16 => B::U16,
            A::U32 => B::U32,
            A::U64 => B::U64,
            A::I8 => B::I8,
            A::I16 => B::I16,
            A::I32 => B::I32,
            A::I64 => B::I64,
            A::F32 => B::F32,
            A::F64 => B::F64,
            A::BOOL => B::BOOL,
            A::STRING => B::STRING,
            // TODO treat enum as number only
            A::Enum => B::U16,
        }
    }
}

impl From<ScalarType> for Api1ScalarType {
    fn from(x: ScalarType) -> Self {
        (&x).into()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Api1ChannelHeader {
    name: String,
    #[serde(rename = "type")]
    ty: Api1ScalarType,
    #[serde(rename = "byteOrder")]
    byte_order: Api1ByteOrder,
    #[serde(default)]
    shape: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_compression_method")]
    compression: Option<CompressionMethod>,
}

impl Api1ChannelHeader {
    pub fn new(
        name: String,
        ty: Api1ScalarType,
        byte_order: Api1ByteOrder,
        shape: Shape,
        compression: Option<CompressionMethod>,
    ) -> Self {
        Self {
            name,
            ty,
            byte_order,
            shape: shape.to_u32_vec(),
            compression,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ty(&self) -> Api1ScalarType {
        self.ty.clone()
    }
}

mod serde_compression_method {
    use super::CompressionMethod;
    use serde::de;
    use serde::de::Visitor;
    use serde::Deserializer;
    use serde::Serializer;
    use std::fmt;

    pub fn serialize<S>(v: &Option<CompressionMethod>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match v {
            Some(v) => {
                let n = match v {
                    CompressionMethod::BitshuffleLZ4 => 1,
                };
                ser.serialize_some(&n)
            }
            None => ser.serialize_none(),
        }
    }

    struct VisC;

    impl<'de> Visitor<'de> for VisC {
        type Value = Option<CompressionMethod>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "compression method index")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v {
                0 => Ok(None),
                1 => Ok(Some(CompressionMethod::BitshuffleLZ4)),
                _ => Err(de::Error::unknown_variant("compression variant index", &["0"])),
            }
        }
    }

    struct Vis;

    impl<'de> Visitor<'de> for Vis {
        type Value = Option<CompressionMethod>;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            write!(fmt, "optional compression method index")
        }

        fn visit_some<D>(self, de: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            de.deserialize_u64(VisC)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Option<CompressionMethod>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_option(Vis)
    }
}

#[test]
fn basic_header_ser_00() {
    let h = Api1ChannelHeader {
        name: "Name".into(),
        ty: Api1ScalarType::F32,
        byte_order: Api1ByteOrder::Big,
        shape: Vec::new(),
        compression: None,
    };
    let js = serde_json::to_string(&h).unwrap();
    let vals = serde_json::from_str::<serde_json::Value>(&js).unwrap();
    let x = vals.as_object().unwrap().get("compression");
    assert_eq!(x, None)
}

#[test]
fn basic_header_ser_01() {
    let h = Api1ChannelHeader {
        name: "Name".into(),
        ty: Api1ScalarType::F32,
        byte_order: Api1ByteOrder::Big,
        shape: Vec::new(),
        compression: Some(CompressionMethod::BitshuffleLZ4),
    };
    let js = serde_json::to_string(&h).unwrap();
    let vals = serde_json::from_str::<serde_json::Value>(&js).unwrap();
    let x = vals.as_object().unwrap().get("compression").unwrap().as_i64();
    assert_eq!(x, Some(1))
}

#[test]
fn basic_header_deser_00() {
    let js = r#"{ "name": "ch1", "type": "float64", "byteOrder": "LITTLE_ENDIAN" }"#;
    let h: Api1ChannelHeader = serde_json::from_str(js).unwrap();
    assert!(h.compression.is_none());
}

#[test]
fn basic_header_deser_01() {
    let js = r#"{ "name": "ch1", "type": "float64", "byteOrder": "LITTLE_ENDIAN", "compression": null }"#;
    let h: Api1ChannelHeader = serde_json::from_str(js).unwrap();
    assert!(h.compression.is_none());
}

#[test]
fn basic_header_deser_02() {
    let js = r#"{ "name": "ch1", "type": "float64", "byteOrder": "LITTLE_ENDIAN", "compression": 0 }"#;
    let h: Api1ChannelHeader = serde_json::from_str(js).unwrap();
    assert!(h.compression.is_none());
}

#[test]
fn basic_header_deser_03() {
    let js = r#"{ "name": "ch1", "type": "float64", "byteOrder": "LITTLE_ENDIAN", "compression": 1 }"#;
    let h: Api1ChannelHeader = serde_json::from_str(js).unwrap();
    assert!(h.compression.is_some());
    assert_eq!(h.compression, Some(CompressionMethod::BitshuffleLZ4));
}

#[test]
fn basic_header_deser_04() {
    let js = r#"{ "name": "ch1", "type": "float64", "byteOrder": "LITTLE_ENDIAN", "compression": 2 }"#;
    let res = serde_json::from_str::<Api1ChannelHeader>(js);
    assert!(res.is_err());
}

// u32be length_1.
// there is exactly length_1 more bytes in this message.
// u8 mtype: 0: channel-header, 1: data

// for mtype == 0:
// The rest is a JSON with the channel header.

// for mtype == 1:
// u64be timestamp
// u64be pulse
// After that comes exactly (length_1 - 17) bytes of data.

#[derive(Debug)]
pub struct Header {
    header: Api1ChannelHeader,
}

impl Header {
    pub fn header(&self) -> &Api1ChannelHeader {
        &self.header
    }
}

#[derive(Debug)]
pub struct Data {
    ts: u64,
    pulse: u64,
    data: Vec<u8>,
}

impl Data {
    pub fn ts(&self) -> u64 {
        self.ts
    }

    pub fn pulse(&self) -> u64 {
        self.pulse
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug)]
pub enum Api1Frame {
    Header(Header),
    Data(Data),
}

fn fail_on_input<'a, T, E>(inp: &'a [u8]) -> Nres<'a, T, E>
where
    E: ParseError<&'a [u8]>,
{
    let e = nom::error::ParseError::from_error_kind(inp, ErrorKind::Fail);
    IResult::Err(Err::Failure(e))
}

fn header<'a, E>(inp: &'a [u8]) -> Nres<'a, Header, E>
where
    E: ParseError<&'a [u8]> + ContextError<&'a [u8]>,
{
    match serde_json::from_slice(inp) {
        Ok(k) => {
            let k: Api1ChannelHeader = k;
            eprintln!("Parsed header OK: {k:?}");
            IResult::Ok((&inp[inp.len()..], Header { header: k }))
        }
        Err(e) => {
            let s = String::from_utf8_lossy(inp);
            error!("can not parse json: {e}\n{s:?}");
            context("json parse", fail_on_input)(inp)
        }
    }
}

fn data<'a, E>(inp: &'a [u8]) -> Nres<'a, Data, E>
where
    E: ParseError<&'a [u8]>,
{
    if inp.len() < 16 {
        IResult::Err(Err::Incomplete(Needed::Size(NonZeroUsize::new(16).unwrap())))
    } else {
        let (inp, ts) = be_u64(inp)?;
        let (inp, pulse) = be_u64(inp)?;
        let (inp, data) = take(inp.len())(inp)?;
        let data = data.into();
        let res = Data { ts, pulse, data };
        IResult::Ok((inp, res))
    }
}

fn api1_frame_complete<'a, E>(inp: &'a [u8]) -> Nres<'a, Api1Frame, E>
where
    E: ParseError<&'a [u8]> + ContextError<&'a [u8]>,
{
    let (inp, mtype) = be_u8(inp)?;
    if mtype == 0 {
        let (inp, val) = header(inp)?;
        if inp.len() != 0 {
            context("header did not consume all bytes", fail_on_input)(inp)
        } else {
            let res = Api1Frame::Header(val);
            IResult::Ok((inp, res))
        }
    } else if mtype == 1 {
        let (inp, val) = data(inp)?;
        if inp.len() != 0 {
            context("data did not consume all bytes", fail_on_input)(inp)
        } else {
            let res = Api1Frame::Data(val);
            IResult::Ok((inp, res))
        }
    } else {
        let e = Err::Incomplete(Needed::Size(NonZeroUsize::new(1).unwrap()));
        IResult::Err(e)
    }
}

fn api1_frame<'a, E>(inp: &'a [u8]) -> Nres<'a, Api1Frame, E>
where
    E: ParseError<&'a [u8]> + ContextError<&'a [u8]>,
{
    let inp_orig = inp;
    let (inp, len) = be_u32(inp)?;
    if len < 1 {
        IResult::Err(Err::Failure(ParseError::from_error_kind(inp, ErrorKind::Fail)))
    } else {
        if inp.len() < len as usize + 4 {
            let e = Err::Incomplete(Needed::Size(NonZeroUsize::new(len as _).unwrap()));
            IResult::Err(e)
        } else {
            let (inp, payload) = nom::bytes::complete::take(len)(inp)?;
            let (inp, len2) = be_u32(inp)?;
            if len != len2 {
                IResult::Err(Err::Failure(ParseError::from_error_kind(inp_orig, ErrorKind::Fail)))
            } else {
                let (left, res) = api1_frame_complete(payload)?;
                if left.len() != 0 {
                    context("frame did not consume all bytes", fail_on_input)(inp_orig)
                } else {
                    IResult::Ok((inp, res))
                }
            }
        }
    }
}

pub fn api1_frames<'a, E>(inp: &'a [u8]) -> Nres<'a, Vec<Api1Frame>, E>
where
    E: ParseError<&'a [u8]> + ContextError<&'a [u8]>,
{
    many0(api1_frame)(inp)
}

#[allow(unused)]
fn verbose_err(inp: &[u8]) -> Nres<'_, u32> {
    use nom::error::ErrorKind;
    use nom::error::ParseError;
    use nom::error::VerboseError;
    use nom::Err;
    let e = ParseError::from_error_kind(inp, ErrorKind::Fail);
    IResult::Err(Err::Failure(e))
}

#[test]
fn combinator_default_err() {
    be_u32::<_, nom::error::Error<_>>([1, 2, 3, 4].as_slice()).unwrap();
}

#[test]
fn test_basic_frames() -> Result<(), err::Error> {
    use std::io::Write;
    let mut buf = Vec::new();
    let js = r#"{"name": "ch1", "type": "float64", "byteOrder": "LITTLE_ENDIAN"}"#;
    buf.write(&(1 + js.as_bytes().len() as u32).to_be_bytes())?;
    buf.write(&[0])?;
    buf.write(js.as_bytes())?;

    buf.write(&25u32.to_be_bytes())?;
    buf.write(&[1])?;
    buf.write(&20u64.to_be_bytes())?;
    buf.write(&21u64.to_be_bytes())?;
    buf.write(&5.123f64.to_be_bytes())?;

    buf.write(&25u32.to_be_bytes())?;
    buf.write(&[1])?;
    buf.write(&22u64.to_be_bytes())?;
    buf.write(&23u64.to_be_bytes())?;
    buf.write(&7.88f64.to_be_bytes())?;

    match api1_frames::<nom::error::Error<_>>(&buf) {
        Ok((_, frames)) => {
            assert_eq!(frames.len(), 3);
        }
        Err(e) => {
            panic!("can not parse result: {e}")
        }
    };
    Ok(())
}
