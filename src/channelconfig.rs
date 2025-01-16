use daqbuf_err as err;
use err::*;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::DAY;
use netpod::timeunits::MS;
use netpod::ByteOrder;
use netpod::DtNano;
use netpod::NodeConfigCached;
use netpod::ScalarType;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::TsNano;
use nom::bytes::complete::take;
use nom::number::complete::be_i16;
use nom::number::complete::be_i32;
use nom::number::complete::be_i64;
use nom::number::complete::be_i8;
use nom::number::complete::be_u8;
use nom::Needed;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, ThisError)]
#[cstm(name = "ConfigParse")]
pub enum ConfigParseError {
    NotSupportedOnNode,
    FileNotFound,
    #[error("PermissionDenied({0:?})")]
    PermissionDenied(PathBuf),
    IO,
    ParseError(String),
    NotSupported,
}

impl<T: fmt::Debug> From<nom::Err<T>> for ConfigParseError {
    fn from(k: nom::Err<T>) -> Self {
        let msg = format!("nom::Err<T> {:?}", k);
        Self::ParseError(msg)
    }
}

impl<I> nom::error::ParseError<I> for ConfigParseError {
    fn from_error_kind(_input: I, kind: nom::error::ErrorKind) -> Self {
        let msg = format!("ParseError kind {:?}", kind);
        Self::ParseError(msg)
    }

    fn append(_input: I, kind: nom::error::ErrorKind, other: Self) -> Self {
        let msg = format!("ParseError kind {:?} other {:?}", kind, other);
        Self::ParseError(msg)
    }
}

type NRes<'a, O> = nom::IResult<&'a [u8], O, ConfigParseError>;

fn mkerr<'a, S, O>(msg: S) -> NRes<'a, O>
where
    S: Into<String>,
{
    let e = ConfigParseError::ParseError(msg.into());
    Err(nom::Err::Error(e))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CompressionMethod {
    BitshuffleLZ4,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub ts: TsNano,
    #[serde(with = "humantime_serde")]
    pub ts_human: SystemTime,
    pub pulse: i64,
    pub ks: i32,
    pub bs: DtNano,
    pub split_count: i32,
    pub status: i32,
    pub bb: i8,
    pub modulo: i32,
    pub offset: i32,
    /*
    Precision:
      0 'default' whatever that is
     -7  f32
    -16  f64
    */
    pub precision: i16,
    pub scalar_type: ScalarType,
    pub is_compressed: bool,
    pub is_shaped: bool,
    pub is_array: bool,
    pub byte_order: ByteOrder,
    pub compression_method: Option<CompressionMethod>,
    pub shape: Option<Vec<u32>>,
    pub source_name: Option<String>,
    pub unit: Option<String>,
    pub description: Option<String>,
    pub optional_fields: Option<String>,
    pub value_converter: Option<String>,
}

impl ConfigEntry {
    pub fn to_shape(&self) -> Result<Shape, Error> {
        let ret = match &self.shape {
            Some(lens) => {
                if lens.len() == 1 {
                    Shape::Wave(lens[0])
                } else if lens.len() == 2 {
                    Shape::Image(lens[0], lens[1])
                } else {
                    // TODO
                    // Need a new Shape variant for images.
                    return Err(Error::with_msg(format!(
                        "Channel config unsupported shape {:?}",
                        self
                    )))?;
                }
            }
            None => Shape::Scalar,
        };
        Ok(ret)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfigs {
    pub format_version: i16,
    pub channel_name: String,
    pub entries: Vec<ConfigEntry>,
}

fn parse_short_string(inp: &[u8]) -> NRes<Option<String>> {
    let (inp, len1) = be_i32(inp)?;
    if len1 == -1 {
        return Ok((inp, None));
    }
    if len1 < 4 {
        return mkerr(format!("bad string len {}", len1));
    }
    if len1 > 500 {
        return mkerr(format!("large string len {}", len1));
    }
    let (inp, snb) = take((len1 - 4) as usize)(inp)?;
    match String::from_utf8(snb.to_vec()) {
        Ok(s1) => Ok((inp, Some(s1))),
        Err(e) => mkerr(format!("{:?}", e)),
    }
}

pub fn parse_entry(inp: &[u8]) -> NRes<Option<ConfigEntry>> {
    let (inp, len1) = be_i32(inp)?;
    if len1 < 0 || len1 > 4000 {
        return mkerr(format!("ConfigEntry bad len1 {}", len1));
    }
    if inp.len() == 0 {
        return Ok((inp, None));
    }
    if inp.len() < len1 as usize - 4 {
        return Err(nom::Err::Incomplete(Needed::new(len1 as usize - 4)));
    }
    let inp_e = &inp[(len1 - 8) as usize..];
    let (inp, ts) = be_i64(inp)?;
    let (inp, pulse) = be_i64(inp)?;
    let (inp, ks) = be_i32(inp)?;
    let (inp, bs) = be_i64(inp)?;
    let bs = DtNano::from_ns(bs as u64 * MS);
    let (inp, split_count) = be_i32(inp)?;
    let (inp, status) = be_i32(inp)?;
    let (inp, bb) = be_i8(inp)?;
    let (inp, modulo) = be_i32(inp)?;
    let (inp, offset) = be_i32(inp)?;
    let (inp, precision) = be_i16(inp)?;
    let (inp, dtlen) = be_i32(inp)?;
    if dtlen > 100 {
        return mkerr(format!("unexpected data type len {}", dtlen));
    }
    let (inp, dtmask) = be_u8(inp)?;
    let is_compressed = dtmask & 0x80 != 0;
    let is_array = dtmask & 0x40 != 0;
    let byte_order = ByteOrder::from_dtype_flags(dtmask);
    let is_shaped = dtmask & 0x10 != 0;
    let (inp, dtype) = be_u8(inp)?;
    if dtype > 13 {
        return mkerr(format!("unexpected data type {}", dtype));
    }
    let scalar_type = match ScalarType::from_dtype_index(dtype) {
        Ok(k) => k,
        Err(e) => {
            return mkerr(format!("Can not convert {} to DType  {:?}", dtype, e));
        }
    };
    let (inp, compression_method) = match is_compressed {
        false => (inp, None),
        true => {
            let (inp, cm) = be_u8(inp)?;
            match cm {
                0 => (inp, Some(CompressionMethod::BitshuffleLZ4)),
                _ => return mkerr(format!("unknown compression")),
            }
        }
    };
    let (inp, shape) = match is_shaped {
        false => (inp, None),
        true => {
            let (mut inp, dim) = be_u8(inp)?;
            if dim > 4 {
                return mkerr(format!("unexpected number of dimensions: {}", dim));
            }
            let mut shape = vec![];
            for _ in 0..dim {
                let t1 = be_i32(inp)?;
                inp = t1.0;
                shape.push(t1.1 as u32);
            }
            (inp, Some(shape))
        }
    };
    let (inp, source_name) = parse_short_string(inp)?;
    let (inp, unit) = parse_short_string(inp)?;
    let (inp, description) = parse_short_string(inp)?;
    let (inp, optional_fields) = parse_short_string(inp)?;
    let (inp, value_converter) = parse_short_string(inp)?;
    assert_eq!(inp.len(), inp_e.len());
    let (inp_e, len2) = be_i32(inp_e)?;
    if len1 != len2 {
        return mkerr(format!("mismatch  len1 {}  len2 {}", len1, len2));
    }
    Ok((
        inp_e,
        Some(ConfigEntry {
            ts: TsNano::from_ns(ts as u64),
            ts_human: SystemTime::UNIX_EPOCH + Duration::from_nanos(ts as u64),
            pulse,
            ks,
            bs,
            split_count: split_count,
            status,
            bb,
            modulo,
            offset,
            precision,
            scalar_type,
            is_compressed: is_compressed,
            is_array: is_array,
            is_shaped: is_shaped,
            byte_order,
            compression_method: compression_method,
            shape,
            source_name: source_name,
            unit,
            description,
            optional_fields: optional_fields,
            value_converter: value_converter,
        }),
    ))
}

/// Parse a complete configuration file from given in-memory input buffer.
fn parse_config_inner(inp: &[u8]) -> NRes<ChannelConfigs> {
    let (inp, ver) = be_i16(inp)?;
    let (inp, len1) = be_i32(inp)?;
    if len1 <= 8 || len1 > 500 {
        return mkerr(format!("no channel name.  len1 {}", len1));
    }
    let (inp, chn) = take((len1 - 8) as usize)(inp)?;
    let channel_name = match String::from_utf8(chn.to_vec()) {
        Ok(k) => k,
        Err(e) => {
            return mkerr(format!("channelName utf8 error {:?}", e));
        }
    };
    let (inp, len2) = be_i32(inp)?;
    if len1 != len2 {
        return mkerr(format!("Mismatch  len1 {}  len2 {}", len1, len2));
    }
    let mut entries = Vec::new();
    let mut inp_a = inp;
    while inp_a.len() > 0 {
        let inp = inp_a;
        let (inp, e) = parse_entry(inp)?;
        if let Some(e) = e {
            entries.push(e);
        }
        inp_a = inp;
    }
    // Do not sort the parsed config entries.
    // We want to deliver the actual order which is found on disk.
    // Important for troubleshooting.
    let ret = ChannelConfigs {
        format_version: ver,
        channel_name,
        entries,
    };
    Ok((inp, ret))
}

pub fn parse_config(inp: &[u8]) -> Result<ChannelConfigs, ConfigParseError> {
    let (_inp, ret) =
        parse_config_inner(inp).map_err(|e| ConfigParseError::ParseError(e.to_string()))?;
    Ok(ret)
}

#[derive(Clone)]
pub enum MatchingConfigEntry<'a> {
    None,
    Single(&'a ConfigEntry),
    // In this case, we only return the entry which best matches to the time range
    Multiple(&'a ConfigEntry),
}

impl<'a> MatchingConfigEntry<'a> {
    pub fn best(&self) -> Option<&ConfigEntry> {
        match self {
            MatchingConfigEntry::None => None,
            MatchingConfigEntry::Single(e) => Some(e),
            MatchingConfigEntry::Multiple(e) => Some(e),
        }
    }
}

pub fn extract_matching_config_entry<'a>(
    range: &NanoRange,
    channel_config: &'a ChannelConfigs,
) -> Result<MatchingConfigEntry<'a>, ConfigParseError> {
    const DO_DEBUG: bool = false;
    if DO_DEBUG {
        debug!("extract_matching_config_entry  range {range:?}");
    }
    let mut a: Vec<_> = channel_config
        .entries
        .iter()
        .enumerate()
        .map(|(i, x)| (i, x))
        .collect();
    a.sort_unstable_by_key(|(_, x)| x.ts.ns());
    let a = a;

    if DO_DEBUG {
        debug!("------------------------------------------------------------------");
        for x in &a {
            debug!("SORTED  {:3}  {:?}", x.0, x.1.ks);
        }
    }

    let b: Vec<_> = a
        .into_iter()
        .rev()
        .map({
            let mut last = None;
            move |(i, x)| {
                let k = last.clone();
                last = Some(x.ts.clone());
                (i, x, k)
            }
        })
        .collect();

    if DO_DEBUG {
        debug!("------------------------------------------------------------------");
        for x in &b {
            debug!("NEIGHB  {:3}  {:?}  {:?}", x.0, x.1.ks, x.2);
        }
    }
    let c: Vec<_> = b
        .into_iter()
        .rev()
        .map(|(i, e, tsn)| {
            if let Some(ts2) = tsn.clone() {
                if e.ts.ns() < range.end() {
                    let p = if e.ts.ns() < range.beg() {
                        range.beg()
                    } else {
                        e.ts.ns()
                    };
                    let q = if ts2.ns() < range.beg() {
                        range.beg()
                    } else {
                        if ts2.ns() < range.end() {
                            ts2.ns()
                        } else {
                            range.end()
                        }
                    };
                    (i, DtNano::from_ns(q - p), e)
                } else {
                    (i, DtNano::from_ns(0), e)
                }
            } else {
                if e.ts.ns() < range.end() {
                    if e.ts.ns() < range.beg() {
                        (i, DtNano::from_ns(range.delta()), e)
                    } else {
                        (i, DtNano::from_ns(range.end() - e.ts.ns()), e)
                    }
                } else {
                    (i, DtNano::from_ns(0), e)
                }
            }
        })
        .collect();

    if DO_DEBUG {
        debug!("------------------------------------------------------------------");
        for (i, dt, e) in &c {
            debug!("WEIGHT  {:3}  {:?}  {:?}  {:?}", i, dt, e.ks, e.ts);
        }
    }

    let mut c = c;
    c.sort_unstable_by_key(|(_, dt, _)| u64::MAX - dt.ns());
    let c = c;

    if DO_DEBUG {
        debug!("------------------------------------------------------------------");
        for (i, dt, e) in &c {
            debug!("WEISOR  {:3}  {:?}  {:?}  {:?}", i, dt, e.ks, e.ts);
        }
    }

    if let Some(&(i, _, _)) = c.first() {
        Ok(MatchingConfigEntry::Single(&channel_config.entries[i]))
    } else {
        Ok(MatchingConfigEntry::None)
    }
}

#[cfg(test)]
mod test {
    use super::parse_config;

    fn read_data() -> Vec<u8> {
        use std::io::Read;
        //let path = "ks/config/S10CB01-RLOD100-PUP10:SIG-AMPLT/latest/00000_Config";
        let cwd = std::env::current_dir();
        netpod::log::info!("CWD: {:?}", cwd);
        let path = "../resources/sf-daqbuf-33-S10CB01-RLOD100-PUP10:SIG-AMPLT-latest-00000_Config";
        //let path = "../resources/sf-daqbuf-21-S10CB01-RLOD100-PUP10:SIG-AMPLT-latest-00000_Config";
        let mut f1 = std::fs::File::open(path).unwrap();
        let mut buf = Vec::new();
        f1.read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn parse_dummy() {
        let config =
            parse_config(&[0, 0, 0, 0, 0, 11, 0x61, 0x62, 0x63, 0, 0, 0, 11, 0, 0, 0, 1]).unwrap();
        assert_eq!(0, config.format_version);
        assert_eq!("abc", config.channel_name);
    }

    #[test]
    fn open_file() {
        let config = parse_config(&read_data()).unwrap();
        assert_eq!(config.format_version, 0);
        assert_eq!(config.entries.len(), 18);
        for e in &config.entries {
            assert!(e.ts.ns() >= 631152000000000000);
            assert!(e.ts.ns() <= 1613640673424172164);
            assert!(e.shape.is_some());
        }
    }
}
