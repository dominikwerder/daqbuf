use crate::framable::FrameDecodable;
use crate::framable::INMEM_FRAME_ENCID;
use crate::framable::INMEM_FRAME_FOOT;
use crate::framable::INMEM_FRAME_HEAD;
use crate::framable::INMEM_FRAME_MAGIC;
use crate::inmem::InMemoryFrame;
use crate::log::*;
use bincode::config::FixintEncoding;
use bincode::config::LittleEndian;
use bincode::config::RejectTrailing;
use bincode::config::WithOtherEndian;
use bincode::config::WithOtherIntEncoding;
use bincode::config::WithOtherTrailing;
use bincode::DefaultOptions;
use bytes::BufMut;
use bytes::BytesMut;
use core::fmt;
use daqbuf_err as err;
use items_0::bincode;
use items_0::streamitem::LogItem;
use items_0::streamitem::StatsItem;
use items_0::streamitem::ERROR_FRAME_TYPE_ID;
use items_0::streamitem::LOG_FRAME_TYPE_ID;
use items_0::streamitem::RANGE_COMPLETE_FRAME_TYPE_ID;
use items_0::streamitem::STATS_FRAME_TYPE_ID;
use items_0::streamitem::TERM_FRAME_TYPE_ID;
use serde::Serialize;
use std::any;
use std::io;

const USE_JSON: bool = false;
const EMIT_JSON_DEBUG: bool = false;
const EMIT_POSTCARD_DEBUG: bool = false;

autoerr::create_error_v1!(
    name(Error, "ItemFrame"),
    enum variants {
        TooLongPayload(usize),
        UnknownEncoder(u32),
        BufferMismatch(u32, usize, u32),
        TyIdMismatch(u32, u32),
        Msg(String),
        Bincode(#[from] Box<bincode::ErrorKind>),
        RmpEnc(#[from] rmp_serde::encode::Error),
        RmpDec(#[from] rmp_serde::decode::Error),
        ErasedSerde(#[from] erased_serde::Error),
        PostcardSer(postcard::Error),
        PostcardDe(postcard::Error, usize, String, String),
        SerdeJson(#[from] serde_json::Error),
    },
);

struct ErrMsg<E>(E)
where
    E: ToString;

impl<E> From<ErrMsg<E>> for Error
where
    E: ToString,
{
    fn from(value: ErrMsg<E>) -> Self {
        Self::Msg(value.0.to_string())
    }
}

pub fn bincode_ser<W>(
    w: W,
) -> bincode::Serializer<
    W,
    WithOtherTrailing<
        WithOtherIntEncoding<WithOtherEndian<DefaultOptions, LittleEndian>, FixintEncoding>,
        RejectTrailing,
    >,
>
where
    W: io::Write,
{
    use bincode::Options;
    let opts = DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();
    let ser = bincode::Serializer::new(w, opts);
    ser
}

fn bincode_to_vec<S>(item: S) -> Result<Vec<u8>, Error>
where
    S: Serialize,
{
    let mut out = Vec::new();
    let mut ser = bincode_ser(&mut out);
    item.serialize(&mut ser)?;
    Ok(out)
}

fn bincode_from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    use bincode::Options;
    let opts = DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();
    let mut de = bincode::Deserializer::from_slice(buf, opts);
    <T as serde::Deserialize>::deserialize(&mut de).map_err(Into::into)
}

fn msgpack_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: Serialize,
{
    rmp_serde::to_vec_named(&item).map_err(Error::from)
}

fn msgpack_erased_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: erased_serde::Serialize,
{
    let mut out = Vec::new();
    {
        let mut ser1 = rmp_serde::Serializer::new(&mut out).with_struct_map();
        let mut ser2 = <dyn erased_serde::Serializer>::erase(&mut ser1);
        item.erased_serialize(&mut ser2)?;
    }
    Ok(out)
}

fn msgpack_from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    rmp_serde::from_slice(buf).map_err(Error::from)
}

fn postcard_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: Serialize,
{
    postcard::to_stdvec(&item)
        .map_err(|e| Error::PostcardSer(e))
        .inspect(|x| {
            if EMIT_POSTCARD_DEBUG {
                let a = &x[0..x.len().min(40)];
                eprintln!("postcard_to_vec  {:?}  {}", a, std::any::type_name::<T>());
            }
        })
}

fn postcard_erased_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: erased_serde::Serialize + fmt::Debug,
{
    use postcard::ser_flavors::Flavor;
    let mut ser1 = postcard::Serializer {
        output: postcard::ser_flavors::AllocVec::new(),
    };
    {
        let mut ser2 = <dyn erased_serde::Serializer>::erase(&mut ser1);
        item.erased_serialize(&mut ser2)
    }?;
    ser1.output
        .finalize()
        .map_err(|e| Error::PostcardSer(e))
        .inspect(|x| {
            if EMIT_POSTCARD_DEBUG {
                let a = &x[0..x.len().min(40)];
                eprintln!(
                    "postcard_erased_to_vec  {:?}  {:?}  {}",
                    a,
                    item,
                    std::any::type_name::<T>()
                );
            }
        })
}

pub fn postcard_from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    let x = postcard::from_bytes(buf).map_err(|e| {
        Error::PostcardDe(
            e,
            buf.len(),
            format!("{:?}", buf[0..buf.len().min(40)].to_vec()),
            std::any::type_name::<T>().into(),
        )
    })?;
    Ok(x)
}

fn json_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: Serialize + fmt::Debug,
{
    serde_json::to_vec(&item).map_err(Into::into).inspect(|x| {
        if EMIT_JSON_DEBUG {
            let s = String::from_utf8_lossy(&x);
            let a = &s[0..x.len().min(80)];
            eprintln!(
                "json_to_vec  {}  {:?}  {}",
                a,
                item,
                std::any::type_name::<T>()
            );
        }
    })
}

fn json_erased_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: erased_serde::Serialize + fmt::Debug,
{
    let out = Vec::new();
    let mut ser = serde_json::Serializer::new(out);
    let x = erased_serde::serialize(&item, &mut ser)?;
    assert_eq!(x, ());
    let ret = ser.into_inner();
    Ok(ret).inspect(|x| {
        if EMIT_JSON_DEBUG {
            let s = String::from_utf8_lossy(&x);
            let a = &s[0..s.len().min(80)];
            eprintln!(
                "json_erased_to_vec  {}  {:?}  {}",
                a,
                item,
                std::any::type_name::<T>()
            );
        }
    })
}

pub fn json_from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    if EMIT_JSON_DEBUG {
        let s = String::from_utf8_lossy(&buf);
        let a = &s[0..s.len().min(80)];
        eprintln!("json_from_slice  {}  {}", a, std::any::type_name::<T>());
    }
    Ok(serde_json::from_slice(buf)?)
}

pub fn encode_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: Serialize + fmt::Debug,
{
    if USE_JSON {
        json_to_vec(item)
    } else if false {
        msgpack_to_vec(item)
    } else if false {
        bincode_to_vec(item)
    } else {
        postcard_to_vec(item)
    }
}

pub fn encode_erased_to_vec<T>(item: T) -> Result<Vec<u8>, Error>
where
    T: erased_serde::Serialize + fmt::Debug,
{
    if USE_JSON {
        json_erased_to_vec(item)
    } else if false {
        msgpack_erased_to_vec(item)
    } else {
        let x = postcard_erased_to_vec(item);
        // let s = std::any::type_name::<T>();
        // warn!("encode_erased_to_vec  is_ok {}  T {}", x.is_ok(), s);
        x
    }
}

pub fn decode_from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    if USE_JSON {
        json_from_slice(buf)
    } else if false {
        msgpack_from_slice(buf)
    } else if false {
        bincode_from_slice(buf)
    } else {
        postcard_from_slice(buf)
    }
}

pub fn make_frame_2<T>(item: T, fty: u32) -> Result<BytesMut, Error>
where
    T: erased_serde::Serialize + fmt::Debug,
{
    let enc = encode_erased_to_vec(item)?;
    if enc.len() > u32::MAX as usize {
        return Err(Error::TooLongPayload(enc.len()));
    }
    let mut h = crc32fast::Hasher::new();
    h.update(&enc);
    let payload_crc = h.finalize();
    // TODO reserve also for footer via constant
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + enc.len());
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(INMEM_FRAME_ENCID);
    buf.put_u32_le(fty);
    buf.put_u32_le(enc.len() as u32);
    buf.put_u32_le(payload_crc);
    // TODO add padding to align to 8 bytes.
    buf.put(enc.as_ref());
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    return Ok(buf);
}

// TODO remove duplication for these similar `make_*_frame` functions:

pub fn make_error_frame(error: &err::Error) -> Result<BytesMut, Error> {
    // error frames are always encoded as json
    match json_to_vec(error) {
        Ok(enc) => {
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + enc.len());
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(ERROR_FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            buf.put(enc.as_ref());
            let mut h = crc32fast::Hasher::new();
            h.update(&buf);
            let frame_crc = h.finalize();
            buf.put_u32_le(frame_crc);
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

pub fn make_log_frame(item: &LogItem) -> Result<BytesMut, Error> {
    match encode_to_vec(item) {
        Ok(enc) => {
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + enc.len());
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(LOG_FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            buf.put(enc.as_ref());
            let mut h = crc32fast::Hasher::new();
            h.update(&buf);
            let frame_crc = h.finalize();
            buf.put_u32_le(frame_crc);
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

pub fn make_stats_frame(item: &StatsItem) -> Result<BytesMut, Error> {
    match encode_to_vec(item) {
        Ok(enc) => {
            let mut h = crc32fast::Hasher::new();
            h.update(&enc);
            let payload_crc = h.finalize();
            let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + enc.len());
            buf.put_u32_le(INMEM_FRAME_MAGIC);
            buf.put_u32_le(INMEM_FRAME_ENCID);
            buf.put_u32_le(STATS_FRAME_TYPE_ID);
            buf.put_u32_le(enc.len() as u32);
            buf.put_u32_le(payload_crc);
            buf.put(enc.as_ref());
            let mut h = crc32fast::Hasher::new();
            h.update(&buf);
            let frame_crc = h.finalize();
            buf.put_u32_le(frame_crc);
            Ok(buf)
        }
        Err(e) => Err(e)?,
    }
}

pub fn make_range_complete_frame() -> Result<BytesMut, Error> {
    let enc = [];
    let mut h = crc32fast::Hasher::new();
    h.update(&enc);
    let payload_crc = h.finalize();
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + enc.len());
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(INMEM_FRAME_ENCID);
    buf.put_u32_le(RANGE_COMPLETE_FRAME_TYPE_ID);
    buf.put_u32_le(enc.len() as u32);
    buf.put_u32_le(payload_crc);
    buf.put(enc.as_ref());
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    Ok(buf)
}

pub fn make_term_frame() -> Result<BytesMut, Error> {
    let enc = [];
    let mut h = crc32fast::Hasher::new();
    h.update(&enc);
    let payload_crc = h.finalize();
    let mut buf = BytesMut::with_capacity(INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + enc.len());
    buf.put_u32_le(INMEM_FRAME_MAGIC);
    buf.put_u32_le(INMEM_FRAME_ENCID);
    buf.put_u32_le(TERM_FRAME_TYPE_ID);
    buf.put_u32_le(enc.len() as u32);
    buf.put_u32_le(payload_crc);
    buf.put(enc.as_ref());
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    let frame_crc = h.finalize();
    buf.put_u32_le(frame_crc);
    Ok(buf)
}

pub fn decode_frame<T>(frame: &InMemoryFrame) -> Result<T, Error>
where
    T: FrameDecodable,
{
    if frame.encid() != INMEM_FRAME_ENCID {
        return Err(Error::UnknownEncoder(frame.encid()));
    }
    if frame.len() as usize != frame.buf().len() {
        return Err(Error::BufferMismatch(
            frame.len(),
            frame.buf().len(),
            frame.tyid(),
        ));
    }
    if frame.tyid() == ERROR_FRAME_TYPE_ID {
        // error frames are always encoded as json
        let k: err::Error = match json_from_slice(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!(
                    "deserialize  len {}  ERROR_FRAME_TYPE_ID  {}",
                    frame.buf().len(),
                    e
                );
                let n = frame.buf().len().min(256);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_error(k))
    } else if frame.tyid() == LOG_FRAME_TYPE_ID {
        let k: LogItem = match decode_from_slice(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!(
                    "deserialize  len {}  LOG_FRAME_TYPE_ID  {}",
                    frame.buf().len(),
                    e
                );
                let n = frame.buf().len().min(128);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_log(k))
    } else if frame.tyid() == STATS_FRAME_TYPE_ID {
        let k: StatsItem = match decode_from_slice(frame.buf()) {
            Ok(item) => item,
            Err(e) => {
                error!(
                    "deserialize  len {}  STATS_FRAME_TYPE_ID  {}",
                    frame.buf().len(),
                    e
                );
                let n = frame.buf().len().min(128);
                let s = String::from_utf8_lossy(&frame.buf()[..n]);
                error!("frame.buf as string: {:?}", s);
                Err(e)?
            }
        };
        Ok(T::from_stats(k))
    } else if frame.tyid() == RANGE_COMPLETE_FRAME_TYPE_ID {
        // There is currently no content in this variant.
        Ok(T::from_range_complete())
    } else {
        let tyid = T::FRAME_TYPE_ID;
        if frame.tyid() != tyid {
            Err(Error::TyIdMismatch(tyid, frame.tyid()))
        } else {
            match decode_from_slice(frame.buf()) {
                Ok(item) => Ok(item),
                Err(e) => {
                    error!(
                        "decode_from_slice error  len {}  tyid {:04x}  T {}",
                        frame.buf().len(),
                        frame.tyid(),
                        any::type_name::<T>()
                    );
                    error!("decode_from_slice error  {}", e);
                    let n = frame.buf().len().min(64);
                    let s = String::from_utf8_lossy(&frame.buf()[..n]);
                    error!(
                        "decode_from_slice bad frame.buf as bytes: {:?}",
                        &frame.buf()[..n]
                    );
                    error!("decode_from_slice bad frame.buf as string: {:?}", s);
                    Err(e)?
                }
            }
        }
    }
}

pub fn crchex<T>(t: T) -> String
where
    T: AsRef<[u8]>,
{
    let mut h = crc32fast::Hasher::new();
    h.update(t.as_ref());
    let crc = h.finalize();
    format!("{:08x}", crc)
}
