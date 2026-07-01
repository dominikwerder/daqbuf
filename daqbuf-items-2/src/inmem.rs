use crate::framable::INMEM_FRAME_FOOT;
use crate::framable::INMEM_FRAME_HEAD;
use crate::framable::INMEM_FRAME_MAGIC;
use crate::log;
use bytes::Bytes;
use std::fmt;

macro_rules! error { ($($arg:expr),*) => ( if true { log::error!($($arg),*); }) }

macro_rules! debug { ($($arg:expr),*) => ( if true { log::debug!($($arg),*); }) }

autoerr::create_error_v1!(
    name(Error, "InMemoryFrameError"),
    enum variants {
        LessThanHeader,
        TryFromSlice(#[from] std::array::TryFromSliceError),
        BadMagic(u32),
        HugeFrame(u32),
        BadCrc,
    },
);

pub enum ParseResult<T> {
    NotEnoughData(usize),
    Parsed(usize, T),
}

pub struct InMemoryFrame {
    pub encid: u32,
    pub tyid: u32,
    pub len: u32,
    pub buf: Bytes,
}

impl InMemoryFrame {
    pub fn encid(&self) -> u32 {
        self.encid
    }

    pub fn tyid(&self) -> u32 {
        self.tyid
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn buf(&self) -> &Bytes {
        &self.buf
    }

    pub fn parse(buf: &[u8]) -> Result<ParseResult<Self>, Error> {
        if buf.len() < INMEM_FRAME_HEAD {
            return Err(Error::LessThanHeader);
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into()?);
        let encid = u32::from_le_bytes(buf[4..8].try_into()?);
        let tyid = u32::from_le_bytes(buf[8..12].try_into()?);
        let len = u32::from_le_bytes(buf[12..16].try_into()?);
        let payload_crc_exp = u32::from_le_bytes(buf[16..20].try_into()?);
        if magic != INMEM_FRAME_MAGIC {
            return Err(Error::BadMagic(magic));
        }
        debug!("frame len {:10}", len);
        if len > 1024 * 1024 * 50 {
            return Err(Error::HugeFrame(len));
        }
        let lentot = INMEM_FRAME_HEAD + INMEM_FRAME_FOOT + len as usize;
        if buf.len() < lentot {
            return Ok(ParseResult::NotEnoughData(lentot));
        }
        let p1 = INMEM_FRAME_HEAD + len as usize;
        let mut h = crc32fast::Hasher::new();
        h.update(&buf[..p1]);
        let frame_crc = h.finalize();
        let mut h = crc32fast::Hasher::new();
        h.update(&buf[INMEM_FRAME_HEAD..p1]);
        let payload_crc = h.finalize();
        let frame_crc_ind = u32::from_le_bytes(buf[p1..p1 + 4].try_into()?);
        let payload_crc_match = payload_crc_exp == payload_crc;
        let frame_crc_match = frame_crc_ind == frame_crc;
        if !frame_crc_match || !payload_crc_match {
            let _ss = String::from_utf8_lossy(&buf[..buf.len().min(256)]);
            let msg = format!(
                "InMemoryFrameAsyncReadStream  tryparse  crc mismatch A  {}  {}",
                payload_crc_match, frame_crc_match,
            );
            error!("{}", msg);
            let e = Error::BadCrc;
            return Err(e);
        }
        let ret = InMemoryFrame {
            len,
            tyid,
            encid,
            buf: Bytes::from(buf[INMEM_FRAME_HEAD..p1].to_vec()),
        };
        Ok(ParseResult::Parsed(lentot, ret))
    }
}

impl fmt::Debug for InMemoryFrame {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        let v = &self.buf;
        let _a = &v[0..v.len().min(40)];
        write!(
            fmt,
            "InMemoryFrame {{ encid: {:x}  tyid: {:x}  len {} }}",
            self.encid, self.tyid, self.len
        )
    }
}
