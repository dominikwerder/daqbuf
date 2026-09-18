use super::Error;
use serde::Serialize;

pub const PVA_PAYLOAD_LEN_MAX: u32 = 1024 * 1024 * 32;
pub const PVA_ARRAY_LEN_MAX: usize = 1024 * 1024 * 16;
pub const PVA_STRING_LEN_MAX: usize = 1024 * 64;
pub const PVA_BITSET_BYTES_MAX: usize = 1024;
pub const PVA_STRUCT_DEPTH_MAX: u32 = 32;
pub const PVA_STRUCT_FIELDS_MAX: usize = 4096;
pub const PVA_INTRO_REGISTRY_MAX: usize = 1024 * 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Endian {
    Little,
    Big,
}

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    endian: Endian,
}

macro_rules! read_prim {
    ($name:ident, $ty:ty) => {
        pub fn $name(&mut self) -> Result<$ty, Error> {
            const N: usize = std::mem::size_of::<$ty>();
            let endian = self.endian;
            let b = self.take(N)?;
            let a: [u8; N] = b.try_into().map_err(|_| Error::BadSlice)?;
            Ok(match endian {
                Endian::Little => <$ty>::from_le_bytes(a),
                Endian::Big => <$ty>::from_be_bytes(a),
            })
        }
    };
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8], endian: Endian) -> Self {
        Self { buf, pos: 0, endian }
    }

    pub fn endian(&self) -> Endian {
        self.endian
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(n).ok_or(Error::BadSize)?;
        if end > self.buf.len() {
            return Err(Error::NotEnoughInput(n, self.remaining()));
        }
        let ret = &self.buf[self.pos..end];
        self.pos = end;
        Ok(ret)
    }

    pub fn rest(&mut self) -> &'a [u8] {
        let ret = &self.buf[self.pos..];
        self.pos = self.buf.len();
        ret
    }

    read_prim!(u16, u16);
    read_prim!(i16, i16);
    read_prim!(u32, u32);
    read_prim!(i32, i32);
    read_prim!(u64, u64);
    read_prim!(i64, i64);
    read_prim!(f32, f32);
    read_prim!(f64, f64);

    pub fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    pub fn i8(&mut self) -> Result<i8, Error> {
        Ok(self.take(1)?[0] as i8)
    }

    pub fn boolean(&mut self) -> Result<bool, Error> {
        Ok(self.u8()? != 0)
    }

    pub fn size(&mut self) -> Result<Option<usize>, Error> {
        let b = self.u8()?;
        if b == 0xff {
            Ok(None)
        } else if b == 0xfe {
            let n = self.i32()?;
            if n < 0 {
                Err(Error::BadSize)
            } else if n == i32::MAX {
                let n = self.i64()?;
                if n < 0 {
                    Err(Error::BadSize)
                } else {
                    Ok(Some(n as usize))
                }
            } else {
                Ok(Some(n as usize))
            }
        } else {
            Ok(Some(b as usize))
        }
    }

    pub fn size_req(&mut self) -> Result<usize, Error> {
        Ok(self.size()?.unwrap_or(0))
    }

    pub fn string_opt(&mut self) -> Result<Option<String>, Error> {
        match self.size()? {
            None => Ok(None),
            Some(n) => {
                if n > PVA_STRING_LEN_MAX {
                    return Err(Error::StringTooLong(n));
                }
                let b = self.take(n)?;
                let s = std::str::from_utf8(b).map_err(|_| Error::BadUtf8)?;
                Ok(Some(s.into()))
            }
        }
    }

    pub fn string(&mut self) -> Result<String, Error> {
        Ok(self.string_opt()?.unwrap_or_default())
    }

    pub fn bitset(&mut self) -> Result<BitSet, Error> {
        let n = self.size_req()?;
        if n > PVA_BITSET_BYTES_MAX {
            return Err(Error::BitSetTooLong(n));
        }
        let b = self.take(n)?;
        Ok(BitSet::from_bytes_lsb(b))
    }

    pub fn status(&mut self) -> Result<Status, Error> {
        let ty = self.u8()?;
        if ty == 0xff {
            Ok(Status::ok())
        } else {
            let kind = StatusKind::from_u8(ty);
            let message = self.string();
            let call_tree = self.string();
            Ok(Status {
                kind,
                message: message?,
                call_tree: call_tree?,
            })
        }
    }
}

pub struct Writer<'a> {
    buf: &'a mut Vec<u8>,
    endian: Endian,
}

macro_rules! write_prim {
    ($name:ident, $ty:ty) => {
        pub fn $name(&mut self, v: $ty) {
            match self.endian {
                Endian::Little => self.buf.extend_from_slice(&v.to_le_bytes()),
                Endian::Big => self.buf.extend_from_slice(&v.to_be_bytes()),
            }
        }
    };
}

impl<'a> Writer<'a> {
    pub fn new(buf: &'a mut Vec<u8>, endian: Endian) -> Self {
        Self { buf, endian }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn raw(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    write_prim!(u16, u16);
    write_prim!(i16, i16);
    write_prim!(u32, u32);
    write_prim!(i32, i32);
    write_prim!(u64, u64);
    write_prim!(i64, i64);
    write_prim!(f32, f32);
    write_prim!(f64, f64);

    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn i8(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    pub fn boolean(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    pub fn size(&mut self, n: usize) {
        if n < 254 {
            self.buf.push(n as u8);
        } else if n < i32::MAX as usize {
            self.buf.push(0xfe);
            self.i32(n as i32);
        } else {
            self.buf.push(0xfe);
            self.i32(i32::MAX);
            self.i64(n as i64);
        }
    }

    pub fn size_null(&mut self) {
        self.buf.push(0xff);
    }

    pub fn string(&mut self, s: &str) {
        self.size(s.len());
        self.buf.extend_from_slice(s.as_bytes());
    }

    pub fn bitset(&mut self, v: &BitSet) {
        let b = v.to_bytes_lsb();
        self.size(b.len());
        self.buf.extend_from_slice(&b);
    }

    pub fn status(&mut self, v: &Status) {
        if v.is_ok_empty() {
            self.buf.push(0xff);
        } else {
            self.buf.push(v.kind.to_u8());
            self.string(&v.message);
            self.string(&v.call_tree);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct BitSet {
    words: Vec<u64>,
}

impl BitSet {
    pub fn new() -> Self {
        Self { words: Vec::new() }
    }

    pub fn from_bytes_lsb(b: &[u8]) -> Self {
        let nw = b.len().div_ceil(8);
        let mut words = vec![0u64; nw];
        for (i, x) in b.iter().enumerate() {
            words[i / 8] |= (*x as u64) << (8 * (i % 8));
        }
        let mut ret = Self { words };
        ret.trim();
        ret
    }

    pub fn to_bytes_lsb(&self) -> Vec<u8> {
        let mut ret = Vec::new();
        for w in &self.words {
            ret.extend_from_slice(&w.to_le_bytes());
        }
        while let Some(x) = ret.last() {
            if *x == 0 {
                ret.pop();
            } else {
                break;
            }
        }
        ret
    }

    fn trim(&mut self) {
        while let Some(x) = self.words.last() {
            if *x == 0 {
                self.words.pop();
            } else {
                break;
            }
        }
    }

    pub fn get(&self, i: u32) -> bool {
        let w = (i / 64) as usize;
        match self.words.get(w) {
            Some(x) => 0 != (*x >> (i % 64)) & 1,
            None => false,
        }
    }

    pub fn set(&mut self, i: u32) {
        let w = (i / 64) as usize;
        if self.words.len() <= w {
            self.words.resize(w + 1, 0);
        }
        self.words[w] |= 1u64 << (i % 64);
    }

    pub fn clear(&mut self, i: u32) {
        let w = (i / 64) as usize;
        if let Some(x) = self.words.get_mut(w) {
            *x &= !(1u64 << (i % 64));
        }
        self.trim();
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|x| *x == 0)
    }

    pub fn count_ones(&self) -> u32 {
        self.words.iter().map(|x| x.count_ones()).sum()
    }

    pub fn iter_ones(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(i, w)| {
            (0..64u32).filter_map(move |k| {
                if 0 != (*w >> k) & 1 {
                    Some(64 * i as u32 + k)
                } else {
                    None
                }
            })
        })
    }

    pub fn union_with(&mut self, o: &Self) {
        if self.words.len() < o.words.len() {
            self.words.resize(o.words.len(), 0);
        }
        for (i, x) in o.words.iter().enumerate() {
            self.words[i] |= *x;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum StatusKind {
    Ok,
    Warning,
    Error,
    Fatal,
}

impl StatusKind {
    pub fn from_u8(x: u8) -> Self {
        match x {
            0 => StatusKind::Ok,
            1 => StatusKind::Warning,
            2 => StatusKind::Error,
            _ => StatusKind::Fatal,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            StatusKind::Ok => 0,
            StatusKind::Warning => 1,
            StatusKind::Error => 2,
            StatusKind::Fatal => 3,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Status {
    pub kind: StatusKind,
    pub message: String,
    pub call_tree: String,
}

impl Status {
    pub fn ok() -> Self {
        Self {
            kind: StatusKind::Ok,
            message: String::new(),
            call_tree: String::new(),
        }
    }

    pub fn is_ok_empty(&self) -> bool {
        self.kind == StatusKind::Ok && self.message.is_empty() && self.call_tree.is_empty()
    }

    pub fn has_data(&self) -> bool {
        matches!(self.kind, StatusKind::Ok | StatusKind::Warning)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn wr(f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf, Endian::Little);
        f(&mut w);
        buf
    }

    #[test]
    fn size_roundtrip() {
        for n in [0usize, 1, 100, 253, 254, 255, 256, 70000, 1 << 24] {
            let b = wr(|w| w.size(n));
            let mut r = Reader::new(&b, Endian::Little);
            assert_eq!(r.size().unwrap(), Some(n));
            assert_eq!(r.remaining(), 0);
        }
        assert_eq!(wr(|w| w.size(253)), vec![253]);
        assert_eq!(wr(|w| w.size(254)), vec![0xfe, 254, 0, 0, 0]);
        assert_eq!(wr(|w| w.size_null()), vec![0xff]);
        let b = [0xffu8];
        assert_eq!(Reader::new(&b, Endian::Little).size().unwrap(), None);
    }

    #[test]
    fn string_roundtrip() {
        for s in ["", "value", "timeStamp.secondsPastEpoch", "µ°ä"] {
            let b = wr(|w| w.string(s));
            let mut r = Reader::new(&b, Endian::Little);
            assert_eq!(r.string().unwrap(), s);
            assert_eq!(r.remaining(), 0);
        }
        let b = [0xffu8];
        assert_eq!(Reader::new(&b, Endian::Little).string_opt().unwrap(), None);
    }

    #[test]
    fn bitset_spec_vectors() {
        let cases: &[(&[u32], &[u8])] = &[
            (&[], &[0x00]),
            (&[0], &[0x01, 0x01]),
            (&[7], &[0x01, 0x80]),
            (&[8], &[0x02, 0x00, 0x01]),
        ];
        for (bits, bytes) in cases {
            let mut bs = BitSet::new();
            for b in *bits {
                bs.set(*b);
            }
            assert_eq!(wr(|w| w.bitset(&bs)), bytes.to_vec());
            let mut r = Reader::new(bytes, Endian::Little);
            let back = r.bitset().unwrap();
            assert_eq!(back, bs);
            assert_eq!(back.iter_ones().collect::<Vec<_>>(), bits.to_vec());
        }
    }

    #[test]
    fn bitset_ops() {
        let mut a = BitSet::new();
        a.set(0);
        a.set(63);
        a.set(64);
        a.set(200);
        assert!(a.get(0) && a.get(63) && a.get(64) && a.get(200));
        assert!(!a.get(1) && !a.get(199));
        assert_eq!(a.count_ones(), 4);
        a.clear(200);
        assert!(!a.get(200));
        assert_eq!(a.iter_ones().collect::<Vec<_>>(), vec![0, 63, 64]);
        assert!(!a.is_empty());
        let mut b = BitSet::new();
        b.set(5);
        a.union_with(&b);
        assert_eq!(a.iter_ones().collect::<Vec<_>>(), vec![0, 5, 63, 64]);
    }

    #[test]
    fn endian_reads() {
        let b = [0x01u8, 0x02, 0x03, 0x04];
        assert_eq!(Reader::new(&b, Endian::Little).u32().unwrap(), 0x04030201);
        assert_eq!(Reader::new(&b, Endian::Big).u32().unwrap(), 0x01020304);
    }

    #[test]
    fn status_roundtrip() {
        let b = wr(|w| w.status(&Status::ok()));
        assert_eq!(b, vec![0xff]);
        assert_eq!(Reader::new(&b, Endian::Little).status().unwrap(), Status::ok());
        let st = Status {
            kind: StatusKind::Error,
            message: "no such channel".into(),
            call_tree: String::new(),
        };
        let b = wr(|w| w.status(&st));
        assert_eq!(Reader::new(&b, Endian::Little).status().unwrap(), st);
    }

    #[test]
    fn short_input_errors() {
        let b = [0x01u8];
        assert!(Reader::new(&b, Endian::Little).u32().is_err());
        let b = [0x05u8, b'a'];
        assert!(Reader::new(&b, Endian::Little).string().is_err());
    }
}
