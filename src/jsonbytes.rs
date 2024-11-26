use bytes::Bytes;
use items_0::WithLen;

pub struct JsonBytes(String);

impl JsonBytes {
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn len(&self) -> u32 {
        self.0.len() as _
    }
}

impl WithLen for JsonBytes {
    fn len(&self) -> usize {
        self.len() as usize
    }
}

impl From<JsonBytes> for String {
    fn from(value: JsonBytes) -> Self {
        value.0
    }
}

pub struct CborBytes(Bytes);

impl CborBytes {
    pub fn new<T: Into<Bytes>>(k: T) -> Self {
        Self(k.into())
    }

    pub fn into_inner(self) -> Bytes {
        self.0
    }

    pub fn len(&self) -> u32 {
        self.0.len() as _
    }
}

impl WithLen for CborBytes {
    fn len(&self) -> usize {
        self.len() as usize
    }
}

impl From<CborBytes> for Bytes {
    fn from(value: CborBytes) -> Self {
        value.0
    }
}
