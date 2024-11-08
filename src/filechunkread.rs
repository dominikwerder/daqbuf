use bytes::BytesMut;
use std::fmt;
use std::time::Duration;

pub struct FileChunkRead {
    buf: BytesMut,
    duration: Duration,
}

impl FileChunkRead {
    pub fn with_buf(buf: BytesMut) -> Self {
        Self {
            buf,
            duration: Duration::from_millis(0),
        }
    }

    pub fn with_buf_dur(buf: BytesMut, duration: Duration) -> Self {
        Self { buf, duration }
    }

    pub fn into_buf(self) -> BytesMut {
        self.buf
    }

    pub fn buf(&self) -> &BytesMut {
        &self.buf
    }

    pub fn buf_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    pub fn buf_take(&mut self) -> BytesMut {
        core::mem::replace(&mut self.buf, BytesMut::new())
    }

    pub fn duration(&self) -> &Duration {
        &self.duration
    }

    pub fn duration_mut(&mut self) -> &mut Duration {
        &mut self.duration
    }
}

impl fmt::Debug for FileChunkRead {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FileChunkRead")
            .field("buf.len", &self.buf.len())
            .field("buf.cap", &self.buf.capacity())
            .field("duration", &self.duration)
            .finish()
    }
}
