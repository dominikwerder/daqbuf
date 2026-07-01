#[cfg(test)]
mod api1;
#[cfg(test)]
mod api4;
pub mod archapp;
pub mod binnedjson;
#[cfg(test)]
mod timeweightedjson;

use bytes::BytesMut;

#[test]
fn bufs() {
    use bytes::Buf;
    use bytes::BufMut;
    let mut buf = BytesMut::with_capacity(1024);
    assert!(buf.as_mut().len() == 0);
    buf.put_u32_le(123);
    assert!(buf.as_mut().len() == 4);
    let mut b2 = buf.split_to(4);
    assert!(b2.capacity() == 4);
    b2.advance(2);
    assert!(b2.capacity() == 2);
    b2.advance(2);
    assert!(b2.capacity() == 0);
    assert!(buf.capacity() == 1020);
    assert!(buf.remaining() == 0);
    assert!(buf.remaining_mut() >= 1020);
    assert!(buf.capacity() == 1020);
}
