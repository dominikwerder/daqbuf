use arrayref::array_ref;
use daqbuf_err as err;
use err::Error;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::TsNano;
use std::mem::size_of;
use taskrun::tokio;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::SeekFrom;

pub fn find_ge(range: NanoRange, expand_right: bool, buf: &[u8]) -> Result<Option<(u64, u64)>, Error> {
    type VT = u64;
    const NT: usize = size_of::<VT>();
    const N: usize = 2 * NT;
    let n1 = buf.len();
    if n1 % N != 0 {
        return Err(Error::with_msg(format!("find_ge  bad len {}", n1)));
    }
    if n1 == 0 {
        warn!("Empty index data");
        return Ok(None);
    }
    let n1 = n1 / N;
    let a = unsafe {
        let ptr = &buf[0] as *const u8 as *const ([u8; NT], [u8; NT]);
        std::slice::from_raw_parts(ptr, n1)
    };
    let mut j = 0;
    let mut k = n1 - 1;
    let x = VT::from_be_bytes(a[j].0);
    let y = VT::from_be_bytes(a[k].0);
    if y < range.beg {
        return Ok(None);
    }
    if x >= range.beg {
        if x < range.end || expand_right {
            return Ok(Some((x, VT::from_be_bytes(a[j].1))));
        } else {
            return Ok(None);
        }
    }
    if x >= y {
        return Err(Error::with_public_msg(format!(
            "search in unordered data  ts1 {x}  ts2 {y}"
        )));
    }
    let mut x = x;
    let mut y = y;
    loop {
        if x >= y {
            return Err(Error::with_public_msg(format!(
                "search (loop) in unordered data  ts1 {x}  ts2 {y}"
            )));
        }
        if k - j < 2 {
            if y < range.end || expand_right {
                let ret = (y, VT::from_be_bytes(a[k].1));
                return Ok(Some(ret));
            } else {
                return Ok(None);
            }
        }
        let m = (k + j) / 2;
        let e = VT::from_be_bytes(a[m].0);
        if e < range.beg {
            j = m;
            x = e;
        } else {
            k = m;
            y = e;
        }
    }
}

pub fn find_largest_smaller_than(
    range: NanoRange,
    _expand_right: bool,
    buf: &[u8],
) -> Result<Option<(u64, u64)>, Error> {
    type NUM = u64;
    const ELESIZE: usize = size_of::<NUM>();
    const N: usize = 2 * ELESIZE;
    let n1 = buf.len();
    if n1 % N != 0 {
        return Err(Error::with_msg(format!("find_ge  bad len {}", n1)));
    }
    if n1 == 0 {
        warn!("Empty index data");
        return Ok(None);
    }
    let n1 = n1 / N;
    let a = unsafe {
        let ptr = &buf[0] as *const u8 as *const ([u8; ELESIZE], [u8; ELESIZE]);
        std::slice::from_raw_parts(ptr, n1)
    };
    let mut j = 0;
    let mut k = n1 - 1;
    let x = NUM::from_be_bytes(a[j].0);
    let y = NUM::from_be_bytes(a[k].0);
    if x >= range.beg {
        return Ok(None);
    }
    if y < range.beg {
        let ret = (y, NUM::from_be_bytes(a[k].1));
        return Ok(Some(ret));
    }
    if x >= y {
        return Err(Error::with_public_msg(format!(
            "search in unordered data  ts1 {x}  ts2 {y}"
        )));
    }
    let mut x = x;
    let mut y = y;
    loop {
        if x >= y {
            return Err(Error::with_public_msg(format!(
                "search (loop) in unordered data  ts1 {x}  ts2 {y}"
            )));
        }
        if k - j < 2 {
            let ret = (x, NUM::from_be_bytes(a[j].1));
            return Ok(Some(ret));
        }
        let m = (k + j) / 2;
        let e = NUM::from_be_bytes(a[m].0);
        if e < range.beg {
            j = m;
            x = e;
        } else {
            k = m;
            y = e;
        }
    }
}

async fn read(buf: &mut [u8], file: &mut File) -> Result<usize, Error> {
    let mut wp = 0;
    loop {
        let n1 = file.read(&mut buf[wp..]).await?;
        if n1 == 0 {
            break;
        } else {
            wp += n1;
        }
        if wp >= buf.len() {
            break;
        }
    }
    Ok(wp)
}

pub fn parse_channel_header(buf: &[u8]) -> Result<(u32,), Error> {
    if buf.len() < 6 {
        return Err(Error::with_msg(format!("parse_channel_header  buf len: {}", buf.len())));
    }
    let ver = i16::from_be_bytes(*array_ref![buf, 0, 2]);
    if ver != 0 {
        return Err(Error::with_msg(format!("unknown file version: {}", ver)));
    }
    let len1 = u32::from_be_bytes(*array_ref![buf, 2, 4]);
    if len1 < 9 || len1 > 256 {
        return Err(Error::with_msg(format!("unexpected data file header len1: {}", len1)));
    }
    if buf.len() < 2 + len1 as usize {
        return Err(Error::with_msg(format!(
            "data file header not contained in buffer len1: {} vs {}",
            len1,
            buf.len()
        )));
    }
    let len2 = u32::from_be_bytes(*array_ref![buf, 2 + len1 as usize - 4, 4]);
    if len1 != len2 {
        return Err(Error::with_msg(format!("len mismatch  len1: {}  len2: {}", len1, len2)));
    }
    Ok((len1 as u32,))
}

pub fn parse_event(buf: &[u8]) -> Result<(u32, TsNano), Error> {
    if buf.len() < 4 {
        return Err(Error::with_msg(format!("parse_event  buf len: {}", buf.len())));
    }
    let len1 = u32::from_be_bytes(*array_ref![buf, 0, 4]);
    if len1 < 9 || len1 > 512 {
        return Err(Error::with_msg(format!("unexpected event len1: {}", len1)));
    }
    if buf.len() < len1 as usize {
        return Err(Error::with_msg(format!(
            "event not contained in buffer len1: {} vs {}",
            len1,
            buf.len()
        )));
    }
    let len2 = u32::from_be_bytes(*array_ref![buf, len1 as usize - 4, 4]);
    if len1 != len2 {
        return Err(Error::with_msg(format!("len mismatch  len1: {}  len2: {}", len1, len2)));
    }
    let ts = u64::from_be_bytes(*array_ref![buf, 12, 8]);
    Ok((len1 as u32, TsNano::from_ns(ts)))
}

pub async fn read_event_at(pos: u64, file: &mut File) -> Result<(u32, TsNano), Error> {
    file.seek(SeekFrom::Start(pos)).await?;
    let mut buf = vec![0; 1024];
    let _n1 = read(&mut buf, file).await?;
    let ev = parse_event(&buf)?;
    Ok(ev)
}

pub async fn position_static_len_datafile(
    mut file: File,
    range: NanoRange,
    expand_right: bool,
) -> Result<(File, bool, u32, u64), Error> {
    let flen = file.seek(SeekFrom::End(0)).await?;
    file.seek(SeekFrom::Start(0)).await?;
    let mut buf = vec![0; 1024];
    let _n1 = read(&mut buf, &mut file).await?;
    let hres = parse_channel_header(&buf)?;
    let headoff = 2 + hres.0 as u64;
    let ev = parse_event(&buf[headoff as usize..])?;
    let evlen = ev.0 as u64;
    let mut j = headoff;
    let mut k = ((flen - headoff) / evlen - 1) * evlen + headoff;
    let x = ev.1.ns();
    let t = read_event_at(k, &mut file).await?;
    if t.0 != evlen as u32 {
        Err(Error::with_msg(format!(
            "inconsistent event lengths:  {}  vs  {}",
            t.0, evlen
        )))?;
    }
    let y = t.1.ns();
    let mut nreads = 2;
    if x >= range.end {
        if expand_right {
            file.seek(SeekFrom::Start(j)).await?;
            return Ok((file, true, nreads, j));
        } else {
            file.seek(SeekFrom::Start(0)).await?;
            return Ok((file, false, nreads, 0));
        }
    }
    if y < range.beg {
        file.seek(SeekFrom::Start(j)).await?;
        return Ok((file, false, nreads, j));
    }
    if x >= range.beg {
        if x < range.end || expand_right {
            file.seek(SeekFrom::Start(j)).await?;
            return Ok((file, true, nreads, j));
        } else {
            file.seek(SeekFrom::Start(0)).await?;
            return Ok((file, false, nreads, 0));
        }
    }
    let mut x = x;
    let mut y = y;
    loop {
        assert!(x < y);
        assert_eq!((k - j) % evlen, 0);
        if k - j < 2 * evlen {
            if y < range.end || expand_right {
                file.seek(SeekFrom::Start(k)).await?;
                return Ok((file, true, nreads, k));
            } else {
                file.seek(SeekFrom::Start(0)).await?;
                return Ok((file, false, nreads, 0));
            }
        }
        let m = j + (k - j) / 2 / evlen * evlen;
        let t = read_event_at(m, &mut file).await?;
        if t.0 != evlen as u32 {
            Err(Error::with_msg(format!(
                "inconsistent event lengths:  {}  vs  {}",
                t.0, evlen
            )))?;
        }
        nreads += 1;
        let e = t.1.ns();
        if e < range.beg {
            x = e;
            j = m;
        } else {
            y = e;
            k = m;
        }
    }
}

pub async fn position_static_len_datafile_at_largest_smaller_than(
    mut file: File,
    range: NanoRange,
    _expand_right: bool,
) -> Result<(File, bool, u32, u64), Error> {
    let flen = file.seek(SeekFrom::End(0)).await?;
    file.seek(SeekFrom::Start(0)).await?;
    let mut buf = vec![0; 1024];
    let _n1 = read(&mut buf, &mut file).await?;
    let hres = parse_channel_header(&buf)?;
    let headoff = 2 + hres.0 as u64;
    let ev = parse_event(&buf[headoff as usize..])?;
    let evlen = ev.0 as u64;
    let mut j = headoff;
    let mut k = ((flen - headoff) / evlen - 1) * evlen + headoff;
    let x = ev.1.ns();
    let t = read_event_at(k, &mut file).await?;
    if t.0 != evlen as u32 {
        Err(Error::with_msg(format!(
            "inconsistent event lengths:  {}  vs  {}",
            t.0, evlen
        )))?;
    }
    let y = t.1.ns();
    let mut nreads = 2;
    if x >= range.beg {
        file.seek(SeekFrom::Start(j)).await?;
        return Ok((file, false, nreads, j));
    }
    if y < range.beg {
        file.seek(SeekFrom::Start(k)).await?;
        return Ok((file, true, nreads, k));
    }
    loop {
        if k - j < 2 * evlen {
            file.seek(SeekFrom::Start(j)).await?;
            return Ok((file, true, nreads, j));
        }
        let m = j + (k - j) / 2 / evlen * evlen;
        let t = read_event_at(m, &mut file).await?;
        if t.0 != evlen as u32 {
            Err(Error::with_msg(format!(
                "inconsistent event lengths:  {}  vs  {}",
                t.0, evlen
            )))?;
        }
        nreads += 1;
        let x = t.1.ns();
        if x < range.beg {
            j = m;
        } else {
            k = m;
        }
    }
}
