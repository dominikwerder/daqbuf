use io::{Cursor, Write};
use std::fs::OpenOptions;
use std::io;
use std::io::SeekFrom;

#[derive(Debug)]
struct Error {}

impl From<io::Error> for Error {
    fn from(_k: io::Error) -> Self {
        Self {}
    }
}

pub struct Out {
    cur: io::Cursor<Vec<u8>>,
}

impl Out {
    pub fn new() -> Self {
        Self {
            cur: Cursor::new(vec![]),
        }
    }

    pub fn write_u8(&mut self, k: u8) -> io::Result<usize> {
        self.write(&k.to_le_bytes())
    }

    pub fn write_u16(&mut self, k: u16) -> io::Result<usize> {
        self.write(&k.to_le_bytes())
    }

    pub fn write_u32(&mut self, k: u32) -> io::Result<usize> {
        self.write(&k.to_le_bytes())
    }

    pub fn write_u64(&mut self, k: u64) -> io::Result<usize> {
        self.write(&k.to_le_bytes())
    }
}

impl io::Seek for Out {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.cur.seek(pos)
    }
}

impl io::Write for Out {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.cur.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.cur.flush()
    }
}

// TODO
#[allow(unused)]
//#[test]
fn emit() {
    write_h5().unwrap();
}

#[allow(unused)]
fn write_h5() -> Result<(), Error> {
    let mut out = Out::new();
    write_superblock(&mut out)?;
    write_local_heap(&mut out)?;
    write_root_object_header(&mut out)?;
    write_file(&out)?;
    let mut child = std::process::Command::new("h5debug").arg("f.h5").spawn()?;
    child.wait()?;
    Ok(())
}

#[allow(unused)]
fn write_file(out: &Out) -> Result<(), Error> {
    eprintln!("Write {} bytes", out.cur.get_ref().len());
    let mut f = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open("f.h5")?;
    f.write_all(out.cur.get_ref())?;
    Ok(())
}

#[allow(unused)]
fn write_padding(out: &mut Out) -> Result<(), Error> {
    let n = out.cur.get_ref().len();
    let m = n % 8;
    if m != 0 {
        for _ in 0..(8 - m) {
            out.write_u8(0)?;
        }
    }
    Ok(())
}

#[allow(unused)]
fn write_superblock(out: &mut Out) -> Result<(), Error> {
    let super_ver = 0;
    let free_ver = 0;
    let root_group_ver = 0;
    let shared_header_ver = 0;
    let group_leaf_k = 4;
    let group_int_k = 16;
    let base_addr = 0;
    let free_index_addr = u64::MAX;
    let eof = 4242;
    write_padding(out)?;
    out.write_all(b"\x89HDF\r\n\x1a\n")?;
    out.write_u8(super_ver)?;
    out.write_u8(free_ver)?;
    out.write_u8(root_group_ver)?;
    out.write_u8(0)?;
    out.write_u8(shared_header_ver)?;
    out.write_u8(8)?;
    out.write_u8(8)?;
    out.write_u8(0)?;
    out.write_u16(group_leaf_k)?;
    out.write_u16(group_int_k)?;
    let consistency = 0;
    out.write_u32(consistency)?;
    out.write_u64(base_addr)?;
    out.write_u64(free_index_addr)?;
    out.write_u64(eof)?;
    let driver = u64::MAX;
    out.write_u64(driver)?;
    // root sym tab entry:
    {
        let link_name_off = 0;
        let obj_header_addr = 1152;
        let cache_type = 1;
        out.write_u64(link_name_off)?;
        out.write_u64(obj_header_addr)?;
        out.write_u32(cache_type)?;
        // reserved:
        out.write_u32(0)?;
        // scratch pad 16 bytes:
        out.write_u64(0)?;
        out.write_u64(0)?;
    }
    Ok(())
}

#[allow(unused)]
fn write_root_object_header(out: &mut Out) -> Result<(), Error> {
    write_padding(out)?;
    let pos0 = out.cur.get_ref().len() as u64;
    eprintln!("write_root_object_header  start at pos0 {}", pos0);
    let ver = 1;
    let nmsg = 1;
    let hard_link_count = 1;
    let header_msgs_data_len = 0;
    out.write_u8(ver)?;
    out.write_u8(0)?;
    out.write_u16(nmsg)?;
    out.write_u32(hard_link_count)?;
    out.write_u32(header_msgs_data_len)?;
    out.write_u32(0)?;
    {
        // Group Info
        let msg_type = 0xa;
        let msg_len = 128;
        let flags = 0;
        out.write_u32(msg_type)?;
        out.write_u32(msg_len)?;
        out.write_u8(flags)?;
        out.write_u8(0)?;
        out.write_u8(0)?;
        out.write_u8(0)?;
    }
    Ok(())
}

#[allow(unused)]
fn write_local_heap(out: &mut Out) -> Result<(), Error> {
    write_padding(out)?;
    let pos0 = out.cur.get_ref().len() as u64;
    eprintln!("write_local_heap  start at pos0 {}", pos0);
    let ver = 0;
    let seg_size = 1024;
    let free_list_off = u64::MAX;
    let seg_addr = pos0 + 32;
    out.write_all(b"HEAP")?;
    out.write_u8(ver)?;
    out.write_u8(0)?;
    out.write_u8(0)?;
    out.write_u8(0)?;
    out.write_u64(seg_size)?;
    out.write_u64(free_list_off)?;
    out.write_u64(seg_addr)?;
    out.write_all(&[0; 1024])?;
    {
        let h = out.cur.position();
        out.cur.set_position(h - 1024);
        out.write_all(b"somename")?;
        out.cur.set_position(h);
    }
    Ok(())
}
