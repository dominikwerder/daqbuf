use std::io::Write;

pub fn log(s: &str) {
    let mut fout = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("tmp-log-out.txt")
        .unwrap();
    let mut buf = s.as_bytes().to_vec();
    buf.extend_from_slice(b"\n");
    fout.write(&buf).unwrap();
}
