/// Input may also contain whitespace.
pub fn decode_hex<INP: AsRef<str>>(inp: INP) -> Result<Vec<u8>, ()> {
    let a: Vec<_> = inp
        .as_ref()
        .bytes()
        .filter(|&x| (x >= b'0' && x <= b'9') || (x >= b'a' && x <= b'f'))
        .collect();
    let ret = hex::decode(a).map_err(|_| ())?;
    Ok(ret)
}
