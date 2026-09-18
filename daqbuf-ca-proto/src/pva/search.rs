use super::Error;
use super::field::IntroRegistry;
use super::pvdata::Endian;
use super::pvdata::Reader;
use super::pvdata::Writer;
use super::value::PvaValue;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;

pub const SEARCH_FLAG_REPLY_REQUIRED: u8 = 0x01;
pub const SEARCH_FLAG_UNICAST: u8 = 0x80;

pub const PVA_SEARCH_CHANNELS_MAX: usize = 1024;
pub const PVA_PROTOCOLS_MAX: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchChannel {
    pub search_instance_id: u32,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchReq {
    pub sequence_id: u32,
    pub flags: u8,
    pub response_addr: [u8; 16],
    pub response_port: u16,
    pub protocols: Vec<String>,
    pub channels: Vec<SearchChannel>,
}

impl SearchReq {
    pub fn response_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(addr_from_bytes(&self.response_addr), self.response_port)
    }

    pub fn parse(r: &mut Reader) -> Result<Self, Error> {
        let sequence_id = r.u32()?;
        let flags = r.u8()?;
        r.take(3)?;
        let mut response_addr = [0; 16];
        response_addr.copy_from_slice(r.take(16)?);
        let response_port = r.u16()?;
        let np = r.size_req()?;
        if np > PVA_PROTOCOLS_MAX {
            return Err(Error::ArrayTooLong(np));
        }
        let mut protocols = Vec::with_capacity(np);
        for _ in 0..np {
            protocols.push(r.string()?);
        }
        let nc = r.u16()? as usize;
        if nc > PVA_SEARCH_CHANNELS_MAX {
            return Err(Error::ArrayTooLong(nc));
        }
        let mut channels = Vec::with_capacity(nc);
        for _ in 0..nc {
            let search_instance_id = r.u32()?;
            let name = r.string()?;
            channels.push(SearchChannel {
                search_instance_id,
                name,
            });
        }
        Ok(Self {
            sequence_id,
            flags,
            response_addr,
            response_port,
            protocols,
            channels,
        })
    }

    pub fn write(&self, w: &mut Writer) {
        w.u32(self.sequence_id);
        w.u8(self.flags);
        w.raw(&[0, 0, 0]);
        w.raw(&self.response_addr);
        w.u16(self.response_port);
        w.size(self.protocols.len());
        for p in &self.protocols {
            w.string(p);
        }
        w.u16(self.channels.len() as u16);
        for c in &self.channels {
            w.u32(c.search_instance_id);
            w.string(&c.name);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchRes {
    pub guid: [u8; 12],
    pub sequence_id: u32,
    pub server_addr: [u8; 16],
    pub server_port: u16,
    pub protocol: String,
    pub found: bool,
    pub search_instance_ids: Vec<u32>,
}

impl SearchRes {
    pub fn server_addr_is_unspecified(&self) -> bool {
        self.server_addr.iter().all(|x| *x == 0)
    }

    pub fn server_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(addr_from_bytes(&self.server_addr), self.server_port)
    }

    pub fn parse(r: &mut Reader) -> Result<Self, Error> {
        let mut guid = [0; 12];
        guid.copy_from_slice(r.take(12)?);
        let sequence_id = r.u32()?;
        let mut server_addr = [0; 16];
        server_addr.copy_from_slice(r.take(16)?);
        let server_port = r.u16()?;
        let protocol = r.string()?;
        let found = r.boolean()?;
        let n = r.u16()? as usize;
        if n > PVA_SEARCH_CHANNELS_MAX {
            return Err(Error::ArrayTooLong(n));
        }
        let mut search_instance_ids = Vec::with_capacity(n);
        for _ in 0..n {
            search_instance_ids.push(r.u32()?);
        }
        Ok(Self {
            guid,
            sequence_id,
            server_addr,
            server_port,
            protocol,
            found,
            search_instance_ids,
        })
    }

    pub fn write(&self, w: &mut Writer) {
        w.raw(&self.guid);
        w.u32(self.sequence_id);
        w.raw(&self.server_addr);
        w.u16(self.server_port);
        w.string(&self.protocol);
        w.boolean(self.found);
        w.u16(self.search_instance_ids.len() as u16);
        for x in &self.search_instance_ids {
            w.u32(*x);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Beacon {
    pub guid: [u8; 12],
    pub flags: u8,
    pub beacon_sequence_id: u8,
    pub change_count: u16,
    pub server_addr: [u8; 16],
    pub server_port: u16,
    pub protocol: String,
    pub server_status: Option<PvaValue>,
}

impl Beacon {
    pub fn server_socket_addr(&self) -> SocketAddr {
        SocketAddr::new(addr_from_bytes(&self.server_addr), self.server_port)
    }

    pub fn parse(r: &mut Reader, reg: &mut IntroRegistry, array_truncate: usize) -> Result<Self, Error> {
        let mut guid = [0; 12];
        guid.copy_from_slice(r.take(12)?);
        let flags = r.u8()?;
        let beacon_sequence_id = r.u8()?;
        let change_count = r.u16()?;
        let mut server_addr = [0; 16];
        server_addr.copy_from_slice(r.take(16)?);
        let server_port = r.u16()?;
        let protocol = r.string()?;
        let server_status = match reg.parse_field(r)? {
            None => None,
            Some(f) => Some(PvaValue::decode(&f, r, reg, array_truncate)?),
        };
        Ok(Self {
            guid,
            flags,
            beacon_sequence_id,
            change_count,
            server_addr,
            server_port,
            protocol,
            server_status,
        })
    }

    pub fn write(&self, w: &mut Writer) {
        w.raw(&self.guid);
        w.u8(self.flags);
        w.u8(self.beacon_sequence_id);
        w.u16(self.change_count);
        w.raw(&self.server_addr);
        w.u16(self.server_port);
        w.string(&self.protocol);
        w.size_null();
    }
}

pub fn addr_to_bytes(addr: &IpAddr) -> [u8; 16] {
    let v6 = match addr {
        IpAddr::V4(x) => x.to_ipv6_mapped(),
        IpAddr::V6(x) => *x,
    };
    v6.octets()
}

pub fn addr_from_bytes(b: &[u8; 16]) -> IpAddr {
    let v6 = Ipv6Addr::from(*b);
    match v6.to_ipv4_mapped() {
        Some(x) => IpAddr::V4(x),
        None => IpAddr::V6(v6),
    }
}

pub fn unspecified_response_addr() -> [u8; 16] {
    addr_to_bytes(&IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

pub fn search_req_for(sequence_id: u32, response_port: u16, channels: Vec<SearchChannel>) -> SearchReq {
    SearchReq {
        sequence_id,
        flags: SEARCH_FLAG_REPLY_REQUIRED | SEARCH_FLAG_UNICAST,
        response_addr: unspecified_response_addr(),
        response_port,
        protocols: vec!["tcp".into()],
        channels,
    }
}

pub fn encode_udp(msgs: &[super::proto::PvaMsgTy]) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    for ty in msgs {
        super::proto::write_message(&mut buf, ty, Endian::Little)?;
    }
    Ok(buf)
}

pub fn decode_udp(
    b: &[u8],
    reg: &mut IntroRegistry,
    array_truncate: usize,
) -> Result<Vec<super::proto::PvaMsgTy>, Error> {
    super::proto::decode_datagram(b, reg, array_truncate)
}

#[cfg(test)]
mod test {
    use super::*;

    fn wr(f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut buf = Vec::new();
        f(&mut Writer::new(&mut buf, Endian::Little));
        buf
    }

    #[test]
    fn search_req_roundtrip() {
        let req = search_req_for(
            7,
            5076,
            vec![
                SearchChannel {
                    search_instance_id: 11,
                    name: "SOME:PV:1".into(),
                },
                SearchChannel {
                    search_instance_id: 12,
                    name: "SOME:PV:2".into(),
                },
            ],
        );
        let b = wr(|w| req.write(w));
        assert_eq!(&b[0..4], &[7, 0, 0, 0]);
        assert_eq!(b[4], SEARCH_FLAG_REPLY_REQUIRED | SEARCH_FLAG_UNICAST);
        assert_eq!(&b[24..26], &[0xd4, 0x13]);
        assert_eq!(b[26], 1);
        assert_eq!(&b[27..31], &[3, b't', b'c', b'p']);
        assert_eq!(&b[31..33], &[2, 0]);
        let mut r = Reader::new(&b, Endian::Little);
        let back = SearchReq::parse(&mut r).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(back, req);
    }

    #[test]
    fn search_res_roundtrip() {
        let res = SearchRes {
            guid: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            sequence_id: 7,
            server_addr: addr_to_bytes(&IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3))),
            server_port: 5075,
            protocol: "tcp".into(),
            found: true,
            search_instance_ids: vec![11, 12],
        };
        let b = wr(|w| res.write(w));
        assert_eq!(&b[12..16], &[7, 0, 0, 0]);
        assert_eq!(&b[32..34], &[0xd3, 0x13]);
        assert_eq!(b[38], 1);
        assert_eq!(&b[39..41], &[2, 0]);
        let mut r = Reader::new(&b, Endian::Little);
        let back = SearchRes::parse(&mut r).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(back, res);
        assert_eq!(
            back.server_socket_addr(),
            "10.0.0.3:5075".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn beacon_roundtrip() {
        let bc = Beacon {
            guid: [9; 12],
            flags: 0,
            beacon_sequence_id: 3,
            change_count: 4,
            server_addr: addr_to_bytes(&IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            server_port: 5075,
            protocol: "tcp".into(),
            server_status: None,
        };
        let b = wr(|w| bc.write(w));
        let mut r = Reader::new(&b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let back = Beacon::parse(&mut r, &mut reg, usize::MAX).unwrap();
        assert_eq!(r.remaining(), 0);
        assert_eq!(back, bc);
    }

    #[test]
    fn addr_mapping() {
        let a = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 7));
        let b = addr_to_bytes(&a);
        assert_eq!(&b[0..12], &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff]);
        assert_eq!(addr_from_bytes(&b), a);
    }
}
