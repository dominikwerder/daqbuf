use crate::ChannelConfigExt;
use crate::SfDbChConf;
use bitshuffle::bitshuffle_compress;
use bytes::BufMut;
use bytes::BytesMut;
use daqbuf_err as err;
use err::Error;
use netpod::ByteOrder;
use netpod::DtNano;
use netpod::GenVar;
use netpod::Node;
use netpod::ScalarType;
use netpod::SfDatabuffer;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::TsNano;
use netpod::log::*;
use netpod::timeunits::*;
use std::path::Path;
use std::path::PathBuf;
use taskrun::tokio;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

const BACKEND: &str = "testbackend-00";

pub async fn gen_test_data() -> Result<(), Error> {
    let backend = String::from(BACKEND);
    let homedir = std::env::var("HOME").unwrap();
    let data_base_path = PathBuf::from(homedir).join("daqbuffer-testdata").join("databuffer");
    let ksprefix = String::from("ks");
    let mut ensemble = Ensemble {
        nodes: Vec::new(),
        channels: Vec::new(),
    };
    {
        let chn = ChannelGenProps {
            config: SfDbChConf {
                channel: SfDbChannel::from_name(&backend, "scalar-i32-be"),
                keyspace: 2,
                time_bin_size: DtNano::from_ns(DAY),
                scalar_type: ScalarType::I32,
                byte_order: ByteOrder::Big,
                shape: Shape::Scalar,
                array: false,
                compression: false,
            },
            gen_var: netpod::GenVar::Default,
            time_spacing: MS * 500,
        };
        ensemble.channels.push(chn);
        let chn = ChannelGenProps {
            config: SfDbChConf {
                channel: SfDbChannel::from_name(&backend, "wave-f64-be-n21"),
                keyspace: 3,
                time_bin_size: DtNano::from_ns(DAY),
                array: true,
                scalar_type: ScalarType::F64,
                shape: Shape::Wave(21),
                byte_order: ByteOrder::Big,
                compression: true,
            },
            gen_var: netpod::GenVar::Default,
            time_spacing: MS * 4000,
        };
        ensemble.channels.push(chn);
        let chn = ChannelGenProps {
            config: SfDbChConf {
                channel: SfDbChannel::from_name(&backend, "wave-u16-le-n77"),
                keyspace: 3,
                time_bin_size: DtNano::from_ns(DAY),
                scalar_type: ScalarType::U16,
                byte_order: ByteOrder::Little,
                shape: Shape::Wave(77),
                array: true,
                compression: true,
            },
            gen_var: netpod::GenVar::Default,
            time_spacing: MS * 500,
        };
        ensemble.channels.push(chn);
        let chn = ChannelGenProps {
            config: SfDbChConf {
                channel: SfDbChannel::from_name(&backend, "tw-scalar-i32-be"),
                keyspace: 2,
                time_bin_size: DtNano::from_ns(DAY),
                scalar_type: ScalarType::I32,
                byte_order: ByteOrder::Little,
                shape: Shape::Scalar,
                array: false,
                compression: false,
            },
            gen_var: netpod::GenVar::TimeWeight,
            time_spacing: MS * 500,
        };
        ensemble.channels.push(chn);
        let chn = ChannelGenProps {
            config: SfDbChConf {
                channel: SfDbChannel::from_name(&backend, "const-regular-scalar-i32-be"),
                keyspace: 2,
                time_bin_size: DtNano::from_ns(DAY),
                scalar_type: ScalarType::I32,
                byte_order: ByteOrder::Little,
                shape: Shape::Scalar,
                array: false,
                compression: false,
            },
            gen_var: netpod::GenVar::ConstRegular,
            time_spacing: MS * 500,
        };
        ensemble.channels.push(chn);
    }
    for i1 in 0..3 {
        let node = Node {
            host: "localhost".into(),
            listen: None,
            port: 7780 + i1 as u16,
            port_raw: 7780 + i1 as u16 + 100,
            sf_databuffer: Some(SfDatabuffer {
                data_base_path: data_base_path.join(format!("node{:02}", i1)),
                ksprefix: ksprefix.clone(),
                splits: None,
            }),
            archiver_appliance: None,
            channel_archiver: None,
            prometheus_api_bind: None,
        };
        ensemble.nodes.push(node);
    }
    for (split, node) in ensemble.nodes.iter().enumerate() {
        gen_node(split as u32, node, &ensemble).await?;
    }
    Ok(())
}

struct Ensemble {
    nodes: Vec<Node>,
    channels: Vec<ChannelGenProps>,
}

pub struct ChannelGenProps {
    config: SfDbChConf,
    time_spacing: u64,
    gen_var: GenVar,
}

async fn gen_node(split: u32, node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    for chn in &ensemble.channels {
        gen_channel(chn, split, node, ensemble).await?
    }
    Ok(())
}

async fn gen_channel(chn: &ChannelGenProps, split: u32, node: &Node, ensemble: &Ensemble) -> Result<(), Error> {
    let sfc = node.sf_databuffer.as_ref().unwrap();
    let config_path = sfc.data_base_path.join("config").join(chn.config.channel.name());
    let channel_path = sfc
        .data_base_path
        .join(format!("{}_{}", sfc.ksprefix, chn.config.keyspace))
        .join("byTime")
        .join(chn.config.channel.name());
    tokio::fs::create_dir_all(&channel_path).await?;
    gen_config(&config_path, &chn.config, node, ensemble)
        .await
        .map_err(|k| Error::with_msg(format!("can not generate config {:?}", k)))?;
    let mut evix = 0;
    let mut ts = TsNano::from_ns(0);
    let mut pulse = 0;
    while ts.ns() < DAY * 3 {
        let res = gen_timebin(
            evix,
            ts,
            pulse,
            chn.time_spacing,
            &channel_path,
            &chn.config,
            split,
            node,
            ensemble,
            &chn.gen_var,
        )
        .await?;
        evix = res.evix;
        ts = res.ts;
        pulse = res.pulse;
    }
    Ok(())
}

async fn gen_config(config_path: &Path, config: &SfDbChConf, _node: &Node, _ensemble: &Ensemble) -> Result<(), Error> {
    let path = config_path.join("latest");
    tokio::fs::create_dir_all(&path).await?;
    let path = path.join("00000_Config");
    info!("try to open  {:?}", path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    let mut buf = BytesMut::with_capacity(1024 * 1);
    let ver = 0;
    buf.put_i16(ver);
    let cnenc = config.channel.name().as_bytes();
    let len1 = cnenc.len() + 8;
    buf.put_i32(len1 as i32);
    buf.put(cnenc);
    buf.put_i32(len1 as i32);

    let ts = 0;
    let pulse = 0;
    let sc = 0;
    let status = 0;
    let bb = 0;
    let modulo = 0;
    let offset = 0;
    let precision = 0;
    let p1 = buf.len();
    buf.put_i32(0x20202020);
    buf.put_i64(ts);
    buf.put_i64(pulse);
    buf.put_u32(config.keyspace as u32);
    buf.put_u64(config.time_bin_size.ns() / MS);
    buf.put_i32(sc);
    buf.put_i32(status);
    buf.put_i8(bb);
    buf.put_i32(modulo);
    buf.put_i32(offset);
    buf.put_i16(precision);

    {
        // this len does not include itself and there seems to be no copy of it afterwards.
        let p3 = buf.len();
        buf.put_i32(404040);
        buf.put_u8(config.dtflags());
        buf.put_u8(config.scalar_type.index());
        if config.compression {
            let method = 0;
            buf.put_i8(method);
        }
        match config.shape {
            Shape::Scalar => {}
            Shape::Wave(k) => {
                buf.put_i8(1);
                buf.put_i32(k as i32);
            }
            Shape::Image(_, _) => {
                // TODO test data
                err::todoval()
            }
        }
        let len = buf.len() - p3 - 4;
        buf.as_mut()[p3..].as_mut().put_i32(len as i32);
    }

    // source name
    buf.put_i32(-1);
    // unit
    buf.put_i32(-1);
    // description
    buf.put_i32(-1);
    // optional fields
    buf.put_i32(-1);
    // value converter
    buf.put_i32(-1);

    let p2 = buf.len();
    let len = p2 - p1 + 4;
    buf.put_i32(len as i32);
    buf.as_mut()[p1..].as_mut().put_i32(len as i32);
    file.write_all(&buf).await?;
    Ok(())
}

struct CountedFile {
    file: File,
    bytes: u64,
}

impl CountedFile {
    pub fn new(file: File) -> Self {
        Self { file, bytes: 0 }
    }
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<u64, Error> {
        let l = buf.len();
        let mut i = 0;
        loop {
            match self.file.write(&buf[i..]).await {
                Ok(n) => {
                    i += n;
                    self.bytes += n as u64;
                    if i >= l {
                        break;
                    }
                }
                Err(e) => Err(e)?,
            }
        }
        Ok(i as u64)
    }
    pub fn written_len(&self) -> u64 {
        self.bytes
    }
}

struct GenTimebinRes {
    evix: u64,
    ts: TsNano,
    pulse: u64,
}

async fn gen_timebin(
    evix: u64,
    ts: TsNano,
    pulse: u64,
    ts_spacing: u64,
    channel_path: &Path,
    config: &SfDbChConf,
    split: u32,
    _node: &Node,
    ensemble: &Ensemble,
    gen_var: &GenVar,
) -> Result<GenTimebinRes, Error> {
    let tb = ts.ns() / config.time_bin_size.ns();
    let path = channel_path.join(format!("{:019}", tb)).join(format!("{:010}", split));
    tokio::fs::create_dir_all(&path).await?;
    let data_path = path.join(format!("{:019}_{:05}_Data", config.time_bin_size.ns() / MS, 0));
    let index_path = path.join(format!("{:019}_{:05}_Data_Index", config.time_bin_size.ns() / MS, 0));
    info!("open data file {:?}", data_path);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(data_path)
        .await?;
    let mut file = CountedFile::new(file);
    let mut index_file = if let Shape::Wave(_) = config.shape {
        info!("open index file {:?}", index_path);
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(index_path)
            .await?;
        let mut f = CountedFile::new(f);
        f.write_all(b"\x00\x00").await?;
        Some(f)
    } else {
        None
    };
    gen_datafile_header(&mut file, config).await?;
    let mut evix = evix;
    let mut ts = ts;
    let mut pulse = pulse;
    let tsmax = TsNano::from_ns((tb + 1) * config.time_bin_size.ns());
    while ts.ns() < tsmax.ns() {
        match gen_var {
            // TODO
            // Splits and nodes are not in 1-to-1 correspondence.
            GenVar::Default => {
                if evix % ensemble.nodes.len() as u64 == split as u64 {
                    gen_event(&mut file, index_file.as_mut(), evix, ts.clone(), pulse, config, gen_var).await?;
                }
            }
            GenVar::ConstRegular => {
                if evix % ensemble.nodes.len() as u64 == split as u64 {
                    gen_event(&mut file, index_file.as_mut(), evix, ts.clone(), pulse, config, gen_var).await?;
                }
            }
            GenVar::TimeWeight => {
                let m = evix % 20;
                if m == 0 || m == 1 {
                    if evix % ensemble.nodes.len() as u64 == split as u64 {
                        gen_event(&mut file, index_file.as_mut(), evix, ts.clone(), pulse, config, gen_var).await?;
                    }
                }
            }
        }
        evix += 1;
        ts = ts.add_ns(ts_spacing);
        pulse += 1;
    }
    let ret = GenTimebinRes { evix, ts, pulse };
    Ok(ret)
}

async fn gen_datafile_header(file: &mut CountedFile, config: &SfDbChConf) -> Result<(), Error> {
    let mut buf = BytesMut::with_capacity(1024);
    let cnenc = config.channel.name().as_bytes();
    let len1 = cnenc.len() + 8;
    buf.put_i16(0);
    buf.put_i32(len1 as i32);
    buf.put(cnenc);
    buf.put_i32(len1 as i32);
    file.write_all(&buf).await?;
    Ok(())
}

async fn gen_event(
    file: &mut CountedFile,
    index_file: Option<&mut CountedFile>,
    evix: u64,
    ts: TsNano,
    pulse: u64,
    config: &SfDbChConf,
    gen_var: &GenVar,
) -> Result<(), Error> {
    let ttl = 0xcafecafe;
    let ioc_ts = 0xcafecafe;
    let mut buf = BytesMut::with_capacity(1024 * 16);
    buf.put_i32(0xcafecafe as u32 as i32);
    buf.put_u64(ttl);
    buf.put_u64(ts.ns());
    buf.put_u64(pulse);
    buf.put_u64(ioc_ts);
    buf.put_u8(0);
    buf.put_u8(0);
    buf.put_i32(-1);
    use streams::dtflags::*;
    if config.compression {
        match config.shape {
            Shape::Wave(ele_count) => {
                let mut flags = COMPRESSION | ARRAY | SHAPE;
                if config.byte_order.is_be() {
                    flags |= BIG_ENDIAN;
                }
                buf.put_u8(flags);
                buf.put_u8(config.scalar_type.index());
                let comp_method = 0 as u8;
                buf.put_u8(comp_method);
                buf.put_u8(1);
                buf.put_u32(ele_count as u32);
                match &config.scalar_type {
                    ScalarType::F64 => {
                        let ele_size = 8;
                        let mut vals = vec![0; (ele_size * ele_count) as usize];
                        for i1 in 0..ele_count {
                            let v = (evix as f64) * 100.0 + i1 as f64;
                            let a = if config.byte_order.is_be() {
                                v.to_be_bytes()
                            } else {
                                v.to_le_bytes()
                            };
                            use std::io::{Cursor, Seek, SeekFrom, Write};
                            let mut c1 = Cursor::new(&mut vals);
                            c1.seek(SeekFrom::Start(i1 as u64 * ele_size as u64))?;
                            Write::write_all(&mut c1, &a)?;
                        }
                        let mut comp = vec![0u8; (ele_size * ele_count + 64) as usize];
                        let n1 =
                            bitshuffle_compress(&vals, &mut comp, ele_count as usize, ele_size as usize, 0).unwrap();
                        buf.put_u64(vals.len() as u64);
                        let comp_block_size = 0;
                        buf.put_u32(comp_block_size);
                        buf.put(&comp[..n1]);
                    }
                    ScalarType::U16 => {
                        let ele_size = 2;
                        let mut vals = vec![0; (ele_size * ele_count) as usize];
                        for i1 in 0..ele_count {
                            let v = (evix as u16).wrapping_mul(100).wrapping_add(i1 as u16);
                            let a = if config.byte_order.is_be() {
                                v.to_be_bytes()
                            } else {
                                v.to_le_bytes()
                            };
                            use std::io::{Cursor, Seek, SeekFrom, Write};
                            let mut c1 = Cursor::new(&mut vals);
                            c1.seek(SeekFrom::Start(i1 as u64 * ele_size as u64))?;
                            Write::write_all(&mut c1, &a)?;
                        }
                        let mut comp = vec![0u8; (ele_size * ele_count + 64) as usize];
                        let n1 =
                            bitshuffle_compress(&vals, &mut comp, ele_count as usize, ele_size as usize, 0).unwrap();
                        buf.put_u64(vals.len() as u64);
                        let comp_block_size = 0;
                        buf.put_u32(comp_block_size);
                        buf.put(&comp[..n1]);
                    }
                    _ => todo!("Datatype not yet supported: {:?}", config.scalar_type),
                }
            }
            _ => todo!("Shape not yet supported: {:?}", config.shape),
        }
    } else {
        match config.shape {
            Shape::Scalar => {
                let mut flags = 0;
                if config.byte_order.is_be() {
                    flags |= BIG_ENDIAN;
                }
                buf.put_u8(flags);
                buf.put_u8(config.scalar_type.index());
                match &config.scalar_type {
                    ScalarType::I32 => {
                        let v = match gen_var {
                            GenVar::Default => evix as i32,
                            GenVar::ConstRegular => 42 as i32,
                            GenVar::TimeWeight => {
                                let m = evix % 20;
                                if m == 0 {
                                    200
                                } else if m == 1 {
                                    400
                                } else {
                                    0
                                }
                            }
                        };
                        if config.byte_order.is_be() {
                            buf.put_i32(v);
                        } else {
                            buf.put_i32_le(v);
                        };
                    }
                    _ => todo!("Datatype not yet supported: {:?}", config.scalar_type),
                }
            }
            _ => todo!("Shape not yet supported: {:?}", config.shape),
        }
    }
    {
        let len = buf.len() as u32 + 4;
        buf.put_u32(len);
        buf.as_mut().put_u32(len);
    }
    let z = file.written_len();
    file.write_all(buf.as_ref()).await?;
    if let Some(f) = index_file {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64(ts.ns());
        buf.put_u64(z);
        f.write_all(&buf).await?;
    }
    Ok(())
}
