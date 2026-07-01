use clap::ArgAction;
use clap::Parser;
use daqbuf_err as err;
use disk::eventchunker::EventChunker;
use disk::eventchunker::EventChunkerConf;
use err::Error;
use netpod::ByteOrder;
use netpod::ByteSize;
use netpod::SfChFetchInfo;
#[allow(unused)]
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Debug, Parser)]
#[command(name = "DAQ buffer tools", author, version)]
pub struct Opts {
    #[arg(short, long, action(ArgAction::Count))]
    pub verbose: u32,
    #[command(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    ReadDatabufferConfigfile(ReadDatabufferConfigfile),
    ReadDatabufferDatafile(ReadDatabufferDatafile),
}

#[derive(Debug, Parser)]
pub struct ReadDatabufferConfigfile {
    #[arg(long)]
    configfile: PathBuf,
}

#[derive(Debug, Parser)]
pub struct ReadDatabufferDatafile {
    #[arg(long)]
    configfile: PathBuf,
    #[arg(long)]
    datafile: PathBuf,
}

pub fn main() -> Result<(), Error> {
    taskrun::run(async {
        if false {
            return Err(Error::with_msg_no_trace(format!("unknown command")));
        }
        let opts = Opts::parse();
        match opts.subcmd {
            SubCmd::ReadDatabufferConfigfile(sub) => {
                let mut file = File::open(&sub.configfile).await?;
                let meta = file.metadata().await?;
                let mut buf = vec![0; meta.len() as usize];
                file.read_exact(&mut buf).await?;
                drop(file);
                let config = match parse::channelconfig::parse_config(&buf) {
                    Ok(k) => k,
                    Err(e) => return Err(Error::with_msg_no_trace(format!("can not parse: {:?}", e))),
                };
                eprintln!("Read config: {:?}", config);
                Ok(())
            }
            SubCmd::ReadDatabufferDatafile(sub) => {
                let mut file = File::open(&sub.configfile).await?;
                let meta = file.metadata().await?;
                let mut buf = vec![0; meta.len() as usize];
                file.read_exact(&mut buf).await?;
                drop(file);
                let config = match parse::channelconfig::parse_config(&buf) {
                    Ok(k) => k,
                    Err(e) => return Err(Error::with_msg_no_trace(format!("can not parse: {:?}", e))),
                };
                eprintln!("Read config: {:?}", config);
                let path = sub.datafile;
                let file = File::open(&path).await?;
                let disk_io_tune = netpod::DiskIoTune::default();
                let inp = Box::pin(disk::file_content_stream(path.clone(), file, disk_io_tune, "req-000"));
                let ce = &config.entries[0];
                let fetch_info = SfChFetchInfo::new(
                    "",
                    config.channel_name,
                    ce.ks as _,
                    ce.bs.clone(),
                    ByteOrder::Little,
                    ce.scalar_type.clone(),
                    ce.to_shape()?,
                );
                let range = NanoRange {
                    beg: u64::MIN,
                    end: u64::MAX,
                };
                let stats_conf = EventChunkerConf {
                    disk_stats_every: ByteSize::from_mb(2),
                };
                let _chunks = EventChunker::from_start(inp, fetch_info, range, stats_conf, 0, path.clone());
                err::todo();
                Ok(())
            }
        }
    })
}
