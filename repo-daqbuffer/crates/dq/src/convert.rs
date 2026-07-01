use clap::ArgAction;
use clap::Parser;
use daqbuf_err as err;
use err::Error;
use netpod::timeunits::*;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Parser)]
#[command(author, version)]
pub struct Opts {
    #[arg(short, long, action(ArgAction::Count))]
    pub verbose: u32,
    #[command(subcommand)]
    pub subcmd: SubCmd,
}

#[derive(Debug, Parser)]
pub enum SubCmd {
    /// Convert a channel from the Archiver Appliance into Databuffer format.
    ConvertArchiverApplianceChannel(ConvertArchiverApplianceChannel),
}

#[derive(Debug, Parser)]
pub struct ConvertArchiverApplianceChannel {
    /// Prefix for keyspaces, e.g. specify `daq` to get scalar keyspace directory `daq_2`.
    #[arg(long)]
    keyspace_prefix: String,
    /// Name of the channel to convert.
    #[arg(long)]
    channel_name: String,
    /// Look for archiver appliance data at given path.
    #[arg(long)]
    input_dir: PathBuf,
    /// Generate Databuffer format at given path.
    #[arg(long)]
    output_dir: PathBuf,
    /// Size of the time-bins in the generated Databuffer format.\nUnit-suffixes: `h` (hours), `d` (days)
    #[arg(default_value = "1d", long)]
    time_bin_size: TimeBinSize,
}

#[derive(Clone, Debug)]
pub struct TimeBinSize {
    #[allow(unused)]
    nanos: u64,
}

impl FromStr for TimeBinSize {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(Error::with_msg_no_trace("Malformed time-bin size"));
        }
        let suff = s.chars().last().unwrap();
        if suff.is_numeric() {
            Err(Error::with_msg_no_trace("Malformed time-bin size"))
        } else if suff == 'h' {
            let bs: u64 = s[..s.len() - 1].parse()?;
            Ok(Self { nanos: bs * HOUR })
        } else if suff == 'd' {
            let bs: u64 = s[..s.len() - 1].parse()?;
            Ok(Self { nanos: bs * DAY })
        } else {
            Err(Error::with_msg_no_trace("Malformed time-bin size"))
        }
    }
}
