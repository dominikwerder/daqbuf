use crate::SfDbChConf;
use daqbuf_err as err;
use err::Error;
use futures_util::StreamExt;
use netpod::Node;
use netpod::SfChFetchInfo;
use netpod::TsNano;
use netpod::timeunits::MS;
use std::path::PathBuf;
use taskrun::tokio;

pub fn datapath_for_keyspace(ks: u32, node: &Node) -> PathBuf {
    node.sf_databuffer
        .as_ref()
        .unwrap()
        .data_base_path
        .join(format!("{}_{}", node.sf_databuffer.as_ref().unwrap().ksprefix, ks))
        .join("byTime")
}

// TODO remove/replace this
pub fn datapath(timebin: u64, config: &SfDbChConf, split: u32, node: &Node) -> PathBuf {
    datapath_for_keyspace(config.keyspace as u32, node)
        .join(config.channel.name())
        .join(format!("{:019}", timebin))
        .join(format!("{:010}", split))
        .join(format!("{:019}_00000_Data", config.time_bin_size.ns() / MS))
}

/**
Return potential datafile paths for the given timebin.

It says "potential datafile paths" because we don't open the file here yet and of course,
files may vanish until then. Also, the timebin may actually not exist.
*/
pub async fn datapaths_for_timebin(
    timebin: u64,
    fetch_info: &SfChFetchInfo,
    node: &Node,
) -> Result<Vec<PathBuf>, Error> {
    let sfc = node.sf_databuffer.as_ref().unwrap();
    let timebin_path = datapath_for_keyspace(fetch_info.ks() as u32, node)
        .join(fetch_info.name())
        .join(format!("{:019}", timebin));
    let rd = tokio::fs::read_dir(timebin_path).await?;
    let mut rd = tokio_stream::wrappers::ReadDirStream::new(rd);
    let mut splits = vec![];
    while let Some(e) = rd.next().await {
        let e = e?;
        let dn = e
            .file_name()
            .into_string()
            .map_err(|s| Error::with_msg(format!("Bad OS path {:?}  path: {:?}", s, e.path())))?;
        if dn.len() != 10 {
            return Err(Error::with_msg(format!("bad split dirname  path: {:?}", e.path())));
        }
        let vv = dn.chars().fold(0, |a, x| if x.is_digit(10) { a + 1 } else { a });
        if vv == 10 {
            let split: u64 = dn.parse()?;
            match &sfc.splits {
                Some(sps) => {
                    if sps.contains(&split) {
                        splits.push(split);
                    }
                }
                None => {
                    splits.push(split);
                }
            }
        }
    }
    let mut ret = Vec::new();
    for split in splits {
        let path = datapath_for_keyspace(fetch_info.ks() as u32, node)
            .join(fetch_info.name())
            .join(format!("{:019}", timebin))
            .join(format!("{:010}", split))
            .join(format!("{:019}_00000_Data", fetch_info.bs().ns() / MS));
        ret.push(path);
    }
    Ok(ret)
}

pub fn channel_timebins_dir_path(fetch_info: &SfChFetchInfo, node: &Node) -> Result<PathBuf, Error> {
    let ret = datapath_for_keyspace(fetch_info.ks() as u32, node).join(fetch_info.name());
    Ok(ret)
}

pub fn data_dir_path(ts: TsNano, fetch_info: &SfChFetchInfo, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let ret = channel_timebins_dir_path(fetch_info, node)?
        .join(format!("{:019}", ts.ns() / fetch_info.bs().ns()))
        .join(format!("{:010}", split));
    Ok(ret)
}

pub fn data_path(ts: TsNano, fetch_info: &SfChFetchInfo, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data", fetch_info.bs().ns() / MS, 0);
    let ret = data_dir_path(ts, fetch_info, split, node)?.join(fname);
    Ok(ret)
}

pub fn index_path(ts: TsNano, fetch_info: &SfChFetchInfo, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data_Index", fetch_info.bs().ns() / MS, 0);
    let ret = data_dir_path(ts, fetch_info, split, node)?.join(fname);
    Ok(ret)
}

pub fn data_dir_path_tb(ks: u32, channel_name: &str, tb: u32, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let ret = datapath_for_keyspace(ks, node)
        .join(channel_name)
        .join(format!("{:019}", tb))
        .join(format!("{:010}", split));
    Ok(ret)
}

pub fn data_path_tb(ks: u32, channel_name: &str, tb: u32, tbs: u32, split: u32, node: &Node) -> Result<PathBuf, Error> {
    let fname = format!("{:019}_{:05}_Data", tbs, 0);
    let ret = data_dir_path_tb(ks, channel_name, tb, split, node)?.join(fname);
    Ok(ret)
}
