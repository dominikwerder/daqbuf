use crate::events3::SeriesInfo;
use crate::events3::mspfwd::ReadMspFwdStream;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaOptsSubmit;
use crate::worker::ScyllaQueueCluster;
use futures_util::StreamExt;
use netpod::DtNano;
use netpod::RangeExcl;
use netpod::TsMs;
use netpod::TsNano;
use std::collections::VecDeque;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }
macro_rules! trace2 { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

fn _keep() {
    error!("");
    warn!("");
    info!("");
    debug!("");
    trace!("");
}

autoerr::create_error_v1!(
    name(Error, "ReadMsp03Bck"),
    enum variants {
        MspFwd(#[from] crate::events3::mspfwd::Error),
        MspBckTooMany,
    },
);

pub async fn msp_bck(
    ks: KeyspaceId,
    series_info: SeriesInfo,
    beg: TsNano,
    scyqu: ScyllaQueueCluster,
    scyopts: ScyllaOptsSubmit,
) -> Result<VecDeque<TsMs>, Error> {
    let clt = scyqu.tag().to_string();
    let kst = ks.name();
    let ks = ks.clone();
    let series = series_info.id();
    let win = DtNano::from_sec(ks.rt().msp_rollover_ivl_on_read().as_secs());
    debug!("{clt}  {kst}  win {win} h", win = win.sec_u64() / 60 / 60);
    let range = ScyllaSeriesRange::new(beg.sub(win), beg);
    // TODO change the limit to larger for non-test-data
    let mut stream = ReadMspFwdStream::new(ks, series, range, RangeExcl::None, 1, scyopts, scyqu);
    let mut msps = VecDeque::new();
    while let Some(x) = stream.next().await {
        msps.extend(x?);
    }
    let n = msps.len();
    let msp_max = 10;
    if n < 6 {
        debug!("{clt}  {kst}  win msp len {n}");
    } else if n <= msp_max {
        debug!("{clt}  {kst}  win msp len {n}");
    } else if n > msp_max {
        debug!("{clt}  {kst}  win msp len {n}");
        // return Err(Error::MspBckTooMany);
        let a = msps.split_off(msps.len() - msp_max);
        msps = a;
    }
    let n = msps.len();
    debug!("{clt}  {kst}  win msp len2 {n}");
    for e in &msps {
        trace2!("got backward msp {e}");
    }
    Ok(msps)
}
