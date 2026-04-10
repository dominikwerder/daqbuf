use crate::events3::SeriesInfo;
use crate::events3::mspfwd;
use crate::events3::mspfwd::ReadMspFwdStream;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use futures_util::StreamExt;
use netpod::DtNano;
use netpod::RangeExcl;
use netpod::TsMs;
use netpod::TsNano;
use std::collections::VecDeque;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

fn _keep() {
    error!("");
    warn!("");
    debug!("");
    trace!("");
}

autoerr::create_error_v1!(
    name(Error, "ReadMsp03Bck"),
    enum variants {
        MspFwd(#[from] mspfwd::Error),
        MspBckTooMany,
    },
);

// impl<T> From<async_channel::SendError<T>> for Error {
//     fn from(_: async_channel::SendError<T>) -> Self {
//         Error::Send
//     }
// }

// impl From<async_channel::RecvError> for Error {
//     fn from(_: async_channel::RecvError) -> Self {
//         Error::Recv
//     }
// }

pub async fn msp_bck(
    ks: KeyspaceId,
    series_info: SeriesInfo,
    beg: TsNano,
    scyqu: ScyllaQueueCluster,
) -> Result<VecDeque<TsMs>, Error> {
    let ks = ks.clone();
    let series = series_info.id();
    let win = DtNano::from_sec(ks.rt().msp_rollover_ivl_on_read().as_secs());
    debug!("backward window {win} h", win = win.sec_u64() / 60 / 60);
    let range = ScyllaSeriesRange::new(beg.sub(win), beg);
    // TODO change the limit to larger for non-test-data
    let mut stream = ReadMspFwdStream::new(ks, series, range, RangeExcl::None, 1, scyqu);
    let mut msps = VecDeque::new();
    while let Some(x) = stream.next().await {
        msps.extend(x?);
        let n = msps.len();
        if n > 20 {
            warn!("many msp in backward window {n}");
        } else if n > 200 {
            error!("too many msp in backward window {n}");
            return Err(Error::MspBckTooMany);
        }
    }
    for e in &msps {
        trace!("got backward msp {e}");
    }
    Ok(msps)
}
