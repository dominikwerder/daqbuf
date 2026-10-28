pub mod ca;
pub mod conf;
pub mod daemon_common;
pub mod errconv;
pub mod futwrap;
pub mod linuxhelper;
pub mod metrics;
pub mod misc;
pub mod netbuf;
pub mod polltimer;
pub mod queueset;
pub mod ratelimit;
pub mod rt;
#[cfg(test)]
pub mod test;
pub mod throttletrace;

use log::*;

pub use channeltools::asynbuf;
pub use channeltools::asynchan2 as asynchan;

pub fn log_test() {
    info!("log-test");
    warn!("log-test");
    error!("log-test");
    debug!("log-test");
    trace!("log-test");
}

#[derive(Debug)]
pub enum WriterStateQuery {
    Select(scywr::channelwriterstate::WriterStateSelect),
    Insert(scywr::channelwriterstate::WriterStateInsert),
}
