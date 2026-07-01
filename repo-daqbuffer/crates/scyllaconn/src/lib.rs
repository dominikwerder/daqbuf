pub mod accounting;
pub mod bincache;
pub mod binned2;
pub mod binned3;
pub mod binwriteindex;
pub mod conn;
pub mod errconv;
pub mod events2;
pub mod events3;
pub mod range;
pub mod status;
pub mod worker;

pub use daqbuf_series::SeriesId;
pub use scylla;

mod log {
    pub use netpod::log::*;
}

pub async fn test_log() {
    use netpod::log::*;
    error!("------");
    warn!("------");
    info!("------");
    debug!("------");
    trace!("------");
}
