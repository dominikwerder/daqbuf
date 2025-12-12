use super::super::conn2;
use futures::StreamExt;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

pub async fn test_00() {
    let backend = "sf-archiver".into();
    let remote_addr = "172.26.120.207:5061".parse().unwrap();
    let local_epics_hostname = "sf-ingest-mg-01.psi.ch".into();
    // let (iqtxs, iqrxs) = scywr::insertqueues::make_pair();
    let test_channel_names = ["TEST:SLOW:SCALAR:F32:000000"];
    let test_channel_names = test_channel_names.into_iter().map(From::from).collect();
    // stop via sending command
    let conn = conn2::conn::CaConn::new(backend, remote_addr, local_epics_hostname, test_channel_names);
    let mut conn = Box::pin(conn);
    while let Some(x) = conn.next().await {
        trace!("{x:?}");
    }
}

pub async fn test_01() {}

pub async fn test_02() {}

pub async fn test_03() {}

pub async fn test_04() {}
