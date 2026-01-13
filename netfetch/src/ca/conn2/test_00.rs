use super::super::conn2;
use crate::ca::connset2::connset::ConnSet;
use futures::StreamExt;
use taskrun::tokio;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

pub async fn test_00() {
    let backend = "sf-archiver".into();
    let remote_addr = "172.26.120.207:5061".parse().unwrap();
    let local_epics_hostname = "sf-ingest-mg-01.psi.ch".into();
    // let (iqtxs, iqrxs) = scywr::insertqueues::make_pair();
    let conn = conn2::conn::CaConn::new(backend, remote_addr, local_epics_hostname);
    let mut conn_comm = conn.comm();
    {
        let conf = crate::conf::ChannelConfig::st_monitor("TEST:SLOW:SCALAR:F32:000000", "test");
        conn_comm.channel_add(conf).await.unwrap();
    }
    let mut conn = std::pin::pin!(conn);
    while let Some(x) = conn.next().await {
        trace!("{x:?}");
    }
}

pub async fn test_01() {
    let buf = std::fs::read("daqingest.yml").unwrap();
    let ingest_opts = serde_yaml::from_slice(&buf).unwrap();
    let mut connset = ConnSet::new("sf-archiver".into(), "".into(), ingest_opts)
        .await
        .unwrap();
    let cmder = connset.cmder().clone();
    let fut = async move {
        trace!("test_01 adding channel");
        let chname = "TEST:SLOW:SCALAR:F32:000000";
        let conf = crate::conf::ChannelConfig::st_monitor(chname, "TEST");
        cmder.channel_add(conf).await.unwrap();
        trace!("test_01 added channel");
    };
    tokio::spawn(fut);
    while let Some(e) = connset.next().await {
        trace!("test_01 connset item {e:?}");
    }
}

pub async fn test_02() {}

pub async fn test_03() {}

pub async fn test_04() {}
