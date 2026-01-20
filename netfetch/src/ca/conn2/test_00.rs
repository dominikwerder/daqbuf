use super::super::conn2;
use crate::ca::conn2::asynchan;
use crate::ca::connset2::connset::ConnSet;
use futures::FutureExt;
use futures::StreamExt;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use taskrun::tokio;

macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

static INT_TX: RwLock<Option<asynchan::Sender<u32>>> = RwLock::new(None);

static SIGINT: AtomicUsize = AtomicUsize::new(0);
static SIGINT_CONFIRM: AtomicUsize = AtomicUsize::new(0);
static SIGTERM: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_SENT: AtomicUsize = AtomicUsize::new(0);

// fn handler_sigint(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void)
fn handler_sigint(_a: libc::c_int) {
    let n = SIGINT.fetch_add(1, Ordering::AcqRel);
    if n >= 1 {
        let _ = ingest_linux::signal::unset_signal_handler(libc::SIGINT);
        std::process::exit(13);
    }
    let mut g = INT_TX.write().unwrap();
    if let Some(e) = g.as_mut() {
        if e.try_send(1).is_err() {
            std::process::exit(88);
        }
    } else {
        std::process::exit(87);
    }
}

fn handler_sigterm(_a: libc::c_int, _b: *const libc::siginfo_t, _c: *const libc::c_void) {
    SIGTERM.store(1, Ordering::Release);
    let _ = ingest_linux::signal::unset_signal_handler(libc::SIGTERM);
}

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
    let (int_tx, int_rx) = asynchan::bounded(16, "SIGINT");
    {
        INT_TX.write().unwrap().replace(int_tx);
    }
    ingest_linux::signal::set_signal_handler(libc::SIGINT, handler_sigint).unwrap();
    // ingest_linux::signal::set_signal_handler(libc::SIGTERM, handler_sigterm).unwrap();
    let buf = std::fs::read("daqingest.yml").unwrap();
    let ingest_opts = serde_yaml::from_slice(&buf).unwrap();
    let mut connset = ConnSet::new("sf-archiver".into(), "".into(), ingest_opts, int_rx)
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
