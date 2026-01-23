use super::super::conn2;
use crate::ca::conn2::asynchan;
use crate::ca::connset2::connset::ConnSet;
use core::panic;
use futures::StreamExt;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use taskrun::tokio;

macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Test00"),
    enum variants {
        SignalHandlerSet,
        SignalHandlerUnset,
    },
);

static INT_TX: AtomicUsize = AtomicUsize::new(0x7fffffff);
static SIGINT: AtomicUsize = AtomicUsize::new(0);

fn handler_sigint(_: libc::c_int, _: *const libc::siginfo_t, _: *const libc::c_void) {
    let n = SIGINT.fetch_add(1, Ordering::AcqRel);
    if n >= 2 {
        std::process::exit(13);
    } else {
        let fd = INT_TX.load(Ordering::Acquire);
        if fd == 0x7fffffff {
            std::process::exit(83);
        } else {
            let ec = unsafe { libc::write(fd as _, b"1".as_ptr() as _, 1) };
            if ec != 1 {
                std::process::exit(84);
            }
        }
    }
}

pub async fn test_01() {
    let (mut int_tx, int_rx) = asynchan::bounded(16, "SIGINT");
    if true {
        let mut fds = [0; 2];
        let ec = unsafe { libc::pipe(&mut fds[0]) };
        if ec != 0 {
            panic!("can not create pipe");
        }
        INT_TX.store(fds[1] as usize, Ordering::Release);
        tokio::task::spawn_blocking(|| {
            std::thread::spawn(|| {
                let act_old: RwLock<libc::sigaction> = RwLock::new(unsafe { std::mem::zeroed() });
                if let Err(e) = ingest_linux::signal::set_signal_handler(libc::SIGINT, handler_sigint, &act_old) {
                    eprintln!("set signal handler error {e}");
                    std::process::exit(1);
                }
            })
            .join()
            .map_err(|e| {
                eprintln!("set handler thread join error {e:?}");
                std::process::exit(1);
            });
        })
        .await
        .map_err(|e| {
            eprintln!("set handler task await error {e}");
            std::process::exit(1);
        });
        std::thread::spawn(move || {
            let mut buf = [0; 8];
            loop {
                let ec = unsafe { libc::read(fds[0], buf.as_mut_ptr() as _, 8) };
                if ec == 0 {
                    break;
                } else {
                    match int_tx.try_send(1u32) {
                        Ok(()) => {}
                        Err(_) => break,
                    }
                }
            }
        });
    }
    if false {
        tokio::spawn(async move {
            let mut rx = int_rx;
            loop {
                match rx.recv().await {
                    Ok(x) => {
                        info!("int_rx.recv {x}");
                    }
                    Err(e) => {
                        info!("int_rx.recv error {e}");
                        break;
                    }
                }
            }
        });
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let i = SIGINT.load(Ordering::Acquire);
            info!("SIGINT counter {i}");
        }
    } else {
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

pub async fn test_02() {}

pub async fn test_03() {}

pub async fn test_04() {}
