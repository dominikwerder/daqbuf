use super::super::conn2;
use crate::ca::conn2::asynchan;
use crate::ca::connset2::connset::ConnSet;
use core::panic;
use futures::FutureExt;
use futures::StreamExt;
use std::mem::MaybeUninit;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use taskrun::tokio;
use taskrun::tokio::io::AsyncReadExt;

macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Test00"),
    enum variants {
        SignalHandlerSet,
        SignalHandlerUnset,
    },
);

static ACT_OLD: AtomicUsize = AtomicUsize::new(0);
static INT_TX: AtomicUsize = AtomicUsize::new(0x7fffffff);
static SIGINT: AtomicUsize = AtomicUsize::new(0);
static SIGINT_CONFIRM: AtomicUsize = AtomicUsize::new(0);
static SIGTERM: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_SENT: AtomicUsize = AtomicUsize::new(0);

type CB1 = fn(libc::c_int) -> ();
type CB3 = fn(libc::c_int, *const libc::siginfo_t, *const libc::c_void) -> ();

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

pub fn set_signal_handler(signum: libc::c_int, cb: CB3) -> Result<(), Error> {
    let mut mask: libc::sigset_t = unsafe { std::mem::zeroed() };
    let ec = unsafe { libc::sigemptyset(&mut mask) };
    if ec != 0 {
        std::process::exit(82);
    }
    let sa_sigaction: libc::sighandler_t = cb as *const libc::c_void as _;
    let act = libc::sigaction {
        sa_sigaction,
        sa_mask: mask,
        sa_flags: libc::SA_SIGINFO | libc::SA_RESTART,
        sa_restorer: None,
    };
    let mut act_old = libc::sigaction {
        sa_sigaction: libc::SIG_DFL,
        sa_mask: unsafe { std::mem::zeroed() },
        sa_flags: 0,
        sa_restorer: None,
    };
    let (ec, msg) = unsafe {
        let ec = libc::sigaction(signum, &act, &mut act_old);
        let errno = *libc::__errno_location();
        (ec, std::ffi::CStr::from_ptr(libc::strerror(errno)))
    };
    if ec != 0 {
        eprintln!("unable to set signal handler: {msg:?}");
        std::process::exit(81);
    }
    ACT_OLD.store(act_old.sa_sigaction, Ordering::Release);
    if false {
        eprintln!("act_old.sa_sigaction {:p}", act_old.sa_sigaction as *const ());
    }
    Ok(())
}

pub async fn test_01() {
    let (mut int_tx, int_rx) = asynchan::bounded(16, "SIGINT");
    let mut fds = [0; 2];
    if true {
        let ec = unsafe { libc::pipe(&mut fds[0]) };
        if ec != 0 {
            panic!("can not create pipe");
        }
        INT_TX.store(fds[1] as usize, Ordering::Release);
    }
    if true {
        tokio::task::spawn_blocking(|| {
            std::thread::spawn(|| {
                if let Err(e) = set_signal_handler(libc::SIGINT, handler_sigint) {
                    eprintln!("set signal handler error {e}");
                    std::process::exit(1);
                }
            })
            .join()
            .map_err(|e| {
                eprintln!("set handler thread join error {e:?}");
                std::process::exit(1);
            });
            eprintln!("set handler thread joined");
        })
        .await
        .map_err(|e| {
            eprintln!("set handler task await error {e}");
            std::process::exit(1);
        });
        info!("set handler task joined");
    }
    if true {
        std::thread::spawn(move || {
            let mut buf = [0; 8];
            loop {
                let ec = unsafe { libc::read(fds[0], buf.as_mut_ptr() as _, 8) };
                eprintln!("-------------------   libc read  ec {ec}");
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
    /*
    {
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
    }
    */
    {
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
    /*
    loop {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let i = SIGINT.load(Ordering::Acquire);
        info!("SIGINT counter {i}");
    }
    */
}

pub async fn test_01_b() {
    let (mut int_tx, int_rx) = asynchan::bounded(16, "SIGINT");
    {
        let mut fds = [0; 2];
        let ec = unsafe { libc::pipe(&mut fds[0]) };
        if ec != 0 {
            panic!("can not create pipe");
        }
        INT_TX.store(fds[1] as usize, Ordering::Release);
        if false {
            use std::os::fd::FromRawFd;
            let mut pipe = unsafe { tokio::fs::File::from_raw_fd(fds[0]) };
            let fut = async move {
                let mut buf = [0; 4];
                info!("-------------------   WAIT FOR PIPE READ");
                let n = pipe.read(&mut buf).await.unwrap();
                info!("-------------------   PIPE READ n {n}");
                if n > 0 {
                    match int_tx.send(2u32).await {
                        Ok(()) => {
                            info!("signal sent to channel");
                        }
                        Err(e) => {
                            info!("signal send to channel error");
                        }
                    }
                }
            };
            tokio::spawn(fut);
        }
        if true {
            let f = move || {
                let mut buf = [0; 8];
                let ec = unsafe { libc::read(fds[0], buf.as_mut_ptr() as _, 8) };
                info!("-------------------   libc read  ec {ec}");
            };
            tokio::task::spawn_blocking(f);
        }
    }
    if false {
        let fut = async {
            loop {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                let i = SIGINT.load(Ordering::Acquire);
                info!("=============================================         test_01 SIGINT {i}");
            }
        };
        tokio::spawn(fut);
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

pub async fn test_02() {
    let fut = async {
        let mut s = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
        loop {
            let i = SIGINT.load(Ordering::Acquire);
            eprintln!("-------------------   TOKIO SIGNAL AWAITING {i}");
            match s.recv().await {
                Some(_) => {
                    let i = SIGINT.fetch_add(1, Ordering::AcqRel);
                    eprintln!("-------------------   TOKIO SIGNAL RECEIVED {i}");
                    if i >= 3 {
                        std::process::exit(14);
                    }
                }
                None => {
                    eprintln!("-------------------   TOKIO SIGNAL RECEIVED break");
                    break;
                }
            }
        }
    };
    tokio::spawn(fut);
    loop {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let i = SIGINT.load(Ordering::Acquire);
        info!("SIGINT counter {i}");
    }
}

pub async fn test_03() {}

pub async fn test_04() {}
