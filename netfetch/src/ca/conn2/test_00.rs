use super::super::conn2;
use crate as netfetch;
use crate::asynchan;
use crate::ca::connset2;
use crate::ca::connset2::connset::ConnSet;
use crate::conf::ChannelConfig;
use crate::daemon_common::ChannelName;
use core::panic;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;

macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Test00"),
    enum variants {
        SignalHandlerSet,
        SignalHandlerUnset,
        Msg(String),
    },
);

static INT_TX: AtomicUsize = AtomicUsize::new(0x7fffffff);
static SIGINT: AtomicUsize = AtomicUsize::new(0);

fn handler_sigint(_: libc::c_int, _: *const libc::siginfo_t, _: *const libc::c_void) {
    let n = SIGINT.fetch_add(1, Ordering::AcqRel);
    if n >= 1 {
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

#[derive(Clone)]
struct Conn2Ctrls {
    cmder: crate::ca::connset2::connset::ConnSetCmder,
}

impl Conn2Ctrls {
    fn new(cmder: crate::ca::connset2::connset::ConnSetCmder) -> Self {
        Self { cmder }
    }
}

impl netfetch::metrics::Conn2Ctrls for Conn2Ctrls {
    fn connection_list_get_v1(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<crate::metrics::ConnectionListV1, Box<dyn std::error::Error>>> + Send>>
    {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.connection_list_get_v1().await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channels_for_addr_v1(
        &self,
        addr: std::net::SocketAddrV4,
    ) -> Pin<Box<dyn Future<Output = Result<crate::metrics::ChannelsForAddrInfoV1, Box<dyn std::error::Error>>> + Send>>
    {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channels_for_addr_v1(addr).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channels_for_addr_v2(
        &self,
        addr: std::net::SocketAddrV4,
        name: String,
    ) -> Pin<Box<dyn Future<Output = Result<crate::metrics::ChannelsForAddrInfoV2, Box<dyn std::error::Error>>> + Send>>
    {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channels_for_addr_v2(addr, name).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn cmd_dyn_v1(
        &self,
        cmd: String,
    ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value, Box<dyn std::error::Error>>> + Send>> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.cmd_dyn_v1(cmd).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channel_add_v1(
        &self,
        name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channel_add_v1(name).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }

    fn channel_remove_v1(
        &self,
        name: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        let cmder = self.cmder.clone();
        let fut = async move {
            let ret = cmder.channel_remove_v1(name).await?;
            Ok(ret)
        };
        Box::pin(fut)
    }
}

struct CaIngestCtrls {
    conn2_ctrls: Conn2Ctrls,
}

impl CaIngestCtrls {
    fn new(conn2_ctrls: Conn2Ctrls) -> Self {
        Self { conn2_ctrls }
    }
}

impl netfetch::metrics::CaIngestCtrls for CaIngestCtrls {
    fn timer_tick(&self, v: u32) -> Box<dyn Future<Output = u32>> {
        todo!()
    }

    fn get_metrics(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<netfetch::metrics::types::MetricsPrometheusShort, Box<dyn std::error::Error>>>
                + Send,
        >,
    > {
        todo!()
    }

    fn channel_add(
        &self,
        conf: ChannelConfig,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        todo!()
    }

    fn channel_remove(
        &self,
        name: ChannelName,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        todo!()
    }

    fn config_reload(&self) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        todo!()
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>> {
        let fut = async move {
            let e = Error::Msg(format!("TODO ctrl shutdown"));
            Err(Box::new(e) as _)
        };
        Box::pin(fut)
    }

    fn channel_states(
        &self,
        name: String,
        limit: u64,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<crate::ca::connset::ChannelStatusesResponse, Box<dyn std::error::Error>>> + Send,
        >,
    > {
        todo!()
    }

    fn conn2_ctrls(&self) -> Pin<Box<dyn Future<Output = Option<Box<dyn crate::metrics::Conn2Ctrls>>> + Send>> {
        let x = self.conn2_ctrls.clone();
        let fut = async move { Some(Box::new(x) as _) };
        Box::pin(fut)
    }
}

struct PostIngestCtrls {}

impl PostIngestCtrls {
    fn new() -> Self {
        Self {}
    }
}

impl netfetch::metrics::PostIngestCtrls for PostIngestCtrls {
    fn resources(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<netfetch::metrics::RoutesResources>, Box<dyn std::error::Error>>> + Send>>
    {
        todo!()
    }
}

pub async fn test_01() {
    let (mut int_tx, mut int_rx) = asynchan::bounded(16, "SIGINT");
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
        if false {
            let buf = std::fs::read("daqingest.yml").unwrap();
            let ingest_opts: crate::conf::CaIngestOpts = serde_yaml::from_slice(&buf).unwrap();
        }
        let (ingest_opts, channels_config) = crate::conf::parse_config("daqingest.yml").await.unwrap();
        let mut connset = ConnSet::new(ingest_opts.backend().into(), "".into(), ingest_opts.clone())
            .await
            .unwrap();
        let cmder = connset.cmder().clone();
        let conn2_ctrls = Conn2Ctrls::new(cmder.clone());
        let (metrics_shutdown_tx, metrics_shutdown_rx) = async_channel::bounded(16);
        let metrics_jh = {
            let fut = netfetch::metrics::metrics_service(
                ingest_opts.api_bind(),
                metrics_shutdown_rx,
                Arc::new(CaIngestCtrls::new(conn2_ctrls)),
                Arc::new(PostIngestCtrls::new()),
            );
            tokio::task::spawn(fut)
        };
        {
            let cmder = cmder.clone();
            tokio::spawn(async move {
                let _ = int_rx.recv().await;
                cmder.shutdown().await.unwrap();
            });
        }
        let fut = async move {
            if true {
                for j in 10..12 {
                    let g = 1000 * j;
                    let h = 2 + g;
                    for i in g..h {
                        let chname = format!("TEST:FAST:SCALAR:F32:{i:06}");
                        // trace!("test_01 adding channel {chname}");
                        let conf = crate::conf::ChannelConfig::st_monitor(chname, "TEST");
                        cmder.channel_add(conf).await.unwrap();
                    }
                }
            } else if false {
                let channels = [
                    "TEST:SLOW:SCALAR:F32:000000",
                    "TEST:SLOW:SCALAR:F32:000001",
                    // "SAT-CVME-TIMAST:SYS_CPU_LOAD",
                    // "X04SA-UIND:GAP-SP",
                    // "X04SA-UIND:GAP-SET",
                    // "X04SA-UIND:GAP-READ",
                    // "X04SA-UIND:GAP-RBV",
                ];
                for chname in channels {
                    trace!("test_01 adding channel {chname}");
                    let conf = crate::conf::ChannelConfig::st_monitor(chname, "TEST");
                    cmder.channel_add(conf).await.unwrap();
                }
            } else {
                if let Some(channels_config) = channels_config {
                    for chconf in channels_config.channels() {
                        cmder.channel_add(chconf.clone()).await.unwrap();
                    }
                }
            }
        };
        let print_ivl = Duration::from_millis(1000);
        let mut print_next = Instant::now() + print_ivl;
        tokio::spawn(fut);
        let mut prep = connset2::event_prepare_write::EventPrepareWrite::new();
        while let Some(e) = connset.next().await {
            let tsnow = Instant::now();
            if tsnow >= print_next {
                print_next = tsnow + print_ivl;
                info!("{}", prep.oneline());
            }
            match e {
                Ok(x) => {
                    for x in x {
                        match x {
                            crate::ca::connset2::connset::ConnSetItem::TestValue(x) => {
                                info!("{x}");
                            }
                            crate::ca::connset2::connset::ConnSetItem::ChannelEventValue(x) => {
                                prep.ingest(x);
                            }
                        }
                    }
                }
                _ => {
                    trace!("test_01 connset item {e:?}");
                }
            }
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
