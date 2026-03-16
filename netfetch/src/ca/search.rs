use super::conn2::asynchan2 as asynchan;
use crate::ca::findioc::FindIocStream;
use crate::conf::CaIngestOpts;
use async_channel::Receiver;
use async_channel::Sender;
use futures::FutureExt;
use futures::StreamExt;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::time::Duration;
use taskrun::tokio;
use tokio::task::JoinHandle;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "IocSearch"),
    enum variants {
        LookupFailure(String),
        IO(#[from] std::io::Error),
    },
);

async fn resolve_address(addr_str: &str) -> Result<SocketAddr, Error> {
    const PORT_DEFAULT: u16 = 5064;
    let ac = match addr_str.parse::<SocketAddr>() {
        Ok(k) => k,
        Err(_) => {
            trace!("can not parse {} as SocketAddr", addr_str);
            match addr_str.parse::<IpAddr>() {
                Ok(k) => SocketAddr::new(k, PORT_DEFAULT),
                Err(_e) => {
                    trace!("can not parse {} as IpAddr", addr_str);
                    let (hostname, port) = if addr_str.contains(":") {
                        let mut it = addr_str.split(":");
                        (
                            it.next().unwrap().to_string(),
                            it.next().unwrap().parse::<u16>().unwrap(),
                        )
                    } else {
                        (addr_str.to_string(), PORT_DEFAULT)
                    };
                    let host = format!("{}:{}", hostname.clone(), port);
                    match tokio::net::lookup_host(host.clone()).await {
                        Ok(k) => k
                            .into_iter()
                            .filter(|addr| if let SocketAddr::V4(_) = addr { true } else { false })
                            .next()
                            .ok_or_else(|| Error::LookupFailure(host))?,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    };
    Ok(ac)
}

pub async fn ca_search_workers_start(
    opts: &CaIngestOpts,
) -> Result<
    (
        Sender<(String, asynchan::Sender<crate::ca::findioc::FindIocRes>)>,
        Receiver<
            Result<
                VecDeque<(
                    crate::ca::findioc::FindIocRes,
                    asynchan::Sender<crate::ca::findioc::FindIocRes>,
                )>,
                crate::ca::findioc::Error,
            >,
        >,
        JoinHandle<Result<(), Error>>,
    ),
    Error,
> {
    let selfname = "ca_search_workers_start";
    let (search_tgts, blacklist) = search_tgts_from_opts(&opts).await?;
    let batch_run_max = Duration::from_millis(1200);
    let in_flight_max = 32;
    let batch_size = 8;
    let (inp_tx, inp2_rx) = async_channel::bounded(64);
    let (inp2_tx, inp_rx) = async_channel::bounded(64);
    tokio::spawn(async move {
        while let Ok(x) = inp2_rx.recv().await {
            trace!("{selfname}  SEE ITEM  {x:?}");
            if inp2_tx.send(x).await.is_err() {
                break;
            }
        }
    });
    let (out_tx, out_rx) = async_channel::bounded(64);
    let finder = FindIocStream::new(inp_rx, search_tgts, blacklist, batch_run_max, in_flight_max, batch_size);
    let jh = taskrun::spawn(finder_run(finder, out_tx));
    Ok((inp_tx, out_rx, jh))
}

async fn search_tgts_from_opts(opts: &CaIngestOpts) -> Result<(Vec<SocketAddrV4>, Vec<SocketAddrV4>), Error> {
    let mut addrs = Vec::new();
    for s in opts.search() {
        match resolve_address(s).await {
            Ok(addr) => {
                trace!("resolved {} as {}", s, addr);
                match addr {
                    SocketAddr::V4(addr) => {
                        addrs.push(addr);
                    }
                    SocketAddr::V6(_) => {
                        error!("no ipv6 for epics");
                    }
                }
            }
            Err(e) => {
                error!("can not resolve {} {}", s, e);
            }
        }
    }
    let blacklist = {
        let mut addrs = Vec::new();
        for s in opts.search_blacklist() {
            match resolve_address(s).await {
                Ok(addr) => {
                    trace!("resolved {} as {}", s, addr);
                    match addr {
                        SocketAddr::V4(addr) => {
                            addrs.push(addr);
                        }
                        SocketAddr::V6(_) => {
                            error!("no ipv6 for epics");
                        }
                    }
                }
                Err(e) => {
                    warn!("can not resolve {} {}", s, e);
                }
            }
        }
        addrs
    };
    Ok((addrs, blacklist))
}

// TODO must actually send out the results so that they can also go into database cache, or?
async fn finder_run(
    finder: FindIocStream,
    tx: Sender<
        Result<
            VecDeque<(
                crate::ca::findioc::FindIocRes,
                asynchan::Sender<crate::ca::findioc::FindIocRes>,
            )>,
            crate::ca::findioc::Error,
        >,
    >,
) -> Result<(), Error> {
    let selfname = "finder_run";
    let mut finder = Box::pin(finder);
    while let Some(x) = finder.next().await {
        match tx.send(x).await {
            Ok(()) => (),
            Err(_) => break,
        }
    }
    trace!("finder_run done");
    Ok(())
}
