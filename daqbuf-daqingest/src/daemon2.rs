pub mod cmd;
pub mod ctrls;

use crate::daemon2::cmd::DaemonCmd;
use crate::daemon2::cmd::DaemonCmder;
use crate::daemon2::ctrls::CaIngestCtrlsV2;
use crate::daemon2::ctrls::PostIngestCtrlsV2;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::seriesbychannel::ChannelInfoQuery;
use dbpg::seriesbychannel::ChannelInfoQuerySender;
use futures_util::StreamExt;
use netfetch::ca::connset2::connset::ConnSet;
use netfetch::ca::connset2::connset::ConnSetCmder;
use netfetch::ca::connset2::connset::ConnSetItem;
use netfetch::ca::connset2::event_prepare_write::EventPrepareWrite;
use netfetch::ca::finder::FinderHandleV02;
use netfetch::conf::CaIngestOptsV2;
use netfetch::conf::ChannelConfig;
use netfetch::conf::ChannelsConfig;
use netfetch::metrics::RoutesResources;
use scywr::insertset::ScyllaInsertSet;
use scywr::insertset::ScyllaInsertSetOpts;
use scywr::insertworker::InsertWorkerOpts;
use scywr::insertworker::InsertWorkerOutputItem;
use scywr::iteminsertqueue::QueryItem;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Daemon2"),
    enum variants {
        Signal(#[from] ingest_linux::signal::Error),
        DbPgSeriesByChannel(#[from] dbpg::seriesbychannel::Error),
        ConnSet(#[from] netfetch::ca::connset2::connset::Error),
        ScyllaInsertSet(#[from] scywr::insertset::Error),
        Conf(#[from] netfetch::conf::ConfError),
        ChannelSend,
        ChannelRecv,
        Msg(String),
    },
);

const PG_LOOKUP_WORKER_COUNT: usize = 2;
const TICK_INTERVAL: Duration = Duration::from_millis(1000);
const SHUTDOWN_DEADLINE: Duration = Duration::from_millis(30000);
const JOIN_TIMEOUT: Duration = Duration::from_millis(10000);
const CMD_QUEUE_CAP: usize = 32;

#[derive(Debug)]
enum State {
    Running,
    Stopping { deadline: Instant },
}

impl State {
    fn deadline_expired(&self) -> bool {
        match self {
            State::Running => false,
            State::Stopping { deadline } => Instant::now() >= *deadline,
        }
    }
}

pub struct Daemon {
    ingest_opts: CaIngestOptsV2,
    connset: ConnSet,
    cmder: ConnSetCmder,
    cmd_tx: Sender<DaemonCmd>,
    cmd_rx: Receiver<DaemonCmd>,
    signals: ingest_linux::signal::SignalHandles,
    signals_done: bool,
    cmd_done: bool,
    insert_out_done: bool,
    insert_set: ScyllaInsertSet,
    insert_out_rx: Receiver<InsertWorkerOutputItem>,
    // TODO hand this to the connset event handling once conn2 emits QueryItem.
    #[allow(unused)]
    insert_input: Sender<VecDeque<QueryItem>>,
    ch_info_query_tx: Sender<ChannelInfoQuery>,
    pg_lookup_jhs: Vec<JoinHandle<Result<(), dbpg::seriesbychannel::Error>>>,
    pg_batcher_jh: JoinHandle<()>,
    finder_jh: JoinHandle<Result<(), netfetch::ca::finder::Error>>,
    metrics_shutdown_tx: Sender<u32>,
    metrics_shutdown_rx: Receiver<u32>,
    metrics_jh: Option<JoinHandle<Result<(), err::Error>>>,
    channels_config: Option<ChannelsConfig>,
    channel_names: BTreeSet<String>,
    metrics: stats::mett::DaemonMetrics,
    state: State,
}

impl Daemon {
    pub async fn new(ingest_opts: CaIngestOptsV2, channels_config: Option<ChannelsConfig>) -> Result<Self, Error> {
        // Install first, so that a signal during the remaining startup is caught as well.
        let signals = ingest_linux::signal::install_shutdown_signals()?;
        let (ch_info_query_tx, pg_lookup_jhs, pg_batcher_jh) = {
            let start = dbpg::seriesbychannel::start_lookup_workers::<dbpg::seriesbychannel::SalterRandom>;
            start(PG_LOOKUP_WORKER_COUNT, ingest_opts.postgresql_config()).await?
        };
        let (finder_handle, finder_jh) = Self::start_finder(&ingest_opts);
        let insert_set = Self::make_insert_set(&ingest_opts).await?;
        let local_epics_hostname = ingest_linux::net::local_hostname();
        let connset = ConnSet::new(
            ingest_opts.backend().into(),
            local_epics_hostname,
            ChannelInfoQuerySender::new(ch_info_query_tx.clone()),
            finder_handle,
        )
        .await?;
        let cmder = connset.cmder().clone();
        let (cmd_tx, cmd_rx) = async_channel::bounded(CMD_QUEUE_CAP);
        let (metrics_shutdown_tx, metrics_shutdown_rx) = async_channel::bounded(8);
        let channel_names = Self::channel_names(&channels_config);
        let insert_input = insert_set.input();
        let insert_out_rx = insert_set.output().clone();
        Ok(Self {
            ingest_opts,
            connset,
            cmder,
            cmd_tx,
            cmd_rx,
            signals,
            signals_done: false,
            cmd_done: false,
            insert_out_done: false,
            insert_set,
            insert_out_rx,
            insert_input,
            ch_info_query_tx,
            pg_lookup_jhs,
            pg_batcher_jh,
            finder_jh,
            metrics_shutdown_tx,
            metrics_shutdown_rx,
            metrics_jh: None,
            channels_config,
            channel_names,
            metrics: stats::mett::DaemonMetrics::new(),
            state: State::Running,
        })
    }

    fn start_finder(
        ingest_opts: &CaIngestOptsV2,
    ) -> (FinderHandleV02, JoinHandle<Result<(), netfetch::ca::finder::Error>>) {
        netfetch::ca::finder::start_finder_handle_v02(
            ingest_opts.backend().into(),
            ingest_opts.postgresql_config().clone(),
            ingest_opts.search().clone(),
            ingest_opts.search_blacklist().clone(),
        )
    }

    async fn make_insert_set(ingest_opts: &CaIngestOptsV2) -> Result<ScyllaInsertSet, Error> {
        let confs: Vec<_> = [
            ingest_opts.scylla_insert_set_conf_1st(),
            ingest_opts.scylla_insert_set_conf_2nd(),
        ]
        .into_iter()
        .flatten()
        .map(|x| x.to_insert_set_config())
        .collect();
        let opts = ScyllaInsertSetOpts::default();
        // TODO take these from config
        let insert_worker_opts = InsertWorkerOpts {
            store_workers_rate: Arc::new(AtomicU64::new(1000 * 500)),
            insert_workers_running: Arc::new(AtomicU64::new(0)),
            insert_frac: Arc::new(AtomicU64::new(1000)),
            array_truncate: Arc::new(AtomicU64::new(1024 * 200)),
        };
        let insert_set = ScyllaInsertSet::new(confs, opts, Arc::new(insert_worker_opts)).await?;
        Ok(insert_set)
    }

    fn channel_names(channels_config: &Option<ChannelsConfig>) -> BTreeSet<String> {
        channels_config
            .iter()
            .flat_map(|x| x.channels())
            .map(|x| x.name().to_string())
            .collect()
    }

    fn routes_resources(&self) -> Option<Arc<RoutesResources>> {
        let scyconfset = self.ingest_opts.scylla_insert_set_conf_1st()?;
        let iqtx = self.insert_set.sinks().first()?.clone();
        let res = RoutesResources::new(
            self.ingest_opts.backend().into(),
            self.ch_info_query_tx.clone(),
            iqtx,
            scyconfset,
            self.ingest_opts.postgresql_config().clone(),
        );
        Some(Arc::new(res))
    }

    fn spawn_metrics(&mut self) {
        let ca_ctrls = CaIngestCtrlsV2::new(self.cmder.clone(), DaemonCmder::new(self.cmd_tx.clone()));
        let post_ctrls = PostIngestCtrlsV2::new(self.routes_resources());
        let fut = netfetch::metrics::metrics_service(
            self.ingest_opts.api_bind(),
            self.metrics_shutdown_rx.clone(),
            Arc::new(ca_ctrls),
            Arc::new(post_ctrls),
        );
        self.metrics_jh = Some(tokio::task::spawn(fut));
    }

    /// Add the channels from the config file.
    ///
    /// Runs as a separate task because each add waits for the connset to acknowledge, which can
    /// only happen while this daemon polls the connset.
    fn spawn_initial_channel_add(&self) {
        let channels: Vec<ChannelConfig> = self
            .channels_config
            .iter()
            .flat_map(|x| x.channels())
            .cloned()
            .collect();
        if channels.is_empty() {
            return;
        }
        let cmder = self.cmder.clone();
        taskrun::spawn(async move {
            let n = channels.len();
            info!("add {n} channels from config");
            for ch_cfg in channels {
                if let Err(e) = cmder.channel_add(ch_cfg).await {
                    error!("daemon2 initial channel_add error {e}");
                }
            }
            info!("added {n} channels from config");
        });
    }

    pub async fn run(mut self) -> Result<(), Error> {
        self.spawn_metrics();
        self.spawn_initial_channel_add();
        let mut ticker = tokio::time::interval(TICK_INTERVAL);
        let mut prep = EventPrepareWrite::new();
        loop {
            tokio::select! {
                x = self.connset.next() => match x {
                    Some(Ok(batch)) => self.handle_connset_batch(batch, &mut prep),
                    Some(Err(e)) => {
                        error!("connset error {e}");
                        self.trigger_shutdown();
                    }
                    None => {
                        info!("connset stream ended");
                        break;
                    }
                },
                x = self.signals.recv(), if !self.signals_done => self.handle_signal(x),
                x = self.cmd_rx.recv(), if !self.cmd_done => match x {
                    Ok(cmd) => self.handle_cmd(cmd).await,
                    Err(_) => {
                        error!("command channel closed");
                        self.cmd_done = true;
                        self.trigger_shutdown();
                    }
                },
                x = self.insert_out_rx.recv(), if !self.insert_out_done => self.handle_insert_out(x),
                _ = ticker.tick() => {
                    self.handle_tick(&mut prep);
                    if self.state.deadline_expired() {
                        warn!("shutdown deadline expired, tear down anyways");
                        break;
                    }
                }
            }
        }
        self.shutdown().await
    }

    fn handle_connset_batch(&mut self, batch: impl IntoIterator<Item = ConnSetItem>, prep: &mut EventPrepareWrite) {
        for item in batch {
            match item {
                ConnSetItem::TestValue(x) => {
                    info!("{x}");
                }
                ConnSetItem::ChannelEventValue(x) => {
                    // TODO convert to `QueryItem` and send batches to `self.insert_input` as soon
                    // as conn2 delivers the full value, timestamp and target information.
                    prep.ingest(x);
                }
            }
        }
    }

    fn handle_signal(&mut self, ev: Option<ingest_linux::signal::SignalEvent>) {
        match ev {
            Some(ev) => {
                info!("received {}", ev.name());
                self.trigger_shutdown();
            }
            None => {
                warn!("signal handling ended");
                self.signals_done = true;
            }
        }
    }

    fn handle_insert_out(&mut self, x: Result<InsertWorkerOutputItem, async_channel::RecvError>) {
        match x {
            Ok(InsertWorkerOutputItem::Metrics(x)) => {
                self.metrics.scy_inswork().ingest(x);
            }
            Err(_) => {
                debug!("insert worker output channel closed");
                self.insert_out_done = true;
            }
        }
    }

    async fn handle_cmd(&mut self, cmd: DaemonCmd) {
        debug!("handle command {}", cmd.name());
        match cmd {
            DaemonCmd::GetMetrics(tx) => {
                if tx.send((&self.metrics).into()).await.is_err() {
                    warn!("can not reply to GetMetrics");
                }
            }
            DaemonCmd::ConfigReload(tx) => {
                let res = self.handle_config_reload().await.map_err(|e| e.to_string());
                if tx.send(res).await.is_err() {
                    warn!("can not reply to ConfigReload");
                }
            }
            DaemonCmd::Shutdown(tx) => {
                if tx.send(()).await.is_err() {
                    warn!("can not reply to Shutdown");
                }
                self.trigger_shutdown();
            }
        }
    }

    /// Re-read the channel config and apply the difference to the connset.
    ///
    /// The connset knows no flag-and-sweep command, therefore the diff is computed here.
    async fn handle_config_reload(&mut self) -> Result<(), Error> {
        let channels_config = netfetch::conf::parse_channels(self.ingest_opts.channels()).await?;
        let names_new = Self::channel_names(&channels_config);
        let add: Vec<ChannelConfig> = channels_config
            .iter()
            .flat_map(|x| x.channels())
            .filter(|x| !self.channel_names.contains(x.name()))
            .cloned()
            .collect();
        let remove: Vec<String> = self.channel_names.difference(&names_new).cloned().collect();
        info!("config reload  add {}  remove {}", add.len(), remove.len());
        self.channels_config = channels_config;
        self.channel_names = names_new;
        // Applied in a separate task because the connset can only acknowledge while this daemon
        // polls it.
        let cmder = self.cmder.clone();
        taskrun::spawn(async move {
            for ch_cfg in add {
                if let Err(e) = cmder.channel_add(ch_cfg).await {
                    error!("config reload channel_add error {e}");
                }
            }
            for name in remove {
                if let Err(e) = cmder.channel_remove(name).await {
                    error!("config reload channel_remove error {e}");
                }
            }
        });
        Ok(())
    }

    fn handle_tick(&mut self, prep: &mut EventPrepareWrite) {
        self.metrics.proc_cpu_v0().set(Self::get_cpu_usage() as _);
        self.metrics.proc_mem_rss().set(Self::get_memory_usage() as _);
        let qm = self.insert_set.queue_metrics();
        let mut st_rf1 = 0;
        let mut st_rf3 = 0;
        let mut mt_rf3 = 0;
        let mut lt_rf3 = 0;
        let mut lt_rf3_lat5 = 0;
        for x in qm.clusters.iter() {
            st_rf1 += x.st_rf1_len;
            st_rf3 += x.st_rf3_len;
            mt_rf3 += x.mt_rf3_len;
            lt_rf3 += x.lt_rf3_len;
            lt_rf3_lat5 += x.lt_rf3_lat5_len;
        }
        self.metrics.iqtx_len_st_rf1().set(st_rf1 as _);
        self.metrics.iqtx_len_st_rf3().set(st_rf3 as _);
        self.metrics.iqtx_len_mt_rf3().set(mt_rf3 as _);
        self.metrics.iqtx_len_lt_rf3().set(lt_rf3 as _);
        self.metrics.iqtx_len_lt_rf3_lat5().set(lt_rf3_lat5 as _);
        info!("{}", prep.oneline());
    }

    fn trigger_shutdown(&mut self) {
        match self.state {
            State::Running => {
                info!("shutdown triggered");
                self.state = State::Stopping {
                    deadline: Instant::now() + SHUTDOWN_DEADLINE,
                };
                let cmder = self.cmder.clone();
                taskrun::spawn(async move {
                    if let Err(e) = cmder.shutdown().await {
                        error!("can not send shutdown to connset {e}");
                    }
                });
            }
            State::Stopping { .. } => {
                info!("shutdown already in progress");
            }
        }
    }

    async fn shutdown(self) -> Result<(), Error> {
        let Self {
            connset,
            insert_set,
            metrics_shutdown_tx,
            metrics_jh,
            pg_lookup_jhs,
            pg_batcher_jh,
            finder_jh,
            signals,
            ..
        } = self;
        debug!("shutdown  insert set");
        if let Err(e) = insert_set.shutdown().await {
            error!("shutdown  insert set  {e}");
        }
        debug!("shutdown  metrics service");
        if metrics_shutdown_tx.send(1).await.is_err() {
            warn!("shutdown  metrics service already gone");
        }
        if let Some(jh) = metrics_jh {
            match jh.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("shutdown  metrics service error  {e}"),
                Err(e) => error!("shutdown  metrics service join error  {e}"),
            }
        }
        // Drops the finder handle and the channel-info sender which the connset holds.
        drop(connset);
        debug!("shutdown  ioc finder");
        Self::join_res("ioc finder", finder_jh).await;
        debug!("shutdown  postgres workers");
        Self::join_unit("pg batcher", pg_batcher_jh).await;
        for jh in pg_lookup_jhs {
            Self::join_res("pg lookup worker", jh).await;
        }
        drop(signals);
        info!("shutdown done");
        Ok(())
    }

    async fn join_res<E: std::fmt::Display>(name: &str, jh: JoinHandle<Result<(), E>>) {
        match tokio::time::timeout(JOIN_TIMEOUT, jh).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => error!("shutdown  {name}  error  {e}"),
            Ok(Err(e)) => error!("shutdown  {name}  join error  {e}"),
            Err(_) => error!("shutdown  {name}  join timeout"),
        }
    }

    async fn join_unit(name: &str, jh: JoinHandle<()>) {
        match tokio::time::timeout(JOIN_TIMEOUT, jh).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("shutdown  {name}  join error  {e}"),
            Err(_) => error!("shutdown  {name}  join timeout"),
        }
    }

    fn get_cpu_usage() -> u64 {
        let ret = match std::fs::read("/proc/self/stat") {
            Ok(buf) => {
                let line = String::from_utf8_lossy(&buf);
                let a: Vec<_> = line.split(" ").collect();
                let utime = a.get(13).and_then(|s| s.parse::<u64>().ok());
                let stime = a.get(14).and_then(|s| s.parse::<u64>().ok());
                match (utime, stime) {
                    (Some(utime), Some(stime)) => Some(utime + stime),
                    _ => None,
                }
            }
            Err(_) => None,
        };
        ret.unwrap_or(0)
    }

    fn get_memory_usage() -> u64 {
        let ret = match std::fs::read("/proc/self/statm") {
            Ok(buf) => {
                let s = String::from_utf8_lossy(&buf);
                let mut it = s.split(" ");
                it.next();
                it.next().and_then(|s| s.parse::<u64>().ok()).map(|n| {
                    let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
                    n * ps
                })
            }
            Err(_) => None,
        };
        ret.unwrap_or(0)
    }
}
