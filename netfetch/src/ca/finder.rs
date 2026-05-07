use super::connset::CURRENT_SEARCH_PENDING_MAX;
use super::connset::SEARCH_BATCH_MAX;
use super::search::ca_search_workers_start;
use crate::ca::conn2::asynchan2 as asynchan;
use crate::ca::findioc::FindIocRes;
use crate::conf::CaIngestOpts;
use async_channel::Receiver;
use async_channel::Sender;
use dbpg::conn::make_pg_client;
use dbpg::iocindex::IocItem;
use dbpg::iocindex::IocSearchIndexWorker;
use dbpg::postgres::Row as PgRow;
use futures::Stream;
use netpod::Database;
use std::collections::VecDeque;
use std::time::Duration;
use std::time::Instant;
use taskrun::tokio;
use tokio::task::JoinHandle;

const SEARCH_DB_WORKER_CNT: usize = 2;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! warn { ($($arg:tt)*) => { if true { log::warn!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! debug_batch { ($($arg:tt)*) => { if false { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "Finder"),
    enum variants {
        Join(#[from] tokio::task::JoinError),
        DbPg(#[from] dbpg::err::Error),
        Postgres(#[from] dbpg::postgres::Error),
        IocSearch(#[from] crate::ca::search::Error),
        AddrSearchSend,
        AddrSearchResultTx,
        AddrSearchResultRx,
        Send,
    },
);

impl From<async_channel::SendError<IocAddrQuery>> for Error {
    fn from(_: async_channel::SendError<IocAddrQuery>) -> Self {
        Self::AddrSearchSend
    }
}

fn transform_pgres(rows: Vec<PgRow>) -> VecDeque<Option<FindIocRes>> {
    let selfname = "transform_pgres";
    let mut ret = VecDeque::new();
    for row in rows {
        let n: Result<i32, _> = row.try_get(0);
        let ch: Result<String, _> = row.try_get(1);
        debug!("{selfname}  {ch:?}");
        match (n, ch) {
            (Ok(_n), Ok(ch)) => {
                if let Some(addr) = row.get::<_, Option<String>>(3).and_then(|x| x.parse().ok()) {
                    let item = FindIocRes::from_results(ch, None, Some(addr), Duration::from_millis(0));
                    ret.push_back(Some(item));
                } else {
                    debug!("{selfname}  {ch}  no addr in database result");
                    ret.push_back(None);
                }
            }
            (_, Err(e)) => {
                error!("bad string from pg: {}", e);
            }
            (Err(e), _) => {
                error!("bad int from pg: {}", e);
            }
        }
    }
    ret
}

#[derive(Debug)]
pub struct IocAddrQuery {
    name: String,
    use_cache: bool,
    tx: asynchan::Sender<FindIocRes>,
}

impl IocAddrQuery {
    pub fn cached_rx(name: String) -> (Self, asynchan::Receiver<FindIocRes>) {
        let (tx, rx) = asynchan::bounded(1, "IocAddrQuery");
        let x = Self {
            name,
            use_cache: true,
            tx,
        };
        (x, rx)
    }

    pub fn uncached_rx(name: String) -> (Self, asynchan::Receiver<FindIocRes>) {
        let (tx, rx) = asynchan::bounded(1, "IocAddrQuery");
        let x = Self {
            name,
            use_cache: false,
            tx,
        };
        (x, rx)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn name_string(&self) -> &String {
        &self.name
    }

    pub fn use_cache(&self) -> bool {
        self.use_cache
    }

    pub fn into_name_tx(self) -> (String, asynchan::Sender<FindIocRes>) {
        (self.name, self.tx)
    }
}

#[derive(Debug, Clone)]
pub struct FinderHandleV02 {
    qtx: Sender<IocAddrQuery>,
}

impl FinderHandleV02 {
    async fn find(&mut self, query: IocAddrQuery, mut rx: asynchan::Receiver<FindIocRes>) -> Result<FindIocRes, Error> {
        self.qtx.send(query).await?;
        let ret = rx.recv().await.map_err(|_| Error::AddrSearchResultRx)?;
        Ok(ret)
    }

    pub async fn find_uncached(&mut self, name: String) -> Result<FindIocRes, Error> {
        let (qu, rx) = IocAddrQuery::uncached_rx(name);
        self.find(qu, rx).await
    }

    pub async fn find_cached(&mut self, name: String) -> Result<FindIocRes, Error> {
        let (qu, rx) = IocAddrQuery::cached_rx(name);
        self.find(qu, rx).await
    }

    pub fn qtx(&self) -> Sender<IocAddrQuery> {
        self.qtx.clone()
    }
}

pub fn start_finder_handle_v02(
    backend: String,
    opts: CaIngestOpts,
) -> (FinderHandleV02, JoinHandle<Result<(), Error>>) {
    info!("start_finder_handle_v02");
    let (qtx, qrx) = async_channel::bounded(CURRENT_SEARCH_PENDING_MAX);
    // old:
    // qtx -> qrx -> rtx -> rrx -> job-tx
    // new:
    // qtx -> qrx -> job-tx
    // let (rtx, rrx) = async_channel::bounded(CURRENT_SEARCH_PENDING_MAX);
    let jh = taskrun::spawn(finder_full(qrx, backend, opts));
    let fh = FinderHandleV02 { qtx };
    (fh, jh)
}

async fn finder_full<S>(qrx: S, backend: String, opts: CaIngestOpts) -> Result<(), Error>
where
    S: Stream<Item = IocAddrQuery> + Send + 'static,
{
    let selfname = "finder_full";
    let (tx1, rx1) = async_channel::bounded(20);
    let (tx2, rx2) = async_channel::bounded(20);
    let jh1 = taskrun::spawn(finder_worker(qrx, tx1, backend, opts.postgresql_config().clone()));
    let jh2 = taskrun::spawn(finder_network_if_not_found(rx2, opts.clone()));
    let jh3 = {
        let fut = async move {
            loop {
                match rx1.recv().await {
                    Ok(v) => {
                        trace!("{selfname}  rx1 recv  {v:?}");
                        let mut a = VecDeque::new();
                        for f in v {
                            match f {
                                FindIocCacheRes::Hit(x, mut tx) => {
                                    let _ = tx.send(x).await;
                                }
                                FindIocCacheRes::Miss(x) => {
                                    a.push_back(x);
                                }
                                FindIocCacheRes::Uncached(x) => {
                                    a.push_back(x);
                                }
                            }
                        }
                        match tx2.send(a).await {
                            Ok(()) => {}
                            Err(_) => {
                                debug!("{selfname}  can not send batch to network lookup");
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        };
        taskrun::spawn(fut)
    };
    jh1.await??;
    debug!("{selfname}  awaited A");
    jh2.await??;
    debug!("{selfname}  awaited B");
    jh3.await?;
    debug!("{selfname}  awaited C");
    Ok(())
}

async fn finder_worker<S>(
    qrx: S,
    tx: Sender<VecDeque<FindIocCacheRes>>,
    backend: String,
    db: Database,
) -> Result<(), Error>
where
    S: Stream<Item = IocAddrQuery> + Send + 'static,
{
    let selfname = "finder_worker";
    let batched = batchtools::batcher::Batcher2::new(qrx, Duration::from_millis(200), SEARCH_BATCH_MAX);
    let (batched_rx, jh_batch) = {
        let (batched_tx, batched_rx) = async_channel::bounded(SEARCH_DB_WORKER_CNT);
        let fut = async move {
            use futures::StreamExt;
            let mut batched = std::pin::pin!(batched);
            while let Some(item) = batched.next().await {
                trace!("{selfname}  {item:?}");
                match batched_tx.send(item).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("finder_worker  batched send error: {}", e);
                        break;
                    }
                }
            }
        };
        let jh = tokio::spawn(fut);
        (batched_rx, jh)
    };
    let mut jhs = Vec::new();
    for _ in 0..SEARCH_DB_WORKER_CNT {
        let jh = tokio::spawn(finder_worker_single(
            batched_rx.clone(),
            tx.clone(),
            backend.clone(),
            db.clone(),
        ));
        jhs.push(jh);
    }
    jh_batch.await?;
    trace!("finder_worker  jh_batch awaited");
    for (i, jh) in jhs.into_iter().enumerate() {
        jh.await??;
        trace!("finder_worker  single {} awaited", i);
    }
    Ok(())
}

#[derive(Debug)]
pub enum FindIocCacheRes {
    Hit(FindIocRes, asynchan::Sender<FindIocRes>),
    Miss(IocAddrQuery),
    Uncached(IocAddrQuery),
}

async fn finder_worker_single(
    inp: Receiver<VecDeque<IocAddrQuery>>,
    tx: Sender<VecDeque<FindIocCacheRes>>,
    backend: String,
    db: Database,
) -> Result<(), Error> {
    let selfname = "finder_worker_single";
    debug!("{selfname}  make_pg_client");
    let (pg, jh) = make_pg_client(&db).await?;
    let sql = concat!(
        "with q1 as (select * from unnest($2::int[], $3::text[]) as unn (n, ch))",
        " select distinct on (q1.n) q1.n, q1.ch, tt.channel, tt.addr, tt.tsmod",
        " from q1 left join ioc_by_channel_log tt",
        " on tt.channel = q1.ch and tt.facility = $1 and tt.archived = 0 and tt.addr is not null",
        " order by q1.n, tsmod desc",
    );
    let qu_select_multi = pg.prepare(sql).await?;
    debug!("{selfname}  before loop");
    loop {
        match inp.recv().await {
            Ok(batch) => {
                trace!("{selfname}  recv  {batch:?}");
                // TODO
                // stats.dbsearcher_batch_recv().inc();
                // stats.dbsearcher_item_recv().add(batch.len() as _);
                let ts1 = Instant::now();
                let (batch, pass_through) = batch.into_iter().fold((Vec::new(), Vec::new()), |(mut a, mut b), x| {
                    if series::dbg::dbg_chn(x.name()) {
                        trace!("searching database for  {:?}  use_cache {}", x.name(), x.use_cache());
                    }
                    if x.use_cache() {
                        a.push(x);
                    } else {
                        b.push(x);
                    }
                    (a, b)
                });
                debug_batch!("run  query batch  len {}", batch.len());
                let names: Vec<_> = batch.iter().map(|x| x.name()).collect();
                let ns: Vec<_> = names.iter().enumerate().map(|(i, _)| i as i32).collect();
                let qres = pg.query(&qu_select_multi, &[&backend, &ns, &names]).await;
                let dt = ts1.elapsed();
                debug_batch!(
                    "done query batch  len {}: {}  {:.3} ms",
                    batch.len(),
                    qres.is_ok(),
                    dt.as_secs_f32() * 1e3
                );
                if dt > Duration::from_millis(5000) {
                    warn!("very slow query");
                }
                match qres {
                    Ok(rows) => {
                        // stats.dbsearcher_select_res_0().add(rows.len() as _);
                        if rows.len() != batch.len() {
                            // stats.dbsearcher_select_error_len_mismatch().inc();
                            error!("query result len {}  batch len {}", rows.len(), batch.len());
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                            continue;
                        }
                        // for xx in rows.into_iter().zip(batch.into_iter()) {}
                        let rows2 = transform_pgres(rows);
                        let ret2: VecDeque<_> = pass_through
                            .into_iter()
                            .map(|x| FindIocCacheRes::Uncached(x))
                            .chain(batch.into_iter().zip(rows2).map(|(b, r)| match r {
                                Some(x) => FindIocCacheRes::Hit(x, b.into_name_tx().1),
                                None => FindIocCacheRes::Miss(b),
                            }))
                            .collect();
                        match tx.send(ret2).await {
                            Ok(_) => {
                                // TODO
                                // stats.dbsearcher_batch_send().inc();
                                // stats.dbsearcher_item_send().add(items_len as _);
                            }
                            Err(e) => {
                                // TODO metrics
                                error!("finder sees: {}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("finder sees error: {}", e);
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                    }
                }
            }
            Err(_e) => break,
        }
    }
    drop(pg);
    jh.await??;
    trace!("finder_worker_single done");
    Ok(())
}

async fn send_not_found_requests(
    reqs: VecDeque<IocAddrQuery>,
    net_tx: &mut async_channel::Sender<(String, asynchan::Sender<FindIocRes>)>,
) -> Result<(), Error> {
    let selfname = "send_not_found_requests";
    for x in reqs {
        let (name, tx) = x.into_name_tx();
        if series::dbg::dbg_chn(&name) {
            trace!("{selfname}  push to network lookup {name}");
        }
        net_tx.send((name, tx)).await.map_err(|_| Error::Send)?;
    }
    trace!("{selfname}  done");
    Ok(())
}

async fn finder_network_if_not_found(rx: Receiver<VecDeque<IocAddrQuery>>, opts: CaIngestOpts) -> Result<(), Error> {
    let selfname = "finder_network_if_not_found";
    let (mut net_tx, net_rx, jh_ca_search) = ca_search_workers_start(&opts).await?;
    let jh2 = taskrun::spawn(process_net_result(net_rx, opts.clone()));
    while let Ok(item) = rx.recv().await {
        send_not_found_requests(item, &mut net_tx).await?;
    }
    drop(net_tx);
    debug!("{selfname}  loop end");
    jh_ca_search.await??;
    debug!("{selfname}  jh_ca_search  awaited");
    jh2.await??;
    debug!("{selfname}  process_net_result  awaited");
    Ok(())
}

async fn process_net_result(
    net_rx: Receiver<Result<VecDeque<(FindIocRes, asynchan::Sender<FindIocRes>)>, crate::ca::findioc::Error>>,
    // tx: Sender<VecDeque<FindIocRes>>,
    opts: CaIngestOpts,
) -> Result<(), Error> {
    let selfname = "process_net_result";
    const IOC_SEARCH_INDEX_WORKER_COUNT: usize = 1;
    let (dbtx, dbrx) = async_channel::bounded(64);
    let mut ioc_search_index_worker_jhs = Vec::new();
    let mut index_worker_pg_jh = Vec::new();
    for _ in 0..IOC_SEARCH_INDEX_WORKER_COUNT {
        let backend = opts.backend().into();
        let (pg, jh) = dbpg::conn::make_pg_client(opts.postgresql_config()).await?;
        index_worker_pg_jh.push(jh);
        let worker = IocSearchIndexWorker::prepare(dbrx.clone(), backend, pg).await?;
        let jh = tokio::spawn(async move { worker.worker().await });
        ioc_search_index_worker_jhs.push(jh);
    }
    drop(dbrx);
    while let Ok(item) = net_rx.recv().await {
        match item {
            Ok(item) => {
                let mut cacheitems = Vec::new();
                for (e, mut tx) in item {
                    if series::dbg::dbg_chn(&e.channel()) {
                        trace!("{selfname}  push to cacheitems {e:?}");
                    }
                    let cacheitem = IocItem::new(
                        e.channel().into(),
                        e.response_addr().into(),
                        e.addr().into(),
                        e.dt().into(),
                    );
                    cacheitems.push(cacheitem);
                    match tx.send(e).await {
                        Ok(()) => {}
                        Err(_) => {
                            // Requester did not wait
                            // TODO count for metrics
                        }
                    }
                }
                for e in cacheitems {
                    let chn = e.channel.clone();
                    if let Err(_) = dbtx.send(e).await {
                        warn!("{selfname}  dbtx send error");
                        break;
                    } else {
                        if series::dbg::dbg_chn(&chn) {
                            trace!("{selfname}  sent to cache dbtx  {chn}");
                        }
                    }
                }
            }
            Err(e) => {
                warn!("error during network search: {}", e);
                break;
            }
        }
    }
    trace!("process_net_result  break loop");
    dbtx.close();
    trace!("process_net_result  dbtx closed");
    for (i, jh) in ioc_search_index_worker_jhs.into_iter().enumerate() {
        jh.await?;
        trace!("process_net_result  search index worker {} awaited", i);
    }
    Ok(())
}

#[cfg(feature = "disabled")]
fn start_finder_ca(tx: Sender<DaemonEvent>, tgts: Vec<SocketAddrV4>) -> (Sender<String>, JoinHandle<()>) {
    let (qtx, qrx) = async_channel::bounded(32);
    let (atx, arx) = async_channel::bounded(32);
    let ioc_finder_fut = async move {
        let mut finder = FindIocStream::new(tgts, FINDER_TIMEOUT, FINDER_IN_FLIGHT_MAX, FINDER_BATCH_SIZE);
        let fut_tick_dur = Duration::from_millis(100);
        let mut finder_more = true;
        let mut finder_fut = OptFut::new(finder.next());
        let mut qrx_fut = OptFut::new(qrx.recv());
        let mut qrx_more = true;
        let mut fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
        let mut asend = OptFut::empty();
        loop {
            tokio::select! {
                _ = &mut asend, if asend.is_enabled() => {
                    asend = OptFut::empty();
                }
                r1 = &mut finder_fut, if finder_fut.is_enabled() => {
                    finder_fut = OptFut::empty();
                    match r1 {
                        Some(item) => {
                            asend = OptFut::new(atx.send(item));
                        }
                        None => {
                            // TODO finder has stopped, do no longer poll on it
                            warn!("Finder has stopped");
                            finder_more = false;
                        }
                    }
                    if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                        qrx_fut = OptFut::new(qrx.recv());
                    }
                    if finder_more {
                        finder_fut = OptFut::new(finder.next());
                    }
                    fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
                }
                r2 = &mut qrx_fut, if qrx_fut.is_enabled() => {
                    qrx_fut = OptFut::empty();
                    match r2 {
                        Ok(item) => {
                            finder.push(item);
                        }
                        Err(e) => {
                            // TODO input is done... ignore from here on.
                            error!("Finder input channel error {}",e);
                            qrx_more = false;
                        }
                    }
                    if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                        qrx_fut = OptFut::new(qrx.recv());
                    }
                    if finder_more {
                        finder_fut = OptFut::new(finder.next());
                    } else {
                        finder_fut = OptFut::empty();
                    }
                    fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
                }
                _ = &mut fut_tick => {
                    if qrx_more && finder.job_queue_len() < FINDER_JOB_QUEUE_LEN_MAX {
                        qrx_fut = OptFut::new(qrx.recv());
                    }
                    if finder_more {
                        finder_fut = OptFut::new(finder.next());
                    } else {
                        finder_fut = OptFut::empty();
                    }
                    fut_tick = Box::pin(tokio::time::sleep(fut_tick_dur));
                }
                else => {
                    error!("all branches are disabled");
                    break;
                }
            };
        }
    };
    let ioc_finder_jh = taskrun::spawn(ioc_finder_fut);
    taskrun::spawn({
        async move {
            while let Ok(item) = arx.recv().await {
                todo!("send the result item");
            }
            warn!("search res fwd inp closed");
        }
    });
    (qtx, ioc_finder_jh)
}
