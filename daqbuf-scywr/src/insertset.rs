use crate::config::ScyllaInsertSetConfig;
use crate::insertqueues::InsertDeques;
use crate::insertqueues::InsertQueuesTx;
use crate::insertqueues::InsertQueuesTxMetrics;
use crate::insertqueues::make_pair_cap;
use crate::insertworker;
use crate::insertworker::InsertWorkerOpts;
use crate::insertworker::InsertWorkerOutputItem;
use crate::iteminsertqueue::InsertTarget;
use crate::iteminsertqueue::QueryItem;
use async_channel::Receiver;
use async_channel::Sender;
use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use taskrun::tokio;
use tokio::task::JoinHandle;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ); }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "ScyllaInsertSet"),
    enum variants {
        ChannelSend(InsertTarget),
        InsertWorker(#[from] insertworker::Error),
    },
);

pub const TARGETS: [InsertTarget; 4] = [
    InsertTarget::StRf1,
    InsertTarget::StRf3,
    InsertTarget::MtRf3,
    InsertTarget::LtRf3,
];

#[derive(Debug, Clone)]
pub struct ScyllaInsertSetOpts {
    pub insert_scylla_sessions: usize,
    pub insert_worker_count: usize,
    pub insert_worker_concurrency: usize,
    pub insert_item_queue_cap: usize,
    pub input_queue_cap: usize,
    pub use_rate_limit_queue: bool,
    pub ignore_writes: bool,
    pub batch_len_max: usize,
    pub batch_interval: Duration,
}

impl Default for ScyllaInsertSetOpts {
    fn default() -> Self {
        Self {
            insert_scylla_sessions: 1,
            insert_worker_count: 10,
            insert_worker_concurrency: 64,
            insert_item_queue_cap: 1000 * 100,
            input_queue_cap: 128,
            use_rate_limit_queue: false,
            ignore_writes: false,
            batch_len_max: 256,
            batch_interval: Duration::from_millis(200),
        }
    }
}

pub struct ScyllaInsertSetMetrics {
    pub input_len: usize,
    pub clusters: Vec<InsertQueuesTxMetrics>,
}

impl fmt::Display for ScyllaInsertSetMetrics {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "ScyllaInsertSetMetrics {{ input_len: {}", self.input_len)?;
        for (i, x) in self.clusters.iter().enumerate() {
            write!(
                fmt,
                ", cluster_{i}: {{ st_rf1: {}, st_rf3: {}, mt_rf3: {}, lt_rf3: {} }}",
                x.st_rf1_len, x.st_rf3_len, x.mt_rf3_len, x.lt_rf3_len
            )?;
        }
        write!(fmt, " }}")
    }
}

pub struct ScyllaInsertSet {
    input_tx: Sender<VecDeque<QueryItem>>,
    sinks: Vec<InsertQueuesTx>,
    sorter_jh: Option<JoinHandle<()>>,
    worker_jhs: Vec<JoinHandle<Result<(), insertworker::Error>>>,
    out_rx: Receiver<InsertWorkerOutputItem>,
}

impl ScyllaInsertSet {
    pub async fn new(
        confs: Vec<ScyllaInsertSetConfig>,
        opts: ScyllaInsertSetOpts,
        insert_worker_opts: Arc<InsertWorkerOpts>,
    ) -> Result<Self, Error> {
        let (input_tx, input_rx) = async_channel::bounded(opts.input_queue_cap);
        let (out_tx, out_rx) = async_channel::bounded(256);
        let mut worker_jhs = Vec::new();
        if confs.is_empty() {
            warn!("no scylla cluster configured, spawn dummy insert workers");
            let jhs = insertworker::spawn_scylla_insert_workers_dummy(
                opts.insert_worker_count,
                opts.insert_worker_concurrency,
                input_rx,
                insert_worker_opts,
                out_tx,
            )
            .await?;
            worker_jhs.extend(jhs);
            let ret = Self {
                input_tx,
                sinks: Vec::new(),
                sorter_jh: None,
                worker_jhs,
                out_rx,
            };
            return Ok(ret);
        }
        let mut sinks = Vec::new();
        for conf in &confs {
            let (iqtx, iqrx) = make_pair_cap(opts.insert_item_queue_cap);
            for target in TARGETS {
                let scyconf = conf.for_target(target).clone();
                info!("spawn insert workers  {}  {}", target, scyconf.short_name());
                let jhs = insertworker::spawn_scylla_insert_workers(
                    target.retention_time(),
                    scyconf,
                    opts.insert_scylla_sessions,
                    opts.insert_worker_count,
                    opts.insert_worker_concurrency,
                    iqrx.receiver_for_target(target).clone(),
                    insert_worker_opts.clone(),
                    opts.use_rate_limit_queue,
                    opts.ignore_writes,
                    out_tx.clone(),
                )
                .await?;
                worker_jhs.extend(jhs);
            }
            sinks.push(iqtx);
        }
        let sorter_jh = spawn_sorter(input_rx, sinks.clone(), &opts);
        let ret = Self {
            input_tx,
            sinks,
            sorter_jh: Some(sorter_jh),
            worker_jhs,
            out_rx,
        };
        Ok(ret)
    }

    pub fn input(&self) -> Sender<VecDeque<QueryItem>> {
        self.input_tx.clone()
    }

    pub fn sinks(&self) -> &[InsertQueuesTx] {
        &self.sinks
    }

    pub fn output(&self) -> &Receiver<InsertWorkerOutputItem> {
        &self.out_rx
    }

    pub fn queue_metrics(&self) -> ScyllaInsertSetMetrics {
        ScyllaInsertSetMetrics {
            input_len: self.input_tx.len(),
            clusters: self.sinks.iter().map(InsertQueuesTxMetrics::from).collect(),
        }
    }

    pub async fn shutdown(self) -> Result<(), Error> {
        debug!("shutdown  close input");
        self.input_tx.close();
        if let Some(jh) = self.sorter_jh {
            if let Err(e) = jh.await {
                error!("shutdown  sorter join error {e}");
            }
        }
        drop(self.sinks);
        for jh in self.worker_jhs {
            match jh.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("shutdown  insert worker error {e}");
                }
                Err(e) => {
                    error!("shutdown  insert worker join error {e}");
                }
            }
        }
        debug!("shutdown  done");
        Ok(())
    }
}

fn spawn_sorter(
    input_rx: Receiver<VecDeque<QueryItem>>,
    sinks: Vec<InsertQueuesTx>,
    opts: &ScyllaInsertSetOpts,
) -> JoinHandle<()> {
    let batch_len_max = opts.batch_len_max;
    let batch_interval = opts.batch_interval;
    tokio::spawn(sorter(input_rx, sinks, batch_len_max, batch_interval))
}

async fn sorter(
    input_rx: Receiver<VecDeque<QueryItem>>,
    sinks: Vec<InsertQueuesTx>,
    batch_len_max: usize,
    batch_interval: Duration,
) {
    if sinks.is_empty() {
        error!("sorter without sinks");
        return;
    }
    let mut acc = InsertDeques::new();
    let mut tick = tokio::time::interval(batch_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let do_break = tokio::select! {
            x = input_rx.recv() => match x {
                Ok(batch) => {
                    for item in batch {
                        acc.deque_for_target(item.target()).push_back(item);
                    }
                    if acc.len() >= batch_len_max {
                        flush(&mut acc, &sinks).await.is_err()
                    } else {
                        false
                    }
                }
                Err(_) => {
                    debug!("sorter  input closed");
                    if let Err(e) = flush(&mut acc, &sinks).await {
                        error!("sorter  final flush error {e}");
                    }
                    true
                }
            },
            _ = tick.tick() => {
                let res = if acc.len() != 0 {
                    flush(&mut acc, &sinks).await.is_err()
                } else {
                    false
                };
                acc.housekeeping();
                res
            }
        };
        if do_break {
            break;
        }
    }
    debug!("sorter  done");
}

async fn flush(acc: &mut InsertDeques, sinks: &[InsertQueuesTx]) -> Result<(), Error> {
    let n = sinks.len();
    if n == 0 {
        return Ok(());
    }
    let mut per_sink: Vec<Vec<(InsertTarget, VecDeque<QueryItem>)>> = (0..n).map(|_| Vec::new()).collect();
    for target in TARGETS {
        let qu = acc.deque_for_target(target);
        if qu.is_empty() {
            continue;
        }
        let batch = mem::replace(qu, VecDeque::new());
        for i in 0..n - 1 {
            per_sink[i].push((target, batch.clone()));
        }
        per_sink[n - 1].push((target, batch));
    }
    let futs = sinks.iter().zip(per_sink).map(|(sink, items)| async move {
        for (target, batch) in items {
            match sink.sender_for_target(target).send(batch).await {
                Ok(()) => {}
                Err(_) => {
                    error!("can not send to lane {target}");
                    return Err(Error::ChannelSend(target));
                }
            }
        }
        Ok::<_, Error>(())
    });
    futures_util::future::try_join_all(futs).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::insertqueues::InsertQueuesRx;
    use crate::iteminsertqueue::MspItem;
    use netpod::TsMs;
    use series::SeriesId;
    use std::time::Instant;

    autoerr::create_error_v1!(
        name(TestError, "ScyllaInsertSetTest"),
        enum variants {
            InsertSet(#[from] Error),
            Timeout,
            Recv,
        },
    );

    fn item(target: InsertTarget, id: u64) -> QueryItem {
        QueryItem::Msp(MspItem::new(
            target,
            SeriesId::new(id),
            TsMs::from_ms_u64(id),
            Instant::now(),
        ))
    }

    fn opts_for_test() -> ScyllaInsertSetOpts {
        ScyllaInsertSetOpts {
            batch_len_max: 4,
            batch_interval: Duration::from_millis(50),
            insert_item_queue_cap: 16,
            input_queue_cap: 16,
            ..Default::default()
        }
    }

    async fn recv_ids(rx: &InsertQueuesRx, target: InsertTarget) -> Result<Vec<u64>, TestError> {
        let fut = rx.receiver_for_target(target).recv();
        match tokio::time::timeout(Duration::from_millis(2000), fut).await {
            Ok(Ok(batch)) => {
                for x in batch.iter() {
                    assert_eq!(x.target(), target);
                }
                let ids = batch
                    .iter()
                    .map(|x| match x {
                        QueryItem::Msp(x) => x.series().id(),
                        _ => panic!("unexpected item kind"),
                    })
                    .collect();
                Ok(ids)
            }
            Ok(Err(_)) => Err(TestError::Recv),
            Err(_) => Err(TestError::Timeout),
        }
    }

    #[test]
    fn sorts_items_into_the_lane_of_their_target() {
        let fut = async {
            let opts = opts_for_test();
            let (tx, rx) = make_pair_cap(opts.insert_item_queue_cap);
            let (inp_tx, inp_rx) = async_channel::bounded(opts.input_queue_cap);
            let jh = spawn_sorter(inp_rx, vec![tx], &opts);
            let batch: VecDeque<_> = [
                item(InsertTarget::LtRf3, 1),
                item(InsertTarget::StRf1, 2),
                item(InsertTarget::MtRf3, 3),
                item(InsertTarget::StRf3, 4),
                item(InsertTarget::LtRf3, 5),
            ]
            .into_iter()
            .collect();
            inp_tx.send(batch).await.unwrap();
            assert_eq!(recv_ids(&rx, InsertTarget::StRf1).await?, vec![2]);
            assert_eq!(recv_ids(&rx, InsertTarget::StRf3).await?, vec![4]);
            assert_eq!(recv_ids(&rx, InsertTarget::MtRf3).await?, vec![3]);
            assert_eq!(recv_ids(&rx, InsertTarget::LtRf3).await?, vec![1, 5]);
            inp_tx.close();
            jh.await.unwrap();
            Ok::<_, TestError>(())
        };
        taskrun::run(fut).unwrap();
    }

    #[test]
    fn duplicates_every_batch_into_both_clusters() {
        let fut = async {
            let opts = opts_for_test();
            let (tx1, rx1) = make_pair_cap(opts.insert_item_queue_cap);
            let (tx2, rx2) = make_pair_cap(opts.insert_item_queue_cap);
            let (inp_tx, inp_rx) = async_channel::bounded(opts.input_queue_cap);
            let jh = spawn_sorter(inp_rx, vec![tx1, tx2], &opts);
            let batch: VecDeque<_> = [
                item(InsertTarget::MtRf3, 10),
                item(InsertTarget::MtRf3, 11),
                item(InsertTarget::LtRf3, 12),
            ]
            .into_iter()
            .collect();
            inp_tx.send(batch).await.unwrap();
            assert_eq!(recv_ids(&rx1, InsertTarget::MtRf3).await?, vec![10, 11]);
            assert_eq!(recv_ids(&rx2, InsertTarget::MtRf3).await?, vec![10, 11]);
            assert_eq!(recv_ids(&rx1, InsertTarget::LtRf3).await?, vec![12]);
            assert_eq!(recv_ids(&rx2, InsertTarget::LtRf3).await?, vec![12]);
            inp_tx.close();
            jh.await.unwrap();
            Ok::<_, TestError>(())
        };
        taskrun::run(fut).unwrap();
    }

    // Items of several input batches are re-batched, and the flush happens on `batch_len_max`.
    #[test]
    fn rebatches_up_to_batch_len_max() {
        let fut = async {
            let opts = opts_for_test();
            assert_eq!(opts.batch_len_max, 4);
            let (tx, rx) = make_pair_cap(opts.insert_item_queue_cap);
            let (inp_tx, inp_rx) = async_channel::bounded(opts.input_queue_cap);
            let jh = spawn_sorter(inp_rx, vec![tx], &opts);
            for id in 1..=4 {
                let batch: VecDeque<_> = [item(InsertTarget::LtRf3, id)].into_iter().collect();
                inp_tx.send(batch).await.unwrap();
            }
            // The four single-item inputs are emitted as one batch.
            assert_eq!(recv_ids(&rx, InsertTarget::LtRf3).await?, vec![1, 2, 3, 4]);
            inp_tx.close();
            jh.await.unwrap();
            Ok::<_, TestError>(())
        };
        taskrun::run(fut).unwrap();
    }

    // A partially filled accumulator is flushed by the interval alone.
    #[test]
    fn flushes_partial_batch_on_interval() {
        let fut = async {
            let opts = opts_for_test();
            let (tx, rx) = make_pair_cap(opts.insert_item_queue_cap);
            let (inp_tx, inp_rx) = async_channel::bounded(opts.input_queue_cap);
            let jh = spawn_sorter(inp_rx, vec![tx], &opts);
            let batch: VecDeque<_> = [item(InsertTarget::StRf3, 7)].into_iter().collect();
            inp_tx.send(batch).await.unwrap();
            assert_eq!(recv_ids(&rx, InsertTarget::StRf3).await?, vec![7]);
            inp_tx.close();
            jh.await.unwrap();
            Ok::<_, TestError>(())
        };
        taskrun::run(fut).unwrap();
    }

    // Closing the input flushes what is left and ends the sorter.
    #[test]
    fn final_flush_on_closed_input() {
        let fut = async {
            let opts = ScyllaInsertSetOpts {
                // Long enough that only the final flush can deliver.
                batch_interval: Duration::from_millis(1000 * 3600),
                ..opts_for_test()
            };
            let (tx, rx) = make_pair_cap(opts.insert_item_queue_cap);
            let (inp_tx, inp_rx) = async_channel::bounded(opts.input_queue_cap);
            let jh = spawn_sorter(inp_rx, vec![tx], &opts);
            let batch: VecDeque<_> = [item(InsertTarget::MtRf3, 21)].into_iter().collect();
            inp_tx.send(batch).await.unwrap();
            inp_tx.close();
            assert_eq!(recv_ids(&rx, InsertTarget::MtRf3).await?, vec![21]);
            jh.await.unwrap();
            Ok::<_, TestError>(())
        };
        taskrun::run(fut).unwrap();
    }

    // A config with keyspace "none" lets the insert workers drain without a live cluster.
    #[test]
    fn construct_and_shutdown_without_cluster() {
        let fut = async {
            let mk = |rt| crate::config::ScyllaIngestConfig::new(Vec::<String>::new(), "none", rt);
            let conf = crate::config::ScyllaInsertSetConfig::new(
                mk(netpod::ttl::RetentionTime::Short),
                mk(netpod::ttl::RetentionTime::Short),
                mk(netpod::ttl::RetentionTime::Medium),
                mk(netpod::ttl::RetentionTime::Long),
            );
            let iws = ScyllaInsertSet::new(
                vec![conf],
                opts_for_test(),
                Arc::new(InsertWorkerOpts {
                    store_workers_rate: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
                    insert_workers_running: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                    insert_frac: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
                    array_truncate: Arc::new(std::sync::atomic::AtomicU64::new(1024)),
                }),
            )
            .await?;
            let batch: VecDeque<_> = [item(InsertTarget::LtRf3, 1)].into_iter().collect();
            iws.input().send(batch).await.unwrap();
            assert_eq!(iws.queue_metrics().clusters.len(), 1);
            iws.shutdown().await?;
            Ok::<_, TestError>(())
        };
        taskrun::run(fut).unwrap();
    }
}
