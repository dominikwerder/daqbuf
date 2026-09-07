use netfetch::ca::connset2::connset::ConnSet;
use netfetch::ca::connset2::connset::ConnSetCmder;
use netfetch::conf::CaIngestOptsV2;
use netfetch::conf::ChannelsConfig;
use scywr::insertset::ScyllaInsertSet;
use scywr::insertset::ScyllaInsertSetOpts;
use scywr::insertworker::InsertWorkerOpts;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

pub struct Daemon {
    #[allow(unused)]
    ingest_opts: CaIngestOptsV2,
    #[allow(unused)]
    connset: ConnSet,
    #[allow(unused)]
    cmder: ConnSetCmder,
    // TODO hand `insert_set.input()` to the connset once conn2 emits QueryItem.
    #[allow(unused)]
    insert_set: ScyllaInsertSet,
}

impl Daemon {
    pub async fn new(ingest_opts: CaIngestOptsV2, channels_config: Option<ChannelsConfig>) -> Result<Self, err::Error> {
        let local_epics_hostname = ingest_linux::net::local_hostname();
        let connset = ConnSet::new(ingest_opts.backend().into(), local_epics_hostname, ingest_opts.clone())
            .await
            .map_err(err::Error::from_string)?;
        let cmder = connset.cmder().clone();
        let insert_set = Self::make_insert_set(&ingest_opts).await?;
        if let Some(channels_config) = channels_config {
            let cmder = cmder.clone();
            taskrun::spawn(async move {
                for ch_cfg in channels_config.channels() {
                    if let Err(e) = cmder.channel_add(ch_cfg.clone()).await {
                        log::error!("daemon2 initial channel_add error {e}");
                    }
                }
            });
        }
        Ok(Self {
            ingest_opts,
            connset,
            cmder,
            insert_set,
        })
    }

    async fn make_insert_set(ingest_opts: &CaIngestOptsV2) -> Result<ScyllaInsertSet, err::Error> {
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
        let insert_set = ScyllaInsertSet::new(confs, opts, Arc::new(insert_worker_opts))
            .await
            .map_err(err::Error::from_string)?;
        Ok(insert_set)
    }
}
