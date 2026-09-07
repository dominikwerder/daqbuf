use netfetch::ca::connset2::connset::ConnSet;
use netfetch::ca::connset2::connset::ConnSetCmder;
use netfetch::conf::CaIngestOptsV2;
use netfetch::conf::ChannelsConfig;

pub struct Daemon {
    #[allow(unused)]
    ingest_opts: CaIngestOptsV2,
    #[allow(unused)]
    connset: ConnSet,
    #[allow(unused)]
    cmder: ConnSetCmder,
}

impl Daemon {
    pub async fn new(
        ingest_opts: CaIngestOptsV2,
        channels_config: Option<ChannelsConfig>,
    ) -> Result<Self, err::Error> {
        let local_epics_hostname = ingest_linux::net::local_hostname();
        let connset = ConnSet::new(ingest_opts.backend().into(), local_epics_hostname, ingest_opts.clone())
            .await
            .map_err(err::Error::from_string)?;
        let cmder = connset.cmder().clone();
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
        })
    }
}
