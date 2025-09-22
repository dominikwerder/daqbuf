use crate::ErrConv;
use crate::create_connection;
use crate::delay_io_medium;
use crate::delay_io_short;
use async_channel::Receiver;
use async_channel::bounded;
use chrono::DateTime;
use chrono::Utc;
use daqbuf_err as err;
use err::Error;
use futures_util::FutureExt;
use futures_util::Stream;
use netpod::Database;
use netpod::NodeConfigCached;
use netpod::log::*;
use pin_project::pin_project;
use serde::Deserialize;
use serde::Serialize;
use std::future::Future;
use std::io::ErrorKind;
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio;
use tokio::fs::DirEntry;
use tokio::fs::ReadDir;
use tokio_postgres::Client;

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeDiskIdent {
    pub rowid: i64,
    pub facility: i64,
    pub split: i32,
    pub hostname: String,
}

impl NodeDiskIdent {
    pub fn rowid(&self) -> i64 {
        self.rowid
    }
    pub fn facility(&self) -> i64 {
        self.facility
    }
}

fn _get_hostname() -> Result<String, Error> {
    let out = std::process::Command::new("hostname").output()?;
    Ok(String::from_utf8(out.stdout[..out.stdout.len() - 1].to_vec())?)
}

pub async fn get_node_disk_ident(node_config: &NodeConfigCached, dbc: &Client) -> Result<NodeDiskIdent, Error> {
    info!("get_node_disk_ident");
    let sql = "select nodes.rowid, facility, split, hostname from nodes, facilities where facilities.name = $1 and facility = facilities.rowid and hostname = $2";
    let rows = dbc
        .query(sql, &[&node_config.node_config.cluster.backend, &node_config.node.host])
        .await
        .err_conv()?;
    if rows.len() != 1 {
        return Err(Error::with_msg(format!(
            "get_node_disk_ident can not find unique entry for {:?} {:?}",
            node_config.node.host, node_config.node_config.cluster.backend
        )));
    }
    let row = &rows[0];
    Ok(NodeDiskIdent {
        rowid: row.get(0),
        facility: row.get(1),
        split: row.get(2),
        hostname: row.get(3),
    })
}

pub async fn get_node_disk_ident_2(
    node_config: Pin<&NodeConfigCached>,
    dbc: Pin<&Client>,
) -> Result<NodeDiskIdent, Error> {
    let sql = "select nodes.rowid, facility, split, hostname from nodes, facilities where facilities.name = $1 and facility = facilities.rowid and hostname = $2";
    let rows = dbc
        .query(sql, &[&node_config.node_config.cluster.backend, &node_config.node.host])
        .await
        .err_conv()?;
    if rows.len() != 1 {
        return Err(Error::with_msg(format!(
            "get_node can't find unique entry for {} {}",
            node_config.node.host, node_config.node_config.cluster.backend
        )));
    }
    let row = &rows[0];
    Ok(NodeDiskIdent {
        rowid: row.get(0),
        facility: row.get(1),
        split: row.get(2),
        hostname: row.get(3),
    })
}

#[pin_project]
pub struct FindChannelNamesFromConfigReadDir {
    #[pin]
    read_dir_fut: Option<Pin<Box<dyn Future<Output = std::io::Result<ReadDir>> + Send>>>,
    #[pin]
    read_dir: Option<Pin<Box<ReadDir>>>,
    #[pin]
    done: bool,
}

impl FindChannelNamesFromConfigReadDir {
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            read_dir_fut: Some(Box::pin(tokio::fs::read_dir(base_dir.as_ref().join("config")))),
            read_dir: None,
            done: false,
        }
    }
}

impl Stream for FindChannelNamesFromConfigReadDir {
    type Item = Result<DirEntry, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = span!(Level::INFO, "FindChNameCfgDir");
        let _spg = span.enter();
        let mut pself = self.project();
        loop {
            break if *pself.done {
                Ready(None)
            } else if let Some(mut fut) = pself.read_dir.as_mut().as_pin_mut() {
                match fut.poll_next_entry(cx) {
                    Ready(Ok(Some(item))) => Ready(Some(Ok(item))),
                    Ready(Ok(None)) => {
                        *pself.done = true;
                        Ready(None)
                    }
                    Ready(Err(e)) => {
                        *pself.done = true;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = pself.read_dir_fut.as_mut().as_pin_mut() {
                match fut.poll(cx) {
                    Ready(Ok(item)) => {
                        *pself.read_dir_fut = None;
                        *pself.read_dir = Some(Box::pin(item));
                        continue;
                    }
                    Ready(Err(e)) => {
                        *pself.done = true;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => Pending,
                }
            } else {
                Pending
            };
        }
    }
}

async fn find_channel_names_from_config(base_dir: impl AsRef<Path>) -> Result<Vec<String>, Error> {
    let mut ret = Vec::new();
    let path2: PathBuf = base_dir.as_ref().join("config");
    let mut rd = tokio::fs::read_dir(&path2).await?;
    while let Ok(Some(entry)) = rd.next_entry().await {
        let fname = String::from_utf8(entry.file_name().into_vec())?;
        ret.push(fname);
    }
    Ok(ret)
}

#[allow(unused)]
async fn update_db_with_channel_name_list(list: Vec<String>, backend: i64, dbc: &Client) -> Result<(), Error> {
    delay_io_short().await;
    dbc.query("begin", &[]).await.err_conv()?;
    for ch in list {
        dbc.query(
            "insert into channels (facility, name) values ($1, $2) on conflict do nothing",
            &[&backend, &ch],
        )
        .await
        .err_conv()?;
    }
    dbc.query("commit", &[]).await.err_conv()?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatedDbWithChannelNames {
    msg: String,
    count: u32,
}

async fn update_db_with_channel_names_inner(
    tx: async_channel::Sender<Result<UpdatedDbWithChannelNames, Error>>,
    node_config: NodeConfigCached,
    db_config: Database,
) -> Result<(), Error> {
    let (dbc, _pgjh) = create_connection(&db_config).await?;
    info!("update_db_with_channel_names connection done");
    let node_disk_ident = match get_node_disk_ident(&node_config, &dbc).await {
        Ok(x) => {
            // info!("node_disk_ident {:?}", x);
            debug!("node_disk_ident {:?}", x);
            x
        }
        Err(e) => {
            let p1 = &node_config.node.host;
            let p2 = &node_config.node_config.cluster.backend;
            error!("can not get_node_disk_ident  {:?}  {:?}", p1, p2);
            return Err(e);
        }
    };
    info!("update_db_with_channel_names get_node_disk_ident done");
    let insert_sql = concat!(
        "insert into channels (facility, name) select facility, name from (values ($1::bigint, $2::text)) v1 (facility, name)",
        " where not exists (select 1 from channels t1 where t1.facility = v1.facility and t1.name = v1.name)",
        " on conflict do nothing",
    );
    if false {
        let fac: i64 = 1;
        let ch = format!("tmp_dummy_04");
        let ret = dbc
            .query(insert_sql, &[&fac, &ch])
            .await
            .err_conv()
            .map_err(|e| format!("in channel name insert: {e}"));
        info!("DUMMY INSERT ATTEMPT: {ret:?}");
    }
    let mut c1 = 0;
    dbc.query("begin", &[]).await.err_conv()?;
    let dbc = Arc::new(dbc);
    let tx = Arc::new(tx);
    let base_path = &node_config
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
        .data_base_path;
    debug!("base_path {:?}", base_path);
    let channel_names = find_channel_names_from_config(base_path).await?;
    for ch in channel_names {
        if ch.contains("BEAT") || ch.contains("BEAM") {
            debug!("ch {:?}", ch);
        }
        debug!("ch {:?}", ch);
        let fac = node_disk_ident.facility;
        crate::delay_io_short().await;
        let ret = dbc
            .query(insert_sql, &[&fac, &ch])
            .await
            .err_conv()
            .map_err(|e| format!("in channel name insert: {e}"));
        let ret = match ret {
            Ok(x) => x,
            Err(e) => {
                error!("failed insert attempt {e}");
                Err(e)?
            }
        };
        {
            let n = ret.len();
            if n > 0 {
                info!("insert n {n}");
            }
        }
        c1 += 1;
        if c1 % 200 == 0 {
            dbc.query("commit", &[]).await.err_conv()?;
            let ret = UpdatedDbWithChannelNames {
                msg: format!("current {}", ch),
                count: c1,
            };
            tx.send(Ok(ret)).await.err_conv()?;
            delay_io_medium().await;
            dbc.query("begin", &[]).await.err_conv()?;
        }
    }
    dbc.query("commit", &[]).await.err_conv()?;
    let ret = UpdatedDbWithChannelNames {
        msg: format!("all done"),
        count: c1,
    };
    tx.send(Ok(ret)).await.err_conv()?;
    Ok::<_, Error>(())
}

pub async fn update_db_with_channel_names(
    node_config: NodeConfigCached,
    db_config: &Database,
) -> Result<Pin<Box<Receiver<Result<UpdatedDbWithChannelNames, Error>>>>, Error> {
    info!("update_db_with_channel_names");
    let (tx, rx) = bounded(16);
    let tx2 = tx.clone();
    let db_config = db_config.clone();
    let block1 = update_db_with_channel_names_inner(tx, node_config.clone(), db_config.clone());
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx2.send(Err(e)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not report error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(Box::pin(rx))
}

pub fn update_db_with_channel_names_3(
    node_config: &NodeConfigCached,
) -> impl Stream<Item = Result<UpdatedDbWithChannelNames, Error>> + 'static {
    let base_path = &node_config
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))
        .unwrap()
        .data_base_path;
    futures_util::future::ready(base_path.clone())
        .then(|path| tokio::fs::read_dir(path))
        .map(Result::unwrap)
        .map(|rd| {
            futures_util::stream::unfold(rd, move |rd| {
                //let fut = rd.next_entry();
                futures_util::future::ready(Ok(None)).map(move |item: Result<Option<u32>, Error>| match item {
                    Ok(Some(item)) => Some((item, rd)),
                    Ok(None) => None,
                    Err(_e) => None,
                })
            })
        })
        .map(|_conf| Err(Error::with_msg("TODO")))
        .into_stream()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatedDbWithAllChannelConfigs {
    msg: String,
    count: u32,
}

async fn update_db_with_all_channel_configs_inner(
    tx: async_channel::Sender<Result<UpdatedDbWithAllChannelConfigs, Error>>,
    node_config: NodeConfigCached,
) -> Result<(), Error> {
    let node_config = &node_config;
    let (dbc, _pgjh) = create_connection(&node_config.node_config.cluster.database).await?;
    let dbc = Arc::new(dbc);
    let node_disk_ident = match get_node_disk_ident(node_config, &dbc).await {
        Ok(x) => {
            // info!("node_disk_ident {:?}", x);
            debug!("node_disk_ident {:?}", x);
            x
        }
        Err(e) => {
            let p1 = &node_config.node.host;
            let p2 = &node_config.node_config.cluster.backend;
            error!("ww can not get_node_disk_ident  {:?}  {:?}", p1, p2);
            return Err(e);
        }
    };
    let rows = dbc
        .query(
            "select rowid, facility, name from channels where facility = $1 order by facility, name",
            &[&node_disk_ident.facility],
        )
        .await
        .err_conv()?;
    let mut c1 = 0;
    dbc.query("begin", &[]).await.err_conv()?;
    let mut count_inserted = 0;
    let mut count_updated = 0;
    let mut count_config_not_found = 0;
    for row in rows {
        let rowid: i64 = row.try_get(0).err_conv()?;
        let _facility: i64 = row.try_get(1).err_conv()?;
        let channel: String = row.try_get(2).err_conv()?;
        debug!("updating config {:?}", channel);
        match update_db_with_channel_config(
            node_config,
            &node_disk_ident,
            rowid,
            &channel,
            dbc.clone(),
            &mut count_inserted,
            &mut count_updated,
        )
        .await
        {
            Err(e) => {
                error!("{:?}", e);
                delay_io_medium().await;
                // TODO recover, open new transaction, test recovery.
                return Err(e);
            }
            Ok(UpdateChannelConfigResult::NotFound) => {
                //warn!("can not find channel config {}", channel);
                count_config_not_found += 1;
                delay_io_short().await;
            }
            Ok(UpdateChannelConfigResult::Done) => {
                c1 += 1;
                if c1 % 200 == 0 {
                    dbc.query("commit", &[]).await.err_conv()?;
                    let msg = format!(
                        "channel no {:6}  inserted {:6}  updated {:6}",
                        c1, count_inserted, count_updated
                    );
                    let ret = UpdatedDbWithAllChannelConfigs { msg, count: c1 };
                    tx.send(Ok(ret)).await.err_conv()?;
                    dbc.query("begin", &[]).await.err_conv()?;
                }
                delay_io_short().await;
            }
        }
    }
    dbc.query("commit", &[]).await.err_conv()?;
    let msg = format!(
        "ALL DONE  channel no {:6}  inserted {:6}  updated {:6}  not_found {:6}",
        c1, count_inserted, count_updated, count_config_not_found,
    );
    let ret = UpdatedDbWithAllChannelConfigs { msg, count: c1 };
    tx.send(Ok(ret)).await.err_conv()?;
    Ok::<_, Error>(())
}

pub async fn update_db_with_all_channel_configs(
    node_config: NodeConfigCached,
) -> Result<Pin<Box<Receiver<Result<UpdatedDbWithAllChannelConfigs, Error>>>>, Error> {
    let (tx, rx) = bounded(16);
    let tx2 = tx.clone();
    let tx3 = tx.clone();
    let block1 = update_db_with_all_channel_configs_inner(tx, node_config).then({
        |item| async move {
            match item {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("Seeing error: {:?}", e);
                    let ret = UpdatedDbWithAllChannelConfigs { msg, count: 0 };
                    tx2.send(Ok(ret)).await.err_conv()?;
                }
            }
            Ok::<_, Error>(())
        }
    });
    let block2 = async move {
        match block1.await {
            Ok(_) => {}
            Err(e) => match tx3.send(Err(e)).await {
                Ok(_) => {}
                Err(e) => {
                    error!("can not deliver error through channel: {:?}", e);
                }
            },
        }
    };
    tokio::spawn(block2);
    Ok(Box::pin(rx))
}

pub async fn update_search_cache(node_config: &NodeConfigCached) -> Result<bool, Error> {
    let (dbc, _pgjh) = create_connection(&node_config.node_config.cluster.database).await?;
    dbc.query("select update_cache()", &[])
        .await
        .err_conv()
        .map_err(|e| format!("error update_search_cache: {e}"))?;
    Ok(true)
}

pub enum UpdateChannelConfigResult {
    NotFound,
    Done,
}

/**
Parse the config of the given channel and update database.
*/
async fn update_db_with_channel_config(
    node_config: &NodeConfigCached,
    node_disk_ident: &NodeDiskIdent,
    channel_id: i64,
    channel: &str,
    dbc: Arc<Client>,
    count_inserted: &mut usize,
    count_updated: &mut usize,
) -> Result<UpdateChannelConfigResult, Error> {
    let base_path = &node_config
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
        .data_base_path;
    let path = base_path
        .join("config")
        .join(channel)
        .join("latest")
        .join("00000_Config");
    let meta = if let Ok(k) = tokio::fs::metadata(&path).await {
        k
    } else {
        return Ok(UpdateChannelConfigResult::NotFound);
    };
    if meta.len() > 40 * 1024 * 1024 {
        return Err(Error::with_msg("meta data too long {meta:?}"));
    }
    let rows = dbc
        .query(
            "select rowid, fileSize, parsedUntil, channel from configs where node = $1 and channel = $2",
            &[&node_disk_ident.rowid(), &channel_id],
        )
        .await
        .err_conv()?;
    if rows.len() > 1 {
        return Err(Error::with_msg("more than one row"));
    }
    let (config_id, do_parse) = if let Some(row) = rows.first() {
        let rowid: i64 = row.get(0);
        let file_size: u32 = row.get::<_, i64>(1) as u32;
        let parsed_until: u32 = row.get::<_, i64>(2) as u32;
        let _channel_id = row.get::<_, i64>(2) as i64;
        if meta.len() < file_size as u64 || meta.len() < parsed_until as u64 {
            let sql = concat!(
                "insert into configs_history (rowid_original, node, channel, fileSize, parsedUntil, config, tsinsert) ",
                "select rowid as rowid_original, node, channel, fileSize, parsedUntil, config, now() from configs where rowid = $1"
            );
            dbc.query(sql, &[&rowid])
                .await
                .err_conv()
                .map_err(|e| format!("on config history insert {e}"))?;
        }
        //ensure!(meta.len() >= parsed_until as u64, ConfigFileOnDiskShrunk{path});
        (Some(rowid), true)
    } else {
        (None, true)
    };
    if do_parse {
        let buf = tokio::fs::read(&path).await?;
        let config = parse::channelconfig::parse_config(&buf).map_err(|e| Error::from(e.to_string()))?;
        match config_id {
            None => {
                dbc.query(
                    "insert into configs (node, channel, fileSize, parsedUntil, config) values ($1, $2, $3, $4, $5) on conflict (node, channel) do update set fileSize = $3, parsedUntil = $4, config = $5",
                    &[
                        &node_disk_ident.rowid(),
                        &channel_id,
                        &(meta.len() as i64),
                        &(buf.len() as i64),
                        &serde_json::to_value(config)?,
                    ],
                ).await.err_conv().map_err(|e| format!("on config insert {e}"))?;
                *count_inserted += 1;
            }
            Some(_config_id) => {
                dbc.query(
                    "update configs set fileSize = $3, parsedUntil = $4, config = $5 where node = $1 and channel = $2",
                    &[
                        &node_disk_ident.rowid(),
                        &channel_id,
                        &(meta.len() as i64),
                        &(buf.len() as i64),
                        &serde_json::to_value(config)?,
                    ],
                )
                .await
                .err_conv()
                .map_err(|e| format!("on config update {e}"))?;
                *count_updated += 1;
            }
        }
    }
    Ok(UpdateChannelConfigResult::Done)
}

pub async fn update_db_with_all_channel_datafiles(
    node_config: &NodeConfigCached,
    node_disk_ident: &NodeDiskIdent,
    ks_prefix: &str,
) -> Result<(), Error> {
    let (dbc, _pgjh) = create_connection(&node_config.node_config.cluster.database).await?;
    let dbc = Arc::new(dbc);
    let rows = dbc
        .query(
            "select rowid, facility, name from channels where facility = $1 order by facility, name",
            &[&node_disk_ident.facility()],
        )
        .await
        .err_conv()?;
    let mut c1 = 0;
    dbc.query("begin", &[]).await.err_conv()?;
    for row in rows {
        let rowid: i64 = row.try_get(0).err_conv()?;
        let _facility: i64 = row.try_get(1).err_conv()?;
        let channel: String = row.try_get(2).err_conv()?;
        update_db_with_channel_datafiles(node_config, node_disk_ident, ks_prefix, rowid, &channel, dbc.clone()).await?;
        c1 += 1;
        if c1 % 40 == 0 {
            trace!("import datafiles  {}  {}", c1, channel);
            dbc.query("commit", &[]).await.err_conv()?;
            dbc.query("begin", &[]).await.err_conv()?;
        }
        if false && c1 >= 30 {
            break;
        }
    }
    dbc.query("commit", &[]).await.err_conv()?;
    Ok(())
}

struct DatafileDbWriter {
    channel_id: i64,
    node_id: i64,
    dbc: Arc<Client>,
    c1: Arc<RwLock<u32>>,
}

#[derive(Debug, Serialize)]
pub struct ChannelDesc {
    name: String,
}

#[derive(Debug, Serialize)]
pub struct ChannelDatafileDesc {
    channel: ChannelDesc,
    ks: u32,
    tb: u32,
    sp: u32,
    bs: u32,
    fs: u64,
    mt: DateTime<Utc>,
    ix_fs: Option<u64>,
    ix_mt: Option<DateTime<Utc>>,
}

impl ChannelDatafileDesc {
    pub fn timebin(&self) -> u32 {
        self.tb
    }
    pub fn binsize(&self) -> u32 {
        self.bs
    }
    pub fn keyspace(&self) -> u32 {
        self.ks
    }
    pub fn split(&self) -> u32 {
        self.sp
    }
}

pub trait ChannelDatafileDescSink {
    fn sink(&self, k: ChannelDatafileDesc) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>;
}

impl ChannelDatafileDescSink for DatafileDbWriter {
    fn sink(&self, k: ChannelDatafileDesc) -> Pin<Box<dyn Future<Output = Result<(), Error>>>> {
        let dbc = self.dbc.clone();
        let c1 = self.c1.clone();
        let channel_id = self.channel_id;
        let node_id = self.node_id;
        Box::pin(async move {
            dbc.query(
                "insert into datafiles (node, channel, tsbeg, tsend, props) values ($1, $2, $3, $4, $5) on conflict do nothing",
                &[
                    &node_id,
                    &channel_id,
                    &(k.timebin() as i64 * k.binsize() as i64),
                    &((k.timebin() + 1) as i64 * k.binsize() as i64),
                    &serde_json::to_value(k)?,
                ]
            ).await.err_conv()?;
            *c1.write()? += 1;
            Ok(())
        })
    }
}

pub async fn find_channel_datafiles_in_ks(
    base_dir: impl AsRef<Path>,
    ks_prefix: &str,
    ks: u32,
    channel: &str,
    cb: &dyn ChannelDatafileDescSink,
) -> Result<Option<()>, Error> {
    let data_dir_path: PathBuf = base_dir
        .as_ref()
        .join(format!("{}_{}", ks_prefix, ks))
        .join("byTime")
        .join(channel);
    let re1 = regex::Regex::new(r"^\d{19}$")?;
    let re2 = regex::Regex::new(r"^\d{10}$")?;
    let re4 = regex::Regex::new(r"^(\d{19})_0{5}_Data$")?;
    let mut rd = match tokio::fs::read_dir(&data_dir_path).await {
        Ok(k) => k,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => return Ok(None),
            _ => Err(e)?,
        },
    };
    while let Ok(Some(entry)) = rd.next_entry().await {
        let fname = String::from_utf8(entry.file_name().into_vec())?;
        if !re1.is_match(&fname) {
            warn!("unexpected file  {}", fname);
            continue;
        }
        let timebin: u32 = fname.parse()?;
        let path = data_dir_path.join(fname);
        let mut rd = tokio::fs::read_dir(&path).await?;
        while let Ok(Some(entry)) = rd.next_entry().await {
            let fname = String::from_utf8(entry.file_name().into_vec())?;
            if !re2.is_match(&fname) {
                warn!("unexpected file  {}", fname);
                continue;
            }
            let split: u32 = fname.parse()?;
            let path = path.join(fname);
            let mut rd = tokio::fs::read_dir(&path).await?;
            while let Ok(Some(entry)) = rd.next_entry().await {
                let fname = String::from_utf8(entry.file_name().into_vec())?;
                if let Some(m) = re4.captures(&fname) {
                    let binsize: u32 = m.get(1).unwrap().as_str().parse()?;
                    let data_path = path.join(&fname);
                    let meta = tokio::fs::metadata(&data_path).await?;
                    let index_path = path.join(format!("{}_Index", fname));
                    let (ix_size, ix_tmod) = if let Ok(meta) = tokio::fs::metadata(&index_path).await {
                        (Some(meta.len()), Some(meta.modified().unwrap().into()))
                    } else {
                        (None, None)
                    };
                    cb.sink(ChannelDatafileDesc {
                        channel: ChannelDesc { name: channel.into() },
                        ks: ks,
                        tb: timebin,
                        sp: split,
                        bs: binsize,
                        fs: meta.len(),
                        ix_fs: ix_size,
                        ix_mt: ix_tmod,
                        mt: meta.modified().unwrap().into(),
                    })
                    .await?;
                }
            }
        }
    }
    Ok(Some(()))
}

pub async fn update_db_with_channel_datafiles(
    node_config: &NodeConfigCached,
    node_disk_ident: &NodeDiskIdent,
    ks_prefix: &str,
    channel_id: i64,
    channel: &str,
    dbc: Arc<Client>,
) -> Result<(), Error> {
    let base_path = &node_config
        .node
        .sf_databuffer
        .as_ref()
        .ok_or_else(|| Error::with_msg(format!("missing sf databuffer config in node")))?
        .data_base_path;
    let writer = DatafileDbWriter {
        node_id: node_disk_ident.rowid(),
        channel_id: channel_id,
        dbc: dbc.clone(),
        c1: Arc::new(RwLock::new(0)),
    };
    let mut n_nothing = 0;
    for ks in &[2, 3, 4] {
        match find_channel_datafiles_in_ks(base_path, ks_prefix, *ks, channel, &writer).await {
            /*Err(Error::ChannelDatadirNotFound { .. }) => {
                n_nothing += 1;
            }*/
            Ok(None) => {
                n_nothing += 1;
            }
            x => {
                x?;
            }
        };
        if false && *writer.c1.read()? >= 10 {
            break;
        }
    }
    if n_nothing >= 3 {
        //warn!("No datafile directories in any keyspace  writer got {:5}  n_nothing {}  channel {}", writer.c1.borrow(), n_nothing, channel);
    }
    Ok(())
}
