pub mod channelconfig;
pub mod channelinfo;
pub mod scan;
pub mod search;
pub mod worker;

pub mod pg {
    pub use tokio_postgres::Client;
    pub use tokio_postgres::Error;
    pub use tokio_postgres::NoTls;
    pub use tokio_postgres::Statement;
    pub use tokio_postgres::types::Type;
}

use daqbuf_err as err;
use err::Error;
use err::Res2;
use err::anyhow;
use netpod::Database;
use netpod::NodeConfigCached;
use netpod::SfDbChannel;
use netpod::TableSizes;
use netpod::log::*;
use pg::Client as PgClient;
use pg::NoTls;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use taskrun::tokio;
use tokio::task::JoinHandle;

trait ErrConv<T> {
    fn err_conv(self) -> Result<T, Error>;
}

impl<T> ErrConv<T> for Result<T, tokio_postgres::Error> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg(e.to_string())),
        }
    }
}

impl<T, A> ErrConv<T> for Result<T, async_channel::SendError<A>> {
    fn err_conv(self) -> Result<T, Error> {
        match self {
            Ok(k) => Ok(k),
            Err(e) => Err(Error::with_msg(e.to_string())),
        }
    }
}

pub async fn delay_us(mu: u64) {
    tokio::time::sleep(Duration::from_micros(mu)).await;
}

pub async fn delay_io_short() {
    delay_us(500).await;
}

pub async fn delay_io_medium() {
    delay_us(2000).await;
}

pub async fn create_connection(db_config: &Database) -> Result<(PgClient, JoinHandle<Result<(), Error>>), Error> {
    info!("create_connection  creating postgres connection");
    // TODO use a common already running worker pool for these queries:
    let d = db_config;
    let uri = format!("postgresql://{}:{}@{}:{}/{}", d.user, d.pass, d.host, d.port, d.name);
    let (cl, conn) = tokio_postgres::connect(&uri, NoTls)
        .await
        .map_err(|e| format!("Can not connect to database: {e}"))?;
    let jh = tokio::spawn(async move {
        match conn.await {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("connection error: {}", e);
                Err(Error::from_string(e))
            }
        }
    });
    Ok((cl, jh))
}

pub async fn channel_exists(channel_name: &str, node_config: &NodeConfigCached) -> Result<bool, Error> {
    let (cl, _pgjh) = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl
        .query("select rowid from channels where name = $1::text", &[&channel_name])
        .await
        .err_conv()?;
    debug!("channel_exists  {} rows", rows.len());
    for row in rows {
        debug!(
            "  db on channel search: {:?}  {:?}  {:?}",
            row,
            row.columns(),
            row.get::<_, i64>(0)
        );
    }
    Ok(true)
}

pub async fn database_size(node_config: &NodeConfigCached) -> Result<u64, Error> {
    let (cl, _pgjh) = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl
        .query(
            "select pg_database_size($1::text)",
            &[&node_config.node_config.cluster.database.name],
        )
        .await
        .err_conv()?;
    if rows.len() == 0 {
        Err(Error::with_msg("could not get database size"))?;
    }
    let size: i64 = rows[0].get(0);
    let size = size as u64;
    Ok(size)
}

pub async fn table_sizes(node_config: &NodeConfigCached) -> Result<TableSizes, Error> {
    let sql = format!(
        "{} {} {} {} {} {} {}",
        "SELECT nspname || '.' || relname AS relation, pg_size_pretty(pg_total_relation_size(C.oid)) AS total_size",
        "FROM pg_class C",
        "LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)",
        "WHERE nspname NOT IN ('pg_catalog', 'information_schema')",
        "AND C.relkind <> 'i'",
        "AND nspname !~ '^pg_toast'",
        "ORDER BY pg_total_relation_size(C.oid) DESC LIMIT 20",
    );
    let sql = sql.as_str();
    let (cl, _pgjh) = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl.query(sql, &[]).await.err_conv()?;
    let mut sizes = TableSizes { sizes: Vec::new() };
    sizes.sizes.push((format!("table"), format!("size")));
    for row in rows {
        sizes.sizes.push((row.get(0), row.get(1)));
    }
    Ok(sizes)
}

pub async fn random_channel(node_config: &NodeConfigCached) -> Result<String, Error> {
    let sql = "select name from channels order by rowid limit 1 offset (random() * (select count(rowid) from channels))::bigint";
    let (cl, _pgjh) = create_connection(&node_config.node_config.cluster.database).await?;
    let rows = cl.query(sql, &[]).await.err_conv()?;
    if rows.len() == 0 {
        Err(Error::with_msg("can not get random channel"))?;
    }
    Ok(rows[0].get(0))
}

pub async fn insert_channel(name: String, facility: i64, dbc: &PgClient) -> Result<(), Error> {
    let rows = dbc
        .query(
            "select count(rowid) from channels where facility = $1 and name = $2",
            &[&facility, &name],
        )
        .await
        .err_conv()?;
    if rows[0].get::<_, i64>(0) == 0 {
        let sql =
            concat!("insert into channels (facility, name) values ($1, $2) on conflict (facility, name) do nothing");
        dbc.query(sql, &[&facility, &name]).await.err_conv()?;
    }
    Ok(())
}

// Currently only for sf-databuffer type backends
// Note: we currently treat the channels primary key as series-id for sf-databuffer type backends.
pub async fn find_series_sf_databuffer(channel: &SfDbChannel, pgclient: Arc<PgClient>) -> Res2<u64> {
    debug!("find_series_sf_databuffer  {:?}", channel);
    let sql = "select rowid from facilities where name = $1";
    let rows = pgclient.query(sql, &[&channel.backend()]).await.err_conv()?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no backend for {channel:?}"))?;
    let backend_id: i64 = row.get(0);
    let sql = "select rowid from channels where facility = $1 and name = $2";
    let rows = pgclient.query(sql, &[&backend_id, &channel.name()]).await.err_conv()?;
    if rows.len() < 1 {
        return Err(anyhow::anyhow!("No series found for {channel:?}"));
    }
    if rows.len() > 1 {
        return Err(anyhow::anyhow!("Multiple series found for {channel:?}"));
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("No series found for {channel:?}"))?;
    let series = row.get::<_, i64>(0) as u64;
    Ok(series)
}

autoerr::create_error_v1!(
    name(FindChannelError, "FindChannel"),
    enum variants {
        UnknownBackend,
        BadSeriesId,
        NoFound,
        MultipleFound,
        Postgres(tokio_postgres::Error),
    },
);

impl From<tokio_postgres::Error> for FindChannelError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::Postgres(value)
    }
}
