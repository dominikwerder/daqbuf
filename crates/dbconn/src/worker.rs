use crate::create_connection;
use async_channel::Receiver;
use async_channel::RecvError;
use async_channel::Sender;
use daqbuf_err as err;
use netpod::ChConf;
use netpod::ChannelSearchQuery;
use netpod::ChannelSearchResult;
use netpod::Database;
use netpod::SeriesKind;
use netpod::SfDbChannel;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use std::io::ErrorKind;
use std::time::Duration;
use taskrun::tokio;
use tokio_postgres::Client;

autoerr::create_error_v1!(
    name(Error, "PgWorker"),
    enum variants {
        Error(#[from] err::Error),
        ChannelSend,
        ChannelRecv,
        Join,
        ChannelConfig(#[from] crate::channelconfig::Error),
        DatabaseConnectionBad,
    },
);

impl From<RecvError> for Error {
    fn from(_value: RecvError) -> Self {
        Self::ChannelRecv
    }
}

impl err::ToErr for Error {
    fn to_err(self) -> err::Error {
        err::Error::from_string(self)
    }
}

pub trait GetOptionPostgresError {
    fn get_option_postgres_error(&self) -> Option<&tokio_postgres::Error>;
}

fn io_error_is_database_connection_issue(kind: ErrorKind) -> bool {
    match kind {
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::HostUnreachable
        | ErrorKind::NetworkUnreachable
        | ErrorKind::NetworkDown
        | ErrorKind::TimedOut
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe
        | ErrorKind::UnexpectedEof
        | ErrorKind::Interrupted => true,
        _ => false,
    }
}

fn code_is_database_connection_issue(code: &tokio_postgres::error::SqlState) -> bool {
    let c = code.code();
    if c.len() != 5 {
        false
    } else {
        if &c[..3] == "080" {
            true
        } else if &c[..3] == "08P" {
            true
        } else if &c[..2] == "52" {
            true
        } else {
            false
        }
    }
}

fn is_retryable_postgres_error(e1: &tokio_postgres::Error) -> bool {
    if e1.is_closed() {
        true
    } else if let Some(e2) = e1.as_db_error() {
        code_is_database_connection_issue(e2.code())
    } else if let Some(e2) = std::error::Error::source(e1) {
        if let Some(e3) = e2.downcast_ref::<std::io::Error>() {
            io_error_is_database_connection_issue(e3.kind())
        } else {
            false
        }
    } else {
        false
    }
}

impl GetOptionPostgresError for crate::channelinfo::Error {
    fn get_option_postgres_error(&self) -> Option<&tokio_postgres::Error> {
        match self {
            Self::Postgres(e) => Some(e),
            _ => None,
        }
    }
}

impl GetOptionPostgresError for crate::search::Error {
    fn get_option_postgres_error(&self) -> Option<&tokio_postgres::Error> {
        match self {
            Self::Postgres(e) => Some(e),
            _ => None,
        }
    }
}

impl GetOptionPostgresError for crate::FindChannelError {
    fn get_option_postgres_error(&self) -> Option<&tokio_postgres::Error> {
        match self {
            Self::Postgres(e) => Some(e),
            _ => None,
        }
    }
}

fn is_bad_connection<T, E>(res: &Result<T, E>) -> bool
where
    E: GetOptionPostgresError,
{
    if let Err(e) = res {
        if let Some(e2) = e.get_option_postgres_error() {
            is_retryable_postgres_error(e2)
        } else {
            false
        }
    } else {
        false
    }
}

#[derive(Debug)]
enum Job {
    ChConfBestMatchingNameRange(
        SfDbChannel,
        NanoRange,
        Sender<Result<ChConf, crate::channelconfig::Error>>,
    ),
    ChConfForSeries(String, u64, Sender<Result<ChConf, crate::channelconfig::Error>>),
    InfoForSeriesIds(
        Vec<u64>,
        Sender<Result<Vec<Option<crate::channelinfo::ChannelInfo>>, crate::channelinfo::Error>>,
    ),
    SearchChannel(
        ChannelSearchQuery,
        Sender<Result<ChannelSearchResult, crate::search::Error>>,
    ),
    SfChannelBySeries(
        netpod::SfDbChannel,
        Sender<Result<netpod::SfDbChannel, crate::FindChannelError>>,
    ),
}

#[derive(Debug, Clone)]
pub struct PgQueue {
    tx: Sender<Job>,
}

impl PgQueue {
    pub async fn chconf_best_matching_name_range(
        &self,
        channel: SfDbChannel,
        range: NanoRange,
    ) -> Result<Result<ChConf, crate::channelconfig::Error>, netpod::AsyncChannelError> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ChConfBestMatchingNameRange(channel, range, tx);
        self.tx.send(job).await.map_err(|_| netpod::AsyncChannelError::Send)?;
        let res = rx.recv().await.map_err(|_| netpod::AsyncChannelError::Recv)?;
        Ok(res)
    }

    pub async fn chconf_for_series(
        &self,
        backend: &str,
        series: u64,
    ) -> Result<Result<ChConf, crate::channelconfig::Error>, netpod::AsyncChannelError> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::ChConfForSeries(backend.into(), series, tx);
        self.tx.send(job).await.map_err(|_| netpod::AsyncChannelError::Send)?;
        let res = rx.recv().await.map_err(|_| netpod::AsyncChannelError::Recv)?;
        Ok(res)
    }

    pub async fn info_for_series_ids(
        &self,
        series_ids: Vec<u64>,
    ) -> Result<Receiver<Result<Vec<Option<crate::channelinfo::ChannelInfo>>, crate::channelinfo::Error>>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::InfoForSeriesIds(series_ids, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        Ok(rx)
    }

    pub async fn search_channel_scylla(
        &self,
        query: ChannelSearchQuery,
    ) -> Result<Result<ChannelSearchResult, crate::search::Error>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::SearchChannel(query, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let ret = rx.recv().await?;
        Ok(ret)
    }

    pub async fn find_sf_channel_by_series(
        &self,
        query: netpod::SfDbChannel,
    ) -> Result<Result<netpod::SfDbChannel, crate::FindChannelError>, Error> {
        let (tx, rx) = async_channel::bounded(1);
        let job = Job::SfChannelBySeries(query, tx);
        self.tx.send(job).await.map_err(|_| Error::ChannelSend)?;
        let ret = rx.recv().await?;
        Ok(ret)
    }
}

#[derive(Debug)]
pub struct PgWorker {
    pgconf: Database,
    rx: Receiver<Job>,
}

impl PgWorker {
    pub async fn new(pgconf: &Database) -> Result<(PgQueue,), Error> {
        let (tx, rx) = async_channel::bounded(64);
        let worker = Self {
            pgconf: pgconf.clone(),
            rx,
        };
        // TODO await join handle
        #[allow(unused)]
        let jh = taskrun::spawn(async move {
            let x = worker.work().await;
            match x {
                Ok(()) => {}
                Err(e) => {
                    error!("received error from PgWorker: {}", e);
                }
            }
        });
        let queue = PgQueue { tx };
        Ok((queue,))
    }

    async fn do_job_attempt(&mut self, job: Job, pg: &Client) -> Result<(), Error> {
        match job {
            Job::ChConfBestMatchingNameRange(channel, range, tx) => {
                let res = crate::channelconfig::chconf_best_matching_for_name_and_range(channel, range, pg).await;
                if is_bad_connection(&res) {
                    Err(Error::DatabaseConnectionBad)
                } else if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                    Ok(())
                } else {
                    Ok(())
                }
            }
            Job::ChConfForSeries(backend, series, tx) => {
                let res = crate::channelconfig::chconf_for_series(&backend, series, pg).await;
                if is_bad_connection(&res) {
                    Err(Error::DatabaseConnectionBad)
                } else if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                    Ok(())
                } else {
                    Ok(())
                }
            }
            Job::InfoForSeriesIds(ids, tx) => {
                let res = crate::channelinfo::info_for_series_ids(&ids, pg).await;
                if is_bad_connection(&res) {
                    Err(Error::DatabaseConnectionBad)
                } else if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                    Ok(())
                } else {
                    Ok(())
                }
            }
            Job::SearchChannel(query, tx) => {
                let res = crate::search::search_channel_scylla(query, pg).await;
                if is_bad_connection(&res) {
                    Err(Error::DatabaseConnectionBad)
                } else if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                    Ok(())
                } else {
                    Ok(())
                }
            }
            Job::SfChannelBySeries(query, tx) => {
                let res = find_sf_channel_by_series(query, pg).await;
                if is_bad_connection(&res) {
                    Err(Error::DatabaseConnectionBad)
                } else if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                    Ok(())
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn do_job(&mut self, job: Job, pg: &Client) -> Result<(), Error> {
        self.do_job_attempt(job, pg).await
    }

    async fn work_inner(&mut self, pg: &Client) -> Result<(), Error> {
        let selfname = "PgWorker::work_inner";
        loop {
            match self.rx.recv().await {
                Ok(job) => {
                    self.do_job(job, pg).await?;
                }
                Err(_) => {
                    error!("{selfname} can not receive from channel");
                    break Err(Error::ChannelRecv);
                }
            }
        }
    }

    async fn work(mut self) -> Result<(), Error> {
        let selfname = "PgWorker::work";
        loop {
            if self.rx.is_closed() {
                info!("Postgres worker input queue closed, exiting");
                break Ok(());
            }
            let (pg, pgjh) = create_connection(&self.pgconf).await?;
            match self.work_inner(&pg).await {
                Ok(()) => {}
                Err(e) => match e {
                    Error::ChannelRecv => {
                        if self.rx.is_closed() == false {
                            warn!("{selfname} sees {e} error, but channel not closed");
                        }
                    }
                    Error::DatabaseConnectionBad => {
                        warn!("{selfname} sees {e}");
                    }
                    _ => {}
                },
            }
            drop(pg);
            match tokio::time::timeout(Duration::from_millis(8000), pgjh).await {
                Ok(Ok(Ok(()))) => {}
                Ok(x) => {
                    warn!("{selfname} postgres connection join unclean: {x:?}");
                }
                Err(_) => {
                    warn!("{selfname} drop postgres connection after await timeout");
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(rand::random_range(1200..3500))).await;
        }
    }
}

// On sf-databuffer, the channel name identifies the series. But we can also have a series id.
// This function is used if the request provides only the series-id, but no name.
async fn find_sf_channel_by_series(
    channel: SfDbChannel,
    pgclient: &Client,
) -> Result<SfDbChannel, crate::FindChannelError> {
    use crate::FindChannelError;
    debug!("find_sf_channel_by_series  {:?}", channel);
    let series = channel.series().ok_or_else(|| FindChannelError::BadSeriesId)?;
    let sql = "select rowid from facilities where name = $1";
    let rows = pgclient.query(sql, &[&channel.backend()]).await?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| FindChannelError::UnknownBackend)?;
    let backend_id: i64 = row.get(0);
    let sql = "select name from channels where facility = $1 and rowid = $2";
    let rows = pgclient.query(sql, &[&backend_id, &(series as i64)]).await?;
    if rows.len() > 1 {
        return Err(FindChannelError::MultipleFound);
    }
    if let Some(row) = rows.into_iter().next() {
        let name = row.get::<_, String>(0);
        let channel = SfDbChannel::from_full(channel.backend(), channel.series(), name, SeriesKind::default());
        Ok(channel)
    } else {
        return Err(FindChannelError::NoFound);
    }
}
