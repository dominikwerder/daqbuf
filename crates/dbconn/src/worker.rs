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
use taskrun::tokio;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

autoerr::create_error_v1!(
    name(Error, "PgWorker"),
    enum variants {
        Error(#[from] err::Error),
        ChannelSend,
        ChannelRecv,
        Join,
        ChannelConfig(#[from] crate::channelconfig::Error),
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

fn is_retryable_io_error(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::HostUnreachable
            | ErrorKind::NetworkUnreachable
            | ErrorKind::NetworkDown
            | ErrorKind::TimedOut
            | ErrorKind::NotConnected
            | ErrorKind::BrokenPipe
            | ErrorKind::Interrupted
    )
}

/// Determines if a PostgreSQL error code indicates a retryable condition
fn is_retryable_db_error_code(code: &tokio_postgres::error::SqlState) -> bool {
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

fn is_retryable_postgres_error(error: &tokio_postgres::Error) -> bool {
    if error.is_closed() {
        true
    } else if let Some(db_error) = error.as_db_error() {
        is_retryable_db_error_code(db_error.code())
    } else if let Some(e2) = std::error::Error::source(error) {
        if let Some(e3) = e2.downcast_ref::<std::io::Error>() {
            is_retryable_io_error(e3.kind())
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
    rx: Receiver<Job>,
    pg: Client,
    pgjh: Option<JoinHandle<Result<(), err::Error>>>,
}

impl PgWorker {
    pub async fn new(pgconf: &Database) -> Result<(PgQueue, Self), Error> {
        let (tx, rx) = async_channel::bounded(64);
        let (pg, pgjh) = create_connection(pgconf).await?;
        let queue = PgQueue { tx };
        let worker = Self {
            rx,
            pg,
            pgjh: Some(pgjh),
        };
        Ok((queue, worker))
    }

    async fn do_job_attempt(&mut self, job: Job) -> Result<(), Error> {
        match job {
            Job::ChConfBestMatchingNameRange(channel, range, tx) => {
                let res = crate::channelconfig::chconf_best_matching_for_name_and_range(channel, range, &self.pg).await;
                if let Err(e) = &res {
                    if let Some(e2) = GetOptionPostgresError::get_option_postgres_error(e) {
                        if is_retryable_postgres_error(e2) {
                            // TODO
                        } else {
                            // TODO
                        }
                    }
                }
                if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                }
            }
            Job::ChConfForSeries(backend, series, tx) => {
                let res = crate::channelconfig::chconf_for_series(&backend, series, &self.pg).await;
                if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                }
            }
            Job::InfoForSeriesIds(ids, tx) => {
                let res = crate::channelinfo::info_for_series_ids(&ids, &self.pg).await;
                if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                }
            }
            Job::SearchChannel(query, tx) => {
                let res = crate::search::search_channel_scylla(query, &self.pg).await;
                if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                }
            }
            Job::SfChannelBySeries(query, tx) => {
                let res = find_sf_channel_by_series(query, &self.pg).await;
                if tx.send(res.map_err(Into::into)).await.is_err() {
                    // TODO count for stats
                }
            }
        }
        Ok(())
    }

    async fn do_job(&mut self, job: Job) -> Result<(), Error> {
        loop {
            match self.do_job_attempt(job).await {
                Ok(()) => break,
                Err(e) => match e {
                    Error::Error(error) => todo!(),
                    Error::ChannelSend => todo!(),
                    Error::ChannelRecv => todo!(),
                    Error::Join => todo!(),
                    Error::ChannelConfig(error) => todo!(),
                },
            }
        }
        Ok(())
    }

    async fn work_inner(mut self) -> Result<(), Error> {
        loop {
            let x = self.rx.recv().await;
            let job = match x {
                Ok(x) => x,
                Err(_) => {
                    error!("PgWorker can not receive from channel");
                    return Err(Error::ChannelRecv);
                }
            };
            self.do_job(job).await?;
        }
    }

    pub async fn work(self) -> Result<(), Error> {
        self.work_inner().await
    }

    pub async fn join(&mut self) -> Result<(), Error> {
        if let Some(jh) = self.pgjh.take() {
            jh.await.map_err(|_| Error::Join)?.map_err(Error::from)?;
            Ok(())
        } else {
            Ok(())
        }
    }

    pub fn close(&self) {
        self.rx.close();
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
