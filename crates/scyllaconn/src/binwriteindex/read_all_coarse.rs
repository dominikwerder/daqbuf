use super::BinWriteIndexRtStream;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::TryStreamExt;
use items_0::streamitem::Sitemty3;
use items_0::streamitem::sitem3_data;
use netpod::DtMs;
use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use query::api4::scyllaopts::ScyllaOptsQuery;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "BinIndexReadAllCoarse"),
    enum variants {
        Worker(#[from] crate::worker::Error),
        BinWriteIndexRead(#[from] super::Error),
    },
);

async fn read_all_coarse(
    series: SeriesId,
    range: NanoRange,
    scylla_opts: ScyllaOptsQuery,
    scyqueue: &ScyllaQueue,
) -> Result<VecDeque<(RetentionTime, MspU32, LspU32, DtMs)>, Error> {
    let rts = {
        use RetentionTime::*;
        [Long, Medium, Short]
    };
    let mut ret = VecDeque::new();
    for rt in rts {
        let pbp = PrebinnedPartitioning::Day1;
        let mut stream = BinWriteIndexRtStream::new(
            rt.clone(),
            series,
            pbp,
            range.clone(),
            scylla_opts.clone(),
            scyqueue.clone(),
        );
        while let Some(x) = stream.try_next().await? {
            match x.into_data() {
                Ok(x) => {
                    for e in x.entries {
                        let binlen = DtMs::from_ms_u64(e.binlen.to_u32() as u64);
                        let item = (rt.clone(), x.msp.clone(), e.lsp, binlen);
                        ret.push_back(item);
                    }
                }
                Err(x) => {
                    // TODO check for other item types.
                    // match directly instead of into-helper.
                }
            }
        }
    }
    Ok(ret)
}

pub fn select_potential_binlen(options: VecDeque<(RetentionTime, MspU32, LspU32, DtMs)>) -> Result<(), Error> {
    // Check first if there are common binlen over all the range.
    // If not, filter out the options which could build content from finer resolution.
    // Then heuristically select the best match.
    // PrebinnedPartitioning::Day1.msp_lsp(val)
    todo!()
}

pub struct ReadAllCoarse {
    scyqueue: Arc<ScyllaQueue>,
    fut: Option<Pin<Box<dyn Future<Output = Result<VecDeque<(RetentionTime, MspU32, LspU32, DtMs)>, Error>> + Send>>>,
    results: VecDeque<(RetentionTime, MspU32, LspU32, DtMs)>,
}

impl ReadAllCoarse {
    pub fn new(series: SeriesId, range: NanoRange, scylla_opts: ScyllaOptsQuery, scyqueue: ScyllaQueue) -> Self {
        let scyqueue = Arc::new(scyqueue);
        let fut = {
            let scyqueue = scyqueue.clone();
            async move { read_all_coarse(series, range, scylla_opts, &scyqueue).await }
        };
        Self {
            scyqueue,
            fut: Some(Box::pin(fut)),
            results: VecDeque::new(),
        }
    }
}

impl Stream for ReadAllCoarse {
    type Item = Sitemty3<(RetentionTime, MspU32, LspU32, DtMs), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match self.fut.as_mut() {
                Some(fut) => match fut.poll_unpin(cx) {
                    Ready(x) => {
                        self.fut = None;
                        match x {
                            Ok(x) => {
                                self.results.extend(x);
                                continue;
                            }
                            Err(e) => Ready(Some(Err(e))),
                        }
                    }
                    Pending => Pending,
                },
                None => {
                    if let Some(item) = self.results.pop_front() {
                        Ready(Some(sitem3_data(item)))
                    } else {
                        Ready(None)
                    }
                }
            };
        }
    }
}
