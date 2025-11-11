pub mod bwxcmb;
pub mod read_all_coarse;

use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::streamitem::Sitemty3;
use items_0::streamitem::sitem3_data;
use log::log_item_emit as lg;
use netpod::DtMs;
use netpod::UseScylla6Workarounds;
use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }

macro_rules! info_item {
    ($($arg:tt)*) => {
        {
            let item = items_0::streamitem::LogItem::info(format!($($arg)*));
            streams::logqueue::push_log_item(item);
        }
    };
}

macro_rules! debug_item {
    ($($arg:tt)*) => {
        {
            let item = items_0::streamitem::LogItem::debug(format!($($arg)*));
            streams::logqueue::push_log_item(item);
        }
    };
}

autoerr::create_error_v1!(
    name(Error, "BinWriteIndexRtStream"),
    enum variants {
        Worker(#[from] crate::worker::Error),
    },
);

struct Fut1(Fut2);

impl fmt::Debug for Fut1 {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("Fut1").finish()
    }
}

type Fut2 =
    Pin<Box<dyn Future<Output = Result<(u32, u32, u32, VecDeque<BinWriteIndexEntry>), crate::worker::Error>> + Send>>;

#[derive(Debug)]
pub struct BinWriteIndexEntry {
    pub lsp: u32,
    pub binlen: u32,
}

#[derive(Debug)]
pub struct BinWriteIndexSet {
    pub msp: MspU32,
    pub entries: VecDeque<BinWriteIndexEntry>,
}

#[derive(Debug)]
pub struct BinWriteIndexRtStream {
    rt: RetentionTime,
    series: SeriesId,
    scyqueue: ScyllaQueue,
    pbp: PrebinnedPartitioning,
    msp: u32,
    lsp_min: u32,
    msp_end: u32,
    lsp_end: u32,
    use_scylla6_workarounds: UseScylla6Workarounds,
    fut1: Option<Fut1>,
}

impl BinWriteIndexRtStream {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(
        rt: RetentionTime,
        series: SeriesId,
        pbp: PrebinnedPartitioning,
        range: NanoRange,
        use_scylla6_workarounds: UseScylla6Workarounds,
        scyqueue: ScyllaQueue,
    ) -> Self {
        lg::info!("============================   log item emitted from binwriteindex.rs");
        lg::info!(
            "============================   log item emitted from binwriteindex.rs WITH PARAM {}",
            42
        );
        lg::debug!("{}::new  INFO/DEBUG test", Self::type_name());
        lg::debug!("{}::new", Self::type_name());
        let (msp_beg, lsp_beg) = pbp.msp_lsp(range.beg_ts().to_ts_ms());
        let (msp_end, lsp_end) = pbp.msp_lsp(
            range
                .end_ts()
                .add_dt_nano(DtMs::from_ms_u64(pbp.bin_len().ms() - 1).dt_ns())
                .to_ts_ms(),
        );
        BinWriteIndexRtStream {
            rt,
            series,
            scyqueue,
            pbp,
            msp: msp_beg,
            lsp_min: lsp_beg,
            msp_end,
            lsp_end,
            use_scylla6_workarounds,
            fut1: None,
        }
    }

    async fn next_query_fut(
        scyqueue: ScyllaQueue,
        rt1: RetentionTime,
        series: SeriesId,
        pbp: PrebinnedPartitioning,
        msp: u32,
        lsp_min: u32,
        lsp_max: u32,
        use_scylla6_workarounds: UseScylla6Workarounds,
    ) -> Result<(u32, u32, u32, VecDeque<BinWriteIndexEntry>), crate::worker::Error> {
        debug!("make_next_query_fut  msp {}  lsp {} {}", msp, lsp_min, lsp_max);
        let res = scyqueue
            .bin_write_index_read(rt1, series, pbp, MspU32(msp), lsp_min, lsp_max, use_scylla6_workarounds)
            .await?;
        Ok((msp, lsp_min, lsp_max, res))
    }

    fn make_next_query_fut(mut self: Pin<&mut Self>, _cx: &mut Context) -> Option<Fut1> {
        info_item!(
            "make_next_query_fut  msp {}  msp_end {}  lsp_min {}  lsp_end {}",
            self.msp,
            self.msp_end,
            self.lsp_min,
            self.lsp_end
        );
        if self.msp <= self.msp_end {
            let msp = self.msp;
            self.msp += 1;
            let lsp_min = self.lsp_min;
            self.lsp_min = 0;
            let lsp_max = if self.msp > self.msp_end {
                self.lsp_end
            } else {
                self.pbp.patch_len()
            };
            let fut = {
                let scyqueue = self.scyqueue.clone();
                let rt = self.rt.clone();
                let series = self.series.clone();
                let pbp = self.pbp.clone();
                let use_scylla6_workarounds = self.use_scylla6_workarounds.clone();
                async move {
                    Self::next_query_fut(
                        scyqueue,
                        rt,
                        series,
                        pbp,
                        msp,
                        lsp_min,
                        lsp_max,
                        use_scylla6_workarounds,
                    )
                    .await
                }
            };
            Some(Fut1(Box::pin(fut)))
        } else {
            debug!("make_next_query_fut  done");
            None
        }
    }
}

impl Stream for BinWriteIndexRtStream {
    type Item = Sitemty3<BinWriteIndexSet, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(fut) = self.fut1.as_mut() {
                match fut.0.poll_unpin(cx) {
                    Ready(Ok(x)) => {
                        self.fut1 = None;
                        let item = BinWriteIndexSet {
                            msp: MspU32(x.0),
                            entries: x.3,
                        };
                        Ready(Some(sitem3_data(item)))
                    }
                    Ready(Err(e)) => {
                        self.fut1 = None;
                        Ready(Some(Err(e.into())))
                    }
                    Pending => Pending,
                }
            } else if let Some(fut) = self.as_mut().make_next_query_fut(cx) {
                self.fut1 = Some(fut);
                continue;
            } else {
                Ready(None)
            };
        }
    }
}
