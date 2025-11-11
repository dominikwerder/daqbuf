use super::mspiter::MspChunker;
use super::msplspiter::MspLspIter;
use crate::binwriteindex::read_all_coarse::ReadAllCoarse;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use items_0::streamitem::Sitemty3;
use items_0::streamitem::sitem3_data;
use items_0::streamitem::sitem3_log_info;
use items_0::timebin::BinningggContainerBinsDyn;
use items_0::timebin::BinsBoxed;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsMs;
use netpod::TsNano;
use netpod::UseScylla6Workarounds;
use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use serde::Serialize;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use streams::timebin::CacheReadProvider;

#[allow(unused)]
fn _logitems() {
    use items_0::streamitem::LogItem;
    use streams::logqueue::push_log_item;
}

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
    name(Error, "Binned2FromBinned"),
    enum variants {
        A,
    },
);

type IndexRow = (RetentionTime, MspU32, LspU32, DtMs);

#[derive(Debug)]
struct FetchJob {
    min: (MspU32, LspU32),
    max: (MspU32, LspU32),
    dbg_ts_1: (TsMs, TsMs),
    dbg_ts_2: (TsMs, TsMs),
    rt: RetentionTime,
    pbp: PrebinnedPartitioning,
}

enum StateA {
    ReadAllCoarse(ReadAllCoarse, VecDeque<IndexRow>),
    ExecuteJobs,
    Done,
}

pub struct FromBinned {
    cache_read_provider: Arc<dyn CacheReadProvider>,
    series: SeriesId,
    binrange: BinnedRange<TsNano>,
    state_a: StateA,
    index_map: HashMap<(MspU32, LspU32), VecDeque<(DtMs, RetentionTime)>>,
    jobs: VecDeque<FetchJob>,
    job_fut: Option<Pin<Box<dyn Future<Output = (Vec<String>, Option<Box<dyn BinningggContainerBinsDyn>>)> + Send>>>,
    logoutbuf: VecDeque<String>,
    binoutbuf: VecDeque<BinsBoxed>,
}

fn def<T: Default>() -> T {
    Default::default()
}

impl FromBinned {
    pub fn new(
        series: SeriesId,
        binrange: BinnedRange<TsNano>,
        use_scylla6_workarounds: UseScylla6Workarounds,
        scyqueue: &ScyllaQueue,
        cache_read_provider: Arc<dyn CacheReadProvider>,
    ) -> Self {
        let state_a = StateA::ReadAllCoarse(
            ReadAllCoarse::new(
                series,
                binrange.to_nano_range(),
                use_scylla6_workarounds,
                scyqueue.clone(),
            ),
            def(),
        );
        Self {
            cache_read_provider,
            series,
            binrange,
            state_a,
            index_map: def(),
            jobs: def(),
            job_fut: None,
            logoutbuf: def(),
            binoutbuf: def(),
        }
    }

    fn push_string<T: ToString>(&mut self, x: T) {
        self.logoutbuf.push_back(x.to_string());
    }

    fn handle_coarse_index(&mut self, rows: VecDeque<IndexRow>) {
        self.push_string(format!("handle_coarse_index"));
        for e in rows {
            self.push_string(format!("{:?}", e));
            let k = (e.1, e.2);
            let h = &mut self.index_map;
            if let Some(v) = h.get_mut(&k) {
                v.push_back((e.3, e.0));
            } else {
                let mut v = VecDeque::new();
                v.push_back((e.3, e.0));
                h.insert(k, v);
            }
        }
        self.index_map.iter_mut().for_each(|(_, v)| {
            v.make_contiguous().sort();
        });
    }

    fn build_day1_jobs(&mut self) {
        info_item!("some log string");
        let mut jobs = VecDeque::new();
        let pbp1 = PrebinnedPartitioning::Day1;
        info_item!("binrange {:?}", self.binrange);
        debug_item!("DEBUG LOG ITEM --------------");
        info_item!("to_nano_range {:?}", self.binrange.to_nano_range());
        let it = MspLspIter::new_covering(self.binrange.to_nano_range(), pbp1.clone());
        for day in it {
            {
                let nday = pbp1.patch_len() as u64 * day.0.to_u64() + day.1.to_u32() as u64;
                let ts = 60 * 60 * 24 * nday;
                let ts = time::UtcDateTime::from_unix_timestamp(ts as _);
                debug_item!("build_day1_jobs  {:?}  ts {:?}", day, ts);
            }
            let k = (day.0, day.1);
            if let Some(ixs) = self.index_map.get(&k) {
                debug_item!("have index {:?}", ixs);
                let mut found = None;
                for ix in ixs.iter().rev() {
                    if ix.0.ms() <= self.binrange.bin_len_dt_ms().ms() {
                        if let Some(found) = found.as_ref() {
                            debug_item!("already found a better solution found {:?}  e {:?}", found, ix);
                        } else {
                            match PrebinnedPartitioning::from_binlen(ix.0) {
                                Ok(pbp2) => {
                                    debug_item!("FOUND {:?}", ix);
                                    found = Some((ix.1.clone(), pbp2));
                                }
                                Err(_) => {
                                    debug_item!("binlen not a pbp {:?}", ix);
                                }
                            }
                        }
                    } else {
                        debug_item!("too coarse {:?}", ix);
                    }
                }
                if let Some(ix) = found {
                    // determine already here the msp/lsp range that this job must query
                    // for the also given pbp.
                    let pbp2 = ix.1.clone();
                    let rbeg = self.binrange.nano_beg().to_ts_ms();
                    let rend = self.binrange.nano_end().to_ts_ms();
                    let gbeg = pbp1.msp_lsp_to_ts(day.0, day.1);
                    let gend = gbeg.add_dt_ms(pbp1.bin_len());
                    let beg = if rbeg > gbeg { rbeg } else { gbeg };
                    let end = if rend < gend { rend } else { gend };
                    {
                        let ts_beg = time::UtcDateTime::from_unix_timestamp(beg.sec() as _);
                        let ts_end = time::UtcDateTime::from_unix_timestamp(end.sec() as _);
                        debug_item!("beg {:?}  end {:?}", ts_beg, ts_end);
                    }
                    let it3 = {
                        let range = NanoRange::from_ms_u64(beg.ms(), end.ms());
                        MspLspIter::new_covering(range, pbp2.clone())
                    };
                    let job = FetchJob {
                        min: it3.mins(),
                        max: it3.maxs(),
                        dbg_ts_1: (beg, end),
                        // TODO remove
                        dbg_ts_2: (beg, end),
                        rt: ix.0,
                        // TODO which pbp is expected here?
                        pbp: pbp2,
                    };
                    jobs.push_back(job);
                }
            } else {
                debug_item!("no index entry");
            }
        }
        self.jobs = jobs;
    }

    fn make_next_job(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = (Vec<String>, Option<Box<dyn BinningggContainerBinsDyn>>)> + Send>>> {
        // iterate over the jobs and subjobs.
        // each cache read provider task reads from a specific msp.
        // therefore, I have to break at the msp boundaries.
        // for that, use a msp iterator.
        if let Some(job) = self.jobs.pop_front() {
            let series = self.series;
            let pbp = job.pbp.clone();
            let binlen = pbp.bin_len();
            // let tsbeg: TsMs = job.2;
            // let tsend: TsMs = job.3;
            // let range = NanoRange::from_ms_u64(tsbeg.ms(), tsend.ms());

            debug_item!("make job  {:?}", job);

            let chunk_it = MspChunker::from_min_max(pbp, job.min, job.max);
            debug_item!("chunk_it {:?}", chunk_it);
            let mut futs = VecDeque::new();
            for chunk in chunk_it {
                debug_item!("chunk {:?}", chunk);
                let offs = chunk.lsp1.to_u32()..chunk.lsp2.to_u32();
                let fut = self
                    .cache_read_provider
                    .read(series.id(), binlen, chunk.msp.to_u64(), offs);
                futs.push_back(fut);
            }

            // let msp = job.min.0.0 as u64;
            // TODO add helper, make safer
            // let offs = (job.min.1.0 as u32)..(job.max.1.0 as u32);

            let fut = async move {
                let mut log = Vec::new();
                let mut bins2: Option<Box<dyn BinningggContainerBinsDyn>> = None;
                for fut in futs {
                    match fut.await {
                        Ok(x) => match x {
                            Some(mut bins) => match bins2.as_mut() {
                                Some(bins2) => {
                                    log.push(format!("drain into len {}", bins.len()));
                                    bins.drain_into(bins2.as_mut(), 0..bins.len());
                                }
                                None => {
                                    bins2 = Some(bins);
                                }
                            },
                            None => {
                                log.push(format!("binned read job returns None"));
                            }
                        },
                        Err(e) => {
                            log.push(format!("error in binned read job {}", e));
                        }
                    }
                }
                (log, bins2)
            };
            Some(Box::pin(fut))
        } else {
            None
        }
    }
}

impl Stream for FromBinned {
    type Item = Sitemty3<BinsBoxed, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(x) = self.logoutbuf.pop_front() {
                Ready(Some(sitem3_log_info(x)))
            } else if let Some(x) = self.binoutbuf.pop_front() {
                Ready(Some(sitem3_data(x)))
            } else {
                use StateA::*;
                let self2 = self.as_mut().get_mut();
                match &mut self2.state_a {
                    ReadAllCoarse(stb, rows) => match stb.poll_next_unpin(cx) {
                        Ready(Some(Ok(x))) => {
                            match x.into_data() {
                                Ok(x) => {
                                    rows.push_back(x);
                                }
                                Err(x) => {
                                    debug_item!("{:?}", x);
                                }
                            }
                            continue;
                        }
                        Ready(Some(Err(e))) => {
                            self2.push_string(e);
                            self2.state_a = StateA::Done;
                            continue;
                        }
                        Ready(None) => {
                            let a = std::mem::replace(rows, def());
                            self2.state_a = StateA::Done;
                            debug_item!("done with reading coarse");
                            self2.handle_coarse_index(a);
                            self2.build_day1_jobs();
                            self2.state_a = StateA::ExecuteJobs;
                            continue;
                        }
                        Pending => Pending,
                    },
                    ExecuteJobs => {
                        if let Some(fut) = self2.job_fut.as_mut() {
                            match fut.poll_unpin(cx) {
                                Ready((log, bins)) => {
                                    self2.job_fut = None;
                                    debug_item!("ExecuteJobs sees Ready");
                                    for x in log {
                                        self.push_string(x);
                                    }
                                    match bins {
                                        Some(bins) => {
                                            self.binoutbuf.push_back(bins);
                                        }
                                        None => {
                                            // TODO report
                                        }
                                    }
                                    continue;
                                }
                                Pending => Pending,
                            }
                        } else if let Some(fut) = self2.make_next_job() {
                            // self2.state_a = StateA::ExecuteJobs;
                            self2.job_fut = Some(fut);
                            continue;
                        } else {
                            self2.state_a = StateA::Done;
                            continue;
                        }
                    }
                    Done => Ready(None),
                }
            };
        }
    }
}
