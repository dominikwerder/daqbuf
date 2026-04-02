use crate::binned2::msplspiter::MspLspIter;
use crate::binned3::index_entry::IndexEntry;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::Stream;
use items_0::streamitem::LogItem;
use items_0::streamitem::Sitemty3;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use query::api4::scyllaopts::ScyllaOptsQuery;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "BinReadLayeredTop"),
    enum variants {
        StateModFn,
        Worker(#[from] crate::worker::Error),
    },
);

fn def<T: Default>() -> T {
    Default::default()
}

type FetchingIndexFutRes = Result<VecDeque<IndexEntry>, Error>;

async fn fetch_index_entries(
    series: SeriesId,
    msp: MspU32,
    lsp: LspU32,
    scylla_opts: ScyllaOptsQuery,
    scyqueue: &ScyllaQueue,
) -> FetchingIndexFutRes {
    let rt = RetentionTime::Long;
    let pbp = PrebinnedPartitioning::Day1;
    match scyqueue
        .bin_write_index_read(rt, series, pbp.clone(), msp, lsp, LspU32(1 + lsp.to_u32()), scylla_opts)
        .await
    {
        Ok(x) => {
            let mut a = VecDeque::new();
            for e in x {
                let y = super::index_entry::IndexEntry {
                    pbp: pbp.clone(),
                    msp,
                    lsp,
                    binlen: DtMs::from_ms_u64(e.binlen.to_u32() as _),
                };
                a.push_back(y);
            }
            Ok(a)
        }
        Err(e) => Err(e.into()),
    }
}

struct FetchingIndex {
    fut: Pin<Box<dyn Future<Output = FetchingIndexFutRes> + Send>>,
}

enum StateModFn {
    State(Box<dyn FnOnce(&mut State) -> () + Send>),
}

struct Common {
    series: SeriesId,
    msplspiter: MspLspIter,
    scylla_opts: ScyllaOptsQuery,
    scyqueue: ScyllaQueue,
    logoutbuf: VecDeque<LogItem>,
}

enum State {
    StartNextDay1,
    FetchingIndex(FetchingIndex),
    DataDone,
}

pub struct BinReadLayeredTop {
    state: State,
    common: Common,
}

impl BinReadLayeredTop {
    pub fn new(
        series: SeriesId,
        binrange: BinnedRange<TsNano>,
        scylla_opts: ScyllaOptsQuery,
        scyqueue: ScyllaQueue,
    ) -> Result<Self, Error> {
        // TODO
        // Compute (PBP) the list of MSP and LSP-ranges to query for Day1 index entries.
        let msplspiter = MspLspIter::new_covering(binrange.full_range(), PrebinnedPartitioning::Day1);
        // Ask scyqueue to fetch the Day1 index entries for all rt for the given series and range.
        let ret = Self {
            state: State::StartNextDay1,
            common: Common {
                series,
                msplspiter,
                scylla_opts,
                scyqueue,
                logoutbuf: VecDeque::new(),
            },
        };
        Ok(ret)
    }

    fn start_next_day1(common: &mut Common) -> StateModFn {
        match common.msplspiter.next() {
            Some(x) => {
                let _series = common.series.clone();
                // SAFETY future must not outlive self.
                let scyqueue = unsafe { netpod::extltref(&common.scyqueue) };
                // self.state = State::FetchingIndex(FetchingIndex {
                //     fut: Box::pin(fetch_index_entries(common.series.clone(), x.0, x.1, scyqueue)),
                // });
                let st = State::FetchingIndex(FetchingIndex {
                    fut: Box::pin(fetch_index_entries(
                        common.series.clone(),
                        x.0,
                        x.1,
                        common.scylla_opts.clone(),
                        scyqueue,
                    )),
                });
                let y = move |state: &mut State| {
                    *state = st;
                };
                let x = StateModFn::State(Box::new(y));
                x
            }
            None => {
                // self.state = State::DataDone;
                todo!()
            }
        }
    }
}

impl Stream for BinReadLayeredTop {
    type Item = Sitemty3<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &mut self.state {
                State::StartNextDay1 => {
                    //
                    match Self::start_next_day1(&mut self.common) {
                        StateModFn::State(f) => {
                            f(&mut self.state);
                            continue;
                        }
                        _ => Ready(Some(Err(Error::StateModFn))),
                    }
                }
                _ => todo!(),
            };
        }
    }
}
