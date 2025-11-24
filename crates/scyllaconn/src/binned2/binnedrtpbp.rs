use crate::binned2::binnedrtmsplsps::BinnedRtMspLsps;
use crate::binned2::mspchunker::MspChunker;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::Stream;
use items_0::streamitem::StreamItem;
use items_2::binning::container_bins::ContainerBins;
use log::log_item_emit as lg;
use netpod::BinnedRange;
use netpod::TsNano;
use netpod::ttl::RetentionTime;
use query::api4::scyllaopts::ScyllaOptsQuery;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "BinnedRtPbpStream"),
    enum variants {
        BinnedRtMspLsps(#[from] super::binnedrtmsplsps::Error),
    },
);

enum State {
    Init,
    PrepareNextRead,
    Reading(BinnedRtMspLsps),
    Done,
}

pub struct BinnedRtPbpStream {
    series: SeriesId,
    rt: RetentionTime,
    pbp: PrebinnedPartitioning,
    range: BinnedRange<TsNano>,
    scylla_opts: ScyllaOptsQuery,
    scyqueue: ScyllaQueue,
    msp_chunker: MspChunker,
    state: State,
}

impl BinnedRtPbpStream {
    pub fn new(
        series: SeriesId,
        rt: RetentionTime,
        pbp: PrebinnedPartitioning,
        range: BinnedRange<TsNano>,
        scylla_opts: ScyllaOptsQuery,
        scyqueue: ScyllaQueue,
    ) -> Self {
        let msp_chunker = MspChunker::from_binned_range(range.clone(), pbp.clone());
        Self {
            series,
            rt,
            pbp,
            range,
            scylla_opts,
            scyqueue,
            msp_chunker,
            state: State::Init,
        }
    }

    fn make_next_fut(&mut self, msp: MspU32, lsps: (LspU32, LspU32)) -> BinnedRtMspLsps {
        let series = self.series.clone();
        let rt = self.rt.clone();
        let binlen = self.pbp.bin_len();
        BinnedRtMspLsps::new(
            series,
            rt,
            msp,
            binlen,
            lsps,
            self.scylla_opts.clone(),
            self.scyqueue.clone(),
        )
    }

    fn prepare_next_read(&mut self) {
        if let Some(e) = self.msp_chunker.next() {
            lg::debug!("BinnedRtPbpStream::init  {e:?}");
            let fut = self.make_next_fut(e.msp, (e.lsp1, e.lsp2));
            self.state = State::Reading(fut);
        } else {
            lg::debug!("BinnedRtPbpStream::init  Done");
            self.state = State::Done;
        }
    }
}

impl Stream for BinnedRtPbpStream {
    type Item = Result<StreamItem<ContainerBins<f32, f32>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &mut self.as_mut().get_mut().state {
                State::Init => {
                    self.state = State::PrepareNextRead;
                    continue;
                }
                State::PrepareNextRead => {
                    self.prepare_next_read();
                    continue;
                }
                State::Reading(fut) => match Pin::new(fut).poll(cx) {
                    Ready(Ok(StreamItem::DataItem(bins))) => {
                        let item = StreamItem::DataItem(bins);
                        self.state = State::PrepareNextRead;
                        Ready(Some(Ok(item)))
                    }
                    Ready(Ok(x)) => Ready(Some(Ok(x))),
                    Ready(Err(e)) => {
                        lg::info!("BinnedRtPbpStream::poll_next  read error {e}");
                        self.state = State::Done;
                        Ready(Some(Err(Error::from(e))))
                    }
                    Pending => Pending,
                },
                State::Done => Ready(None),
            };
        }
    }
}
