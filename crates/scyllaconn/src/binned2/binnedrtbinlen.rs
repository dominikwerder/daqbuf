use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::Stream;
use netpod::BinnedRange;
use netpod::TsNano;
use netpod::UseScylla6Workarounds;
use netpod::ttl::RetentionTime;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "BinnedRtBinlenStream"),
    enum variants {
        Logic,
    },
);

enum State {
    Init,
    Reading,
    Done,
}

pub struct BinnedRtBinlenStream {
    series: SeriesId,
    rt: RetentionTime,
    pbp: PrebinnedPartitioning,
    range: BinnedRange<TsNano>,
    use_scylla6_workarounds: UseScylla6Workarounds,
    scyqueue: ScyllaQueue,
    msp_lsp_min: (MspU32, LspU32),
    msp_lsp_max: (MspU32, LspU32),
    state: State,
}

impl BinnedRtBinlenStream {
    pub fn new(
        series: SeriesId,
        rt: RetentionTime,
        pbp: PrebinnedPartitioning,
        range: BinnedRange<TsNano>,
        use_scylla6_workarounds: UseScylla6Workarounds,
        scyqueue: ScyllaQueue,
    ) -> Self {
        let msp_lsp_min = pbp.msp_lsp(range.nano_beg().to_ts_ms());
        let msp_lsp_max = pbp.msp_lsp(range.nano_end().to_ts_ms());
        Self {
            series,
            rt,
            pbp,
            range,
            use_scylla6_workarounds,
            scyqueue,
            msp_lsp_min,
            msp_lsp_max,
            state: State::Init,
        }
    }

    fn make_next_fut(&mut self) -> Option<()> {
        let series = self.series.clone();
        let rt = self.rt.clone();
        let msp = todo!();
        let binlen = todo!();
        let lsps = todo!();
        super::binnedrtmsplsps::BinnedRtMspLsps::new(
            series,
            rt,
            msp,
            binlen,
            lsps,
            self.use_scylla6_workarounds.clone(),
            self.scyqueue.clone(),
        );
    }

    fn init(&mut self) {}
}

impl Stream for BinnedRtBinlenStream {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &mut self.as_mut().get_mut().state {
                State::Init => self.init(),
                State::Reading => todo!(),
                State::Done => todo!(),
            };
        }
        todo!()
    }
}
