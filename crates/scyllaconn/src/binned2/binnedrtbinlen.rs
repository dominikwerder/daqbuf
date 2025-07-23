use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::PrebinnedPartitioning;
use netpod::BinnedRange;
use netpod::TsNano;
use netpod::UseScylla6Workarounds;
use netpod::ttl::RetentionTime;

/*
Given RT, PBP and range, loop over all the bins to retrieve.

When there is no content, skip over.
*/

pub struct BinnedRtBinlenStream {
    series: SeriesId,
    rt: RetentionTime,
    pbp: PrebinnedPartitioning,
    range: BinnedRange<TsNano>,
    use_scylla6_workarounds: UseScylla6Workarounds,
    scyqueue: ScyllaQueue,
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
        Self {
            series,
            rt,
            pbp,
            range,
            use_scylla6_workarounds,
            scyqueue,
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
}
