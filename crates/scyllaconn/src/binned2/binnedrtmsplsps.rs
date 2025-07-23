/*
Fetches the bins for a given RT, binlen and MSP.
Issues the scylla commands.
Assembles the results.
Does basic sanity checks.
May re-chunk the result if too large.
*/

use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use items_2::binning::container_bins::ContainerBins;
use netpod::DtMs;
use netpod::UseScylla6Workarounds;
use netpod::ttl::RetentionTime;
use std::fmt;
use std::pin::Pin;

autoerr::create_error_v1!(
    name(Error, "BinnedRtMsp"),
    enum variants {
        ReadJob(#[from] streams::timebin::cached::reader::Error),
    },
);

type Fut =
    Pin<Box<dyn Future<Output = Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error>> + Send>>;

struct FutW(Fut);

impl fmt::Debug for FutW {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("Fut").finish()
    }
}

pub struct BinnedRtMspLsps {
    series: SeriesId,
    rt: RetentionTime,
    msp: MspU32,
    lsps: (LspU32, LspU32),
    binlen: DtMs,
    use_scylla6_workarounds: UseScylla6Workarounds,
    scyqueue: ScyllaQueue,
    fut: Option<FutW>,
}

impl BinnedRtMspLsps {
    pub fn new(
        series: SeriesId,
        rt: RetentionTime,
        msp: MspU32,
        binlen: DtMs,
        lsps: (LspU32, LspU32),
        use_scylla6_workarounds: UseScylla6Workarounds,
        scyqueue: ScyllaQueue,
    ) -> Self {
        Self {
            series,
            rt,
            msp,
            lsps,
            binlen,
            use_scylla6_workarounds,
            scyqueue,
            fut: None,
        }
    }

    fn make_next_fut(&mut self) -> Option<Fut> {
        let rt = self.rt.clone();
        let series = self.series.id();
        let binlen = self.binlen.clone();
        let msp = self.msp.to_u64();
        let offs = self.lsps.0.to_u32()..self.lsps.1.to_u32();
        // SAFETY we only use scyqueue while we self are alive.
        let scyqueue = unsafe { &*(&self.scyqueue as *const ScyllaQueue) };
        let fut = scyqueue.read_prebinned_f32(rt, series, binlen, msp, offs, self.use_scylla6_workarounds.clone());
        let fut = Box::pin(fut);
        Some(fut)
    }
}
