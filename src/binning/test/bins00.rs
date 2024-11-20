use super::bins_gen::bins_gen_dim0_f32_v00;
use super::compare::exp_avgs;
use super::compare::exp_cnts;
use super::compare::exp_maxs;
use super::compare::exp_mins;
use super::events00::pu;
use crate::binning::container_events::ContainerEvents;
use crate::binning::test::bins_gen::boxed_conts;
use crate::binning::timeweight::timeweight_bins_stream::BinnedBinsTimeweightStream;
use crate::binning::timeweight::timeweight_events::BinnedEventsTimeweight;
use futures_util::StreamExt;
use items_0::timebin::BinningggContainerBinsDyn;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsNano;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "Error")]
enum Error {
    Timeweight(#[from] crate::binning::timeweight::timeweight_events::Error),
    AssertMsg(String),
    Compare(#[from] super::compare::Error),
}

#[test]
fn test_bin_events_f32_simple_01() -> Result<(), Error> {
    let fut = async {
        let beg = TsNano::from_ms(100);
        let end = TsNano::from_ms(500);
        let bin_len = DtMs::from_ms_u64(100);
        let nano_range = NanoRange {
            beg: beg.ns(),
            end: end.ns(),
        };
        let range = BinnedRange::from_nano_range(nano_range, bin_len);
        let inp = bins_gen_dim0_f32_v00();
        let inp = boxed_conts(inp);
        let mut stream = BinnedBinsTimeweightStream::new(range, inp);
        while let Some(bin) = stream.next().await {
            eprintln!("bin {:?}", bin);
        }
        // exp_cnts(&bins, "2     3")?;
        // exp_mins(&bins, "2.    1.")?;
        // exp_maxs(&bins, "2.4   2.4")?;
        // exp_avgs(&bins, "2.30  1.5333")?;
        Ok(())
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(fut)
}
