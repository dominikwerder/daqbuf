use super::compare::exp_avgs;
use super::compare::exp_cnts;
use super::compare::exp_maxs;
use super::compare::exp_mins;
use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::ContainerEvents;
use crate::binning::timeweight::timeweight_events::BinnedEventsTimeweight;
use crate::testgen::events_gen::new_events_gen_dim1_f32_v00;
use futures_util::StreamExt;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::EnumVariant;
use netpod::TsNano;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use std::task::Context;

autoerr::create_error_v1!(
    name(Error, "Error"),
    enum variants {
        Timeweight(#[from] crate::binning::timeweight::timeweight_events::Error),
        AssertMsg(String),
        Compare(#[from] super::compare::Error),
    },
);

#[test]
fn test_bin_events_dim1_f32_00() -> Result<(), Error> {
    let beg = TsNano::from_ms(110);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut inp = new_events_gen_dim1_f32_v00(range.full_range());
    let mut binner = BinnedEventsTimeweight::new(range);
    while let Some(evs) = inp.next() {
        binner.ingest(&evs)?;
    }
    binner.input_done_range_final()?;
    let bins = binner.output();
    eprintln!("{:?}", bins);
    Ok(())
}
