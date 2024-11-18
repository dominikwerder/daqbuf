use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::EventValueType;
use crate::log::*;
use items_0::timebin::BinnedBinsTimeweightTrait;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use netpod::BinnedRange;
use netpod::TsNano;

#[allow(unused)]
macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace!($($arg)*); }) }

#[derive(Debug)]
pub struct BinnedBinsTimeweight<EVT>
where
    EVT: EventValueType,
{
    range: BinnedRange<TsNano>,
    out: ContainerBins<EVT>,
    produce_cnt_zero: bool,
}

impl<EVT> BinnedBinsTimeweight<EVT>
where
    EVT: EventValueType,
{
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        trace_init!("BinnedBinsTimeweight::new  {}", range);
        let active_beg = range.nano_beg();
        let active_end = active_beg.add_dt_nano(range.bin_len.to_dt_nano());
        let active_len = active_end.delta(active_beg);
        Self {
            range,
            out: ContainerBins::new(),
            produce_cnt_zero: true,
        }
    }
}

impl<EVT> BinnedBinsTimeweightTrait for BinnedBinsTimeweight<EVT>
where
    EVT: EventValueType,
{
    fn ingest(&mut self, evs: &BinsBoxed) -> Result<(), BinningggError> {
        todo!()
    }

    fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        todo!()
    }

    fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        todo!()
    }

    fn output(&mut self) -> Result<Option<BinsBoxed>, BinningggError> {
        todo!()
    }
}
