use crate::log::*;
use items_0::timebin::BinnedBinsTimeweightTrait;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use netpod::BinnedRange;
use netpod::TsNano;

#[derive(Debug)]
pub struct BinnedBinsTimeweightLazy {
    range: BinnedRange<TsNano>,
    binned: Option<Box<dyn BinnedBinsTimeweightTrait>>,
    produce_cnt_zero: bool,
}

impl BinnedBinsTimeweightLazy {
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        Self {
            range,
            binned: None,
            produce_cnt_zero: false,
        }
    }

    pub fn set_cnt_zero(self) -> Self {
        let mut ret = self;
        ret.produce_cnt_zero = true;
        ret
    }

    pub fn ingest(&mut self, evs: &BinsBoxed) -> Result<(), BinningggError> {
        self.binned
            .get_or_insert_with(|| {
                let mut binner = evs.binned_bins_timeweight_traitobj(self.range.clone());
                if self.produce_cnt_zero {
                    binner.cnt_zero_enable();
                }
                binner
            })
            .ingest(evs)
    }

    pub fn input_done_range_final(&mut self) -> Result<(), BinningggError> {
        self.binned
            .as_mut()
            .map(|x| x.input_done_range_final())
            .unwrap_or_else(|| {
                debug!("TODO something to do if we miss the binner here?");
                Ok(())
            })
    }

    pub fn input_done_range_open(&mut self) -> Result<(), BinningggError> {
        self.binned
            .as_mut()
            .map(|x| x.input_done_range_open())
            .unwrap_or(Ok(()))
    }

    pub fn output(&mut self) -> Result<Option<BinsBoxed>, BinningggError> {
        self.binned.as_mut().map(|x| x.output()).unwrap_or(Ok(None))
    }
}
