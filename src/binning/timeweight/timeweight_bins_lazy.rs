use crate::log::*;
use items_0::timebin::BinnedBinsTimeweightTrait;
use items_0::timebin::BinningggError;
use items_0::timebin::BinsBoxed;
use netpod::BinnedRange;
use netpod::TsNano;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "BinnedBinsLazy")]
pub enum Error {}

#[derive(Debug)]
pub struct BinnedBinsTimeweightLazy {
    range: BinnedRange<TsNano>,
    binned: Option<Box<dyn BinnedBinsTimeweightTrait>>,
}

impl BinnedBinsTimeweightLazy {
    pub fn new(range: BinnedRange<TsNano>) -> Self {
        Self {
            range,
            binned: None,
        }
    }

    pub fn ingest(&mut self, evs: &BinsBoxed) -> Result<(), BinningggError> {
        self.binned
            .get_or_insert_with(|| evs.binned_bins_timeweight_traitobj(self.range.clone()))
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
