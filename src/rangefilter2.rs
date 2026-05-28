#[cfg(test)]
mod test;

use crate::log::*;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::streamitem::sitem_err_from_string;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use netpod::range::evrange::NanoRange;
use netpod::RangeFilterStats;
use netpod::TsNano;
use netpod::TsNanoVecFmt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace_inp { ($det:expr, $($arg:expr),*) => ( if false && $det { trace!($($arg),*); } ) }

macro_rules! trace_init { ($det:expr, $($arg:expr),*) => ( if false { trace!($($arg),*); } ) }

macro_rules! trace_emit { ($det:expr, $($arg:expr),*) => ( if false { trace!($($arg),*); } ) }

autoerr::create_error_v1!(
    name(Error, "Rangefilter"),
    enum variants {
        DrainUnclean,
    },
);

pub struct RangeFilter2<INP, ITY>
where
    INP: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: MergeableTy,
{
    inp: INP,
    range: NanoRange,
    range_str: String,
    one_before: bool,
    one_before_done: bool,
    stats: RangeFilterStats,
    slot1: Option<ITY>,
    have_range_complete: bool,
    inp_done: bool,
    raco_done: bool,
    done: bool,
    complete: bool,
    trdet: bool,
}

impl<INP, ITY> RangeFilter2<INP, ITY>
where
    INP: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: MergeableTy,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: INP, range: NanoRange, one_before: bool) -> Self {
        let trdet = false;
        trace_init!(
            self.trdet,
            "{}::new  range: {:?}  one_before {:?}",
            Self::type_name(),
            range,
            one_before
        );
        Self {
            inp,
            range_str: format!("{:?}", range),
            range,
            one_before,
            one_before_done: false,
            stats: RangeFilterStats::new(),
            slot1: None,
            have_range_complete: false,
            inp_done: false,
            raco_done: false,
            done: false,
            complete: false,
            trdet,
        }
    }

    fn prune_high(&mut self, mut item: ITY, ts: TsNano) -> Result<ITY, Error> {
        let n = item.len();
        let ret = match item.find_highest_index_lt(ts) {
            Some(ihlt) => {
                if ihlt + 1 == n {
                    // TODO gather stats, this should be the most common case.
                    self.stats.items_no_prune_high += 1;
                    item
                } else {
                    self.stats.items_part_prune_high += 1;
                    match item.drain_into_new(ihlt + 1..n) {
                        DrainIntoNewResult::Done(_) => {}
                        DrainIntoNewResult::Partial(_) => {
                            error!("full, logic error");
                        }
                        DrainIntoNewResult::NotCompatible => {
                            error!("logic error");
                        }
                    }
                    item
                }
            }
            None => {
                // TODO should not happen often, observe.
                self.stats.items_all_prune_high += 1;
                match item.drain_into_new(0..n) {
                    DrainIntoNewResult::Done(_) => {}
                    DrainIntoNewResult::Partial(_) => {
                        error!("full, logic error");
                    }
                    DrainIntoNewResult::NotCompatible => {
                        error!("logic error");
                    }
                }
                item
            }
        };
        Ok(ret)
    }

    fn handle_item(&mut self, item: ITY) -> Result<Option<ITY>, Error> {
        if let Some(ts_min) = item.ts_min() {
            if ts_min.ns() < self.range.beg() {
                debug!("ITEM  BEFORE RANGE  (how many?)");
            }
        }
        let min = item.ts_min();
        let max = item.ts_max();
        trace_inp!(
            self.trdet,
            "see event  len {}  min {:?}  max {:?}",
            item.len(),
            min,
            max
        );
        let mut item = self.prune_high(item, self.range.end_ts())?;
        trace_inp!(self.trdet, "item len after prune_high {}", item.len());
        if self.one_before {
            let lige = item.find_lowest_index_ge(self.range.beg_ts());
            trace_emit!(self.trdet, "YES one_before_range  ilge {:?}", lige);
            match lige {
                Some(lige) => {
                    if lige == 0 {
                        if let Some(sl1) = self.slot1.take() {
                            self.slot1 = Some(item);
                            Ok(Some(sl1))
                        } else {
                            Ok(Some(item))
                        }
                    } else {
                        trace_emit!(self.trdet, "discarding events  len {:?}", lige - 1);
                        match item.drain_into_new(0..lige - 1) {
                            DrainIntoNewResult::Done(_) => {}
                            DrainIntoNewResult::Partial(_) => return Err(Error::DrainUnclean),
                            DrainIntoNewResult::NotCompatible => return Err(Error::DrainUnclean),
                        }
                        self.slot1 = None;
                        Ok(Some(item))
                    }
                }
                None => {
                    // TODO keep stats about this case
                    trace_emit!(self.trdet, "drain into to keep one before");
                    let n = item.len();
                    match item.drain_into_new(n.max(1) - 1..n) {
                        DrainIntoNewResult::Done(keep) => {
                            self.slot1 = Some(keep);
                        }
                        DrainIntoNewResult::Partial(_) => return Err(Error::DrainUnclean),
                        DrainIntoNewResult::NotCompatible => return Err(Error::DrainUnclean),
                    }
                    Ok(None)
                }
            }
        } else {
            let lige = item.find_lowest_index_ge(TsNano::from_ns(self.range.beg));
            trace_emit!(self.trdet, "NOT one_before_range  ilge {:?}", lige);
            match lige {
                Some(lige) => {
                    match item.drain_into_new(0..lige) {
                        DrainIntoNewResult::Done(_) => {}
                        DrainIntoNewResult::Partial(_) => return Err(Error::DrainUnclean),
                        DrainIntoNewResult::NotCompatible => return Err(Error::DrainUnclean),
                    }
                    Ok(Some(item))
                }
                None => {
                    // TODO count case for stats
                    Ok(None)
                }
            }
        }
    }
}

impl<INP, ITY> RangeFilter2<INP, ITY>
where
    INP: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: MergeableTy,
{
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        loop {
            break if self.complete {
                error!("{} poll_next on complete", Self::type_name());
                Ready(Some(sitem_err_from_string("poll next on complete")))
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.raco_done {
                self.done = true;
                let k = std::mem::replace(&mut self.stats, RangeFilterStats::new());
                let k = StatsItem::RangeFilterStats(k);
                Ready(Some(Ok(StreamItem::Stats(k))))
            } else if self.inp_done {
                self.raco_done = true;
                if self.have_range_complete {
                    let item = Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete));
                    Ready(Some(item))
                } else {
                    continue;
                }
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => {
                            match self.handle_item(item) {
                                Ok(Some(item)) => {
                                    trace_emit!(
                                        self.trdet,
                                        "emit {}",
                                        TsNanoVecFmt(MergeableTy::tss_for_testing(&item).iter())
                                    );
                                    let item =
                                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                                    Ready(Some(item))
                                }
                                Ok(None) => continue,
                                Err(e) => {
                                    error!("sees: {}", e);
                                    self.inp_done = true;
                                    Ready(Some(sitem_err_from_string(e)))
                                }
                            }
                        }
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => {
                            self.have_range_complete = true;
                            continue;
                        }
                        k => Ready(Some(k)),
                    },
                    Ready(None) => {
                        self.inp_done = true;
                        if let Some(sl1) = self.slot1.take() {
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                                sl1,
                            )))))
                        } else {
                            continue;
                        }
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl<INP, ITY> Stream for RangeFilter2<INP, ITY>
where
    INP: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: MergeableTy,
{
    type Item = Sitemty<ITY>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let span1 = span!(
            Level::INFO,
            "RangeFilter2",
            range = tracing::field::Empty,
            one_before = tracing::field::Empty
        );
        span1.record("range", &self.range_str.as_str());
        span1.record("one_before", &self.one_before);
        let _spg = span1.enter();
        RangeFilter2::poll_next(self, cx)
    }
}

impl<INP, ITY> fmt::Debug for RangeFilter2<INP, ITY>
where
    INP: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: MergeableTy,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RangeFilter2")
            .field("stats", &self.stats)
            .field("one_before", &self.one_before)
            .finish()
    }
}
