#[cfg(test)]
mod test;

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
use netpod::OneBeforeFlag;
use netpod::RangeFilterStats;
use netpod::TsNano;
use netpod::TsNanoVecFmt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace_inp { ($det:expr, $($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_init { ($det:expr, $($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }
macro_rules! trace_emit { ($det:expr, $($arg:tt)*) => { if false { log::trace!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "RangeFilter2"),
    enum variants {
        DrainUnclean,
        Unordered,
        Logic,
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
    slot2: Option<ITY>,
    have_range_complete: bool,
    inp_done: bool,
    raco_done: bool,
    done: bool,
    complete: bool,
    trdet: bool,
    tsmax: TsNano,
}

impl<INP, ITY> RangeFilter2<INP, ITY>
where
    INP: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: MergeableTy,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: INP, range: NanoRange, one_before: OneBeforeFlag) -> Self {
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
            one_before: one_before.as_bool(),
            one_before_done: false,
            stats: RangeFilterStats::new(),
            slot1: None,
            slot2: None,
            have_range_complete: false,
            inp_done: false,
            raco_done: false,
            done: false,
            complete: false,
            trdet,
            tsmax: TsNano::from_ns(0),
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
        // TODO count the events before range for metrics.
        if let (Some(min), Some(max)) = (item.ts_min(), item.ts_max()) {
            trace_inp!(self.trdet, "see event  len {}  min {}  max {}", item.len(), min, max);
            if min < self.tsmax {
                return Err(Error::Unordered);
            }
            self.tsmax = max;
            let mut item = self.prune_high(item, self.range.end_ts())?;
            trace_inp!(self.trdet, "item len after prune_high {}", item.len());
            if self.one_before && !self.one_before_done {
                if let Some(ilge) = item.find_lowest_index_ge(self.range.beg_ts()) {
                    trace_emit!(self.trdet, "YES one_before_range  ilge {}", ilge);
                    self.one_before_done = true;
                    if ilge == 0 {
                        if let Some(sl1) = self.slot1.take() {
                            trace_emit!(
                                self.trdet,
                                "one_before -> done B  at {}",
                                sl1.tss_for_testing()[sl1.len() - 1]
                            );
                            self.slot2 = Some(item);
                            Ok(Some(sl1))
                        } else {
                            trace_emit!(self.trdet, "one_before -> done C");
                            Ok(Some(item))
                        }
                    } else {
                        trace_emit!(
                            self.trdet,
                            "one_before -> done A  at {}",
                            item.tss_for_testing()[ilge - 1]
                        );
                        trace_emit!(self.trdet, "discarding events  len {:?}", ilge - 1);
                        match item.drain_into_new(0..ilge - 1) {
                            DrainIntoNewResult::Done(_) => {}
                            DrainIntoNewResult::Partial(_) => return Err(Error::DrainUnclean),
                            DrainIntoNewResult::NotCompatible => return Err(Error::DrainUnclean),
                        }
                        self.slot1 = None;
                        Ok(Some(item))
                    }
                } else {
                    trace_emit!(self.trdet, "YES one_before_range  ilge None");
                    // TODO keep stats about this case
                    trace_emit!(self.trdet, "drain into to keep one before");
                    let n = item.len();
                    if n == 0 {
                        // checked already above, should never get here
                        return Err(Error::Logic);
                    } else {
                        match item.drain_into_new(n - 1..n) {
                            DrainIntoNewResult::Done(keep) => {
                                self.slot1 = Some(keep);
                            }
                            DrainIntoNewResult::Partial(_) => return Err(Error::DrainUnclean),
                            DrainIntoNewResult::NotCompatible => return Err(Error::DrainUnclean),
                        }
                    }
                    Ok(None)
                }
            } else {
                if let Some(ilge) = item.find_lowest_index_ge(self.range.beg_ts()) {
                    if ilge == 0 {
                        Ok(Some(item))
                    } else {
                        self.stats.items_prune_low = self.stats.items_prune_low.saturating_add(1);
                        match item.drain_into_new(0..ilge) {
                            DrainIntoNewResult::Done(_) => {}
                            DrainIntoNewResult::Partial(_) => return Err(Error::DrainUnclean),
                            DrainIntoNewResult::NotCompatible => return Err(Error::DrainUnclean),
                        }
                        if item.len() == 0 {
                            // TODO count case for stats
                            Ok(None)
                        } else {
                            Ok(Some(item))
                        }
                    }
                } else {
                    // TODO count case for stats
                    Ok(None)
                }
            }
        } else {
            self.stats.recv_empty = self.stats.recv_empty.saturating_add(1);
            Ok(None)
        }
    }
}

impl<INP, ITY> RangeFilter2<INP, ITY>
where
    INP: Stream<Item = Sitemty<ITY>> + Unpin,
    ITY: MergeableTy,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<<Self as Stream>::Item>> {
        use Poll::*;
        let selfname = Self::type_name();
        loop {
            break if self.complete {
                error!("{selfname} poll_next on complete");
                Ready(Some(sitem_err_from_string("poll next on complete")))
            } else if self.done {
                self.complete = true;
                Ready(None)
            } else if self.raco_done {
                self.done = true;
                let k = std::mem::replace(&mut self.stats, RangeFilterStats::new());
                trace_emit!(self.trdet, "{k:?}");
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
            } else if let Some(item) = self.slot2.take() {
                let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                Ready(Some(item))
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item))) => match self.handle_item(item) {
                            Ok(Some(item)) => {
                                trace_emit!(
                                    self.trdet,
                                    "emit {}",
                                    TsNanoVecFmt(MergeableTy::tss_for_testing(&item).iter())
                                );
                                let item = Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)));
                                Ready(Some(item))
                            }
                            Ok(None) => continue,
                            Err(e) => {
                                error!("sees: {}", e);
                                self.inp_done = true;
                                Ready(Some(sitem_err_from_string(e)))
                            }
                        },
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => {
                            self.have_range_complete = true;
                            continue;
                        }
                        k => Ready(Some(k)),
                    },
                    Ready(None) => {
                        self.inp_done = true;
                        if let Some(sl1) = self.slot1.take() {
                            Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(sl1)))))
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
        use crate::log::tracing;
        use crate::log::Level;
        let span1 = tracing::span!(
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
