#[cfg(test)]
mod test;

use crate::log;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::DrainIntoDstResult;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::on_sitemty_data;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::SitemErrTy;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::streamitem::sitem_data;
use items_0::streamitem::sitem_err2_from_string;
use netpod::TsNano;
use std::collections::VecDeque;
use std::fmt;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

const OUT_MAX_BYTES: u32 = 1024 * 1024 * 20;
const DO_DETECT_NON_MONO: bool = true;

macro_rules! trace2 { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace3 { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace4 { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace_emit { ($($arg:expr),*) => ( if false { log::trace!($($arg),*); } ) }

macro_rules! trace_inp_special { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); } ) }

macro_rules! trace_emit_special { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); } ) }

macro_rules! debug_inp { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); } ) }

autoerr::create_error_v1!(
    name(Error, "MergerError"),
    enum variants {
        NoPendingButMissing,
        Input(SitemErrTy),
        ShouldFindTsMin,
        ItemShouldHaveTsMax,
        PartialPathDrainedAllItems,
        InputNotMonotonic,
        Logic,
    },
);

type MergeInp<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

struct Inps<T>(Vec<Option<MergeInp<T>>>);

impl<T> fmt::Debug for Inps<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("Vec<Inp>").finish()
    }
}

#[derive(Debug)]
pub struct Merger<T> {
    inps: Inps<T>,
    items: Vec<Option<T>>,
    out: Option<T>,
    do_clear_out: bool,
    out_max_len: usize,
    range_complete: Vec<bool>,
    outbuf: VecDeque<Sitemty<T>>,
    log_queue: VecDeque<LogItem>,
    dim0ix_max: TsNano,
    done_inp: bool,
    done_range_complete: bool,
    complete: bool,
    stats_process_item_empty: u32,
    take_item_single_inp_cnt: u32,
    take_item_full_cnt: u32,
    take_item_partial_cnt: u32,
}

impl<T> Merger<T>
where
    T: MergeableTy,
{
    pub fn new(inps: Vec<MergeInp<T>>, out_max_len: Option<u32>) -> Self {
        let n = inps.len();
        Self {
            inps: Inps(inps.into_iter().map(|x| Some(x)).collect()),
            items: (0..n).into_iter().map(|_| None).collect(),
            out: None,
            do_clear_out: false,
            out_max_len: out_max_len.unwrap_or(1000) as usize,
            range_complete: vec![false; n],
            outbuf: VecDeque::new(),
            log_queue: VecDeque::new(),
            dim0ix_max: TsNano::from_ns(0),
            done_inp: false,
            done_range_complete: false,
            complete: false,
            stats_process_item_empty: 0,
            take_item_single_inp_cnt: 0,
            take_item_full_cnt: 0,
            take_item_partial_cnt: 0,
        }
    }

    fn drain_into_dst_upto(src: &mut T, dst: &mut T, upto: TsNano) -> DrainIntoDstResult {
        match src.find_lowest_index_gt(upto) {
            Some(ilgt) => src.drain_into(dst, 0..ilgt),
            None => {
                // TODO should not be here.
                src.drain_into(dst, 0..src.len())
            }
        }
    }

    fn drain_into_new_upto(src: &mut T, upto: TsNano) -> DrainIntoNewResult<T> {
        match src.find_lowest_index_gt(upto) {
            Some(ilgt) => src.drain_into_new(0..ilgt),
            None => {
                // TODO should not be here.
                src.drain_into_new(0..src.len())
            }
        }
    }

    fn take_into_output_upto(&mut self, src: &mut T, upto: TsNano) -> DrainIntoDstResult {
        // TODO optimize the case when some large batch should be added to some existing small batch already in out.
        // TODO maybe use two output slots?
        if let Some(out) = self.out.as_mut() {
            Self::drain_into_dst_upto(src, out, upto)
        } else {
            trace2!("move into fresh");
            match Self::drain_into_new_upto(src, upto) {
                DrainIntoNewResult::Done(x) => {
                    self.out = Some(x);
                    DrainIntoDstResult::Done
                }
                DrainIntoNewResult::Partial(x) => {
                    self.out = Some(x);
                    DrainIntoDstResult::Partial
                }
                DrainIntoNewResult::NotCompatible => DrainIntoDstResult::NotCompatible,
            }
        }
    }

    fn take_into_output_all(&mut self, src: &mut T) -> DrainIntoDstResult {
        // TODO optimize the case when some large batch should be added to some existing small batch already in out.
        // TODO maybe use two output slots?
        self.take_into_output_upto(src, TsNano::from_ns(u64::MAX))
    }

    fn take_outbuf_filtered(&mut self) -> Option<T> {
        // TODO currently nothing done here
        self.out.take()
    }

    fn process(mut self: Pin<&mut Self>, _cx: &mut Context) -> Result<(), Error> {
        trace4!("process");
        let mut log_items = Vec::new();
        let self2 = self.as_mut().get_mut();
        let mut tslows = [None, None];
        for (i1, itemopt) in self2.items.iter_mut().enumerate() {
            if let Some(item) = itemopt {
                if let Some(t1) = item.ts_min() {
                    if let Some((_, a)) = tslows[0] {
                        if t1 < a {
                            tslows[1] = tslows[0];
                            tslows[0] = Some((i1, t1));
                        } else {
                            if let Some((_, b)) = tslows[1] {
                                if t1 < b {
                                    tslows[1] = Some((i1, t1));
                                } else {
                                    // nothing to do
                                }
                            } else {
                                tslows[1] = Some((i1, t1));
                            }
                        }
                    } else {
                        tslows[0] = Some((i1, t1));
                    }
                } else {
                    self2.stats_process_item_empty += 1;
                    *itemopt = None;
                    return Ok(());
                }
            }
        }
        if DO_DETECT_NON_MONO {
            if let Some((i1, t1)) = tslows[0].as_ref() {
                if *t1 <= self.dim0ix_max {
                    self.dim0ix_max = *t1;
                    let item = LogItem::info(format!(
                        "dim0ix_max  {} vs {}   diff {}  i1 {i1}",
                        self.dim0ix_max,
                        t1,
                        self.dim0ix_max.ns() - t1.ns()
                    ));
                    log_items.push(item);
                }
            }
        }
        trace4!("tslows {:?}", tslows);
        let ret = if let Some((il0, _tl0)) = tslows[0] {
            if let Some((_il1, tl1)) = tslows[1] {
                // There is a second input, take only up to the second highest timestamp
                let item = self.items[il0].as_mut().unwrap();
                if let Some(th0) = item.ts_max() {
                    if th0 <= tl1 {
                        // Can take the whole item
                        self.take_item_full_cnt += 1;
                        let mut item = self.items[il0].take().unwrap();
                        trace3!("Take all from item {:?}", item);
                        match self.take_into_output_all(&mut item) {
                            DrainIntoDstResult::Done => {
                                // TODO can we eliminate the unwraps?
                                self.dim0ix_max = self.out.as_ref().unwrap().ts_max().unwrap();
                                Ok(())
                            }
                            DrainIntoDstResult::Partial => {
                                // TODO count for stats
                                trace3!("Put item back");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                // TODO can we eliminate the unwraps?
                                self.dim0ix_max = self.out.as_ref().unwrap().ts_max().unwrap();
                                Ok(())
                            }
                            DrainIntoDstResult::NotCompatible => {
                                // TODO count for stats
                                trace3!("Put item back");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                // TODO we assume that nothing got drained.
                                Ok(())
                            }
                        }
                    } else {
                        // Take only up to including the lowest ts of the second-lowest input
                        self.take_item_partial_cnt += 1;
                        let mut item = self.items[il0].take().unwrap();
                        trace3!("Take up to {} from item {:?}", tl1, item);
                        match self.take_into_output_upto(&mut item, tl1) {
                            DrainIntoDstResult::Done => {
                                if item.len() == 0 {
                                    // TODO should never be here because we should have taken the whole item
                                    Err(Error::PartialPathDrainedAllItems)
                                } else {
                                    self.items[il0] = Some(item);
                                    // TODO can we eliminate the unwraps?
                                    self.dim0ix_max = self.out.as_ref().unwrap().ts_max().unwrap();
                                    Ok(())
                                }
                            }
                            DrainIntoDstResult::Partial => {
                                // TODO count for stats
                                trace3!("Put item back because Partial");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                // TODO can we eliminate the unwraps?
                                self.dim0ix_max = self.out.as_ref().unwrap().ts_max().unwrap();
                                Ok(())
                            }
                            DrainIntoDstResult::NotCompatible => {
                                // TODO count for stats
                                trace3!("Put item back because NotCompatible");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                // TODO we assume that nothing got drained.
                                Ok(())
                            }
                        }
                    }
                } else {
                    Err(Error::ItemShouldHaveTsMax)
                }
            } else {
                // No other input, take the whole item
                self.take_item_single_inp_cnt += 1;
                let mut item = self.items[il0].take().unwrap();
                trace3!("Take all from item (no other input) {:?}", item);
                match self.take_into_output_all(&mut item) {
                    DrainIntoDstResult::Done => {
                        // TODO can we eliminate the unwraps?
                        self.dim0ix_max = self.out.as_ref().unwrap().ts_max().unwrap();
                        Ok(())
                    }
                    DrainIntoDstResult::Partial => {
                        // TODO count for stats
                        trace3!("Put item back");
                        self.items[il0] = Some(item);
                        self.do_clear_out = true;
                        // TODO can we eliminate the unwraps?
                        self.dim0ix_max = self.out.as_ref().unwrap().ts_max().unwrap();
                        Ok(())
                    }
                    DrainIntoDstResult::NotCompatible => {
                        // TODO count for stats
                        trace3!("Put item back");
                        self.items[il0] = Some(item);
                        self.do_clear_out = true;
                        // TODO we assume that nothing got drained.
                        Ok(())
                    }
                }
            }
        } else {
            Err(Error::ShouldFindTsMin)
        };
        let dim0ix_max = self.dim0ix_max;
        if let Some((_, tl1)) = tslows[1] {
            if tl1 <= dim0ix_max {
                for item in self.items.iter_mut().filter_map(|x| x.as_mut()) {
                    loop {
                        break if let Some(ts_min) = item.ts_min() {
                            if ts_min <= dim0ix_max {
                                // TODO add plain discard function to avoid the loop
                                match Self::drain_into_new_upto(item, dim0ix_max) {
                                    DrainIntoNewResult::Done(_) => {}
                                    DrainIntoNewResult::Partial(_) => continue,
                                    DrainIntoNewResult::NotCompatible => {
                                        // TODO drain into new result must not have this variant
                                        return Err(Error::Logic);
                                    }
                                }
                            }
                        };
                    }
                }
            }
        }
        ret
    }

    fn refill(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        trace4!("refill");
        use Poll::*;
        let mut has_pending = false;
        for i in 0..self.inps.0.len() {
            if self.items[i].is_none() {
                while let Some(inp) = self.inps.0[i].as_mut() {
                    match inp.poll_next_unpin(cx) {
                        Ready(Some(Ok(k))) => match k {
                            StreamItem::DataItem(k) => match k {
                                RangeCompletableItem::Data(k) => {
                                    if k.is_monotonic() == false {
                                        return Err(Error::InputNotMonotonic);
                                    }
                                    self.items[i] = Some(k);
                                    trace4!("refilled {}", i);
                                }
                                RangeCompletableItem::RangeComplete => {
                                    self.range_complete[i] = true;
                                    trace_inp_special!("range_complete {:?}", self.range_complete);
                                    continue;
                                }
                            },
                            StreamItem::Log(item) => {
                                // TODO limit queue length
                                self.outbuf.push_back(Ok(StreamItem::Log(item)));
                                continue;
                            }
                            StreamItem::Stats(item) => {
                                // TODO limit queue length
                                self.outbuf.push_back(Ok(StreamItem::Stats(item)));
                                continue;
                            }
                        },
                        Ready(Some(Err(e))) => {
                            self.inps.0[i] = None;
                            return Err(Error::Input(e));
                        }
                        Ready(None) => {
                            self.inps.0[i] = None;
                        }
                        Pending => {
                            has_pending = true;
                        }
                    }
                    break;
                }
            }
        }
        if has_pending { Ok(Pending) } else { Ok(Ready(())) }
    }

    fn poll3(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Option<Error>>> {
        use ControlFlow::*;
        use Poll::*;
        trace4!("poll3");
        #[allow(unused)]
        let ninps = self.inps.0.iter().filter(|a| a.is_some()).count();
        let nitems = self.items.iter().filter(|a| a.is_some()).count();
        let nitemsmissing = self
            .inps
            .0
            .iter()
            .zip(self.items.iter())
            .filter(|(a, b)| a.is_some() && b.is_none())
            .count();
        trace3!("ninps {}  nitems {}  nitemsmissing {}", ninps, nitems, nitemsmissing);
        if nitemsmissing != 0 {
            let e = Error::NoPendingButMissing;
            return Break(Ready(Some(e)));
        }
        let last_emit = nitems == 0;
        if nitems != 0 {
            match Self::process(self.as_mut(), cx) {
                Ok(()) => {}
                Err(e) => return Break(Ready(Some(e))),
            }
        }
        if let Some(out) = self.out.as_ref() {
            if out.len() >= self.out_max_len || out.byte_estimate() >= OUT_MAX_BYTES || self.do_clear_out || last_emit {
                if out.len() > 2 * self.out_max_len {
                    debug_inp!("over length item  {} vs {}", out.len(), self.out_max_len);
                }
                if out.byte_estimate() > 2 * OUT_MAX_BYTES {
                    debug_inp!("over weight item  {} vs {}", out.byte_estimate(), OUT_MAX_BYTES);
                }
                trace3!("decide to output");
                self.do_clear_out = false;
                if let Some(out) = self.take_outbuf_filtered() {
                    let item = sitem_data(out);
                    self.outbuf.push_back(item);
                }
                Continue(())
            } else {
                trace4!("not enough output yet  {}", out.len());
                Continue(())
            }
        } else {
            trace4!("no output candidate");
            if last_emit { Break(Ready(None)) } else { Continue(()) }
        }
    }

    fn poll2(mut self: Pin<&mut Self>, cx: &mut Context) -> ControlFlow<Poll<Option<Error>>> {
        use ControlFlow::*;
        use Poll::*;
        match Self::refill(Pin::new(&mut self), cx) {
            Ok(Ready(())) => Self::poll3(self, cx),
            Ok(Pending) => Break(Pending),
            Err(e) => Break(Ready(Some(e))),
        }
    }
}

impl<T> Stream for Merger<T>
where
    T: MergeableTy,
{
    type Item = Sitemty<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        // let span1 = log::span!(log::Level::INFO, "Merger", pc = self.poll_count);
        let span1 = log::span!(log::Level::INFO, "Merger");
        let _spg = span1.enter();
        loop {
            trace3!("poll");
            break if self.complete {
                panic!("poll after complete");
            } else if self.done_range_complete {
                self.complete = true;
                Ready(None)
            } else if let Some(item) = self.log_queue.pop_front() {
                Ready(Some(Ok(StreamItem::Log(item))))
            } else if let Some(item) = self.outbuf.pop_front() {
                trace_emit!("emit item");
                let item = on_sitemty_data!(item, |k: T| {
                    trace_emit!("emit item len {}", k.len());
                    sitem_data(k)
                });
                Ready(Some(item))
            } else if self.done_inp == false {
                match Self::poll2(self.as_mut(), cx) {
                    ControlFlow::Continue(()) => continue,
                    ControlFlow::Break(k) => match k {
                        Ready(Some(e)) => {
                            self.done_inp = true;
                            Ready(Some(Err(sitem_err2_from_string(e))))
                        }
                        Ready(None) => {
                            self.done_inp = true;
                            if let Some(out) = self.take_outbuf_filtered() {
                                trace_emit_special!("done_data emit buffered  len {}", out.len());
                                self.outbuf.push_back(sitem_data(out));
                            }
                            continue;
                        }
                        Pending => Pending,
                    },
                }
            } else {
                self.done_range_complete = true;
                if self.range_complete.iter().all(|x| *x) {
                    trace_emit_special!("emit RangeComplete");
                    let item = Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete));
                    self.outbuf.push_back(item);
                }
                continue;
            };
        }
    }
}
