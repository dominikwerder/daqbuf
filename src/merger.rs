use crate::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::container::ByteEstimate;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Events;
use items_0::MergeError;
use items_0::WithLen;
use netpod::log::*;
use netpod::TsMs;
use std::collections::VecDeque;
use std::fmt;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

const OUT_MAX_BYTES: u64 = 1024 * 200;
const DO_DETECT_NON_MONO: bool = true;

macro_rules! trace2 { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! trace3 { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

macro_rules! trace4 { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

pub trait Mergeable<Rhs = Self>: fmt::Debug + WithLen + ByteEstimate + Unpin {
    fn ts_min(&self) -> Option<u64>;
    fn ts_max(&self) -> Option<u64>;
    fn new_empty(&self) -> Self;
    fn clear(&mut self);
    // TODO when MergeError::Full gets returned, any guarantees about what has been modified or kept unchanged?
    fn drain_into(&mut self, dst: &mut Self, range: (usize, usize)) -> Result<(), MergeError>;
    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize>;
    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize>;
    fn find_highest_index_lt(&self, ts: u64) -> Option<usize>;
    // TODO only for testing:
    fn tss(&self) -> Vec<TsMs>;
}

impl Mergeable for Box<dyn Events> {
    fn ts_min(&self) -> Option<u64> {
        self.as_ref().ts_min()
    }

    fn ts_max(&self) -> Option<u64> {
        self.as_ref().ts_max()
    }

    fn new_empty(&self) -> Self {
        self.as_ref().new_empty_evs()
    }

    fn clear(&mut self) {
        Events::clear(self.as_mut())
    }

    fn drain_into(&mut self, dst: &mut Self, range: (usize, usize)) -> Result<(), MergeError> {
        self.as_mut().drain_into_evs(dst, range)
    }

    fn find_lowest_index_gt(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_lowest_index_gt_evs(ts)
    }

    fn find_lowest_index_ge(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_lowest_index_ge_evs(ts)
    }

    fn find_highest_index_lt(&self, ts: u64) -> Option<usize> {
        self.as_ref().find_highest_index_lt_evs(ts)
    }

    fn tss(&self) -> Vec<netpod::TsMs> {
        Events::tss(self)
            .iter()
            .map(|x| netpod::TsMs::from_ns_u64(*x))
            .collect()
    }
}

type MergeInp<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;

pub struct Merger<T> {
    inps: Vec<Option<MergeInp<T>>>,
    items: Vec<Option<T>>,
    out: Option<T>,
    do_clear_out: bool,
    out_max_len: usize,
    range_complete: Vec<bool>,
    out_of_band_queue: VecDeque<Sitemty<T>>,
    log_queue: VecDeque<LogItem>,
    dim0ix_max: u64,
    done_emit_first_empty: bool,
    done_data: bool,
    done_buffered: bool,
    done_range_complete: bool,
    complete: bool,
    poll_count: usize,
}

impl<T> fmt::Debug for Merger<T>
where
    T: Mergeable,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let inps: Vec<_> = self.inps.iter().map(|x| x.is_some()).collect();
        fmt.debug_struct(std::any::type_name::<Self>())
            .field("inps", &inps)
            .field("items", &self.items)
            .field("out_max_len", &self.out_max_len)
            .field("range_complete", &self.range_complete)
            .field("out_of_band_queue", &self.out_of_band_queue.len())
            .field("done_data", &self.done_data)
            .field("done_buffered", &self.done_buffered)
            .field("done_range_complete", &self.done_range_complete)
            .finish()
    }
}

impl<T> Merger<T>
where
    T: Mergeable,
{
    pub fn new(inps: Vec<MergeInp<T>>, out_max_len: Option<u32>) -> Self {
        let n = inps.len();
        Self {
            inps: inps.into_iter().map(|x| Some(x)).collect(),
            items: (0..n).into_iter().map(|_| None).collect(),
            out: None,
            do_clear_out: false,
            out_max_len: out_max_len.unwrap_or(1000) as usize,
            range_complete: vec![false; n],
            out_of_band_queue: VecDeque::new(),
            log_queue: VecDeque::new(),
            dim0ix_max: 0,
            done_emit_first_empty: false,
            done_data: false,
            done_buffered: false,
            done_range_complete: false,
            complete: false,
            poll_count: 0,
        }
    }

    fn drain_into_upto(src: &mut T, dst: &mut T, upto: u64) -> Result<(), MergeError> {
        match src.find_lowest_index_gt(upto) {
            Some(ilgt) => {
                src.drain_into(dst, (0, ilgt))?;
            }
            None => {
                // TODO should not be here.
                src.drain_into(dst, (0, src.len()))?;
            }
        }
        Ok(())
    }

    fn take_into_output_all(&mut self, src: &mut T) -> Result<(), MergeError> {
        // TODO optimize the case when some large batch should be added to some existing small batch already in out.
        // TODO maybe use two output slots?
        self.take_into_output_upto(src, u64::MAX)
    }

    fn take_into_output_upto(&mut self, src: &mut T, upto: u64) -> Result<(), MergeError> {
        // TODO optimize the case when some large batch should be added to some existing small batch already in out.
        // TODO maybe use two output slots?
        if let Some(out) = self.out.as_mut() {
            Self::drain_into_upto(src, out, upto)?;
        } else {
            trace2!("move into fresh");
            let mut fresh = src.new_empty();
            Self::drain_into_upto(src, &mut fresh, upto)?;
            self.out = Some(fresh);
        }
        Ok(())
    }

    fn process(mut self: Pin<&mut Self>, _cx: &mut Context) -> Result<ControlFlow<()>, Error> {
        use ControlFlow::*;
        trace4!("process");
        let mut log_items = Vec::new();
        let mut tslows = [None, None];
        for (i1, itemopt) in self.items.iter_mut().enumerate() {
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
                    // the item seems empty.
                    // TODO count for stats.
                    trace2!("empty item, something to do here?");
                    *itemopt = None;
                    return Ok(Continue(()));
                }
            }
        }
        if DO_DETECT_NON_MONO {
            if let Some((i1, t1)) = tslows[0].as_ref() {
                if *t1 <= self.dim0ix_max {
                    self.dim0ix_max = *t1;
                    let item = LogItem {
                        node_ix: *i1 as _,
                        level: Level::INFO,
                        msg: format!(
                            "dim0ix_max  {} vs {}   diff {}",
                            self.dim0ix_max,
                            t1,
                            self.dim0ix_max - t1
                        ),
                    };
                    log_items.push(item);
                }
            }
        }
        trace4!("tslows {tslows:?}");
        if let Some((il0, _tl0)) = tslows[0] {
            if let Some((_il1, tl1)) = tslows[1] {
                // There is a second input, take only up to the second highest timestamp
                let item = self.items[il0].as_mut().unwrap();
                if let Some(th0) = item.ts_max() {
                    if th0 <= tl1 {
                        // Can take the whole item
                        // TODO gather stats about this case. Should be never for databuffer, and often for scylla.
                        let mut item = self.items[il0].take().unwrap();
                        trace3!("Take all from item {item:?}");
                        match self.take_into_output_all(&mut item) {
                            Ok(()) => Ok(Break(())),
                            Err(MergeError::Full) | Err(MergeError::NotCompatible) => {
                                // TODO count for stats
                                trace3!("Put item back");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                Ok(Break(()))
                            }
                        }
                    } else {
                        // Take only up to the lowest ts of the second-lowest input
                        let mut item = self.items[il0].take().unwrap();
                        trace3!("Take up to {tl1} from item {item:?}");
                        let res = self.take_into_output_upto(&mut item, tl1);
                        match res {
                            Ok(()) => {
                                if item.len() == 0 {
                                    // TODO should never be here because we should have taken the whole item
                                    Err(format!("Should have taken the whole item instead").into())
                                } else {
                                    self.items[il0] = Some(item);
                                    Ok(Break(()))
                                }
                            }
                            Err(MergeError::Full) | Err(MergeError::NotCompatible) => {
                                // TODO count for stats
                                info!("Put item back because {res:?}");
                                self.items[il0] = Some(item);
                                self.do_clear_out = true;
                                Ok(Break(()))
                            }
                        }
                    }
                } else {
                    // TODO should never be here because ts-max should always exist here.
                    Err(format!("selected input without max ts").into())
                }
            } else {
                // No other input, take the whole item
                let mut item = self.items[il0].take().unwrap();
                trace3!("Take all from item (no other input) {item:?}");
                match self.take_into_output_all(&mut item) {
                    Ok(()) => Ok(Break(())),
                    Err(_) => {
                        // TODO count for stats
                        trace3!("Put item back");
                        self.items[il0] = Some(item);
                        self.do_clear_out = true;
                        Ok(Break(()))
                    }
                }
            }
        } else {
            Err(format!("after low ts search nothing found").into())
        }
    }

    fn refill(mut self: Pin<&mut Self>, cx: &mut Context) -> Result<Poll<()>, Error> {
        trace4!("refill");
        use Poll::*;
        let mut has_pending = false;
        for i in 0..self.inps.len() {
            if self.items[i].is_none() {
                while let Some(inp) = self.inps[i].as_mut() {
                    match inp.poll_next_unpin(cx) {
                        Ready(Some(Ok(k))) => match k {
                            StreamItem::DataItem(k) => match k {
                                RangeCompletableItem::Data(k) => {
                                    if self.done_emit_first_empty == false {
                                        trace!("emit first empty marker item");
                                        self.done_emit_first_empty = true;
                                        let item = k.new_empty();
                                        let item = sitem_data(item);
                                        self.out_of_band_queue.push_back(item);
                                    }
                                    self.items[i] = Some(k);
                                    trace4!("refilled {}", i);
                                }
                                RangeCompletableItem::RangeComplete => {
                                    self.range_complete[i] = true;
                                    trace!("range_complete {:?}", self.range_complete);
                                    continue;
                                }
                            },
                            StreamItem::Log(item) => {
                                // TODO limit queue length
                                self.out_of_band_queue.push_back(Ok(StreamItem::Log(item)));
                                continue;
                            }
                            StreamItem::Stats(item) => {
                                // TODO limit queue length
                                self.out_of_band_queue
                                    .push_back(Ok(StreamItem::Stats(item)));
                                continue;
                            }
                        },
                        Ready(Some(Err(e))) => {
                            self.inps[i] = None;
                            return Err(e.into());
                        }
                        Ready(None) => {
                            self.inps[i] = None;
                        }
                        Pending => {
                            has_pending = true;
                        }
                    }
                    break;
                }
            }
        }
        if has_pending {
            Ok(Pending)
        } else {
            Ok(Ready(()))
        }
    }

    fn poll3(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> ControlFlow<Poll<Option<Result<T, Error>>>> {
        use ControlFlow::*;
        use Poll::*;
        trace4!("poll3");
        #[allow(unused)]
        let ninps = self.inps.iter().filter(|a| a.is_some()).count();
        let nitems = self.items.iter().filter(|a| a.is_some()).count();
        let nitemsmissing = self
            .inps
            .iter()
            .zip(self.items.iter())
            .filter(|(a, b)| a.is_some() && b.is_none())
            .count();
        trace3!("ninps {ninps}  nitems {nitems}  nitemsmissing {nitemsmissing}");
        if nitemsmissing != 0 {
            let e = Error::from(format!("missing but no pending"));
            return Break(Ready(Some(Err(e))));
        }
        let last_emit = nitems == 0;
        if nitems != 0 {
            match Self::process(Pin::new(&mut self), cx) {
                Ok(Break(())) => {}
                Ok(Continue(())) => {}
                Err(e) => return Break(Ready(Some(Err(e)))),
            }
        }
        if let Some(o) = self.out.as_ref() {
            if o.len() >= self.out_max_len
                || o.byte_estimate() >= OUT_MAX_BYTES
                || self.do_clear_out
                || last_emit
            {
                if o.len() > self.out_max_len {
                    debug!(
                        "MERGER OVERWEIGHT ITEM  {} vs {}",
                        o.len(),
                        self.out_max_len
                    );
                }
                trace3!("decide to output");
                self.do_clear_out = false;
                //Break(Ready(Some(Ok(self.out.take().unwrap()))))
                let item = sitem_data(self.out.take().unwrap());
                self.out_of_band_queue.push_back(item);
                Continue(())
            } else {
                trace4!("not enough output yet");
                Continue(())
            }
        } else {
            trace!("no output candidate");
            if last_emit {
                Break(Ready(None))
            } else {
                Continue(())
            }
        }
    }

    fn poll2(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> ControlFlow<Poll<Option<Result<T, Error>>>> {
        use ControlFlow::*;
        use Poll::*;
        match Self::refill(Pin::new(&mut self), cx) {
            Ok(Ready(())) => Self::poll3(self, cx),
            Ok(Pending) => Break(Pending),
            Err(e) => Break(Ready(Some(Err(e)))),
        }
    }
}

impl<T> Stream for Merger<T>
where
    T: Mergeable,
{
    type Item = Sitemty<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        self.poll_count += 1;
        let span1 = span!(Level::INFO, "Merger", pc = self.poll_count);
        let _spg = span1.enter();
        loop {
            trace3!("poll");
            break if let Some(item) = self.log_queue.pop_front() {
                Ready(Some(Ok(StreamItem::Log(item))))
            } else if self.poll_count == usize::MAX {
                self.done_range_complete = true;
                continue;
            } else if self.complete {
                panic!("poll after complete");
            } else if self.done_range_complete {
                self.complete = true;
                Ready(None)
            } else if self.done_buffered {
                self.done_range_complete = true;
                if self.range_complete.iter().all(|x| *x) {
                    trace!("emit RangeComplete");
                    Ready(Some(Ok(StreamItem::DataItem(
                        RangeCompletableItem::RangeComplete,
                    ))))
                } else {
                    continue;
                }
            } else if self.done_data {
                trace!("done_data");
                self.done_buffered = true;
                if let Some(out) = self.out.take() {
                    trace!("done_data emit buffered  len {}", out.len());
                    Ready(Some(sitem_data(out)))
                } else {
                    continue;
                }
            } else if let Some(item) = self.out_of_band_queue.pop_front() {
                let item = on_sitemty_data!(item, |k: T| {
                    trace3!("emit out-of-band data  len {}", k.len());
                    sitem_data(k)
                });
                Ready(Some(item))
            } else {
                match Self::poll2(self.as_mut(), cx) {
                    ControlFlow::Continue(()) => continue,
                    ControlFlow::Break(k) => match k {
                        Ready(Some(Ok(out))) => {
                            if true {
                                error!("THIS BRANCH SHOULD NO LONGER OCCUR, REFACTOR");
                                self.done_data = true;
                                let e = Error::from(format!("TODO refactor direct emit in merger"));
                                return Ready(Some(Err(e.into())));
                            }
                            trace!("emit buffered  len {}", out.len());
                            Ready(Some(sitem_data(out)))
                        }
                        Ready(Some(Err(e))) => {
                            self.done_data = true;
                            Ready(Some(Err(e.into())))
                        }
                        Ready(None) => {
                            self.done_data = true;
                            continue;
                        }
                        Pending => Pending,
                    },
                }
            };
        }
    }
}
