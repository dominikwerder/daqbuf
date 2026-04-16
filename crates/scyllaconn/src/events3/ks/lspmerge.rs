use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::streamitem::sitem2_data;
use netpod::TsNano;
use netpod::hpp::HaveProgressPending;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ) }

fn _keep() {
    error!("");
    warn!("");
    info!("");
    debug!("");
    trace!("");
}

autoerr::create_error_v1!(
    name(Error, "LspMerge"),
    enum variants {
        FindNextTsWithoutBuf,
        FindNextTsOnEmptyBuf,
        LoopTooMany,
        Logic,
        BadDrain(usize, usize),
        ItemInconsistent,
        NotCompatible,
        InputBoxed(Box<dyn std::error::Error + Send>),
        InconsistentA,
        InconsistentB,
        InconsistentC,
        InconsistentD,
    },
);

const DO_CHECK_CONSISTENT: bool = false;

#[derive(Debug)]
struct Inp<S, T, E>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: fmt::Debug + Unpin + MergeableTy,
    E: fmt::Debug + Unpin + std::error::Error,
{
    evs: Option<S>,
    buf: Option<T>,
}

#[derive(Debug, Clone, Copy)]
struct InputIx(usize);

impl fmt::Display for InputIx {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

#[derive(Debug, Clone)]
enum NextTs {
    None,
    One(TsNano, InputIx),
    Two(TsNano, InputIx, TsNano, InputIx),
}

#[derive(Debug)]
struct Merging<S, T, E>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: fmt::Debug + Unpin + MergeableTy,
    E: fmt::Debug + Unpin + std::error::Error,
{
    inps: Vec<Inp<S, T, E>>,
    evbuf: Option<T>,
    lsp_limit: u32,
    lsp_minfill: u32,
}

impl<S, T, E> Merging<S, T, E>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: fmt::Debug + Unpin + MergeableTy,
    E: fmt::Debug + Unpin + std::error::Error + Send + 'static,
{
    fn find_next_ts(&self) -> Result<NextTs, Error> {
        let mut best: Option<(TsNano, usize)> = None;
        let mut second: Option<(TsNano, usize)> = None;
        for (ix, inp) in self.inps.iter().enumerate() {
            if let Some(buf) = inp.buf.as_ref() {
                let tsmin = MergeableTy::ts_min(buf);
                if let Some(ts) = tsmin {
                    match best {
                        Some((ts_1st, _)) => {
                            if ts < ts_1st {
                                second = best;
                                best = Some((ts, ix));
                            } else {
                                match second {
                                    None => {
                                        second = Some((ts, ix));
                                    }
                                    Some((ts_2nd, _)) => {
                                        if ts < ts_2nd {
                                            second = Some((ts, ix));
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            best = Some((ts, ix));
                        }
                    }
                } else {
                    return Err(Error::FindNextTsOnEmptyBuf);
                }
            } else {
                return Err(Error::FindNextTsWithoutBuf);
            }
        }
        let ret = match (best, second) {
            (None, _) => NextTs::None,
            (Some((ts1, ix1)), None) => NextTs::One(ts1, InputIx(ix1)),
            (Some((ts1, ix1)), Some((ts2, ix2))) => NextTs::Two(ts1, InputIx(ix1), ts2, InputIx(ix2)),
        };
        Ok(ret)
    }

    fn poll_all_inp(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Sitemty2<(), Error>>> {
        use Poll::*;
        // TODO when does this loop actually terminate?
        let mut il1 = 0u32;
        'outer: loop {
            il1 += 1;
            if il1 > 200 {
                error!("poll_all_inp  loop iteration too many");
                break Ready(Some(Err(Error::LoopTooMany)));
            }
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            for inp in &mut self2.inps {
                if let Some(buf) = inp.buf.as_mut() {
                    if buf.len() == 0 {
                        hpp.mark_progress();
                        inp.buf = None;
                    }
                } else {
                    if let Some(inps) = inp.evs.as_mut() {
                        match inps.poll_next_unpin(cx) {
                            Ready(Some(x)) => {
                                hpp.mark_progress();
                                match x {
                                    Ok(x) => match x {
                                        StreamItem::DataItem(x) => match x {
                                            RangeCompletableItem::Data(x) => {
                                                if x.len() == 0 {
                                                    // TODO count for metrics
                                                } else if DO_CHECK_CONSISTENT && !x.is_consistent() {
                                                    break 'outer Ready(Some(Err(Error::ItemInconsistent)));
                                                } else {
                                                    inp.buf = Some(x);
                                                }
                                            }
                                            RangeCompletableItem::RangeComplete => {
                                                break 'outer Ready(Some(Ok(StreamItem::DataItem(
                                                    RangeCompletableItem::RangeComplete,
                                                ))));
                                            }
                                        },
                                        StreamItem::Log(x) => {
                                            break 'outer Ready(Some(Ok(StreamItem::Log(x))));
                                        }
                                        StreamItem::Stats(x) => {
                                            break 'outer Ready(Some(Ok(StreamItem::Stats(x))));
                                        }
                                    },
                                    Err(e) => break 'outer Ready(Some(Err(Error::InputBoxed(Box::new(e))))),
                                }
                            }
                            Ready(None) => {
                                hpp.mark_progress();
                                inp.evs = None;
                            }
                            Pending => {
                                hpp.mark_pending();
                            }
                        }
                    } else {
                        // will get removed
                    }
                }
            }
            {
                let n1 = self2.inps.len();
                self2.inps.retain(|x| x.buf.is_some() || x.evs.is_some());
                let n2 = self2.inps.len();
                if n1 != n2 {
                    hpp.mark_progress();
                }
            }
            let n_buf_none = self2.inps.iter().filter(|x| x.buf.is_none());
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else if n_buf_none.count() == 0 {
                Ready(Some(sitem2_data(())))
            } else {
                Ready(None)
            };
        }
    }

    fn check_inputs(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Sitemty2<T, Error>>> {
        use Poll::*;
        let selfname = "check_inputs";
        let mut il1 = 0u32;
        loop {
            il1 += 1;
            if il1 > 140000 {
                error!("{selfname}  loop iteration too many");
                break Ready(Some(Err(Error::LoopTooMany)));
            }
            let mut hpp = HaveProgressPending::new();
            match self.as_mut().poll_all_inp(cx) {
                Ready(Some(x)) => {
                    hpp.mark_progress();
                    match x {
                        Ok(x) => {
                            match x {
                                StreamItem::DataItem(x) => match x {
                                    RangeCompletableItem::Data(()) => {
                                        const LOG_DETAIL: bool = false;
                                        let self2 = self.as_mut().get_mut();
                                        let draininfo = match self2.find_next_ts()? {
                                            NextTs::None => {
                                                if self2.inps.len() == 0 {
                                                    if let Some(evbuf) = self2.evbuf.take() {
                                                        break Ready(Some(sitem2_data(evbuf)));
                                                    } else {
                                                        break Ready(None);
                                                    }
                                                } else {
                                                    error!("check_inputs  find_next_ts  None  but inps not empty");
                                                    self2.inps.clear();
                                                    None
                                                }
                                            }
                                            NextTs::One(ts1, ix1) => {
                                                let b1 = &mut self2.inps.get_mut(ix1.0).unwrap().buf;
                                                let b2 = b1.as_mut().unwrap();
                                                let i3 = b2.len();
                                                if LOG_DETAIL {
                                                    info!("NextTs::One  {ix1}  {ts1}");
                                                }
                                                Some((ix1, i3))
                                            }
                                            NextTs::Two(ts1, ix1, ts2, ix2) => {
                                                let b1 = &mut self2.inps.get_mut(ix1.0).unwrap().buf;
                                                let b2 = b1.as_mut().unwrap();
                                                let i3 = MergeableTy::find_pp_le(b2, ts2);
                                                if LOG_DETAIL {
                                                    info!("NextTs::Two  {ix1}  {ts1}  {ix2}  {ts2}");
                                                }
                                                Some((ix1, i3))
                                            }
                                        };
                                        if false && let Some((ix1, i3)) = draininfo {
                                            // Basic merge
                                            let b1 = &mut self2.inps.get_mut(ix1.0).unwrap().buf;
                                            let b2 = b1.as_mut().unwrap();
                                            let b2len0 = b2.len();
                                            let ndr = i3.min(self2.lsp_minfill as _);
                                            match MergeableTy::drain_into_new(b2, 0..ndr) {
                                                DrainIntoNewResult::Done(c) | DrainIntoNewResult::Partial(c) => {
                                                    let b2len = b2.len();
                                                    if b2len == 0 {
                                                        *b1 = None;
                                                    }
                                                    if b2len >= b2len0 {
                                                        break Ready(Some(Err(Error::BadDrain(b2len0, b2len))));
                                                    } else if c.len() > self2.lsp_minfill as _ {
                                                        break Ready(Some(Err(Error::Logic)));
                                                    } else {
                                                        break Ready(Some(sitem2_data(c)));
                                                    }
                                                }
                                                DrainIntoNewResult::NotCompatible => {
                                                    // should not happen because we drain into new
                                                    // TODO metrics
                                                    *b1 = None;
                                                    break Ready(Some(Err(Error::NotCompatible)));
                                                }
                                            }
                                        } else if let Some((ix1, i3)) = draininfo {
                                            let b1 = &mut self2.inps.get_mut(ix1.0).unwrap().buf;
                                            let b2 = b1.as_mut().unwrap();
                                            let b2len0 = b2.len();
                                            if DO_CHECK_CONSISTENT && !b2.is_consistent() {
                                                let e = Error::InconsistentB;
                                                break Ready(Some(Err(e)));
                                            }
                                            if let Some(evbuf) = self2.evbuf.as_mut() {
                                                let evbuflen0 = evbuf.len();
                                                if evbuflen0 >= self2.lsp_limit as _ {
                                                    let c = self2.evbuf.take().unwrap();
                                                    break Ready(Some(sitem2_data(c)));
                                                } else {
                                                    let space = self2.lsp_limit as usize - evbuflen0;
                                                    let ndr = i3.min(space);
                                                    use items_0::merge::DrainIntoDstResult;
                                                    if LOG_DETAIL {
                                                        info!("BEFORE DRAIN INTO:");
                                                        for (i, x) in evbuf.tss_for_testing().iter().enumerate() {
                                                            info!("{i:4} {x}");
                                                        }
                                                        info!("-----------------");
                                                        info!("drain_into  {ix1}  ndr {ndr}");
                                                    }
                                                    match MergeableTy::drain_into(b2, evbuf, 0..ndr) {
                                                        DrainIntoDstResult::Done | DrainIntoDstResult::Partial => {
                                                            if LOG_DETAIL {
                                                                info!("AFTER  DRAIN INTO:");
                                                                for (i, x) in evbuf.tss_for_testing().iter().enumerate()
                                                                {
                                                                    info!("{i:4} {x}");
                                                                }
                                                                info!("-----------------");
                                                            }
                                                            let evbuflen2 = evbuf.len();
                                                            if DO_CHECK_CONSISTENT && !b2.is_consistent() {
                                                                let e = Error::InconsistentC;
                                                                break Ready(Some(Err(e)));
                                                            }
                                                            if DO_CHECK_CONSISTENT && !evbuf.is_consistent() {
                                                                for (i, x) in evbuf.tss_for_testing().iter().enumerate()
                                                                {
                                                                    info!("{i:4} {x}");
                                                                }
                                                                info!("evbuflen0 {evbuflen0}  evbuflen2 {evbuflen2}");
                                                                let e = Error::InconsistentD;
                                                                break Ready(Some(Err(e)));
                                                            }
                                                            let b2len2 = b2.len();
                                                            if b2len2 == 0 {
                                                                *b1 = None;
                                                            }
                                                            if b2len2 >= b2len0 {
                                                                let e = Error::BadDrain(b2len0, b2len2);
                                                                break Ready(Some(Err(e)));
                                                            } else if evbuf.len() > self2.lsp_limit as _ {
                                                                let e = Error::BadDrain(b2len0, b2len2);
                                                                break Ready(Some(Err(e)));
                                                            } else if evbuf.len() >= self2.lsp_limit as _ {
                                                                let c = self2.evbuf.take().unwrap();
                                                                break Ready(Some(sitem2_data(c)));
                                                            } else {
                                                            }
                                                        }
                                                        DrainIntoDstResult::NotCompatible => {
                                                            if b2.len() == 0 {
                                                                *b1 = None;
                                                            }
                                                            let c = self2.evbuf.take().unwrap();
                                                            break Ready(Some(sitem2_data(c)));
                                                        }
                                                    }
                                                }
                                            } else {
                                                // no tmp buffer
                                                if i3 >= b2len0 && b2len0 <= self2.lsp_limit as _ {
                                                    let c = b1.take().unwrap();
                                                    break Ready(Some(sitem2_data(c)));
                                                } else {
                                                    let ndr = i3.min(self2.lsp_limit as _);
                                                    match MergeableTy::drain_into_new(b2, 0..ndr) {
                                                        DrainIntoNewResult::Done(c)
                                                        | DrainIntoNewResult::Partial(c) => {
                                                            let b2len2 = b2.len();
                                                            if b2len2 == 0 {
                                                                *b1 = None;
                                                            }
                                                            if DO_CHECK_CONSISTENT && !c.is_consistent() {
                                                                let e = Error::InconsistentA;
                                                                break Ready(Some(Err(e)));
                                                            } else if b2len2 >= b2len0 {
                                                                let e = Error::BadDrain(b2len0, b2len2);
                                                                break Ready(Some(Err(e)));
                                                            } else if c.len() > self2.lsp_limit as _ {
                                                                let e = Error::BadDrain(b2len0, b2len2);
                                                                break Ready(Some(Err(e)));
                                                            } else if c.len() >= self2.lsp_minfill as _ {
                                                                break Ready(Some(sitem2_data(c)));
                                                            } else {
                                                                if LOG_DETAIL {
                                                                    info!("KEEPING AS BUF:");
                                                                    for (i, x) in c.tss_for_testing().iter().enumerate()
                                                                    {
                                                                        info!("{i:4} {x}");
                                                                    }
                                                                    info!("-----------------");
                                                                }
                                                                self2.evbuf = Some(c);
                                                                // break Ready(Some(sitem2_data(c)));
                                                            }
                                                        }
                                                        DrainIntoNewResult::NotCompatible => {
                                                            // should not happen because we drain into new
                                                            // TODO metrics
                                                            *b1 = None;
                                                            break Ready(Some(Err(Error::NotCompatible)));
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            // can not drain or take
                                        }
                                    }
                                    RangeCompletableItem::RangeComplete => {
                                        break Ready(Some(Ok(StreamItem::DataItem(
                                            RangeCompletableItem::RangeComplete,
                                        ))));
                                    }
                                },
                                StreamItem::Log(x) => {
                                    break Ready(Some(Ok(StreamItem::Log(x))));
                                }
                                StreamItem::Stats(x) => {
                                    break Ready(Some(Ok(StreamItem::Stats(x))));
                                }
                            }
                        }
                        Err(e) => break Ready(Some(Err(e.into()))),
                    }
                }
                Ready(None) => {}
                Pending => {
                    hpp.mark_pending();
                }
            }
            // TODO check whether we have to open next msp before we can make a decision.
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}

#[derive(Debug)]
enum State<S, T, E>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: fmt::Debug + Unpin + MergeableTy,
    E: fmt::Debug + Unpin + std::error::Error,
{
    Merging(Merging<S, T, E>),
    Done,
}

/**
Merge a known number of event streams.
*/
#[derive(Debug)]
pub struct LspMerge<S, T, E>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: fmt::Debug + Unpin + MergeableTy,
    E: fmt::Debug + Unpin + std::error::Error,
{
    state: State<S, T, E>,
}

impl<S, T, E> LspMerge<S, T, E>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: fmt::Debug + Unpin + MergeableTy,
    E: fmt::Debug + Unpin + std::error::Error,
{
    pub fn new(inps: Vec<S>, lsp_limit: u32) -> Self {
        let inps = inps
            .into_iter()
            .map(|x| Inp {
                evs: Some(x),
                buf: None,
            })
            .collect();
        let state = State::Merging(Merging {
            inps,
            evbuf: None,
            lsp_limit,
            lsp_minfill: lsp_limit / 3,
        });
        Self { state }
    }
}

impl<S, T, E> Stream for LspMerge<S, T, E>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: fmt::Debug + Unpin + MergeableTy,
    E: fmt::Debug + Unpin + std::error::Error + Send + 'static,
{
    type Item = Sitemty2<T, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let selfname = "poll_next";
        let mut il1 = 0u32;
        loop {
            il1 += 1;
            if il1 > 200 {
                error!("{selfname}  loop iteration too many");
                self.state = State::Done;
                break Ready(Some(Err(Error::LoopTooMany)));
            }
            let mut hpp = HaveProgressPending::new();
            let self2 = self.as_mut().get_mut();
            match &mut self2.state {
                State::Merging(st1) => match Pin::new(st1).check_inputs(cx) {
                    Ready(Some(x)) => {
                        hpp.mark_progress();
                        match x {
                            Ok(x) => break Ready(Some(Ok(x))),
                            Err(e) => {
                                self2.state = State::Done;
                                break Ready(Some(Err(e)));
                            }
                        }
                    }
                    Ready(None) => {
                        hpp.mark_progress();
                        self2.state = State::Done;
                    }
                    Pending => {
                        hpp.mark_pending();
                    }
                },
                State::Done => {}
            }
            break if hpp.have_progress() {
                continue;
            } else if hpp.have_pending() {
                Pending
            } else {
                Ready(None)
            };
        }
    }
}
