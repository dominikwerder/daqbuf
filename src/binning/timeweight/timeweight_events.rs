use crate::binning::aggregator::AggregatorTimeWeight;
use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::ContainerEvents;
use crate::binning::container_events::ContainerEventsTakeUpTo;
use crate::binning::container_events::EventSingle;
use crate::binning::container_events::EventSingleRef;
use crate::binning::container_events::EventValueType;
use crate::binning::container_events::PartialOrdEvtA;
use crate::log;
use items_0::timebin::IngestReport;
use netpod::BinnedRange;
use netpod::DtNano;
use netpod::TsNano;
use serde::Serialize;
use std::fmt;
use std::mem;

macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }

macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }

macro_rules! trace_ { ($($arg:tt)*) => ( if false { log::trace!($($arg)*); } ) }

macro_rules! trace_init { ($($arg:tt)*) => ( if true { trace_!($($arg)*); } ) }

macro_rules! trace_output { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

macro_rules! trace_cycle { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

macro_rules! trace_event_next { ($fmt:expr, $($arg:tt)*) => (
    if false {
        trace_!("{}  {}", "\x1b[1mEVENT POP FRONT\x1b[0m  ", format_args!($fmt, $($arg)*));
    }
) }

macro_rules! trace_ingest_init_lst { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

macro_rules! trace_ingest_minmax { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

macro_rules! trace_ingest_event { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

macro_rules! trace_ingest_container { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

macro_rules! trace_ingest_container_2 { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

macro_rules! trace_fill_until { ($($arg:tt)*) => ( if true { trace_!($($arg)*); }) }

const COL1: &'static str = "\x1b[1m";
const RST: &'static str = "\x1b[0m";

const VERIFY_INPUT_EVENTS: bool = false;
const OUT_LEN_MAX: usize = 20000;

#[cold]
#[inline]
#[allow(unused)]
fn cold() {}

const DEBUG_CHECKS: bool = true;

autoerr::create_error_v1!(
    name(Error, "BinnedEventsTimeweight"),
    enum variants {
        BadContainer(#[from] super::super::container_events::EventsContainerError),
        Unordered,
        EventAfterRange,
        NoLstAfterFirst,
        EmptyContainerInnerHandler,
        NoLstButMinMax,
        WithLstButEventBeforeRange,
        WithMinMaxButEventBeforeRange,
        NoMinMaxAfterInit,
        ExpectEventWithinRange,
        IngestNoProgress(usize, usize),
        EventActiveRangeBefore(String),
        EventActiveRangeAfter(String),
        EventActiveRangeLE,
    },
);

type MinMax<EVT> = (EventSingle<EVT>, EventSingle<EVT>);

#[derive(Clone)]
struct LstRef<'a, EVT>(&'a EventSingle<EVT>);

struct LstMut<'a, EVT>(&'a mut EventSingle<EVT>);

#[derive(Debug)]
struct InnerB<EVT>
where
    EVT: EventValueType,
{
    cnt: u64,
    active_beg: TsNano,
    active_end: TsNano,
    active_len: DtNano,
    filled_until: TsNano,
    filled_width: DtNano,
    agg: <EVT as EventValueType>::AggregatorTimeWeight,
}

impl<EVT> InnerB<EVT>
where
    EVT: EventValueType,
{
    // NOTE that this is also used during bin-cycle.
    fn ingest_event_with_lst_gt_range_beg_agg(
        &mut self,
        ev: EventSingleRef<EVT>,
        lst: LstRef<EVT>,
    ) {
        let selfname = "ingest_event_with_lst_gt_range_beg_agg";
        trace_ingest_event!("{}  {:?}", selfname, ev);
        if DEBUG_CHECKS {
            if ev.ts <= self.active_beg {
                panic!("logic error");
            }
            if ev.ts >= self.active_end {
                panic!("logic error");
            }
        }
        let dt = ev.ts.delta(self.filled_until);
        trace_ingest_event!("{}  dt {:?}  ev {:?}", selfname, dt, ev);
        // TODO can the caller already take the value and replace it afterwards with the current value?
        // This fn could swap the value in lst and directly use it.
        // This would require that any call path does not mess with lst.
        // NOTE that this fn is also used during bin-cycle.
        self.agg.ingest(dt, self.active_len, lst.0.val.clone());
        self.filled_width = self.filled_width.add(dt);
        self.filled_until = ev.ts;
    }

    fn ingest_event_with_lst_gt_range_beg_2(
        &mut self,
        ev: EventSingleRef<EVT>,
        lst: LstMut<EVT>,
    ) -> Result<(), Error> {
        let selfname = "ingest_event_with_lst_gt_range_beg_2";
        trace_ingest_event!("{}", selfname);
        self.ingest_event_with_lst_gt_range_beg_agg(ev.clone(), LstRef(lst.0));
        InnerA::apply_lst_after_event_handled(ev, lst);
        // self.cnt += 1;
        Ok(())
    }

    fn ingest_event_with_lst_gt_range_beg(
        &mut self,
        ev: EventSingleRef<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        let selfname = "ingest_event_with_lst_gt_range_beg";
        trace_ingest_event!("{}", selfname);
        // TODO if the event is exactly on the current bin first edge, then there is no contribution to the avg yet
        // and I must initialize the min/max with the current event.
        InnerA::apply_min_max(&ev, minmax);
        self.ingest_event_with_lst_gt_range_beg_2(ev.clone(), lst)?;
        Ok(())
    }

    fn ingest_event_with_lst_eq_range_beg(
        &mut self,
        ev: EventSingleRef<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        let selfname = "ingest_event_with_lst_eq_range_beg";
        trace_ingest_event!("{}", selfname);
        // TODO if the event is exactly on the current bin first edge, then there is no contribution to the avg yet
        // and I must initialize the min/max with the current event.
        InnerA::apply_min_max_range_beg(&ev, minmax);
        InnerA::apply_lst_after_event_handled(ev, lst);
        Ok(())
    }

    fn ingest_with_lst_gt_range_beg(
        &mut self,
        evs: &mut ContainerEventsTakeUpTo<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        let selfname = "ingest_with_lst_gt_range_beg";
        trace_ingest_event!("{}  len {}", selfname, evs.len());
        while let Some(ev) = evs.next() {
            trace_event_next!("{:?}  {:30}", ev, selfname);
            if DEBUG_CHECKS {
                if ev.ts <= self.active_beg {
                    return Err(Error::EventActiveRangeLE);
                }
                if ev.ts >= self.active_end {
                    return Err(Error::EventActiveRangeAfter(selfname.into()));
                }
            }
            self.ingest_event_with_lst_gt_range_beg(ev.clone(), LstMut(lst.0), minmax)?;
            self.cnt += 1;
        }
        Ok(())
    }

    fn ingest_with_lst_ge_range_beg(
        &mut self,
        evs: &mut ContainerEventsTakeUpTo<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        let selfname = "ingest_with_lst_ge_range_beg";
        trace_ingest_event!("{}  len {}", selfname, evs.len());
        while let Some(ev) = evs.next() {
            trace_event_next!("{:?}  {:30}", ev, selfname);
            assert!(ev.ts >= self.active_beg);
            assert!(ev.ts < self.active_end);
            if ev.ts == self.active_beg {
                trace_ingest_event!("{}  ts == active_beg", selfname);
                self.ingest_event_with_lst_eq_range_beg(ev, LstMut(lst.0), minmax)?;
                self.cnt += 1;
            } else {
                trace_ingest_event!("{}  ts != active_beg", selfname);
                self.ingest_event_with_lst_gt_range_beg(ev.clone(), LstMut(lst.0), minmax)?;
                self.cnt += 1;
                break;
            }
        }
        trace_ingest_event!(
            "{}  defer remainder to  ingest_with_lst_gt_range_beg  len {}",
            selfname,
            evs.len()
        );
        self.ingest_with_lst_gt_range_beg(evs, LstMut(lst.0), minmax)
    }

    fn ingest_with_lst_minmax(
        &mut self,
        evs: &mut ContainerEventsTakeUpTo<EVT>,
        lst: LstMut<EVT>,
        minmax: &mut MinMax<EVT>,
    ) -> Result<(), Error> {
        let selfname = "ingest_with_lst_minmax";
        trace_ingest_event!("{}  len {}", selfname, evs.len());
        // TODO how to handle the min max? I don't take event data yet out of the container.
        if let Some(ts0) = evs.ts_first() {
            trace_ingest_event!("{selfname}  EVENT TIMESTAMP FRONT  {:?}", ts0);
            if ts0 < self.active_beg {
                Err(Error::EventActiveRangeBefore(selfname.into()))
            } else if ts0 >= self.active_end {
                info!("ts0 >= self.active_end  {}  {}", ts0, self.active_end);
                Err(Error::EventActiveRangeAfter(selfname.into()))
            } else {
                self.ingest_with_lst_ge_range_beg(evs, lst, minmax)
            }
        } else {
            Ok(())
        }
    }

    // PRECONDITION: filled_until < ts <= active_end
    fn fill_until(&mut self, ts: TsNano, lst: LstRef<EVT>) {
        let b = self;
        assert!(b.filled_until < ts);
        assert!(ts <= b.active_end);
        let dt = ts.delta(b.filled_until);
        trace_fill_until!("fill_until  ts {:?}  dt {:?}  lst {:?}", ts, dt, lst.0);
        assert!(b.filled_until < ts);
        assert!(ts <= b.active_end);
        b.agg.ingest(dt, b.active_len, lst.0.val.clone());
        b.filled_width = b.filled_width.add(dt);
        b.filled_until = ts;
    }
}

#[derive(Debug)]
struct InnerA<EVT>
where
    EVT: EventValueType,
{
    inner_b: InnerB<EVT>,
    minmax: Option<(EventSingle<EVT>, EventSingle<EVT>)>,
}

impl<EVT> InnerA<EVT>
where
    EVT: EventValueType,
{
    fn apply_min_max(ev: &EventSingleRef<EVT>, minmax: &mut MinMax<EVT>) {
        if let Some(std::cmp::Ordering::Less) = ev.val.cmp_a(&minmax.0.val) {
            trace_ingest_minmax!("apply_min_max  update min  {ev:?}");
            minmax.0 = ev.into();
        }
        if let Some(std::cmp::Ordering::Greater) = ev.val.cmp_a(&minmax.1.val) {
            trace_ingest_minmax!("apply_min_max  update max  {ev:?}");
            minmax.1 = ev.into();
        }
    }

    fn apply_min_max_range_beg(ev: &EventSingleRef<EVT>, minmax: &mut MinMax<EVT>) {
        let selfname = "apply_min_max_range_beg";
        trace_ingest_minmax!("{selfname}  update min max  {ev:?}");
        minmax.0 = ev.into();
        minmax.1 = ev.into();
    }

    fn apply_lst_after_event_handled(ev: EventSingleRef<EVT>, lst: LstMut<EVT>) {
        *lst.0 = ev.into();
    }

    fn init_minmax(&mut self, ev: &EventSingleRef<EVT>) {
        trace_ingest_minmax!("init_minmax  {:?}", ev);
        self.minmax = Some((ev.into(), ev.into()));
    }

    fn init_minmax_with_lst(&mut self, ev: &EventSingleRef<EVT>, lst: LstRef<EVT>) {
        trace_ingest_minmax!("init_minmax_with_lst  {:?}  {:?}", ev, lst.0);
        let minmax = self.minmax.insert((lst.0.clone(), lst.0.clone()));
        Self::apply_min_max(ev, minmax);
    }

    fn ingest_with_lst(
        &mut self,
        evs: &mut ContainerEventsTakeUpTo<EVT>,
        lst: LstMut<EVT>,
    ) -> Result<(), Error> {
        let selfname = "ingest_with_lst";
        trace_ingest_container!("{}  len {}", selfname, evs.len());
        let b = &mut self.inner_b;
        if let Some(minmax) = self.minmax.as_mut() {
            b.ingest_with_lst_minmax(evs, lst, minmax)
        } else {
            let mut run_ingest_with_lst_minmax = false;
            let _ = run_ingest_with_lst_minmax;
            if let Some(ev) = evs.next() {
                trace_event_next!("{:?}  {:30}", ev, selfname);
                let beg = b.active_beg;
                let end = b.active_end;
                if ev.ts < beg {
                    return Err(Error::EventActiveRangeBefore(selfname.into()));
                } else if ev.ts >= end {
                    return Err(Error::EventActiveRangeAfter(selfname.into()));
                } else {
                    if ev.ts == beg {
                        self.init_minmax(&ev);
                        InnerA::apply_lst_after_event_handled(ev, lst);
                        let b = &mut self.inner_b;
                        b.cnt += 1;
                        return Ok(());
                    } else {
                        self.init_minmax_with_lst(&ev, LstRef(lst.0));
                        let b = &mut self.inner_b;
                        {
                            b.ingest_event_with_lst_gt_range_beg_2(ev, LstMut(lst.0))?;
                            b.cnt += 1;
                            run_ingest_with_lst_minmax = true;
                        }
                    }
                }
            } else {
                return Ok(());
            }
            if run_ingest_with_lst_minmax {
                if let Some(minmax) = self.minmax.as_mut() {
                    let b = &mut self.inner_b;
                    b.ingest_with_lst_minmax(evs, lst, minmax)
                } else {
                    return Err(Error::NoMinMaxAfterInit);
                }
            } else {
                Ok(())
            }
        }
    }

    fn reset_01(&mut self, lst: LstRef<EVT>) {
        let selfname = "reset_01";
        let b = &mut self.inner_b;
        trace_cycle!(
            "{}  active_end {:?}  filled_until {:?}",
            selfname,
            b.active_end,
            b.filled_until
        );
        let div = b.active_len.ns();
        let old_end = b.active_end;
        let ts1 = TsNano::from_ns(b.active_end.ns() / div * div);
        assert!(ts1 == old_end);
        b.active_beg = ts1;
        b.active_end = ts1.add_dt_nano(b.active_len);
        b.filled_until = ts1;
        b.filled_width = DtNano::from_ns(0);
        b.cnt = 0;
        trace_ingest_minmax!("reset_01  update min/max to lst  {:?}", lst.0);
        self.minmax = Some((lst.0.clone(), lst.0.clone()));
    }

    fn push_out_and_reset(
        &mut self,
        lst: LstRef<EVT>,
        range_final: bool,
        out: &mut ContainerBins<EVT, EVT::AggTimeWeightOutputAvg>,
    ) {
        let selfname = "push_out_and_reset";
        trace_output!("{}  range_final {}", selfname, range_final);
        // TODO there is not always good enough input to produce a meaningful bin.
        // TODO can we always reset, and what exactly does reset mean here?
        // TODO what logic can I save here? To output a bin I need to have min, max, lst.
        let b = &mut self.inner_b;
        let minmax = self.minmax.get_or_insert_with(|| {
            trace_cycle!("{}  minmax not yet set", selfname);
            trace_ingest_minmax!("{}  setting min/max to lst {:?}", selfname, lst.0);
            (lst.0.clone(), lst.0.clone())
        });
        {
            let filled_width_fraction = b.filled_width.fraction_f32_of(b.active_len);
            let res = b.agg.result_and_reset_for_new_bin(filled_width_fraction);
            trace_ingest_minmax!(
                "{}  push out  min {:?}  max {:?}",
                selfname,
                minmax.0,
                minmax.1
            );
            out.push_back(
                b.active_beg,
                b.active_end,
                b.cnt,
                minmax.0.val.clone(),
                minmax.1.val.clone(),
                res,
                lst.0.val.clone(),
                range_final,
            );
        }
        self.reset_01(lst);
    }
}

#[derive(Serialize)]
pub struct BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    range: BinnedRange<TsNano>,
    produce_cnt_zero: bool,
    #[serde(skip)]
    lst: Option<EventSingle<EVT>>,
    #[serde(skip)]
    inner_a: InnerA<EVT>,
    #[serde(skip)]
    out: ContainerBins<EVT, EVT::AggTimeWeightOutputAvg>,
}

impl<EVT> fmt::Debug for BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BinnedEventsTimeweight")
            .field("range", &self.range)
            .field("produce_cnt_zero", &self.produce_cnt_zero)
            .field("lst", &self.lst)
            .field("inner_a", &self.inner_a)
            .field("out", &self.out)
            .finish()
    }
}

impl<EVT> BinnedEventsTimeweight<EVT>
where
    EVT: EventValueType,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(range: BinnedRange<TsNano>) -> Self {
        trace_init!("{}::new  {}", Self::type_name(), range);
        let active_beg = range.nano_beg();
        let active_end = active_beg.add_dt_nano(range.bin_len.to_dt_nano());
        let active_len = active_end.delta(active_beg);
        Self {
            range,
            produce_cnt_zero: false,
            lst: None,
            inner_a: InnerA::<EVT> {
                inner_b: InnerB {
                    cnt: 0,
                    active_beg,
                    active_end,
                    active_len,
                    filled_until: active_beg,
                    filled_width: DtNano::from_ns(0),
                    agg: <<EVT as EventValueType>::AggregatorTimeWeight as AggregatorTimeWeight<
                        EVT,
                    >>::new(),
                },
                minmax: None,
            },
            out: ContainerBins::new(),
        }
    }

    pub fn cnt_zero_enable(&mut self) {
        self.produce_cnt_zero = true;
    }

    fn ingest_event_without_lst(&mut self, ev: EventSingleRef<EVT>) -> Result<(), Error> {
        let selfname = "ingest_event_without_lst";
        let b = &self.inner_a.inner_b;
        if ev.ts < b.active_beg {
            if false {
                return Err(Error::EventActiveRangeBefore(selfname.into()));
            }
            trace_ingest_init_lst!("{selfname}  set lst  {:?}", ev);
            self.lst = Some((&ev).into());
            trace_ingest_minmax!("{selfname}  call init_minmax");
            self.inner_a.init_minmax(&ev);
            Ok(())
        } else if ev.ts >= b.active_end {
            Err(Error::EventActiveRangeAfter(selfname.into()))
        } else {
            trace_ingest_init_lst!("{selfname}  set lst  {:?}", ev);
            self.lst = Some((&ev).into());
            trace_ingest_minmax!("{selfname}  call init_minmax");
            self.inner_a.init_minmax(&ev);
            let b = &mut self.inner_a.inner_b;
            b.cnt += 1;
            b.filled_until = ev.ts;
            Ok(())
        }
    }

    fn ingest_without_lst(&mut self, evs: &mut ContainerEventsTakeUpTo<EVT>) -> Result<(), Error> {
        let selfname = "ingest_without_lst";
        trace_ingest_container!("{}  len {}", selfname, evs.len());
        let mut run_ingest_with_lst = false;
        let _ = run_ingest_with_lst;
        if let Some(ev) = evs.next() {
            trace_event_next!("{selfname}  {:?}", ev);
            assert!(ev.ts < self.inner_a.inner_b.active_end);
            self.ingest_event_without_lst(ev)?;
            run_ingest_with_lst = true;
        } else {
        }
        if run_ingest_with_lst {
            if let Some(lst) = self.lst.as_mut() {
                self.inner_a.ingest_with_lst(evs, LstMut(lst))
            } else {
                Err(Error::NoLstAfterFirst)
            }
        } else {
            Ok(())
        }
    }

    // Caller asserts that evs is ordered within the current container
    // and with respect to the last container, if any.
    fn ingest_ordered(&mut self, evs: &mut ContainerEventsTakeUpTo<EVT>) -> Result<(), Error> {
        let selfname = "ingest_ordered";
        trace_ingest_container!("--------------------------------------------------");
        trace_ingest_container!("{}  len {}", selfname, evs.len());
        if let Some(lst) = self.lst.as_mut() {
            self.inner_a.ingest_with_lst(evs, LstMut(lst))
        } else {
            if self.inner_a.minmax.is_some() {
                Err(Error::NoLstButMinMax)
            } else {
                self.ingest_without_lst(evs)
            }
        }
    }

    fn cycle_01(&mut self, ts: TsNano) {
        let b = &self.inner_a.inner_b;
        trace_cycle!("cycle_01  {:?}  {:?}", ts, b.active_end);
        assert!(b.active_beg < ts);
        assert!(b.active_beg <= b.filled_until);
        assert!(b.filled_until < ts);
        assert!(b.filled_until <= b.active_end);
        let div = b.active_len.ns();
        if let Some(lst) = self.lst.as_ref() {
            let lst = LstRef(lst);
            if self.produce_cnt_zero {
                let mut i = 0;
                loop {
                    i += 1;
                    assert!(i < 100000, "too many iterations");
                    let b = &self.inner_a.inner_b;
                    if self.out.len() > OUT_LEN_MAX {
                        // TODO change api such that we can produce arbitrary bins as stream.
                        info!("produced too many bins  out len {}", self.out.len());
                        break;
                    } else if ts > b.filled_until {
                        if ts >= b.active_end {
                            if b.filled_until < b.active_end {
                                self.inner_a.inner_b.fill_until(b.active_end, lst.clone());
                            }
                            self.inner_a
                                .push_out_and_reset(lst.clone(), true, &mut self.out);
                        } else {
                            self.inner_a.inner_b.fill_until(ts, lst.clone());
                        }
                    } else {
                        break;
                    }
                }
            } else {
                let b = &self.inner_a.inner_b;
                if ts > b.filled_until {
                    if ts >= b.active_end {
                        if b.filled_until < b.active_end {
                            self.inner_a.inner_b.fill_until(b.active_end, lst.clone());
                        }
                        self.inner_a
                            .push_out_and_reset(lst.clone(), true, &mut self.out);
                    } else {
                        // TODO should not hit this case. Prove it, assert it.
                        self.inner_a.inner_b.fill_until(ts, lst.clone());
                    }
                } else {
                    // TODO should never hit this case. Count.
                }
                // TODO jump to next bin
                // TODO merge with the other reset
                // Below uses the same code
                let ts1 = TsNano::from_ns(ts.ns() / div * div);
                let b = &mut self.inner_a.inner_b;
                b.active_beg = ts1;
                b.active_end = ts1.add_dt_nano(b.active_len);
                b.filled_until = ts1;
                b.filled_width = DtNano::from_ns(0);
                b.cnt = 0;
                b.agg.reset_for_new_bin();
                // assert!(self.inner_a.minmax.is_none());
                trace_cycle!("cycled direct to  {:?}  {:?}", b.active_beg, b.active_end);
            }
        } else {
            assert!(self.inner_a.minmax.is_none());
            // TODO merge with the other reset
            let ts1 = TsNano::from_ns(ts.ns() / div * div);
            let b = &mut self.inner_a.inner_b;
            b.active_beg = ts1;
            b.active_end = ts1.add_dt_nano(b.active_len);
            b.filled_until = ts1;
            b.filled_width = DtNano::from_ns(0);
            b.cnt = 0;
            b.agg.reset_for_new_bin();
            trace_cycle!("cycled direct to  {:?}  {:?}", b.active_beg, b.active_end);
        }
    }

    fn cycle_02(&mut self) {
        let b = &self.inner_a.inner_b;
        trace_cycle!("cycle_02  {:?}", b.active_end);
        if let Some(lst) = self.lst.as_ref() {
            let lst = LstRef(lst);
            self.inner_a.push_out_and_reset(lst, false, &mut self.out);
        } else {
            // there is nothing we can produce
            // TODO count for stats
        }
    }

    pub fn ingest(&mut self, evs: &ContainerEvents<EVT>) -> Result<IngestReport, Error> {
        // It is this type's task to find and store the one-before event.
        // We then pass it to the aggregation.
        // AggregatorTimeWeight needs a function for that.
        // What about counting the events that actually fall into the range?
        // Maybe that should be done in this type.
        // That way we can pass the values and weights to the aggregation, and count the in-range here.
        // This type must also "close" the current aggregation by passing the "last" and init the next.
        // ALSO: need to keep track of the "lst". Probably best done in this type as well?

        // TODO should rely on external stream adapter for verification to not duplicate things.
        if VERIFY_INPUT_EVENTS {
            evs.verify()?;
        }
        let mut evs = ContainerEventsTakeUpTo::new(evs);

        loop {
            trace_ingest_container!("+++++++++++++++++++++++++++++++++++++++++++++++++++");
            trace_ingest_container!(
                "main-ingest-loop  UNCONSTRAINED  len {}  pos {}",
                evs.len(),
                evs.pos()
            );
            break if let Some(ts) = evs.ts_first() {
                trace_ingest_event!("ingest  EVENT TIMESTAMP FRONT  {:?}", ts);
                let b = &mut self.inner_a.inner_b;
                if ts >= self.range.nano_end() {
                    return Err(Error::EventAfterRange);
                }
                if ts >= b.active_end {
                    assert!(
                        b.filled_until < b.active_end,
                        "{} < {}",
                        b.filled_until,
                        b.active_end
                    );
                    self.cycle_01(ts);
                }
                let n1 = evs.len();
                // TODO instead of mutable constrain/expand, use cheap derived subslices.
                // But inner must still communicate back how much was consumed.
                evs.constrain_up_to_ts(self.inner_a.inner_b.active_end);
                {
                    trace_ingest_container!(
                        "main-ingest-loop    CONSTRAINED  len {}  pos {}",
                        evs.len(),
                        evs.pos()
                    );
                    if let Some(lst) = self.lst.as_ref() {
                        if ts < lst.ts {
                            return Err(Error::Unordered);
                        } else {
                            self.ingest_ordered(&mut evs)?
                        }
                    } else {
                        self.ingest_ordered(&mut evs)?
                    };
                    trace_ingest_container_2!("ingest  after still left  evs len {}", evs.len());
                }
                evs.extend_to_all();
                let n2 = evs.len();
                trace_ingest_container_2!("ingest  extended again to all  evs len {}", evs.len());
                if n2 == 0 {
                    // done
                } else if n2 >= n1 {
                    let e = Error::IngestNoProgress(n1, n2);
                    debug!("{}", e);
                    return Err(e);
                } else {
                    continue;
                }
            } else {
                // done
            };
        }
        Ok(IngestReport::ConsumedAll)
    }

    pub fn input_done_range_final(&mut self) -> Result<(), Error> {
        trace_cycle!("{}input_done_range_final{}", COL1, RST);
        self.cycle_01(self.range.nano_end());
        Ok(())
    }

    pub fn input_done_range_open(&mut self) -> Result<(), Error> {
        trace_cycle!("{}input_done_range_open{}", COL1, RST);
        self.cycle_02();
        Ok(())
    }

    pub fn output_len(&self) -> usize {
        self.out.len()
    }

    pub fn output(&mut self) -> ContainerBins<EVT, EVT::AggTimeWeightOutputAvg> {
        mem::replace(&mut self.out, ContainerBins::new())
    }
}
