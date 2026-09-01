use super::cached::reader::CacheReadProvider;
use super::cached::reader::EventsReadProvider;
use super::opts::BinningOptions;
use crate::log;
use crate::timebin::fromevents::BinnedFromEvents;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::sitem_err_from_string;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinsBoxed;
use items_2::binning::timeweight::timeweight_bins_stream::BinnedBinsTimeweightStream;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRange;
use netpod::ChannelTypeConfigGen;
use netpod::DtMs;
use netpod::ReqCtx;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

macro_rules! debug_init { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! debug_setup { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! trace_cache { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace_handle { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "BinCachedGapFill"),
    enum variants {
        CacheReader(#[from] super::cached::reader::Error),
        GapFromFiner(TsNano, TsNano, DtMs),
        MissingBegFromFiner(TsNano, TsNano, DtMs),
        InputBeforeRange(NanoRange, BinnedRange<TsNano>),
        EventsReader(#[from] super::fromevents::Error),
    },
);

type Input = Pin<Box<dyn Stream<Item = Sitemty<BinsBoxed>> + Send>>;

// Try to read from cache for the given bin len.
// For gaps in the stream, construct an alternative input from finer bin len with a binner.
pub struct GapFill {
    dbgname: String,
    ch_conf: ChannelTypeConfigGen,
    binning_opts: BinningOptions,
    transform_query: TransformQuery,
    sub: EventsSubQuerySettings,
    log_level: String,
    ctx: Arc<ReqCtx>,
    range: BinnedRange<TsNano>,
    do_time_weight: bool,
    bin_len_layers: Vec<DtMs>,
    inp: Option<Input>,
    inp_range_final: bool,
    inp_buf: Option<BinsBoxed>,
    inp_finer: Option<Input>,
    inp_finer_range_final: bool,
    inp_finer_range_final_cnt: u32,
    inp_finer_range_final_max: u32,
    inp_finer_fills_gap: bool,
    last_bin_ts2: Option<TsNano>,
    exp_finer_range: NanoRange,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    events_read_provider: Arc<dyn EventsReadProvider>,
    bins_for_cache_write: Option<BinsBoxed>,
    done: bool,
    cache_writing: Option<super::cached::reader::CacheWriting>,
}

impl GapFill {
    // bin_len of the given range must be a cacheable bin_len.
    pub fn new(
        dbgname_parent: String,
        ch_conf: ChannelTypeConfigGen,
        binning_opts: BinningOptions,
        transform_query: TransformQuery,
        sub: EventsSubQuerySettings,
        log_level: String,
        ctx: Arc<ReqCtx>,
        range: BinnedRange<TsNano>,
        do_time_weight: bool,
        bin_len_layers: Vec<DtMs>,
        cache_read_provider: Arc<dyn CacheReadProvider>,
        events_read_provider: Arc<dyn EventsReadProvider>,
    ) -> Result<Self, Error> {
        let dbgname = format!("{}--[{}]", dbgname_parent, range);
        debug_init!("new  dbgname {}", dbgname);
        let inp = if binning_opts.cache_usage().is_cache_read() {
            let series = ch_conf.series().expect("series id for cache read");
            let stream = super::cached::reader::CachedReader::new(series, range.clone(), cache_read_provider.clone())?
                .map(|x| match x {
                    Ok(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))),
                    Err(e) => sitem_err_from_string(e),
                });
            Box::pin(stream) as Pin<Box<dyn Stream<Item = Sitemty<BinsBoxed>> + Send>>
        } else {
            let stream = futures_util::stream::empty();
            Box::pin(stream)
        };
        let ret = Self {
            dbgname,
            ch_conf,
            binning_opts,
            transform_query,
            sub,
            log_level,
            ctx,
            range,
            do_time_weight,
            bin_len_layers,
            inp: Some(inp),
            inp_range_final: false,
            inp_buf: None,
            inp_finer: None,
            inp_finer_range_final: false,
            inp_finer_range_final_cnt: 0,
            inp_finer_range_final_max: 0,
            inp_finer_fills_gap: false,
            last_bin_ts2: None,
            // TODO just dummy:
            exp_finer_range: NanoRange { beg: 0, end: 0 },
            cache_read_provider,
            events_read_provider,
            bins_for_cache_write: None,
            done: false,
            cache_writing: None,
        };
        Ok(ret)
    }

    fn handle_bins_finer(mut self: Pin<&mut Self>, bins: BinsBoxed) -> Result<BinsBoxed, Error> {
        trace_handle!("{}  handle_bins_finer  {}", self.dbgname, bins);
        for (&ts1, &ts2) in bins.edges_iter() {
            if let Some(last) = self.last_bin_ts2 {
                if ts1 != last {
                    return Err(Error::GapFromFiner(ts1, last, self.range.bin_len_dt_ms()));
                }
            } else if ts1 != self.range.nano_beg() {
                return Err(Error::MissingBegFromFiner(
                    ts1,
                    self.range.nano_beg(),
                    self.range.bin_len_dt_ms(),
                ));
            }
            self.last_bin_ts2 = Some(ts2);
        }
        if bins.len() != 0 {
            let mut bins2 = bins.clone();
            let dst = self.bins_for_cache_write.get_or_insert_with(|| bins.empty());
            bins2.drain_into(dst.as_mut(), 0..bins2.len());
        }
        if self.binning_opts.cache_usage().is_cache_write() {
            self.cache_write_intermediate()?;
        }
        // TODO make sure that input does not send "made-up" empty future bins.
        // On the other hand, if the request is over past range, but the channel was silent ever since?
        // Then we should in principle know that from is-alive status checking.
        // So, until then, allow made-up bins?
        // Maybe, for now, only write those bins before some last non-zero-count bin. The only safe way.
        Ok(bins)
    }

    fn setup_sub(self: Pin<&mut Self>, range: NanoRange) -> Result<(), Error> {
        trace_handle!("{}  SETUP SUB STREAM  {}", self.dbgname, range);
        self.setup_inp_finer(range, true)?;
        Ok(())
    }

    fn handle_bins(mut self: Pin<&mut Self>, bins: BinsBoxed) -> Result<(BinsBoxed, BinsBoxed), Error> {
        trace_handle!("{}  handle_bins  {}", self.dbgname, bins);
        // TODO could use an interface to iterate over opaque bin items that only expose
        // edge and count information with all remaining values opaque.
        for (i, (&ts1, &ts2)) in bins.edges_iter().enumerate() {
            if ts1 < self.range.nano_beg() {
                return Err(Error::InputBeforeRange(
                    NanoRange::from_ns_u64(ts1.ns(), ts2.ns()),
                    self.range.clone(),
                ));
            }
            if let Some(last) = self.last_bin_ts2 {
                if ts1 != last {
                    trace_handle!("{}  detect a gap  BETWEEN  last {}  ts1 {}", self.dbgname, last, ts1);
                    let mut ret = bins.empty();
                    let mut bins = bins;
                    bins.drain_into(ret.as_mut(), 0..i);
                    let range = NanoRange {
                        beg: last.ns(),
                        end: ts1.ns(),
                    };
                    self.setup_sub(range)?;
                    return Ok((bins, ret));
                } else {
                    // nothing to do
                }
            } else if ts1 != self.range.nano_beg() {
                trace_handle!(
                    "{}  detect a gap  BEGIN  beg {}  ts1 {}",
                    self.dbgname,
                    self.range.nano_beg(),
                    ts1
                );
                let range = NanoRange {
                    beg: self.range.nano_beg().ns(),
                    end: ts1.ns(),
                };
                self.setup_sub(range)?;
                let empty = bins.empty();
                return Ok((bins, empty));
            }
            self.last_bin_ts2 = Some(ts2);
        }
        let empty = bins.empty();
        Ok((empty, bins))
    }

    fn setup_inp_finer(mut self: Pin<&mut Self>, range: NanoRange, inp_finer_fills_gap: bool) -> Result<(), Error> {
        self.inp_finer_range_final = false;
        self.inp_finer_range_final_max += 1;
        self.inp_finer_fills_gap = inp_finer_fills_gap;
        self.exp_finer_range = range.clone();
        if let Some(bin_len_finer) =
            super::grid::find_next_finer_bin_len(self.range.bin_len.to_dt_ms(), &self.bin_len_layers)
        {
            debug_setup!(
                "{}  setup_inp_finer  next finer from bins  {}  {}  from {}",
                self.dbgname,
                range,
                bin_len_finer,
                self.range.bin_len.to_dt_ms()
            );
            let range_finer = BinnedRange::from_nano_range(range, bin_len_finer);
            let range_finer_one_before_bin = range_finer.one_before_bin();
            let inp_finer = GapFill::new(
                self.dbgname.clone(),
                self.ch_conf.clone(),
                self.binning_opts.clone(),
                self.transform_query.clone(),
                self.sub.clone(),
                self.log_level.clone(),
                self.ctx.clone(),
                range_finer_one_before_bin,
                self.do_time_weight,
                self.bin_len_layers.clone(),
                self.cache_read_provider.clone(),
                self.events_read_provider.clone(),
            )?;
            let stream = Box::pin(inp_finer);
            let range = BinnedRange::from_nano_range(range_finer.full_range(), self.range.bin_len.to_dt_ms());
            let stream = if self.do_time_weight {
                BinnedBinsTimeweightStream::new(range, stream)
            } else {
                panic!("TODO unweighted")
            };
            self.inp_finer = Some(Box::pin(stream));
        } else if self.binning_opts.allow_from_events() {
            debug_setup!("{}  setup_inp_finer  next finer from events  {}", self.dbgname, range);
            let series_range = SeriesRange::TimeRange(range.clone());
            let one_before_range = true;
            let select = EventsSubQuerySelect::new(
                self.ch_conf.clone(),
                series_range,
                one_before_range,
                self.transform_query.clone(),
            );
            let evq = EventsSubQuery::from_parts(
                select,
                self.sub.clone(),
                self.ctx.reqid().into(),
                self.log_level.clone(),
            );
            let range = BinnedRange::from_nano_range(range.clone(), self.range.bin_len.to_dt_ms());
            let inp = BinnedFromEvents::new(range, evq, self.do_time_weight, self.events_read_provider.clone())?;
            self.inp_finer = Some(Box::pin(inp));
        } else {
            let stream = futures_util::stream::iter([]);
            self.inp_finer = Some(Box::pin(stream));
        }
        Ok(())
    }

    fn cache_write_on_end(mut self: Pin<&mut Self>) -> Result<(), Error> {
        if self.inp_finer_fills_gap {
            // TODO can consider all incoming bins as final by assumption.
        }
        if let Some(bins) = &self.bins_for_cache_write {
            if bins.len() >= 2 {
                // TODO guard behind flag.
                // TODO emit to a async user-given channel, if given.
                // Therefore, move to poll loop.
                // Should only write to cache with non-zero count, therefore, not even emit others?
                // TODO afterwards set to None.
                self.bins_for_cache_write = None;
            }
        }
        Ok(())
    }

    fn cache_write_intermediate(self: Pin<&mut Self>) -> Result<(), Error> {
        // TODO See cache_write_on_end
        trace_cache!("maybe write to cache");
        Ok(())
    }
}

impl Stream for GapFill {
    type Item = Sitemty<BinsBoxed>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.done {
                Ready(None)
            } else if let Some(fut) = self.cache_writing.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(Ok(())) => {
                        self.cache_writing = None;
                        continue;
                    }
                    Ready(Err(e)) => {
                        self.cache_writing = None;
                        Ready(Some(sitem_err_from_string(e)))
                    }
                    Pending => Pending,
                }
            } else if let Some(inp_finer) = self.inp_finer.as_mut() {
                match inp_finer.poll_next_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        StreamItem::DataItem(RangeCompletableItem::Data(x)) => {
                            match self.as_mut().handle_bins_finer(x) {
                                Ok(x) => Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))))),
                                Err(e) => Ready(Some(sitem_err_from_string(e))),
                            }
                        }
                        StreamItem::DataItem(RangeCompletableItem::RangeComplete) => {
                            trace_handle!("{}  RECV RANGE FINAL", self.dbgname);
                            self.inp_finer_range_final = true;
                            self.inp_finer_range_final_cnt += 1;
                            if self.binning_opts.cache_usage().is_cache_write() {
                                match self.as_mut().cache_write_on_end() {
                                    Ok(()) => continue,
                                    Err(e) => Ready(Some(sitem_err_from_string(e))),
                                }
                            } else {
                                continue;
                            }
                        }
                        StreamItem::Log(x) => Ready(Some(Ok(StreamItem::Log(x)))),
                        StreamItem::Stats(x) => Ready(Some(Ok(StreamItem::Stats(x)))),
                    },
                    Ready(Some(Err(e))) => Ready(Some(sitem_err_from_string(e))),
                    Ready(None) => {
                        trace_handle!(
                            "{}  inp_finer Ready(None)  last_bin_ts2 {:?}",
                            self.dbgname,
                            self.last_bin_ts2
                        );
                        let exp_finer_range =
                            ::core::mem::replace(&mut self.exp_finer_range, NanoRange { beg: 0, end: 0 });
                        self.inp_finer = None;
                        if let Some(j) = self.last_bin_ts2 {
                            if j.ns() < exp_finer_range.end() {
                                trace_handle!(
                                    "{}  inp_finer Ready(None)  last_bin_ts2 {:?}  exp_finer_range {:?}",
                                    self.dbgname,
                                    self.last_bin_ts2,
                                    exp_finer_range
                                );
                                if self.inp_finer_fills_gap {
                                    Ready(Some(sitem_err_from_string("finer input didn't deliver to the end")))
                                } else {
                                    log::warn!(
                                        "{}  inp_finer  Ready(None)  last_bin_ts2 {:?}  not delivered to the end, but maybe in the future",
                                        self.dbgname, self.last_bin_ts2
                                    );
                                    continue;
                                }
                            } else if j.ns() > exp_finer_range.end() {
                                continue;
                            } else {
                                continue;
                            }
                        } else {
                            log::warn!("-----------------------------------------------------------------");
                            log::warn!(
                                "{}  inp_finer  Ready(None)  last_bin_ts2 {:?}",
                                self.dbgname,
                                self.last_bin_ts2
                            );
                            if self.inp_finer_fills_gap {
                                log::error!(
                                    "{}  inp_finer  Ready(None)  last_bin_ts2 {:?}  inp_finer_fills_gap {}",
                                    self.dbgname,
                                    self.last_bin_ts2,
                                    self.inp_finer_fills_gap
                                );
                                Ready(Some(sitem_err_from_string(
                                    "finer input delivered nothing, received nothing at all so far",
                                )))
                            } else {
                                log::warn!(
                                    "{}  inp_finer  Ready(None)  last_bin_ts2 {:?}",
                                    self.dbgname,
                                    self.last_bin_ts2
                                );
                                continue;
                            }
                        }
                    }
                    Pending => Pending,
                }
            } else if let Some(x) = self.inp_buf.take() {
                match self.as_mut().handle_bins(x) {
                    Ok((keep, item)) => {
                        if keep.len() != 0 {
                            self.inp_buf = Some(keep);
                        }
                        let item = sitem_data(item);
                        Ready(Some(item))
                    }
                    Err(e) => Ready(Some(sitem_err_from_string(e))),
                }
            } else if let Some(inp) = self.inp.as_mut() {
                match inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(x))) => match x {
                        StreamItem::DataItem(RangeCompletableItem::Data(x)) => {
                            if x.len() != 0 {
                                self.inp_buf = Some(x);
                            }
                            continue;
                        }
                        StreamItem::DataItem(RangeCompletableItem::RangeComplete) => {
                            self.inp_range_final = true;
                            continue;
                        }
                        StreamItem::Log(x) => Ready(Some(Ok(StreamItem::Log(x)))),
                        StreamItem::Stats(x) => Ready(Some(Ok(StreamItem::Stats(x)))),
                    },
                    Ready(Some(Err(e))) => Ready(Some(sitem_err_from_string(e))),
                    Ready(None) => {
                        self.inp = None;
                        // TODO assert that we have emitted up to the requested range.
                        // If not, request the remaining range from "finer" input.
                        if let Some(j) = self.last_bin_ts2 {
                            if j != self.range.nano_end() {
                                let range = NanoRange {
                                    beg: j.ns(),
                                    end: self.range.full_range().end(),
                                };
                                debug_setup!(
                                    "{}  received something but not all, setup rest from finer  {}  {}  {}",
                                    self.dbgname,
                                    self.range,
                                    j,
                                    range
                                );
                                match self.as_mut().setup_inp_finer(range, false) {
                                    Ok(()) => {
                                        continue;
                                    }
                                    Err(e) => Ready(Some(sitem_err_from_string(e))),
                                }
                            } else {
                                debug_setup!("{}  received everything", self.dbgname);
                                Ready(None)
                            }
                        } else {
                            let range = self.range.to_nano_range();
                            debug_setup!(
                                "{}  received nothing at all, setup full range from finer  {}  {}",
                                self.dbgname,
                                self.range,
                                range
                            );
                            match self.as_mut().setup_inp_finer(range, false) {
                                Ok(()) => {
                                    continue;
                                }
                                Err(e) => Ready(Some(sitem_err_from_string(e))),
                            }
                        }
                    }
                    Pending => Pending,
                }
            } else {
                self.done = true;
                if self.inp_finer_range_final_cnt == self.inp_finer_range_final_max {
                    trace_handle!("{}  range finale  all", self.dbgname);
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    trace_handle!("{}  substreams not final", self.dbgname);
                    continue;
                }
            };
        }
    }
}
