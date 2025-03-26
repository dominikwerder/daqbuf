use super::cached::reader::CacheReadProvider;
use super::cached::reader::EventsReadProvider;
use super::opts::BinningOptions;
use crate::log;
use crate::timebin::fromevents::BinnedFromEvents;
use crate::timebin::gapfill::GapFill;
use crate::timebin::grid::find_next_finer_bin_len;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinsBoxed;
use items_2::binning::timeweight::timeweight_bins_stream::BinnedBinsTimeweightStream;
use netpod::query::CacheUsage;
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
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

macro_rules! info_init { ($($arg:expr),*) => ( if true { log::info!($($arg),*); } ) }

macro_rules! trace_init { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); } ) }

autoerr::create_error_v1!(
    name(Error, "TimeBinnedFromLayers"),
    enum variants {
        GapFill(#[from] super::gapfill::Error),
        BinnedFromEvents(#[from] super::fromevents::Error),
        FinerGridMismatch(DtMs, DtMs),
    },
);

type BoxedInput = Pin<Box<dyn Stream<Item = Sitemty<BinsBoxed>> + Send>>;

pub struct TimeBinnedFromLayers {
    inp: BoxedInput,
    outbuf: VecDeque<<Self as Stream>::Item>,
}

impl TimeBinnedFromLayers {
    pub fn type_name() -> &'static str {
        core::any::type_name::<Self>()
    }

    pub fn new(
        ch_conf: ChannelTypeConfigGen,
        binning_opts: BinningOptions,
        cache_usage: CacheUsage,
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
        trace_init!(
            "{}::new  {:?}  {:?}  {:?}  {:?}",
            Self::type_name(),
            ch_conf.series(),
            range,
            bin_len_layers,
            binning_opts
        );
        let bin_len = DtMs::from_ms_u64(range.bin_len.ms());
        if binning_opts.pbd_enable() {
            let inp = futures_util::stream::iter([]);
            let mut ret = Self {
                inp: Box::pin(inp),
                outbuf: VecDeque::new(),
            };
            info_init!("pbd_enable");
            let item = LogItem::from_node(0, log::Level::TRACE, "test-log-item-trace".into());
            let item = StreamItem::Log(item);
            ret.outbuf.push_back(Ok(item));
            let item = LogItem::from_node(0, log::Level::DEBUG, "test-log-item-debug".into());
            let item = StreamItem::Log(item);
            ret.outbuf.push_back(Ok(item));
            let item = LogItem::from_node(0, log::Level::INFO, "test-log-item-info".into());
            let item = StreamItem::Log(item);
            ret.outbuf.push_back(Ok(item));
            let item = LogItem::from_node(0, log::Level::WARN, "test-log-item-warn".into());
            let item = StreamItem::Log(item);
            ret.outbuf.push_back(Ok(item));
            let item = StatsItem::Binning;
            let item = StreamItem::Stats(item);
            ret.outbuf.push_back(Ok(item));
            Ok(ret)
        } else if cache_usage.is_cache_read() && bin_len_layers.contains(&bin_len) {
            trace_init!("{}::new  bin_len in layers  {:?}", Self::type_name(), range);
            let inp = GapFill::new(
                "FromLayers-ongrid".into(),
                ch_conf.clone(),
                binning_opts.clone(),
                transform_query.clone(),
                sub.clone(),
                log_level.clone(),
                ctx.clone(),
                range,
                do_time_weight,
                bin_len_layers,
                cache_read_provider,
                events_read_provider.clone(),
            )?;
            let ret = Self {
                inp: Box::pin(inp),
                outbuf: VecDeque::new(),
            };
            Ok(ret)
        } else {
            trace_init!(
                "{}::new  bin_len off layers  {:?}",
                Self::type_name(),
                range
            );
            let x = if cache_usage.is_cache_read() {
                find_next_finer_bin_len(bin_len, &bin_len_layers)
            } else {
                None
            };
            match x {
                Some(finer) => {
                    if bin_len.ms() % finer.ms() != 0 {
                        return Err(Error::FinerGridMismatch(bin_len, finer));
                    }
                    let range_finer = BinnedRange::from_nano_range(range.to_nano_range(), finer);
                    trace_init!(
                        "{}::new  next finer from bins {:?}  {:?}",
                        Self::type_name(),
                        finer,
                        range_finer
                    );
                    let inp = GapFill::new(
                        "FromLayers-finergrid".into(),
                        ch_conf.clone(),
                        binning_opts.clone(),
                        transform_query.clone(),
                        sub.clone(),
                        log_level.clone(),
                        ctx.clone(),
                        range_finer.clone(),
                        do_time_weight,
                        bin_len_layers,
                        cache_read_provider,
                        events_read_provider.clone(),
                    )?;
                    let inp = BinnedBinsTimeweightStream::new(range, Box::pin(inp));
                    let ret = Self {
                        inp: Box::pin(inp),
                        outbuf: VecDeque::new(),
                    };
                    Ok(ret)
                }
                None => {
                    if binning_opts.allow_from_events() {
                        trace_init!("{}::new  next finer from events", Self::type_name());
                        let series_range = SeriesRange::TimeRange(range.to_nano_range());
                        let one_before_range = true;
                        let select = EventsSubQuerySelect::new(
                            ch_conf.clone(),
                            series_range,
                            one_before_range,
                            transform_query.clone(),
                        );
                        let evq = EventsSubQuery::from_parts(
                            select,
                            sub.clone(),
                            ctx.reqid().into(),
                            log_level.clone(),
                        );
                        let inp = BinnedFromEvents::new(
                            range,
                            evq,
                            do_time_weight,
                            events_read_provider,
                        )?;
                        let ret = Self {
                            inp: Box::pin(inp),
                            outbuf: VecDeque::new(),
                        };
                        trace_init!("{}::new  setup from events", Self::type_name());
                        Ok(ret)
                    } else {
                        let inp = futures_util::stream::iter([]);
                        let ret = Self {
                            inp: Box::pin(inp),
                            outbuf: VecDeque::new(),
                        };
                        trace_init!("{}::new  setup nothing", Self::type_name());
                        trace_init!("bin from events disabled on user request");
                        Ok(ret)
                    }
                }
            }
        }
    }
}

impl Stream for TimeBinnedFromLayers {
    type Item = Sitemty<BinsBoxed>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let Some(x) = self.outbuf.pop_front() {
            Ready(Some(x))
        } else {
            match self.inp.poll_next_unpin(cx) {
                Ready(Some(x)) => Ready(Some(x)),
                Ready(None) => Ready(None),
                Pending => Pending,
            }
        }
    }
}
