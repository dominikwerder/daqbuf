use super::cached::reader::CacheReadProvider;
use super::cached::reader::EventsReadProvider;
use super::opts::BinningOptions;
use crate::log::*;
use crate::timebin::fromevents::BinnedFromEvents;
use crate::timebin::gapfill::GapFill;
use crate::timebin::grid::find_next_finer_bin_len;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinsBoxed;
use items_2::binning::timeweight::timeweight_bins_stream::BinnedBinsTimeweightStream;
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

macro_rules! trace_init { ($($arg:tt)*) => ( if false { trace!($($arg)*); } ) }

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
}

impl TimeBinnedFromLayers {
    pub fn type_name() -> &'static str {
        core::any::type_name::<Self>()
    }

    pub fn new(
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
        trace_init!(
            "{}::new  {:?}  {:?}  {:?}",
            Self::type_name(),
            ch_conf.series(),
            range,
            bin_len_layers
        );
        let bin_len = DtMs::from_ms_u64(range.bin_len.ms());
        if bin_len_layers.contains(&bin_len) {
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
            let ret = Self { inp: Box::pin(inp) };
            Ok(ret)
        } else {
            trace_init!(
                "{}::new  bin_len off layers  {:?}",
                Self::type_name(),
                range
            );
            match find_next_finer_bin_len(bin_len, &bin_len_layers) {
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
                    let ret = Self { inp: Box::pin(inp) };
                    Ok(ret)
                }
                None => {
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
                    let inp =
                        BinnedFromEvents::new(range, evq, do_time_weight, events_read_provider)?;
                    let ret = Self { inp: Box::pin(inp) };
                    trace_init!("{}::new  setup from events", Self::type_name());
                    Ok(ret)
                }
            }
        }
    }
}

impl Stream for TimeBinnedFromLayers {
    type Item = Sitemty<BinsBoxed>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(x)) => Ready(Some(x)),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
