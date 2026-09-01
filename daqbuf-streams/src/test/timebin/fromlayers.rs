use crate::eventsplainreader::DummyCacheReadProvider;
use crate::eventsplainreader::TestCacheReadProvider;
use crate::log::*;
use crate::test::events_reader::TestEventsReadProvider;
use crate::timebin::fromlayers::TimeBinnedFromLayers;
use crate::timebin::opts::BinningOptions;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_data;
use items_0::timebin::BinningggContainerBinsDyn;
use items_2::binning::container_bins::ContainerBins;
use netpod::query::CacheUsage;
use netpod::range::evrange::NanoRange;
use netpod::range::evrange::SeriesRange;
use netpod::BinnedRange;
use netpod::ChConf;
use netpod::ChannelTypeConfigGen;
use netpod::DtMs;
use netpod::ReqCtx;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::Shape;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use std::sync::Arc;

autoerr::create_error_v1!(
    name(Error, "Test"),
    enum variants {
        FromLayers(#[from] crate::timebin::fromlayers::Error),
        Msg(String),
    },
);

// TODO
#[cfg(target_os = "cuda")]
async fn timebin_from_layers_00_inner() -> Result<(), Error> {
    let ctx = Arc::new(ReqCtx::for_test());
    let ch_conf = ChannelTypeConfigGen::Scylla(ChConf::new(
        "test",
        123,
        SeriesKind::ChannelData,
        ScalarType::F32,
        Shape::Scalar,
        "test-reader-dim0-f32-00",
    ));
    let binning_opts = BinningOptions::default();
    let transform_query = TransformQuery::default_time_binned();
    let nano_range = NanoRange {
        beg: 1000 * 1000 * 1000 * 10,
        end: 1000 * 1000 * 1000 * 20,
    };
    let bin_len = DtMs::from_ms_u64(1000);
    let range = BinnedRange::from_nano_range(nano_range, bin_len);
    let cache_read_provider = Arc::new(TestCacheReadProvider::new());
    let events_read_provider = Arc::new(TestEventsReadProvider::new());
    // let one_before_range = true;
    // let series_range = SeriesRange::TimeRange(nano_range.clone());
    // let select = EventsSubQuerySelect::new(
    //     ch_conf.clone(),
    //     series_range,
    //     one_before_range,
    //     transform_query.clone(),
    // );
    let settings = EventsSubQuerySettings::default();
    let log_level = "INFO";
    let bin_len_layers = [1000].into_iter().map(DtMs::from_ms_u64).collect();
    let do_time_weight = true;
    let mut stream = TimeBinnedFromLayers::new(
        ch_conf,
        binning_opts,
        transform_query,
        settings,
        log_level.into(),
        ctx,
        range,
        do_time_weight,
        bin_len_layers,
        cache_read_provider,
        events_read_provider,
    )?;
    let mut exp = ContainerBins::new();
    let s = r"
        10s   11s    100     10000.1    10990.1    10495.1    10990.1
        11s   12s    100     11000.1    11990.1    11495.1    11990.1
        12s   13s     55        42.0       46.0       44.0       43.0
        13s   14s     55        42.0       46.0       44.0       43.0
        14s   15s     55        42.0       46.0       44.0       43.0
        15s   16s    100     15000.1    15990.1    15495.1    15990.1
        16s   17s    100     16000.1    16990.1    16495.1    16990.1
        17s   18s     55        42.0       46.0       44.0       43.0
        18s   19s     55        42.0       46.0       44.0       43.0
        19s   20s     55        42.0       46.0       44.0       43.0
    ";
    for line in s.split("\n") {
        let a: Vec<_> = line.split(" ").filter(|&x| x != " " && x != "").collect();
        info!("len  {}   {:?}", a.len(), a);
        if a.len() == 7 {
            let dt1 = humantime::parse_duration(&a[0]).unwrap();
            let dt2 = humantime::parse_duration(&a[1]).unwrap();
            exp.push_back(
                TsNano::from_ms(dt1.as_millis() as u64),
                TsNano::from_ms(dt2.as_millis() as u64),
                a[2].parse().unwrap(),
                a[3].parse().unwrap(),
                a[4].parse().unwrap(),
                a[5].parse().unwrap(),
                a[6].parse().unwrap(),
                true,
            );
        }
    }
    info!("PARSED EXP  len {}", exp.len());
    let mut cbins = ContainerBins::new();
    while let Some(item) = stream.next().await {
        let _ = on_sitemty_data!(item, |x: Box<dyn BinningggContainerBinsDyn>| {
            if let Some(bins) = x.as_any_ref().downcast_ref::<ContainerBins<f32, f32>>() {
                for (&ts1, &ts2, &cnt, min, max, agg, lst, &fnl) in itertools::izip!(
                    bins.ts1s_iter(),
                    bins.ts2s_iter(),
                    bins.cnts_iter(),
                    bins.mins_iter(),
                    bins.maxs_iter(),
                    bins.aggs_iter(),
                    bins.lsts_iter(),
                    bins.fnls_iter(),
                ) {
                    trace!("=========");
                    trace!(
                        "{:?}  {:5}  {:8.2}  {:8.2}  {:8.2}  {:8.2}",
                        ts1,
                        cnt,
                        min,
                        max,
                        agg,
                        lst,
                    );
                    trace!("=========");
                    cbins.push_back(ts1, ts2, cnt, min, max, agg, lst, fnl);
                }
            } else {
                panic!("expect f32 bins")
            }
            sitem_data(x)
        });
    }
    let cmp = items_2::binning::container_bins::compare_boxed_f32(&exp, &cbins);
    assert_eq!(cmp, true);
    Ok(())
}

// TODO
#[cfg(target_os = "cuda")]
#[test]
fn timebin_from_layers_00() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(timebin_from_layers_00_inner()).unwrap()
}

// TODO
#[cfg(target_os = "cuda")]
async fn timebin_from_layers_1layer_inner() -> Result<(), Error> {
    let ctx = Arc::new(ReqCtx::for_test());
    let ch_conf = ChannelTypeConfigGen::Scylla(ChConf::new(
        "testing",
        123,
        SeriesKind::ChannelData,
        ScalarType::F32,
        Shape::Scalar,
        "basictest-f32",
    ));
    let cache_usage = CacheUsage::Ignore;
    let binning_opts: BinningOptions = todo!();
    let transform_query = TransformQuery::default_time_binned();
    let nano_range = NanoRange {
        beg: 1000 * 1000 * 1000 * 1,
        end: 1000 * 1000 * 1000 * 2,
    };
    let cache_read_provider = Arc::new(DummyCacheReadProvider::new());
    let events_read_provider = Arc::new(TestEventsReadProvider::new());
    // let one_before_range = true;
    // let series_range = SeriesRange::TimeRange(nano_range.clone());
    // let select = EventsSubQuerySelect::new(
    //     ch_conf.clone(),
    //     series_range,
    //     one_before_range,
    //     transform_query.clone(),
    // );
    let settings = EventsSubQuerySettings::default();
    // let reqid = ctx.reqid().into();
    let log_level = "INFO";
    // let query = EventsSubQuery::from_parts(select, settings.clone(), reqid, log_level.into());
    let bin_len_layers = [20, 100].into_iter().map(DtMs::from_ms_u64).collect();
    let do_time_weight = true;
    let bin_len = DtMs::from_ms_u64(200);
    let range = BinnedRange::from_nano_range(nano_range, bin_len);
    let mut stream = TimeBinnedFromLayers::new(
        ch_conf,
        binning_opts,
        transform_query,
        settings,
        log_level.into(),
        ctx,
        range,
        do_time_weight,
        bin_len_layers,
        cache_read_provider,
        events_read_provider,
    )?;
    while let Some(x) = stream.next().await {
        let item = x.map_err(|e| Error::Msg(e.to_string()))?;
        trace!("item {:?}", item);
    }
    Ok(())
}

// TODO
#[cfg(target_os = "cuda")]
#[test]
fn timebin_from_layers_1layer() {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(timebin_from_layers_1layer_inner()).unwrap()
}
