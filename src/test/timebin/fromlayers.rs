use crate::eventsplainreader::DummyCacheReadProvider;
use crate::log::*;
use crate::test::events_reader::TestEventsReader;
use crate::timebin::fromlayers::TimeBinnedFromLayers;
use futures_util::StreamExt;
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
use query::api4::events::EventsSubQuery;
use query::api4::events::EventsSubQuerySelect;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "Test")]
pub enum Error {
    FromLayers(#[from] crate::timebin::fromlayers::Error),
    Msg(String),
}

async fn timebin_from_layers_inner() -> Result<(), Error> {
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
    let transform_query = TransformQuery::default_time_binned();
    let nano_range = NanoRange {
        beg: 1000 * 1000 * 1000 * 1,
        end: 1000 * 1000 * 1000 * 2,
    };
    let cache_read_provider = Arc::new(DummyCacheReadProvider::new());
    let events_read_provider = Arc::new(TestEventsReader::new(nano_range.clone()));
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
    let bin_len_layers = [].into_iter().map(DtMs::from_ms_u64).collect();
    let do_time_weight = true;
    let bin_len = DtMs::from_ms_u64(200);
    let range = BinnedRange::from_nano_range(nano_range, bin_len);
    let mut stream = TimeBinnedFromLayers::new(
        ch_conf,
        cache_usage,
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

#[test]
fn timebin_from_layers() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(timebin_from_layers_inner()).unwrap()
}

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
    let transform_query = TransformQuery::default_time_binned();
    let nano_range = NanoRange {
        beg: 1000 * 1000 * 1000 * 1,
        end: 1000 * 1000 * 1000 * 2,
    };
    let cache_read_provider = Arc::new(DummyCacheReadProvider::new());
    let events_read_provider = Arc::new(TestEventsReader::new(nano_range.clone()));
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
        cache_usage,
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

#[test]
fn timebin_from_layers_1layer() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(timebin_from_layers_1layer_inner()).unwrap()
}
