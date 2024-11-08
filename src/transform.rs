use futures_util::Stream;
use futures_util::StreamExt;
use items_0::collect_s::CollectableDyn;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::transform::CollectableStreamBox;
use items_0::transform::EventStreamBox;
use items_0::transform::EventStreamTrait;
use items_0::transform::TransformEvent;
use items_0::transform::TransformProperties;
use items_0::transform::WithTransformProperties;
use items_2::transform::make_transform_identity;
use items_2::transform::make_transform_min_max_avg;
use items_2::transform::make_transform_pulse_id_diff;
use query::transform::EventTransformQuery;
use query::transform::TimeBinningTransformQuery;
use query::transform::TransformQuery;
use std::pin::Pin;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "Transform")]
pub enum Error {
    #[error("UnhandledQuery({0:?})")]
    UnhandledQuery(EventTransformQuery),
}

pub fn build_event_transform(tr: &TransformQuery) -> Result<TransformEvent, Error> {
    let trev = tr.get_tr_event();
    match trev {
        EventTransformQuery::ValueFull => Ok(make_transform_identity()),
        EventTransformQuery::MinMaxAvgDev => Ok(make_transform_min_max_avg()),
        EventTransformQuery::ArrayPick(..) => Err(Error::UnhandledQuery(trev.clone())),
        EventTransformQuery::PulseIdDiff => Ok(make_transform_pulse_id_diff()),
        EventTransformQuery::EventBlobsVerbatim => Err(Error::UnhandledQuery(trev.clone())),
        EventTransformQuery::EventBlobsUncompressed => Err(Error::UnhandledQuery(trev.clone())),
    }
}

pub fn build_merged_event_transform(tr: &TransformQuery) -> Result<TransformEvent, Error> {
    let trev = tr.get_tr_event();
    match trev {
        EventTransformQuery::PulseIdDiff => Ok(make_transform_pulse_id_diff()),
        _ => Ok(make_transform_identity()),
    }
}

// TODO remove, in its current usage it reboxes
pub struct EventsToTimeBinnable {
    inp: Pin<Box<dyn EventStreamTrait>>,
}

impl EventsToTimeBinnable {
    pub fn new<INP>(inp: INP) -> Self
    where
        INP: EventStreamTrait + 'static,
    {
        Self { inp: Box::pin(inp) }
    }
}

impl WithTransformProperties for EventsToTimeBinnable {
    fn query_transform_properties(&self) -> TransformProperties {
        self.inp.query_transform_properties()
    }
}

pub fn build_full_transform_collectable(
    tr: &TransformQuery,
    inp: EventStreamBox,
) -> Result<CollectableStreamBox, Error> {
    // TODO this must return a Stream!
    //let evs = build_event_transform(tr, inp)?;
    let trtb = tr.get_tr_time_binning();
    let a: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn CollectableDyn>>> + Send>> =
        Box::pin(inp.0.map(|item| match item {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::Data(item) => {
                        let item: Box<dyn CollectableDyn> = Box::new(item);
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))
                    }
                    RangeCompletableItem::RangeComplete => {
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))
                    }
                },
                StreamItem::Log(item) => Ok(StreamItem::Log(item)),
                StreamItem::Stats(item) => Ok(StreamItem::Stats(item)),
            },
            Err(e) => Err(e),
        }));
    let stream: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn CollectableDyn>>> + Send>> =
        Box::pin(futures_util::stream::empty());
    let stream = Box::pin(futures_util::stream::empty()) as _;
    match trtb {
        TimeBinningTransformQuery::None => Ok(CollectableStreamBox(stream)),
        TimeBinningTransformQuery::TimeWeighted => todo!(),
        TimeBinningTransformQuery::Unweighted => todo!(),
    }
}
