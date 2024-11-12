use items_0::transform::EventStreamTrait;
use items_0::transform::TransformEvent;
use items_0::transform::TransformProperties;
use items_0::transform::WithTransformProperties;
use items_2::transform::make_transform_identity;
use items_2::transform::make_transform_min_max_avg;
use items_2::transform::make_transform_pulse_id_diff;
use query::transform::EventTransformQuery;
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
