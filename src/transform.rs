use items_0::transform::EventStreamTrait;
use items_0::transform::TransformProperties;
use items_0::transform::WithTransformProperties;
use query::transform::EventTransformQuery;
use std::pin::Pin;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "Transform")]
pub enum Error {
    #[error("UnhandledQuery({0:?})")]
    UnhandledQuery(EventTransformQuery),
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
