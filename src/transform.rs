//! Helper functions to create transforms which act locally on a batch of events.
//! Tailored to the usage pattern given by `TransformQuery`.

use crate::channelevents::ChannelEvents;
use crate::eventsdim0::EventsDim0;
use items_0::transform::EventTransform;
use items_0::transform::TransformEvent;
use items_0::transform::TransformProperties;
use items_0::transform::WithTransformProperties;
use items_0::Appendable;
use items_0::AsAnyMut;
use items_0::Empty;
use items_0::Events;
use items_0::EventsNonObj;
use netpod::log::*;
use std::mem;

struct TransformEventIdentity {}

impl WithTransformProperties for TransformEventIdentity {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

impl EventTransform for TransformEventIdentity {
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        src
    }
}

pub fn make_transform_identity() -> TransformEvent {
    TransformEvent(Box::new(TransformEventIdentity {}))
}

struct TransformEventMinMaxAvg {}

impl WithTransformProperties for TransformEventMinMaxAvg {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

impl EventTransform for TransformEventMinMaxAvg {
    fn transform(&mut self, mut src: Box<dyn Events>) -> Box<dyn Events> {
        src.to_min_max_avg()
    }
}

pub fn make_transform_min_max_avg() -> TransformEvent {
    TransformEvent(Box::new(TransformEventMinMaxAvg {}))
}

struct TransformEventPulseIdDiff {
    pulse_last: Option<u64>,
}

impl WithTransformProperties for TransformEventPulseIdDiff {
    fn query_transform_properties(&self) -> TransformProperties {
        todo!()
    }
}

impl EventTransform for TransformEventPulseIdDiff {
    fn transform(&mut self, src: Box<dyn Events>) -> Box<dyn Events> {
        let (tss, pulses) = EventsNonObj::into_tss_pulses(src);
        let mut item = EventsDim0::empty();
        let pulse_last = &mut self.pulse_last;
        for (ts, pulse) in tss.into_iter().zip(pulses) {
            let value = if let Some(last) = pulse_last {
                pulse as i64 - *last as i64
            } else {
                0
            };
            item.push(ts, pulse, value);
            *pulse_last = Some(pulse);
        }
        Box::new(ChannelEvents::Events(Box::new(item)))
    }
}

pub fn make_transform_pulse_id_diff() -> TransformEvent {
    TransformEvent(Box::new(TransformEventPulseIdDiff { pulse_last: None }))
}
