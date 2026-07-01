use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::container::ByteEstimate;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Empty;
use items_2::eventfull::EventFull;
use netpod::range::evrange::SeriesRange;
use netpod::timeunits::MS;
use netpod::ScalarType;
use netpod::Shape;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;

pub trait TypedGenerator {
    type RustScalar;
}

pub struct EventBlobsGeneratorI32Test00 {
    ts: u64,
    dts: u64,
    tsend: u64,
    #[allow(unused)]
    c1: u64,
    scalar_type: ScalarType,
    be: bool,
    shape: Shape,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    done: bool,
    done_range_final: bool,
}

impl TypedGenerator for EventBlobsGeneratorI32Test00 {
    type RustScalar = i32;
}

impl EventBlobsGeneratorI32Test00 {
    pub fn new(node_ix: u64, node_count: u64, range: SeriesRange) -> Self {
        let range = match range {
            SeriesRange::TimeRange(k) => k,
            SeriesRange::PulseRange(_) => todo!(),
        };
        let dt = MS * 1000;
        let ts = (range.beg / dt + node_ix) * dt;
        let dts = dt * node_count as u64;
        let tsend = range.end;
        Self {
            ts,
            dts,
            tsend,
            c1: 0,
            scalar_type: ScalarType::I32,
            be: true,
            shape: Shape::Scalar,
            timeout: None,
            done: false,
            done_range_final: false,
        }
    }

    fn make_batch(&mut self) -> Sitemty<EventFull> {
        // TODO should not repeat self type name
        type T = <EventBlobsGeneratorI32Test00 as TypedGenerator>::RustScalar;
        let mut item = EventFull::empty();
        let mut ts = self.ts;
        loop {
            if ts >= self.tsend || item.byte_estimate() > 200 {
                break;
            }
            let pulse = ts;
            let value = (ts / (MS * 100) % 1000) as T;
            item.push(
                ts,
                pulse,
                value.to_be_bytes().to_vec(),
                self.scalar_type.clone(),
                self.be,
                self.shape.clone(),
                None,
            );
            ts += self.dts;
        }
        self.ts = ts;
        let w = sitem_data(item);
        w
    }
}

impl Stream for EventBlobsGeneratorI32Test00 {
    type Item = Sitemty<EventFull>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.done_range_final {
                Ready(None)
            } else if self.ts >= self.tsend {
                self.done = true;
                self.done_range_final = true;
                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
            } else if false {
                // To use the generator without throttling, use this scope
                Ready(Some(self.make_batch()))
            } else if let Some(fut) = self.timeout.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = None;
                        Ready(Some(self.make_batch()))
                    }
                    Pending => Pending,
                }
            } else {
                self.timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(2))));
                continue;
            };
        }
    }
}

pub struct EventBlobsGeneratorI32Test01 {
    ts: u64,
    dts: u64,
    tsend: u64,
    #[allow(unused)]
    c1: u64,
    scalar_type: ScalarType,
    be: bool,
    shape: Shape,
    timeout: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    done: bool,
    done_range_final: bool,
}

impl TypedGenerator for EventBlobsGeneratorI32Test01 {
    type RustScalar = i32;
}

impl EventBlobsGeneratorI32Test01 {
    pub fn new(node_ix: u64, node_count: u64, range: SeriesRange) -> Self {
        let range = match range {
            SeriesRange::TimeRange(k) => k,
            SeriesRange::PulseRange(_) => todo!(),
        };
        let dt = MS * 500;
        let ts = (range.beg / dt + node_ix) * dt;
        let dts = dt * node_count as u64;
        let tsend = range.end;
        Self {
            ts,
            dts,
            tsend,
            c1: 0,
            scalar_type: ScalarType::I32,
            be: true,
            shape: Shape::Scalar,
            timeout: None,
            done: false,
            done_range_final: false,
        }
    }

    fn make_batch(&mut self) -> Sitemty<EventFull> {
        type T = i32;
        let mut item = EventFull::empty();
        let mut ts = self.ts;
        loop {
            if self.ts >= self.tsend || item.byte_estimate() > 400 {
                break;
            }
            let pulse = ts;
            let value = (ts / self.dts) as T;
            item.push(
                ts,
                pulse,
                value.to_be_bytes().to_vec(),
                self.scalar_type.clone(),
                self.be,
                self.shape.clone(),
                None,
            );
            ts += self.dts;
        }
        self.ts = ts;
        let w = sitem_data(item);
        w
    }
}

impl Stream for EventBlobsGeneratorI32Test01 {
    type Item = Sitemty<EventFull>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.done_range_final {
                Ready(None)
            } else if self.ts >= self.tsend {
                self.done = true;
                self.done_range_final = true;
                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
            } else if false {
                // To use the generator without throttling, use this scope
                Ready(Some(self.make_batch()))
            } else if let Some(fut) = self.timeout.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(()) => {
                        self.timeout = None;
                        Ready(Some(self.make_batch()))
                    }
                    Pending => Pending,
                }
            } else {
                self.timeout = Some(Box::pin(tokio::time::sleep(Duration::from_millis(2))));
                continue;
            };
        }
    }
}
