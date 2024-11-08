use futures_util::stream::StreamExt;
use futures_util::Stream;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::transform::TransformProperties;
use items_0::transform::WithTransformProperties;
use items_0::Events;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct IntoBoxedEventStream<INP, T>
where
    T: Events,
    INP: Stream<Item = Sitemty<T>> + WithTransformProperties,
{
    //inp: Pin<Box<dyn Stream<Item = Sitemty<T>>>>,
    inp: Pin<Box<INP>>,
}

impl<INP, T> Stream for IntoBoxedEventStream<INP, T>
where
    T: Events,
    INP: Stream<Item = Sitemty<T>> + WithTransformProperties,
{
    type Item = Sitemty<Box<dyn Events>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => Ready(Some(match item {
                Ok(item) => Ok(match item {
                    StreamItem::DataItem(item) => StreamItem::DataItem(match item {
                        RangeCompletableItem::RangeComplete => RangeCompletableItem::RangeComplete,
                        RangeCompletableItem::Data(item) => RangeCompletableItem::Data(Box::new(item)),
                    }),
                    StreamItem::Log(item) => StreamItem::Log(item),
                    StreamItem::Stats(item) => StreamItem::Stats(item),
                }),
                Err(e) => Err(e),
            })),
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

impl<INP, T> WithTransformProperties for IntoBoxedEventStream<INP, T>
where
    T: Events,
    INP: Stream<Item = Sitemty<T>> + WithTransformProperties,
{
    fn query_transform_properties(&self) -> TransformProperties {
        self.inp.query_transform_properties()
    }
}
