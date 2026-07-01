use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::MergeableTy;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use netpod::TsNano;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug)]
pub struct Dedup<S> {
    inp: S,
    tsmin: TsNano,
}

impl<S> Dedup<S>
where
    S: Stream + Unpin,
{
    pub fn new(inp: S) -> Self {
        Self {
            inp,
            tsmin: TsNano::from_ns(0),
        }
    }

    fn clean_item<T, E>(&mut self, mut item: T) -> T
    where
        S: Stream<Item = Sitemty2<T, E>> + Unpin,
        T: MergeableTy,
    {
        item.retain_unique_ts(self.tsmin);
        if let Some(tsmax) = item.ts_max() {
            self.tsmin = tsmax;
        }
        item
    }
}

impl<S, T, E> Stream for Dedup<S>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: MergeableTy,
{
    type Item = <S as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(x)) => match x {
                Ok(x) => match x {
                    StreamItem::DataItem(x) => match x {
                        RangeCompletableItem::Data(x) => {
                            let item = self.clean_item(x);
                            let x = RangeCompletableItem::Data(item);
                            Ready(Some(Ok(StreamItem::DataItem(x))))
                        }
                        RangeCompletableItem::RangeComplete => Ready(Some(Ok(
                            StreamItem::DataItem(RangeCompletableItem::RangeComplete),
                        ))),
                    },
                    StreamItem::Log(x) => Ready(Some(Ok(StreamItem::Log(x)))),
                    StreamItem::Stats(x) => Ready(Some(Ok(StreamItem::Stats(x)))),
                },
                Err(e) => Ready(Some(Err(e))),
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
