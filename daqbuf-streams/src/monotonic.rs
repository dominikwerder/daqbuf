use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::MergeableTy;
use items_0::streamitem::sitem2_data;
use items_0::streamitem::sitem2_log;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::WithLen;
use netpod::TsNano;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug)]
pub struct CheckMonotonic<S>
where
    S: Stream + Unpin,
    <S as Stream>::Item: fmt::Debug + Unpin,
    // S: Stream<Item = Sitemty2<T, E>> + Unpin,
    // T: WithLen + MergeableTy + Unpin + Send + 'static,
    // E: fmt::Debug + Unpin + Send + 'static,
{
    inp: Option<S>,
    tmax: TsNano,
    name: String,
    buf: Option<<S as Stream>::Item>,
}

impl<S> CheckMonotonic<S>
where
    S: Stream + Unpin,
    <S as Stream>::Item: fmt::Debug + Unpin,
{
    pub fn new(inp: S, name: String) -> Self {
        Self {
            inp: Some(inp),
            tmax: TsNano::from_ns(0),
            name,
            buf: None,
        }
    }
}

impl<S, T, E> Stream for CheckMonotonic<S>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: WithLen + MergeableTy + Unpin,
    E: fmt::Debug + Unpin,
{
    type Item = <S as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let Some(x) = self.buf.take() {
            return Ready(Some(x));
        }
        if let Some(inp) = self.inp.as_mut() {
            match inp.poll_next_unpin(cx) {
                Ready(Some(x)) => match x {
                    Ok(x) => match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(x) => {
                                if !x.is_consistent() {
                                    log::error!("{}  inconsistent", self.name);
                                    Ready(None)
                                } else {
                                    if let Some(t1) = x.ts_min() {
                                        if t1 < self.tmax {
                                            self.buf = Some(sitem2_data(x));
                                            let msg = format!(
                                                "{}  bad order  {}  {}",
                                                self.name, self.tmax, t1
                                            );
                                            let x = LogItem::info(msg);
                                            Ready(Some(sitem2_log(x)))
                                        } else {
                                            if let Some(t2) = x.ts_max() {
                                                self.tmax = t2;
                                                let x = RangeCompletableItem::Data(x);
                                                let x = StreamItem::DataItem(x);
                                                Ready(Some(Ok(x)))
                                            } else {
                                                self.buf = Some(sitem2_data(x));
                                                let msg = format!(
                                                    "{}  min but no max  {}  {}",
                                                    self.name, self.tmax, t1
                                                );
                                                let x = LogItem::info(msg);
                                                Ready(Some(sitem2_log(x)))
                                            }
                                        }
                                    } else {
                                        let x = RangeCompletableItem::Data(x);
                                        let x = StreamItem::DataItem(x);
                                        Ready(Some(Ok(x)))
                                    }
                                }
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
                Ready(None) => {
                    self.inp = None;
                    Ready(None)
                }
                Pending => Pending,
            }
        } else {
            Ready(None)
        }
    }
}
