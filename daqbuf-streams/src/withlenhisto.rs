use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::WithLen;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

#[derive(Debug)]
pub struct WithLenHisto<S> {
    inp: Option<S>,
    histo: [u32; 32],
    name: String,
}

impl<S> WithLenHisto<S> {
    pub fn new(inp: S, name: String) -> Self {
        Self {
            inp: Some(inp),
            histo: [0; 32],
            name,
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
struct WithLenHistoRes {
    histo: [u32; 32],
    name: String,
}

impl<S, T, E> Stream for WithLenHisto<S>
where
    S: Stream<Item = Sitemty2<T, E>> + Unpin,
    T: WithLen,
{
    type Item = <S as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let Some(inp) = self.inp.as_mut() {
            match inp.poll_next_unpin(cx) {
                Ready(Some(x)) => match x {
                    Ok(x) => match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(x) => {
                                let n = 32 - (x.len() as u32).leading_zeros();
                                self.histo[n as usize] += 1;
                                Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                                    x,
                                )))))
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
                    let res = WithLenHistoRes {
                        histo: self.histo.clone(),
                        name: self.name.clone(),
                    };
                    let msg = format!("{res:?}");
                    let item = LogItem::info(msg);
                    Ready(Some(Ok(StreamItem::Log(item))))
                }
                Pending => Pending,
            }
        } else {
            Ready(None)
        }
    }
}
