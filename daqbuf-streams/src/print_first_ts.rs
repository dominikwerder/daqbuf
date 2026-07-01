use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::MergeableTy;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use netpod::TsNanoVecFmt;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

pub struct PrintFirstTs<INP>
where
    INP: Stream + Unpin,
{
    inp: INP,
    tag: String,
    printed: bool,
    done: bool,
}

impl<INP> PrintFirstTs<INP>
where
    INP: Stream + Unpin,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: INP, tag: String) -> Self {
        Self {
            inp,
            tag,
            printed: false,
            done: false,
        }
    }
}

impl<INP, ITY, E> Stream for PrintFirstTs<INP>
where
    INP: Stream<Item = Sitemty2<ITY, E>> + Unpin,
    ITY: MergeableTy,
{
    type Item = Sitemty2<ITY, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.done {
                Ready(None)
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(item)) => match item {
                        Ok(StreamItem::DataItem(RangeCompletableItem::Data(ref item2))) => {
                            if !self.printed {
                                if let Some(ts) = MergeableTy::tss_for_testing(item2).iter().next()
                                {
                                    TsNanoVecFmt(MergeableTy::tss_for_testing(item2).iter());
                                    self.printed = true;
                                    debug!("{}  {}", self.tag, ts);
                                }
                            }
                            Ready(Some(item))
                        }
                        Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)) => {
                            Ready(Some(item))
                        }
                        x => Ready(Some(x)),
                    },
                    Ready(None) => {
                        self.done = true;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            };
        }
    }
}

impl<INP> fmt::Debug for PrintFirstTs<INP>
where
    INP: Stream + Unpin,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("PrintFirstTs").finish()
    }
}
