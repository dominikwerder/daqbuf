use futures_util::Stream;
use futures_util::StreamExt;
use items_0::merge::MergeableTy;
use items_0::streamitem::sitem2_log;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use netpod::range::evrange::NanoRange;
use std::fmt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

pub struct AssertMaxOneBefore<INP>
where
    INP: Stream + Unpin,
{
    inp: INP,
    range: NanoRange,
    tag: String,
    before_cnt: u32,
    done: bool,
}

impl<INP> AssertMaxOneBefore<INP>
where
    INP: Stream + Unpin,
{
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(inp: INP, range: NanoRange, tag: String) -> Self {
        Self {
            inp,
            range,
            tag,
            before_cnt: 0,
            done: false,
        }
    }
}

impl<INP, ITY, E> Stream for AssertMaxOneBefore<INP>
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
                            if let Some(ilge) =
                                MergeableTy::find_lowest_index_ge(item2, self.range.beg_ts())
                            {
                                self.before_cnt += ilge as u32;
                                if self.before_cnt > 1 {
                                    self.done = true;
                                    debug!("before_cnt {}  tag {}", self.before_cnt, self.tag);
                                    let msg =
                                        format!("before_cnt {}  tag {}", self.before_cnt, self.tag);
                                    let x = LogItem::info(msg);
                                    let item = sitem2_log(x);
                                    return Ready(Some(item));
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

impl<INP> fmt::Debug for AssertMaxOneBefore<INP>
where
    INP: Stream + Unpin,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("AssertMaxOneBefore").finish()
    }
}
