use super::BinWriteIndexRtStream;
use crate::worker::ScyllaOptsSubmit;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::Stream;
use futures_util::StreamExt;
use netpod::log;
use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use query::api4::scyllaopts::ScyllaOptsQuery;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "BinWriteIndexStream"),
    enum variants {
        A,
    },
);

type Fut1Res = <BinWriteIndexRtStream as Stream>::Item;

#[derive(Debug)]
enum InpSt {
    Polling(BinWriteIndexRtStream),
    Ready(BinWriteIndexRtStream, Fut1Res),
    Done,
}

#[derive(Debug)]
pub struct BinWriteIndexStream {
    rtss: VecDeque<InpSt>,
    #[allow(unused)]
    scylla_opts: ScyllaOptsQuery,
}

impl BinWriteIndexStream {
    pub fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    pub fn new(series: SeriesId, range: NanoRange, scylla_opts: ScyllaOptsQuery, scyqueue: ScyllaQueue) -> Self {
        debug!("{}::new", Self::type_name());
        let mut rtss = VecDeque::new();
        let rts = [RetentionTime::Short, RetentionTime::Medium, RetentionTime::Long];
        for rt in rts {
            let s = BinWriteIndexRtStream::new(
                rt.clone(),
                series.clone(),
                PrebinnedPartitioning::Day1,
                range.clone(),
                ScyllaOptsSubmit::no_choice(),
                scyqueue.clone(),
            );
            rtss.push_back(InpSt::Polling(s));
        }
        BinWriteIndexStream { rtss, scylla_opts }
    }

    fn abort(&mut self) {
        for inp in self.rtss.iter_mut() {
            *inp = InpSt::Done;
        }
    }
}

impl Stream for BinWriteIndexStream {
    type Item = Result<(), Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // TODO poll the streams in `self.rtss`. When any of them returns an item, store it
        // in the variant InpSt::Ready(..) together with the remaining stream.
        // When all input streams have an item ready, store all items in a new variable
        // in the type Self and transition all inputs to Polling again.
        // If any of the input streams returns None, we return None as well.
        use Poll::*;
        loop {
            let mut ready_cnt: u16 = 0;
            let mut do_abort = false;
            let mut have_pending = false;
            for inp in self.rtss.iter_mut() {
                match inp {
                    InpSt::Polling(fut) => match fut.poll_next_unpin(cx) {
                        Ready(Some(item)) => {
                            let fut = netpod::todoval();
                            *inp = InpSt::Ready(fut, item);
                            ready_cnt += 1;
                            if true {
                                todo!("bwxcmb keep stream in state");
                            }
                        }
                        Ready(None) => {
                            do_abort = true;
                        }
                        Pending => {
                            have_pending = true;
                        }
                    },
                    InpSt::Ready(a, b) => {
                        let _ = (a, b);
                        ready_cnt += 1;
                    }
                    InpSt::Done => {}
                }
            }
            if do_abort {
                self.abort();
            }
            break if ready_cnt == self.rtss.len() as u16 {
                todo!("TODO bwxcmb")
            } else if have_pending {
                Pending
            } else {
                self.abort();
                Ready(None)
            };
        }
    }
}
