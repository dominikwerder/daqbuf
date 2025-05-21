use crate::binwriteindex::read_all_coarse::ReadAllCoarse;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use daqbuf_series::msp::LspU32;
use daqbuf_series::msp::MspU32;
use futures_util::Stream;
use futures_util::StreamExt;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsNano;
use netpod::range::evrange::NanoRange;
use netpod::ttl::RetentionTime;
use serde::Serialize;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "Binned2FromBinned"),
    enum variants {
        A,
    },
);

type IndexRow = (RetentionTime, MspU32, LspU32, DtMs);

enum StateA {
    ReadAllCoarse(ReadAllCoarse, VecDeque<IndexRow>),
    Done,
}

pub struct FromBinned {
    // range: NanoRange,
    binrange: BinnedRange<TsNano>,
    state_a: StateA,
    outbuf: VecDeque<String>,
}

fn def<T: Default>() -> T {
    Default::default()
}

impl FromBinned {
    pub fn new(series: SeriesId, binrange: BinnedRange<TsNano>, scyqueue: &ScyllaQueue) -> Self {
        let state_a = StateA::ReadAllCoarse(
            ReadAllCoarse::new(series, binrange.to_nano_range(), scyqueue.clone()),
            def(),
        );
        Self {
            binrange,
            state_a,
            outbuf: def(),
        }
    }

    fn push_string<T: ToString>(&mut self, x: T) {
        self.outbuf.push_back(x.to_string());
    }

    fn push_json<T: Serialize>(&mut self, x: T) {
        let js = serde_json::to_string(&x).unwrap();
        self.outbuf.push_back(js);
    }

    fn handle_coarse_index(&mut self, rows: VecDeque<IndexRow>) {
        self.push_string(format!("handle_coarse_index"));
        for e in rows {
            self.push_string(format!("{:?}", e));
        }
    }
}

impl Stream for FromBinned {
    type Item = Result<String, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(x) = self.outbuf.pop_front() {
                Ready(Some(Ok(x)))
            } else {
                use StateA::*;
                let self2 = self.as_mut().get_mut();
                match &mut self2.state_a {
                    ReadAllCoarse(stb, rows) => match stb.poll_next_unpin(cx) {
                        Ready(Some(Ok(x))) => {
                            match x.into_data() {
                                Ok(x) => {
                                    rows.push_back(x);
                                }
                                Err(x) => {
                                    self2.push_string(format!("{:?}", x));
                                }
                            }
                            continue;
                        }
                        Ready(Some(Err(e))) => {
                            self2.push_string(e);
                            self2.state_a = StateA::Done;
                            continue;
                        }
                        Ready(None) => {
                            let a = std::mem::replace(rows, def());
                            self2.handle_coarse_index(a);
                            self2.state_a = StateA::Done;
                            self2.push_json(&"done with reading coarse");
                            continue;
                        }
                        Pending => Pending,
                    },
                    Done => Ready(None),
                }
            };
        }
    }
}
