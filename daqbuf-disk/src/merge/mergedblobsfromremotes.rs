use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryFutureExt;
use futures_util::pin_mut;
use httpclient::postimpl::HttpSimplePostImpl1;
use items_0::streamitem::Sitemty;
use items_0::streamitem::sitem_err2_from_string;
use items_2::eventfull::EventFull;
use items_2::merger::Merger;
use netpod::Cluster;
use netpod::ReqCtx;
use netpod::log::*;
use query::api4::events::EventsSubQuery;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use streamio::tcprawclient::x_processed_event_blobs_stream_from_node;

type T001<T> = Pin<Box<dyn Stream<Item = Sitemty<T>> + Send>>;
type T002<T> = Pin<Box<dyn Future<Output = Result<T001<T>, items_0::streamitem::SitemErrTy>> + Send>>;

pub struct MergedBlobsFromRemotes {
    tcp_establish_futs: Vec<T002<EventFull>>,
    nodein: Vec<Option<T001<EventFull>>>,
    merged: Option<T001<EventFull>>,
    completed: bool,
    errored: bool,
}

impl MergedBlobsFromRemotes {
    pub fn new(subq: EventsSubQuery, ctx: &ReqCtx, cluster: Cluster) -> Self {
        debug!("MergedBlobsFromRemotes::new  subq {:?}", subq);
        let mut tcp_establish_futs = Vec::new();
        for node in &cluster.nodes {
            let post = HttpSimplePostImpl1::new();
            let post = Box::new(post);
            let f = x_processed_event_blobs_stream_from_node(subq.clone(), node.clone(), post, ctx.clone());
            let f = f.map_err(sitem_err2_from_string);
            let f: T002<EventFull> = Box::pin(f);
            tcp_establish_futs.push(f);
        }
        let n = tcp_establish_futs.len();
        Self {
            tcp_establish_futs,
            nodein: (0..n).into_iter().map(|_| None).collect(),
            merged: None,
            completed: false,
            errored: false,
        }
    }
}

impl Stream for MergedBlobsFromRemotes {
    type Item = Sitemty<EventFull>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        'outer: loop {
            break if self.completed {
                panic!("poll_next on completed");
            } else if self.errored {
                self.completed = true;
                return Ready(None);
            } else if let Some(fut) = &mut self.merged {
                match fut.poll_next_unpin(cx) {
                    Ready(Some(Ok(k))) => Ready(Some(Ok(k))),
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        self.completed = true;
                        Ready(None)
                    }
                    Pending => Pending,
                }
            } else {
                let mut pend = false;
                let mut c1 = 0;
                for i1 in 0..self.tcp_establish_futs.len() {
                    if self.nodein[i1].is_none() {
                        let f = &mut self.tcp_establish_futs[i1];
                        pin_mut!(f);
                        match f.poll(cx) {
                            Ready(Ok(k)) => {
                                self.nodein[i1] = Some(k);
                            }
                            Ready(Err(e)) => {
                                self.errored = true;
                                return Ready(Some(Err(e)));
                            }
                            Pending => {
                                pend = true;
                            }
                        }
                    } else {
                        c1 += 1;
                    }
                }
                if pend {
                    Pending
                } else {
                    if c1 == self.tcp_establish_futs.len() {
                        let inps = self.nodein.iter_mut().map(|k| k.take().unwrap()).collect();
                        // TODO set out_max_len dynamically
                        let s1 = Merger::new(inps, Some(128));
                        self.merged = Some(Box::pin(s1));
                    }
                    continue 'outer;
                }
            };
        }
    }
}
