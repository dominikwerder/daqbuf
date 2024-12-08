use crate as streams;
use crate::log::*;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::Sitemty;
use items_0::timebin::BinsBoxed;
use items_2::channelevents::ChannelEvents;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::TsNano;
use query::api4::events::EventsSubQuery;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "BinCachedReader"),
    enum variants {
        TodoImpl,
        ChannelSend,
        ChannelRecv,
        Scylla(String),
    },
);

macro_rules! trace_emit { ($($arg:tt)*) => ( if true { trace!($($arg)*); } ) }

pub type BinsReadResErr = streams::timebin::cached::reader::Error;
pub type BinsReadRes = Result<Option<BinsBoxed>, BinsReadResErr>;
pub type BinsReadFutBoxed = Pin<Box<dyn Future<Output = BinsReadRes> + Send>>;

pub fn off_max() -> u64 {
    1000
}

pub fn part_len(bin_len: DtMs) -> DtMs {
    DtMs::from_ms_u64(bin_len.ms() * off_max())
}

pub struct EventsReading {
    stream: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
}

impl EventsReading {
    pub fn new(stream: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>) -> Self {
        Self { stream }
    }
}

impl Stream for EventsReading {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.stream.poll_next_unpin(cx) {
            Ready(Some(item)) => {
                use items_0::streamitem::RangeCompletableItem::*;
                use items_0::streamitem::StreamItem::*;
                match &item {
                    Ok(DataItem(Data(cevs))) => match cevs {
                        ChannelEvents::Events(_) => Ready(Some(item)),
                        ChannelEvents::Status(_) => Ready(Some(item)),
                    },
                    _ => Ready(Some(item)),
                }
            }
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub trait EventsReadProvider: Send + Sync {
    fn read(&self, evq: EventsSubQuery) -> EventsReading;
}

pub struct CacheReading {
    fut: BinsReadFutBoxed,
}

impl CacheReading {
    pub fn new(fut: BinsReadFutBoxed) -> Self {
        Self { fut }
    }
}

impl Future for CacheReading {
    type Output = BinsReadRes;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

pub struct CacheWriting {
    fut: Pin<Box<dyn Future<Output = Result<(), streams::timebin::cached::reader::Error>> + Send>>,
}

impl CacheWriting {
    pub fn new(
        fut: Pin<
            Box<dyn Future<Output = Result<(), streams::timebin::cached::reader::Error>> + Send>,
        >,
    ) -> Self {
        Self { fut }
    }
}

impl Future for CacheWriting {
    type Output = Result<(), streams::timebin::cached::reader::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.fut.poll_unpin(cx)
    }
}

pub trait CacheReadProvider: Send + Sync {
    fn read(&self, series: u64, bin_len: DtMs, msp: u64, offs: Range<u32>) -> CacheReading;
    fn write(&self, series: u64, bins: BinsBoxed) -> CacheWriting;
}

pub struct CachedReader {
    series: u64,
    range: BinnedRange<TsNano>,
    ts1next: TsNano,
    bin_len: DtMs,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    reading: Option<Pin<Box<dyn Future<Output = Result<Option<BinsBoxed>, Error>> + Send>>>,
}

impl CachedReader {
    pub fn new(
        series: u64,
        range: BinnedRange<TsNano>,
        cache_read_provider: Arc<dyn CacheReadProvider>,
    ) -> Result<Self, Error> {
        let ret = Self {
            series,
            ts1next: range.nano_beg(),
            bin_len: range.bin_len.to_dt_ms(),
            range,
            cache_read_provider,
            reading: None,
        };
        Ok(ret)
    }
}

impl Stream for CachedReader {
    type Item = Result<BinsBoxed, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if let Some(fut) = self.reading.as_mut() {
                match fut.poll_unpin(cx) {
                    Ready(x) => {
                        self.reading = None;
                        match x {
                            Ok(Some(bins)) => {
                                trace_emit!(
                                    "- - - - - - - - - - - -  emit cached bins  {}  bin_len {}",
                                    bins.len(),
                                    self.bin_len
                                );
                                Ready(Some(Ok(bins)))
                            }
                            Ok(None) => {
                                continue;
                            }
                            Err(e) => Ready(Some(Err(e))),
                        }
                    }
                    Pending => Pending,
                }
            } else {
                if self.ts1next < self.range.nano_end() {
                    let div = part_len(self.bin_len).ns();
                    let msp = self.ts1next.ns() / div;
                    let off = (self.ts1next.ns() - div * msp) / self.bin_len.ns();
                    let off2 = (self.range.nano_end().ns() - div * msp) / self.bin_len.ns();
                    let off2 = off2.min(off_max());
                    self.ts1next = TsNano::from_ns(self.bin_len.ns() * off2 + div * msp);
                    let offs = off as u32..off2 as u32;
                    let fut = self
                        .cache_read_provider
                        .read(self.series, self.bin_len, msp, offs);
                    self.reading = Some(Box::pin(fut));
                    continue;
                } else {
                    Ready(None)
                }
            };
        }
    }
}
