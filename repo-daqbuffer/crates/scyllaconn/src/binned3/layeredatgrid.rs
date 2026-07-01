use crate::binned3::index_entry::IndexEntry;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use futures_util::Stream;
use items_0::streamitem::Sitemty3;
use netpod::BinnedRange;
use netpod::TsNano;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "BinReadLayeredAtGrid"),
    enum variants {
        IndexEntriesOrderBad,
    },
);

pub struct BinReadLayeredAtGrid {}

impl BinReadLayeredAtGrid {
    pub fn new(
        series: SeriesId,
        binrange: BinnedRange<TsNano>,
        index_entries: VecDeque<IndexEntry>,
        scyqueue: &ScyllaQueue,
    ) -> Result<Self, Error> {
        let _ = series;
        let _ = binrange;
        let _ = scyqueue;
        if !crate::binned3::index_entry::check_good_order(&index_entries) {
            return Err(Error::IndexEntriesOrderBad);
        }
        todo!()
    }
}

impl Stream for BinReadLayeredAtGrid {
    type Item = Sitemty3<(), Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
