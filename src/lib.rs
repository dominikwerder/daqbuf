use futures_util::Stream;
use items_0::streamitem::Sitemty;
use items_2::channelevents::ChannelEvents;
use std::pin::Pin;

pub mod cbor_stream;
pub mod collect;
#[cfg(feature = "indev")]
pub mod collect_adapter;
pub mod dedup;
pub mod dtflags;
pub mod events;
pub mod eventsplainreader;
pub mod filechunkread;
pub mod firsterr;
pub mod framed_bytes;
pub mod frames;
pub mod generators;
pub mod instrument;
pub mod itemclone;
pub mod json_stream;
pub mod lenframe;
pub mod lenframed;
pub mod logfilter;
pub mod logqueue;
pub mod monotonic;
pub mod needminbuffer;
pub mod print_on_done;
pub mod rangefilter2;
#[cfg(test)]
pub mod rt;
pub mod slidebuf;
pub mod streamtimeout;
pub mod tcprawclient;
#[cfg(test)]
pub mod test;
pub mod teststream;
pub mod timebin;
pub mod timebinnedjson;
pub mod tojsonf32;
pub mod wasmtransform;
pub mod withlenhisto;

pub type ChannelEventsStream = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;

#[allow(unused)]
fn todoval<T>() -> T {
    todo!()
}

mod log {
    pub use netpod::log_macros_branch::*;
}
