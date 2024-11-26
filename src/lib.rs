pub mod boxed;
pub mod cbor_stream;
pub mod collect;
#[cfg(feature = "indev")]
pub mod collect_adapter;
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
pub mod needminbuffer;
pub mod plaineventscbor;
pub mod plaineventsjson;
pub mod plaineventsstream;
pub mod print_on_done;
pub mod rangefilter2;
pub mod slidebuf;
pub mod streamtimeout;
pub mod tcprawclient;
#[cfg(test)]
pub mod test;
pub mod teststream;
pub mod timebin;
pub mod timebinnedjson;
pub mod transform;

#[allow(unused)]
fn todoval<T>() -> T {
    todo!()
}

mod log {
    #[cfg(not(test))]
    pub use netpod::log::*;
    #[cfg(test)]
    pub use netpod::log_direct::*;
}
