use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem::*;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem::*;
use items_0::Empty;
use items_2::channelevents::ChannelEvents;
use items_2::eventsdim0::EventsDim0;
use netpod::EnumVariant;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct ConvertForBinning {
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
}

impl ConvertForBinning {
    pub fn new(inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>) -> Self {
        Self { inp }
    }
}

impl Stream for ConvertForBinning {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match &item {
                Ok(DataItem(Data(cevs))) => match cevs {
                    ChannelEvents::Events(evs) => {
                        if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<EnumVariant>>() {
                            let mut dst = EventsDim0::<u16>::empty();
                            for ((&ts, &pulse), val) in evs
                                .tss()
                                .iter()
                                .zip(evs.pulses.iter())
                                .zip(evs.private_values_ref().iter())
                            {
                                dst.push_back(ts, pulse, val.ix());
                            }
                            let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                            Ready(Some(item))
                        } else if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<bool>>() {
                            let mut dst = EventsDim0::<u8>::empty();
                            for ((&ts, &pulse), &val) in evs
                                .tss()
                                .iter()
                                .zip(evs.pulses.iter())
                                .zip(evs.private_values_ref().iter())
                            {
                                dst.push_back(ts, pulse, val as u8);
                            }
                            let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                            Ready(Some(item))
                        } else if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<String>>() {
                            let mut dst = EventsDim0::<u64>::empty();
                            for ((&ts, &pulse), _) in evs
                                .tss()
                                .iter()
                                .zip(evs.pulses.iter())
                                .zip(evs.private_values_ref().iter())
                            {
                                dst.push_back(ts, pulse, 1);
                            }
                            let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                            Ready(Some(item))
                        } else {
                            Ready(Some(item))
                        }
                    }
                    ChannelEvents::Status(_) => Ready(Some(item)),
                },
                _ => Ready(Some(item)),
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}

pub struct ConvertForTesting {
    inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>,
}

impl ConvertForTesting {
    pub fn new(inp: Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>) -> Self {
        Self { inp }
    }
}

impl Stream for ConvertForTesting {
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match &item {
                Ok(DataItem(Data(cevs))) => match cevs {
                    ChannelEvents::Events(evs) => {
                        if let Some(evs) = evs.as_any_ref().downcast_ref::<EventsDim0<f64>>() {
                            let buf = std::fs::read("evmod").unwrap_or(Vec::new());
                            let s = String::from_utf8_lossy(&buf);
                            if s.contains("u8") {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<u8>::empty();
                                for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                    let v = (val * 1e6) as u8;
                                    dst.push_back(*ts, 0, v);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("i16") {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<i16>::empty();
                                for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                    let v = (val * 1e6) as i16 - 50;
                                    dst.push_back(*ts, 0, v);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("bool") {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<bool>::empty();
                                for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                    let g = u64::from_ne_bytes(val.to_ne_bytes());
                                    let val = g % 2 == 0;
                                    dst.push_back(*ts, 0, val);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("enum") {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<EnumVariant>::empty();
                                for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                    let buf = val.to_ne_bytes();
                                    let h = buf[0] ^ buf[1] ^ buf[2] ^ buf[3] ^ buf[4] ^ buf[5] ^ buf[6] ^ buf[7];
                                    dst.push_back(*ts, 0, EnumVariant::new(h as u16, h.to_string()));
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("string") {
                                use items_0::Empty;
                                let mut dst = EventsDim0::<String>::empty();
                                for (ts, val) in evs.tss().iter().zip(evs.private_values_ref().iter()) {
                                    dst.push_back(*ts, 0, val.to_string());
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else {
                                Ready(Some(item))
                            }
                        } else {
                            Ready(Some(item))
                        }
                    }
                    ChannelEvents::Status(_) => Ready(Some(item)),
                },
                _ => Ready(Some(item)),
            },
            Ready(None) => Ready(None),
            Pending => Pending,
        }
    }
}
