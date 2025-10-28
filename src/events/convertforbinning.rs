use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem::*;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem::*;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::EnumVariant;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub struct ConvertForBinning<INP> {
    inp: INP,
}

impl<INP> ConvertForBinning<INP> {
    pub fn new(inp: INP) -> Self {
        Self { inp }
    }
}

impl<INP> Stream for ConvertForBinning<INP>
where
    INP: Stream<Item = Sitemty<ChannelEvents>> + Unpin,
{
    type Item = Sitemty<ChannelEvents>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match &item {
                Ok(DataItem(Data(cevs))) => match cevs {
                    ChannelEvents::Events(evs) => {
                        let evs = evs.to_f32_for_binning_v01();
                        let item = ChannelEvents::Events(evs);
                        let item = sitem_data(item);
                        Ready(Some(item))
                    }
                    // ChannelEvents::Events(evs) => {
                    //     if let Some(evs) = evs
                    //         .as_any_ref()
                    //         .downcast_ref::<ContainerEvents<EnumVariant>>()
                    //     {
                    //         let mut dst = ContainerEvents::new();
                    //         for (ts, val) in evs.iter_zip() {
                    //             dst.push_back(ts, val.ix);
                    //         }
                    //         let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                    //         Ready(Some(item))
                    //     } else if let Some(evs) =
                    //         evs.as_any_ref().downcast_ref::<ContainerEvents<bool>>()
                    //     {
                    //         let mut dst = ContainerEvents::new();
                    //         for (ts, val) in evs.iter_zip() {
                    //             dst.push_back(ts, val as u8);
                    //         }
                    //         let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                    //         Ready(Some(item))
                    //     } else if let Some(evs) =
                    //         evs.as_any_ref().downcast_ref::<ContainerEvents<String>>()
                    //     {
                    //         let mut dst = ContainerEvents::new();
                    //         for (ts, _) in evs.iter_zip() {
                    //             dst.push_back(ts, 1);
                    //         }
                    //         let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                    //         Ready(Some(item))
                    //     } else {
                    //         Ready(Some(item))
                    //     }
                    // }
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
        type Cont<T> = ContainerEvents<T>;
        match self.inp.poll_next_unpin(cx) {
            Ready(Some(item)) => match &item {
                Ok(DataItem(Data(cevs))) => match cevs {
                    ChannelEvents::Events(evs) => {
                        if let Some(evs) = evs.as_any_ref().downcast_ref::<Cont<f64>>() {
                            let buf = std::fs::read("evmod").unwrap_or(Vec::new());
                            let s = String::from_utf8_lossy(&buf);
                            if s.contains("u8") {
                                let mut dst = Cont::new();
                                for (ts, val) in evs.iter_zip() {
                                    let v = (val * 1e6) as u8;
                                    dst.push_back(ts, v);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("i16") {
                                let mut dst = Cont::new();
                                for (ts, val) in evs.iter_zip() {
                                    let v = (val * 1e6) as i16 - 50;
                                    dst.push_back(ts, v);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("bool") {
                                let mut dst = Cont::new();
                                for (ts, val) in evs.iter_zip() {
                                    let g = u64::from_ne_bytes(val.to_ne_bytes());
                                    let val = g % 2 == 0;
                                    dst.push_back(ts, val);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("enum") {
                                let mut dst = Cont::new();
                                for (ts, val) in evs.iter_zip() {
                                    let buf = val.to_ne_bytes();
                                    let h = buf[0]
                                        ^ buf[1]
                                        ^ buf[2]
                                        ^ buf[3]
                                        ^ buf[4]
                                        ^ buf[5]
                                        ^ buf[6]
                                        ^ buf[7];
                                    let val = EnumVariant::new(h as i16, h.to_string());
                                    dst.push_back(ts, val);
                                }
                                let item = Ok(DataItem(Data(ChannelEvents::Events(Box::new(dst)))));
                                Ready(Some(item))
                            } else if s.contains("string") {
                                let mut dst = Cont::new();
                                for (ts, val) in evs.iter_zip() {
                                    dst.push_back(ts, val.to_string());
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
