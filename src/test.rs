#[cfg(test)]
pub mod eventsdim0;
#[cfg(test)]
pub mod eventsdim1;

use crate::channelevents::ConnStatus;
use crate::channelevents::ConnStatusEvent;
use crate::eventsdim0::EventsDim0;
use crate::merger::Mergeable;
use crate::merger::Merger;
use crate::runfut;
use crate::streams::TransformerExt;
use crate::streams::VecStream;
use crate::testgen::make_some_boxed_d0_f32;
use crate::ChannelEvents;
use crate::Error;
use crate::Events;
use futures_util::stream;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Appendable;
use items_0::Empty;
use items_0::WithLen;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::*;
use netpod::BinnedRangeEnum;
use std::time::Duration;
use std::time::Instant;

#[cfg(test)]
pub fn runfut<T, F>(fut: F) -> Result<T, err::Error>
where
    F: std::future::Future<Output = Result<T, Error>>,
{
    use futures_util::TryFutureExt;
    let fut = fut.map_err(|e| e.into());
    taskrun::run(fut)
}

#[test]
fn items_move_events() {
    let evs = make_some_boxed_d0_f32(10, SEC, SEC, 0, 1846713782);
    let v0 = ChannelEvents::Events(evs);
    let mut v1 = v0.clone();
    eprintln!("{v1:?}");
    eprintln!("{}", v1.len());
    let mut v2 = v1.new_empty();
    match v1.find_lowest_index_gt(4) {
        Some(ilgt) => {
            v1.drain_into(&mut v2, (0, ilgt)).unwrap();
        }
        None => {
            v1.drain_into(&mut v2, (0, v1.len())).unwrap();
        }
    }
    eprintln!("{}", v1.len());
    eprintln!("{}", v2.len());
    match v1.find_lowest_index_gt(u64::MAX) {
        Some(ilgt) => {
            v1.drain_into(&mut v2, (0, ilgt)).unwrap();
        }
        None => {
            v1.drain_into(&mut v2, (0, v1.len())).unwrap();
        }
    }
    eprintln!("{}", v1.len());
    eprintln!("{}", v2.len());
    eprintln!("{v1:?}");
    eprintln!("{v2:?}");
    assert_eq!(v1.len(), 0);
    assert_eq!(v2.len(), 10);
    assert_eq!(v2, v0);
}

#[test]
fn items_merge_00() {
    let fut = async {
        use crate::merger::Merger;
        let evs0 = make_some_boxed_d0_f32(10, SEC * 1, SEC * 2, 0, 1846713782);
        let evs1 = make_some_boxed_d0_f32(10, SEC * 2, SEC * 2, 0, 828764893);
        let v0 = ChannelEvents::Events(evs0);
        let v1 = ChannelEvents::Events(evs1);
        let stream0 = Box::pin(stream::iter(vec![sitem_data(v0)]));
        let stream1 = Box::pin(stream::iter(vec![sitem_data(v1)]));
        let mut merger = Merger::new(vec![stream0, stream1], Some(8));
        while let Some(item) = merger.next().await {
            eprintln!("{item:?}");
        }
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn items_merge_01() {
    let fut = async {
        use crate::merger::Merger;
        let evs0 = make_some_boxed_d0_f32(10, SEC * 1, SEC * 2, 0, 1846713782);
        let evs1 = make_some_boxed_d0_f32(10, SEC * 2, SEC * 2, 0, 828764893);
        let v0 = ChannelEvents::Events(evs0);
        let v1 = ChannelEvents::Events(evs1);
        let v2 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 100, ConnStatus::Connect)));
        let v3 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 2300, ConnStatus::Disconnect)));
        let v4 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 2800, ConnStatus::Connect)));
        let stream0 = Box::pin(stream::iter(vec![sitem_data(v0)]));
        let stream1 = Box::pin(stream::iter(vec![sitem_data(v1)]));
        let stream2 = Box::pin(stream::iter(vec![sitem_data(v2), sitem_data(v3), sitem_data(v4)]));
        let mut merger = Merger::new(vec![stream0, stream1, stream2], Some(8));
        let mut total_event_count = 0;
        while let Some(item) = merger.next().await {
            eprintln!("{item:?}");
            let item = item?;
            match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {}
                    RangeCompletableItem::Data(item) => {
                        total_event_count += item.len();
                    }
                },
                StreamItem::Log(_) => {}
                StreamItem::Stats(_) => {}
            }
        }
        assert_eq!(total_event_count, 23);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn items_merge_02() {
    let fut = async {
        let evs0 = make_some_boxed_d0_f32(100, SEC * 1, SEC * 2, 0, 1846713782);
        let evs1 = make_some_boxed_d0_f32(100, SEC * 2, SEC * 2, 0, 828764893);
        let v0 = ChannelEvents::Events(evs0);
        let v1 = ChannelEvents::Events(evs1);
        let v2 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 100, ConnStatus::Connect)));
        let v3 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 2300, ConnStatus::Disconnect)));
        let v4 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 2800, ConnStatus::Connect)));
        let stream0 = Box::pin(stream::iter(vec![sitem_data(v0)]));
        let stream1 = Box::pin(stream::iter(vec![sitem_data(v1)]));
        let stream2 = Box::pin(stream::iter(vec![sitem_data(v2), sitem_data(v3), sitem_data(v4)]));
        let mut merger = Merger::new(vec![stream0, stream1, stream2], Some(8));
        let mut total_event_count = 0;
        while let Some(item) = merger.next().await {
            eprintln!("{item:?}");
            let item = item.unwrap();
            match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {}
                    RangeCompletableItem::Data(item) => {
                        total_event_count += item.len();
                    }
                },
                StreamItem::Log(_) => {}
                StreamItem::Stats(_) => {}
            }
        }
        assert_eq!(total_event_count, 203);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn merge_00() {
    let fut = async {
        let mut events_vec1: Vec<Sitemty<ChannelEvents>> = Vec::new();
        let mut events_vec2: Vec<Sitemty<ChannelEvents>> = Vec::new();
        {
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            let cev = ChannelEvents::Events(Box::new(events.clone()));
            events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
            let cev = ChannelEvents::Events(Box::new(events.clone()));
            events_vec2.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
        }
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Sitemty<ChannelEvents>> = Vec::new();
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = crate::merger::Merger::new(vec![inp1, inp2], Some(32));

        // Expect an empty first item.
        let item = merger.next().await;
        let item = match item {
            Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))) => item,
            _ => panic!(),
        };
        assert_eq!(item.len(), 0);

        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), None);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn merge_01() {
    let fut = async {
        let events_vec1 = {
            let mut vec = Vec::new();
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events.clone()));
            let mut events = EventsDim0::empty();
            for i in 10..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events.clone()));
            vec
        };
        let exp = events_vec1.clone();
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Sitemty<ChannelEvents>> = Vec::new();
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = crate::merger::Merger::new(vec![inp1, inp2], Some(10));

        // Expect an empty first item.
        let item = merger.next().await;
        let item = match item {
            Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))) => item,
            _ => panic!(),
        };
        assert_eq!(item.len(), 0);

        let item = merger.next().await;
        assert_eq!(item.as_ref(), exp.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), exp.get(1));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), None);
        Ok(())
    };
    runfut(fut).unwrap();
}

fn push_evd0(vec: &mut Vec<Sitemty<ChannelEvents>>, events: Box<dyn Events>) {
    let cev = ChannelEvents::Events(events);
    vec.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
}

#[test]
fn merge_02() {
    let fut = async {
        let events_vec1 = {
            let mut vec = Vec::new();
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            let mut events = EventsDim0::empty();
            for i in 10..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            vec
        };
        let events_vec2 = {
            let mut vec = Vec::new();
            let mut events = EventsDim0::empty();
            for i in 0..10 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            let mut events = EventsDim0::empty();
            for i in 10..12 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            let mut events = EventsDim0::empty();
            for i in 12..20 {
                events.push(i * 100, i, i as f32 * 100.);
            }
            push_evd0(&mut vec, Box::new(events));
            vec
        };

        let inp2_events_a = {
            let ev = ConnStatusEvent {
                ts: 1199,
                datetime: std::time::SystemTime::UNIX_EPOCH,
                status: ConnStatus::Disconnect,
            };
            let item: Sitemty<ChannelEvents> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                ChannelEvents::Status(Some(ev)),
            )));
            vec![item]
        };

        let inp2_events_b = {
            let ev = ConnStatusEvent {
                ts: 1199,
                datetime: std::time::SystemTime::UNIX_EPOCH,
                status: ConnStatus::Disconnect,
            };
            let item: Sitemty<ChannelEvents> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                ChannelEvents::Status(Some(ev)),
            )));
            vec![item]
        };

        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2: Vec<Sitemty<ChannelEvents>> = inp2_events_a;
        let inp2 = futures_util::stream::iter(inp2);
        let inp2 = Box::pin(inp2);
        let mut merger = crate::merger::Merger::new(vec![inp1, inp2], Some(10));

        // Expect an empty first item.
        let item = merger.next().await;
        let item = match item {
            Some(Ok(StreamItem::DataItem(RangeCompletableItem::Data(item)))) => item,
            _ => panic!(),
        };
        assert_eq!(item.len(), 0);

        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(1));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), inp2_events_b.get(0));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), events_vec2.get(2));
        let item = merger.next().await;
        assert_eq!(item.as_ref(), None);
        Ok(())
    };
    runfut(fut).unwrap();
}

#[test]
fn bin_01() {
    const TSBASE: u64 = SEC * 1600000000;
    fn val(ts: u64) -> f32 {
        2f32 + ((ts / SEC) % 2) as f32 + 0.2 * ((ts / (MS * 100)) % 2) as f32
    }
    let fut = async {
        let mut events_vec1 = Vec::new();
        let mut t = TSBASE;
        for _ in 0..20 {
            let mut events = EventsDim0::empty();
            for _ in 0..10 {
                events.push(t, t, val(t));
                t += MS * 100;
            }
            let cev = ChannelEvents::Events(Box::new(events));
            events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
        }
        events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)));
        let inp1 = events_vec1;
        let inp1 = futures_util::stream::iter(inp1);
        let inp1 = Box::pin(inp1);
        let inp2 = Box::pin(futures_util::stream::empty()) as _;
        let stream = crate::merger::Merger::new(vec![inp1, inp2], Some(32));
        // covering_range result is subject to adjustments, instead, manually choose bin edges
        let range = NanoRange {
            beg: TSBASE + SEC * 1,
            end: TSBASE + SEC * 10,
        };
        // let binrange = BinnedRangeEnum::covering_range(range.into(), 9).map_err(|e| format!("{e}"))?;
        // let stream = Box::pin(stream);
        // let deadline = Instant::now() + Duration::from_millis(4000);
        // let do_time_weight = true;
        // let emit_empty_bins = false;
        // let res = BinnedCollected::new(
        //     binrange,
        //     ScalarType::F32,
        //     Shape::Scalar,
        //     do_time_weight,
        //     emit_empty_bins,
        //     deadline,
        //     Box::pin(stream),
        // )
        // .await?;
        // eprintln!("res {:?}", res);
        Ok::<_, Error>(())
    };
    runfut(fut).unwrap();
}

#[test]
fn binned_timeout_00() {
    if true {
        return;
    }
    // TODO items_2::binnedcollected::BinnedCollected is currently not used.
    trace!("binned_timeout_01 uses a delay");
    const TSBASE: u64 = SEC * 1600000000;
    fn val(ts: u64) -> f32 {
        2f32 + ((ts / SEC) % 2) as f32 + 0.2 * ((ts / (MS * 100)) % 2) as f32
    }
    eprintln!("binned_timeout_01  ENTER");
    let fut = async {
        eprintln!("binned_timeout_01  IN FUT");
        let mut events_vec1: Vec<Sitemty<ChannelEvents>> = Vec::new();
        let mut t = TSBASE;
        for _ in 0..20 {
            let mut events = EventsDim0::empty();
            for _ in 0..10 {
                events.push(t, t, val(t));
                t += MS * 100;
            }
            let cev = ChannelEvents::Events(Box::new(events));
            events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::Data(cev))));
        }
        events_vec1.push(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)));
        let inp1 = VecStream::new(events_vec1.into_iter().collect());
        let inp1 = inp1.enumerate2().then2(|(i, k)| async move {
            if i == 5 {
                let _ = tokio::time::sleep(Duration::from_millis(10000)).await;
            }
            k
        });
        let edges: Vec<_> = (0..10).into_iter().map(|x| TSBASE + SEC * (1 + x)).collect();
        let range = NanoRange {
            beg: TSBASE + SEC * 1,
            end: TSBASE + SEC * 10,
        };
        let binrange = BinnedRangeEnum::covering_range(range.into(), 9)?;
        eprintln!("edges1: {:?}", edges);
        //eprintln!("edges2: {:?}", binrange.edges());
        let timeout = Duration::from_millis(400);
        // let inp1 = Box::pin(inp1);
        // let deadline = Instant::now() + timeout;
        // let do_time_weight = true;
        // let emit_empty_bins = false;
        // TODO with new binning

        // let res = BinnedCollected::new(
        //     binrange,
        //     ScalarType::F32,
        //     Shape::Scalar,
        //     do_time_weight,
        //     emit_empty_bins,
        //     deadline,
        //     inp1,
        // )
        // .await?;
        // let r2: &BinsDim0CollectedResult<f32> = res.result.as_any_ref().downcast_ref().expect("res seems wrong type");
        // eprintln!("rs: {r2:?}");
        // assert_eq!(SEC * r2.ts_anchor_sec(), TSBASE + SEC);
        // assert_eq!(r2.counts(), &[10, 10, 10]);
        // assert_eq!(r2.mins(), &[3.0, 2.0, 3.0]);
        // assert_eq!(r2.maxs(), &[3.2, 2.2, 3.2]);
        // assert_eq!(r2.missing_bins(), 6);
        // assert_eq!(r2.continue_at(), Some(IsoDateTime::from_ns_u64(TSBASE + SEC * 4)));
        Ok::<_, Error>(())
    };
    runfut(fut).unwrap();
}
