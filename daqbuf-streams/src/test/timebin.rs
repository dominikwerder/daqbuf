// #[cfg(test)]
mod fromlayers;

// use crate::collect::collect;
// use crate::generators::GenerateI32V00;
// use crate::generators::GenerateI32V01;
// use crate::itemclone::Itemclone;
// use crate::test::runfut;
// use crate::timebin::TimeBinnedStream;
// use crate::transform::build_event_transform;
// use err::Error;
// use futures_util::stream;
// use futures_util::StreamExt;
// use items_0::on_sitemty_data;
// use items_0::streamitem::sitem_data;
// use items_0::streamitem::RangeCompletableItem;
// use items_0::streamitem::StreamItem;
// use items_0::AppendAllFrom;
// use items_0::Empty;
// use items_2::binsdim0::BinsDim0;
// use items_2::channelevents::ChannelEvents;
// use items_2::channelevents::ConnStatus;
// use items_2::channelevents::ConnStatusEvent;
// use items_2::eventsdim0::EventsDim0;
// use items_2::testgen::make_some_boxed_d0_f32;
// use netpod::range::evrange::NanoRange;
// use netpod::range::evrange::SeriesRange;
// use netpod::timeunits::MS;
// use netpod::timeunits::SEC;
// use netpod::BinnedRangeEnum;
// use query::transform::TransformQuery;
// use serde_json::Value as JsValue;
// use std::collections::VecDeque;
// use std::time::Duration;
// use std::time::Instant;

// fn nano_range_from_str(beg_date: &str, end_date: &str) -> Result<NanoRange, Error> {
//     let beg_date = beg_date.parse()?;
//     let end_date = end_date.parse()?;
//     let range = NanoRange::from_date_time(beg_date, end_date);
//     Ok(range)
// }

// #[test]
// fn time_bin_00() -> Result<(), Error> {
//     let fut = async {
//         let range = nano_range_from_str("1970-01-01T00:00:00Z", "1970-01-01T00:00:08Z")?;
//         let range = SeriesRange::TimeRange(range);
//         let min_bin_count = 8;
//         let binned_range = BinnedRangeEnum::covering_range(range, min_bin_count)?;
//         let evs0 = make_some_boxed_d0_f32(10, SEC * 1, MS * 500, 0, 1846713782);
//         let v00 = ChannelEvents::Events(Box::new(EventsDim0::<f32>::empty()));
//         let v01 = ChannelEvents::Events(evs0);
//         let v02 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 100, ConnStatus::Connect)));
//         let v03 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 6000, ConnStatus::Disconnect)));
//         let stream0 = Box::pin(stream::iter(vec![
//             //
//             sitem_data(v00),
//             sitem_data(v02),
//             sitem_data(v01),
//             sitem_data(v03),
//         ]));
//         let mut exps = {
//             let mut d = VecDeque::new();
//             let bins = BinsDim0::empty();
//             d.push_back(bins);
//             let mut bins = BinsDim0::empty();
//             // Currently can not cosntruct bins without minmaxlst
//             // bins.push(SEC * 0, SEC * 1, 0, 0.0, 0.0, 0.0);
//             bins.push(SEC * 1, SEC * 2, 2, 0.0535830, 100.0589, 50.05624, 100.0589);
//             bins.push(SEC * 2, SEC * 3, 2, 200.06143, 300.07645, 250.06894, 300.07645);
//             bins.push(SEC * 3, SEC * 4, 2, 400.08554, 500.05222, 450.06888, 500.05222);
//             bins.push(SEC * 4, SEC * 5, 2, 600.0025, 700.09094, 650.04675, 700.09094);
//             d.push_back(bins);
//             let mut bins = BinsDim0::empty();
//             bins.push(SEC * 5, SEC * 6, 2, 800.0619, 900.02844, 850.04517, 900.02844);
//             d.push_back(bins);
//             d
//         };
//         let mut binned_stream = TimeBinnedStream::new(stream0, binned_range, true);
//         while let Some(item) = binned_stream.next().await {
//             eprintln!("{item:?}");
//             match item {
//                 Ok(item) => match item {
//                     StreamItem::DataItem(item) => match item {
//                         RangeCompletableItem::Data(item) => {
//                             if let Some(item) = item.as_any_ref().downcast_ref::<BinsDim0<f32>>() {
//                                 let exp = exps.pop_front().unwrap();
//                                 if !item.equal_slack(&exp) {
//                                     eprintln!("-----------------------");
//                                     eprintln!("item {:?}", item);
//                                     eprintln!("-----------------------");
//                                     eprintln!("exp  {:?}", exp);
//                                     eprintln!("-----------------------");
//                                     return Err(Error::with_msg_no_trace(format!("bad, content not equal")));
//                                 }
//                             } else {
//                                 return Err(Error::with_msg_no_trace(format!("bad, got item with unexpected type")));
//                             }
//                         }
//                         RangeCompletableItem::RangeComplete => {}
//                     },
//                     StreamItem::Log(_) => {}
//                     StreamItem::Stats(_) => {}
//                 },
//                 Err(e) => Err(e).unwrap(),
//             }
//         }
//         Ok(())
//     };
//     runfut(fut)
// }

// #[test]
// fn time_bin_01() -> Result<(), Error> {
//     let fut = async {
//         let range = nano_range_from_str("1970-01-01T00:00:00Z", "1970-01-01T00:00:08Z")?;
//         let range = SeriesRange::TimeRange(range);
//         let min_bin_count = 8;
//         let binned_range = BinnedRangeEnum::covering_range(range, min_bin_count)?;
//         let v00 = ChannelEvents::Events(Box::new(EventsDim0::<f32>::empty()));
//         let evs0 = make_some_boxed_d0_f32(10, SEC * 1, MS * 500, 0, 1846713782);
//         let evs1 = make_some_boxed_d0_f32(10, SEC * 6, MS * 500, 0, 1846713781);
//         let v01 = ChannelEvents::Events(evs0);
//         let v02 = ChannelEvents::Events(evs1);
//         let stream0 = stream::iter(vec![
//             //
//             sitem_data(v00),
//             sitem_data(v01),
//             sitem_data(v02),
//         ]);
//         let stream0 = stream0.then({
//             let mut i = 0;
//             move |x| {
//                 let delay = if i == 1 { 2000 } else { 0 };
//                 i += 1;
//                 let dur = Duration::from_millis(delay);
//                 async move {
//                     tokio::time::sleep(dur).await;
//                     x
//                 }
//             }
//         });
//         let stream0 = Box::pin(stream0);
//         let mut binned_stream = TimeBinnedStream::new(stream0, binned_range, true);
//         while let Some(item) = binned_stream.next().await {
//             if true {
//                 eprintln!("{item:?}");
//             }
//             match item {
//                 Ok(item) => match item {
//                     StreamItem::DataItem(item) => match item {
//                         RangeCompletableItem::Data(item) => {
//                             if let Some(_) = item.as_any_ref().downcast_ref::<BinsDim0<f32>>() {
//                             } else {
//                                 return Err(Error::with_msg_no_trace(format!("bad, got item with unexpected type")));
//                             }
//                         }
//                         RangeCompletableItem::RangeComplete => {}
//                     },
//                     StreamItem::Log(_) => {}
//                     StreamItem::Stats(_) => {}
//                 },
//                 Err(e) => Err(e).unwrap(),
//             }
//         }
//         // TODO assert that we get the bins which are sure to be ready.
//         // TODO assert correct numbers.
//         // TODO assert that we don't get bins which may be still changing.
//         // TODO add similar test case with a RangeComplete event at different places before the timeout.
//         Ok(())
//     };
//     runfut(fut)
// }

// #[test]
// fn time_bin_02() -> Result<(), Error> {
//     let fut = async {
//         let do_time_weight = true;
//         let deadline = Instant::now() + Duration::from_millis(4000);
//         let range = nano_range_from_str("1970-01-01T00:20:04Z", "1970-01-01T00:22:10Z")?;
//         let range = SeriesRange::TimeRange(range);
//         // TODO add test: 26 bins should result in next higher resolution.
//         let min_bin_count = 25;
//         let expected_bin_count = 26;
//         let binned_range = BinnedRangeEnum::covering_range(range.clone(), min_bin_count)?;
//         eprintln!("binned_range: {:?}", binned_range);
//         for i in 0.. {
//             if let Some(r) = binned_range.range_at(i) {
//                 eprintln!("Series Range to cover: {r:?}");
//             } else {
//                 break;
//             }
//         }
//         let event_range = binned_range.binned_range_time().full_range();
//         let series_range = SeriesRange::TimeRange(event_range);
//         // TODO the test stream must be able to generate also one-before (on demand) and RangeComplete (by default).
//         let stream = GenerateI32V00::new(0, 1, series_range, true);
//         // TODO apply first some box dyn EventTransform which later is provided by TransformQuery.
//         // Then the Merge will happen always by default for backends where this is needed.
//         // TODO then apply the transform chain for the after-merged-stream.
//         let stream = stream.map(|x| {
//             let x = on_sitemty_data!(x, |x| Ok(StreamItem::DataItem(RangeCompletableItem::Data(
//                 Box::new(x) as Box<dyn TimeBinnable>
//             ))));
//             x
//         });
//         let stream = Box::pin(stream);
//         let mut binned_stream = TimeBinnedStream::new(stream, binned_range.clone(), do_time_weight);
//         // From there on it should no longer be neccessary to distinguish whether its still events or time bins.
//         // Then, optionally collect for output type like json, or stream as batches.
//         // TODO the timebinner should already provide batches to make this efficient.
//         if false {
//             while let Some(e) = binned_stream.next().await {
//                 eprintln!("see item {e:?}");
//                 let _x = on_sitemty_data!(e, |e| {
//                     //
//                     Ok(StreamItem::DataItem(RangeCompletableItem::Data(e)))
//                 });
//             }
//         } else {
//             let res = collect(binned_stream, deadline, 200, None, Some(binned_range)).await?;
//             assert_eq!(res.len(), expected_bin_count);
//             // use crate::json_stream::JsonBytes;
//             let v = res.to_json_value()?;
//             let d = serde_json::to_vec(&v)?;
//             let s = String::from_utf8_lossy(&d);
//             eprintln!("{s}");
//             let jsval: JsValue = serde_json::from_slice(&d)?;
//             {
//                 let ts_anchor = jsval.get("tsAnchor").unwrap().as_u64().unwrap();
//                 assert_eq!(ts_anchor, 1200);
//             }
//             {
//                 let counts = jsval.get("counts").unwrap().as_array().unwrap();
//                 assert_eq!(counts.len(), expected_bin_count);
//                 for v in counts {
//                     assert_eq!(v.as_u64().unwrap(), 5);
//                 }
//             }
//             {
//                 let ts1ms = jsval.get("ts1Ms").unwrap().as_array().unwrap();
//                 let mins = jsval.get("mins").unwrap().as_array().unwrap();
//                 assert_eq!(mins.len(), expected_bin_count);
//                 for (ts1ms, min) in ts1ms.iter().zip(mins) {
//                     assert_eq!((ts1ms.as_u64().unwrap() / 100) % 1000, min.as_u64().unwrap());
//                 }
//             }
//             {
//                 let ts1ms = jsval.get("ts1Ms").unwrap().as_array().unwrap();
//                 let maxs = jsval.get("maxs").unwrap().as_array().unwrap();
//                 assert_eq!(maxs.len(), expected_bin_count);
//                 for (ts1ms, max) in ts1ms.iter().zip(maxs) {
//                     assert_eq!((40 + ts1ms.as_u64().unwrap() / 100) % 1000, max.as_u64().unwrap());
//                 }
//             }
//             {
//                 let range_final = jsval.get("rangeFinal").unwrap().as_bool().unwrap();
//                 assert_eq!(range_final, true);
//             }
//         }
//         Ok(())
//     };
//     runfut(fut)
// }

// Should fail because of missing empty item.
// But should have some option to suppress the error log for this test case.
// #[test]
// fn time_bin_03() -> Result<(), Error> {
//     // TODO re-enable with error log suppressed.
//     if true {
//         return Ok(());
//     }
//     let fut = async {
//         let range = nano_range_from_str("1970-01-01T00:00:00Z", "1970-01-01T00:00:08Z")?;
//         let range = SeriesRange::TimeRange(range);
//         let min_bin_count = 8;
//         let binned_range = BinnedRangeEnum::covering_range(range, min_bin_count)?;
//         let evs0 = make_some_boxed_d0_f32(10, SEC * 1, MS * 500, 0, 1846713782);
//         //let v00 = ChannelEvents::Events(Box::new(EventsDim0::<f32>::empty()));
//         let v01 = ChannelEvents::Events(evs0);
//         let v02 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 100, ConnStatus::Connect)));
//         let v03 = ChannelEvents::Status(Some(ConnStatusEvent::new(MS * 6000, ConnStatus::Disconnect)));
//         let stream0 = Box::pin(stream::iter(vec![
//             //
//             //sitem_data(v00),
//             sitem_data(v02),
//             sitem_data(v01),
//             sitem_data(v03),
//         ]));
//         let mut binned_stream = TimeBinnedStream::new(stream0, binned_range, true);
//         while let Some(item) = binned_stream.next().await {
//             eprintln!("{item:?}");
//             match item {
//                 Err(e) => {
//                     if e.to_string().contains("must emit but can not even create empty A") {
//                         return Ok(());
//                     } else {
//                         return Err(Error::with_msg_no_trace("should not succeed"));
//                     }
//                 }
//                 _ => {
//                     return Err(Error::with_msg_no_trace("should not succeed"));
//                 }
//             }
//         }
//         return Err(Error::with_msg_no_trace("should not succeed"));
//     };
//     runfut(fut)
// }

// #[test]
// fn timebin_multi_stage_00() -> Result<(), Error> {
//     // TODO chain two timebin stages with different binning grid.
//     let fut = async {
//         let do_time_weight = true;
//         let one_before_range = do_time_weight;
//         let range = nano_range_from_str("1970-01-01T00:00:10Z", "1970-01-01T00:01:03Z")?;
//         let range = SeriesRange::TimeRange(range);
//         let binned_range_0 = BinnedRangeEnum::covering_range(range.clone(), 22)?;
//         dbg!(&binned_range_0);
//         let range: SeriesRange = binned_range_0.binned_range_time().to_nano_range().into();
//         let binned_range_1 = BinnedRangeEnum::covering_range(range.clone(), 48)?;
//         dbg!(&binned_range_1);
//         let stream_evs = GenerateI32V01::new(0, 1, range.clone(), one_before_range);
//         let exp1 = {
//             let mut bins = BinsDim0::<i32>::empty();
//             for i in 0..54 {
//                 bins.push(
//                     SEC * (10 + i),
//                     SEC * (11 + i),
//                     2,
//                     20 + 2 * i as i32,
//                     21 + 2 * i as i32,
//                     20.5 + 2. * i as f32,
//                     21 + 2 * i as i32,
//                 );
//             }
//             bins
//         };
//         let exp2 = {
//             let mut bins = BinsDim0::<i32>::empty();
//             for i in 0..27 {
//                 bins.push(
//                     SEC * (10 + 2 * i),
//                     SEC * (12 + 2 * i),
//                     4,
//                     20 + 4 * i as i32,
//                     23 + 4 * i as i32,
//                     21.5 + 4. * i as f32,
//                     23 + 4 * i as i32,
//                 );
//             }
//             bins
//         };
//         // NOTE:
//         // can store all bins in cache for which there is some non-empty bin following, or if the container has range-final.
//         let (q1tx, q1rx) = async_channel::bounded(128);
//         let (q2tx, q2rx) = async_channel::bounded(128);
//         let stream_evs = Box::pin(stream_evs);
//         let binned_stream = {
//             TimeBinnedStream::new(stream_evs, binned_range_1, do_time_weight).map(|x| {
//                 //eprintln!("STAGE  1 -- {:?}", x);
//                 x
//             })
//         };
//         let binned_stream = Itemclone::new(binned_stream, q1tx).map(|x| match x {
//             Ok(x) => x,
//             Err(e) => Err(e),
//         });
//         let binned_stream = {
//             TimeBinnedStream::new(Box::pin(binned_stream), binned_range_0, do_time_weight).map(|x| {
//                 eprintln!("STAGE --  2 {:?}", x);
//                 x
//             })
//         };
//         let binned_stream = Itemclone::new(binned_stream, q2tx).map(|x| match x {
//             Ok(x) => x,
//             Err(e) => Err(e),
//         });
//         let mut have_range_final = false;
//         let mut binned_stream = binned_stream;
//         while let Some(item) = binned_stream.next().await {
//             //eprintln!("{item:?}");
//             match item {
//                 Ok(item) => match item {
//                     StreamItem::DataItem(item) => match item {
//                         RangeCompletableItem::Data(item) => {
//                             if let Some(item) = item.as_any_ref().downcast_ref::<BinsDim0<i32>>() {
//                                 if false {
//                                     eprintln!("-----------------------");
//                                     eprintln!("item {:?}", item);
//                                     eprintln!("-----------------------");
//                                 }
//                             } else {
//                                 return Err(Error::with_msg_no_trace(format!("bad, got item with unexpected type")));
//                             }
//                         }
//                         RangeCompletableItem::RangeComplete => {
//                             have_range_final = true;
//                         }
//                     },
//                     StreamItem::Log(_) => {}
//                     StreamItem::Stats(_) => {}
//                 },
//                 Err(e) => Err(e).unwrap(),
//             }
//         }
//         assert!(have_range_final);
//         {
//             eprintln!("---------------------------------------------------------------------");
//             let mut coll = BinsDim0::empty();
//             let stream = q1rx;
//             while let Ok(item) = stream.recv().await {
//                 //eprintln!("RECV [q1rx] {:?}", item);
//                 // TODO use the transformed item
//                 let _item = on_sitemty_data!(item, |mut item: Box<dyn TimeBinned>| {
//                     if let Some(k) = item.as_any_mut().downcast_mut::<BinsDim0<i32>>() {
//                         coll.append_all_from(k);
//                     }
//                     sitem_data(item)
//                 });
//             }
//             eprintln!("collected 1: {:?}", coll);
//             assert_eq!(coll, exp1);
//         }
//         {
//             eprintln!("---------------------------------------------------------------------");
//             let mut coll = BinsDim0::empty();
//             let stream = q2rx;
//             while let Ok(item) = stream.recv().await {
//                 //eprintln!("RECV [q2rx] {:?}", item);
//                 // TODO use the transformed item
//                 let _item = on_sitemty_data!(item, |mut item: Box<dyn TimeBinned>| {
//                     if let Some(k) = item.as_any_mut().downcast_mut::<BinsDim0<i32>>() {
//                         coll.append_all_from(k);
//                     }
//                     sitem_data(item)
//                 });
//             }
//             eprintln!("collected 1: {:?}", coll);
//             assert_eq!(coll, exp2);
//         }
//         Ok(())
//     };
//     runfut(fut)
// }
