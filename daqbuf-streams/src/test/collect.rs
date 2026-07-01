// #[test]
// fn collect_channel_events_00() -> Result<(), Error> {
//     let fut = async {
//         let evs0 = make_some_boxed_d0_f32(20, SEC * 10, SEC * 1, 0, 28736487);
//         let evs1 = make_some_boxed_d0_f32(20, SEC * 30, SEC * 1, 0, 882716583);
//         let stream = stream::iter(vec![
//             sitem_data(evs0),
//             sitem_data(evs1),
//             Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
//         ]);
//         let deadline = Instant::now() + Duration::from_millis(4000);
//         let events_max = 10000;
//         let res = crate::collect::collect(stream, deadline, events_max, None, None).await?;
//         //eprintln!("collected result: {res:?}");
//         if let Some(res) = res.as_any_ref().downcast_ref::<EventsDim0CollectorOutput<f32>>() {
//             eprintln!("Great, a match");
//             eprintln!("{res:?}");
//             assert_eq!(res.len(), 40);
//         } else {
//             return Err(Error::with_msg(format!("bad type of collected result")));
//         }
//         Ok(())
//     };
//     runfut(fut)
// }

// #[test]
// fn collect_channel_events_01() -> Result<(), Error> {
//     let fut = async {
//         let evs0 = make_some_boxed_d0_f32(20, SEC * 10, SEC * 1, 0, 28736487);
//         let evs1 = make_some_boxed_d0_f32(20, SEC * 30, SEC * 1, 0, 882716583);
//         let stream = stream::iter(vec![
//             sitem_data(evs0),
//             sitem_data(evs1),
//             Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
//         ]);
//         // TODO build like in request code
//         let deadline = Instant::now() + Duration::from_millis(4000);
//         let events_max = 10000;
//         let bytes_max = 80 * 10000;
//         let stream = PlainEventStream::new(stream);
//         let stream = EventsToTimeBinnable::new(stream);
//         let stream = TimeBinnableToCollectable::new(stream);
//         let stream = Box::pin(stream);
//         let res = Collect::new(stream, deadline, events_max, bytes_max, None, None).await?;
//         if let CollectResult::Some(res) = res {
//             if let Some(res) = res.as_any_ref().downcast_ref::<EventsDim0CollectorOutput<f32>>() {
//                 eprintln!("Great, a match");
//                 eprintln!("{res:?}");
//                 assert_eq!(res.len(), 40);
//             } else {
//                 return Err(Error::with_msg(format!("bad type of collected result")));
//             }
//             Ok(())
//         } else {
//             return Err(Error::with_msg(format!("bad type of collected result")));
//         }
//     };
//     runfut(fut)
// }

// #[test]
// fn collect_channel_events_pulse_id_diff() -> Result<(), Error> {
//     let fut = async {
//         let trqu = TransformQuery::from_url(&"https://data-api.psi.ch/?binningScheme=pulseIdDiff".parse()?)?;
//         info!("{trqu:?}");
//         let evs0 = make_some_boxed_d0_f32(20, SEC * 10, SEC * 1, 0, 28736487);
//         let evs1 = make_some_boxed_d0_f32(20, SEC * 30, SEC * 1, 0, 882716583);
//         let stream = stream::iter(vec![
//             sitem_data(evs0),
//             sitem_data(evs1),
//             Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
//         ]);
//         let mut tr = build_event_transform(&trqu)?;
//         let stream = stream.map(move |x| {
//             on_sitemty_data!(x, |x| {
//                 let x = tr.0.transform(x);
//                 Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
//             })
//         });
//         let stream = PlainEventStream::new(stream);
//         let stream = EventsToTimeBinnable::new(stream);
//         let deadline = Instant::now() + Duration::from_millis(4000);
//         let events_max = 10000;
//         let bytes_max = 80 * 10000;
//         let stream = Box::pin(stream);
//         let stream = build_time_binning_transform(&trqu, stream)?;
//         let stream = TimeBinnableToCollectable::new(stream);
//         let stream = Box::pin(stream);
//         let res = Collect::new(stream, deadline, events_max, bytes_max, None, None).await?;
//         if let CollectResult::Some(res) = res {
//             if let Some(res) = res.as_any_ref().downcast_ref::<EventsDim0CollectorOutput<i64>>() {
//                 eprintln!("Great, a match");
//                 eprintln!("{res:?}");
//                 assert_eq!(res.len(), 40);
//             } else {
//                 return Err(Error::with_msg(format!("bad type of collected result")));
//             }
//             Ok(())
//         } else {
//             return Err(Error::with_msg(format!("bad type of collected result")));
//         }
//     };
//     runfut(fut)
// }
