use crate::rangefilter2::RangeFilter2;
use crate::rt::run_test;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::binning::container_events::ContainerEvents;
use items_2::channelevents::ChannelEvents;
use netpod::range::evrange::NanoRange;
use netpod::DtNano;
use netpod::OneBeforeFlag;
use netpod::TsNano;
use std::collections::VecDeque;

autoerr::create_error_v1!(
    name(Error, "RangefilterTest"),
    enum variants {
        Logic,
    },
);

fn pu(c: &mut ContainerEvents<f32>, ts: TsNano, v: f32) {
    c.push_back(ts, v);
}

fn dataitem(c: ContainerEvents<f32>) -> Sitemty<ChannelEvents> {
    Ok(StreamItem::DataItem(RangeCompletableItem::Data(
        ChannelEvents::Events(Box::new(c)),
    )))
}

async fn fetch_into_tss_items<INP>(mut inp: INP) -> VecDeque<VecDeque<TsNano>>
where
    INP: Stream<Item = Sitemty<ChannelEvents>> + Unpin,
{
    let mut tss_items = VecDeque::new();
    while let Some(e) = inp.next().await {
        if let Ok(StreamItem::DataItem(RangeCompletableItem::Data(evs))) = e {
            eprintln!("fetch_into_tss_items  sees  {:?}", evs);
            match evs {
                ChannelEvents::Events(x) => {
                    tss_items.push_back(x.tss_for_testing());
                }
                ChannelEvents::Status(_) => {}
            }
        } else {
            eprintln!("other item ----------: {:?}", e);
        }
    }
    tss_items
}

fn gen_vstss<C>(range: &NanoRange, cuts: C) -> VecDeque<VecDeque<TsNano>>
where
    C: IntoIterator<Item = u64>,
{
    let dt = DtNano::from_ms(1);
    let mut cuts: VecDeque<_> = cuts.into_iter().map(|x| TsNano::from_ms(x)).collect();
    let mut ts = range.beg_ts();
    let mut ret = VecDeque::new();
    let mut buf = VecDeque::new();
    loop {
        if ts >= range.end_ts() {
            break;
        }
        if cuts.front().map_or(false, |&x| ts >= x) {
            cuts.pop_front();
            ret.push_back(std::mem::replace(&mut buf, VecDeque::new()));
        }
        buf.push_back(ts);
        ts = ts.add_dt_nano(dt);
    }
    ret.push_back(std::mem::replace(&mut buf, VecDeque::new()));
    ret
}

fn gen_vec_evs<C>(range: &NanoRange, cuts: C) -> VecDeque<ContainerEvents<f32>>
where
    C: IntoIterator<Item = u64>,
{
    let vstss_inp = gen_vstss(range, cuts);
    let mut co = ContainerEvents::<f32>::new();
    let mut ret = VecDeque::new();
    for b in vstss_inp {
        let c = &mut co;
        for ts in b {
            pu(c, ts, 3.);
        }
        ret.push_back(co);
        co = ContainerEvents::<f32>::new();
    }
    ret
}

fn gen_inp_stream<C>(range: &NanoRange, cuts: C) -> impl Stream<Item = Sitemty<ChannelEvents>>
where
    C: IntoIterator<Item = u64>,
{
    let mut items = VecDeque::new();
    let a = gen_vec_evs(range, cuts);
    for evs in a {
        items.push_back(dataitem(evs));
    }
    eprintln!("INPUT GENERATED {:?}", items);
    futures_util::stream::iter(items)
}

#[test]
fn test_single_prune_nothing_00() {
    let range1 = NanoRange::from_ms_u64(10, 20);
    let range2 = NanoRange::from_ms_u64(10, 20);
    let vtss_exp = gen_vstss(&range1, []);
    let inp = gen_inp_stream(&range2, []);
    let one_before = OneBeforeFlag::from_bool(false);
    let stream = RangeFilter2::new(inp, range1, one_before);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        eprintln!("{:?}", tss_items);
        assert_eq!(tss_items, vtss_exp);
        Ok::<_, Error>(())
    };
    run_test(fut).unwrap();
}

#[test]
fn test_prune_high_00() {
    let range1 = NanoRange::from_ms_u64(10, 20);
    let range2 = NanoRange::from_ms_u64(10, 21);
    let vtss_exp = gen_vstss(&range1, []);
    let inp = gen_inp_stream(&range2, []);
    let one_before = OneBeforeFlag::from_bool(false);
    let stream = RangeFilter2::new(inp, range1, one_before);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        // eprintln!("{:?}", tss_items);
        assert_eq!(tss_items, vtss_exp);
        Ok::<_, Error>(())
    };
    run_test(fut).unwrap();
}

#[test]
fn test_prune_high_01() {
    let range1 = NanoRange::from_ms_u64(10, 20);
    let range2 = NanoRange::from_ms_u64(10, 21);
    let vtss_exp = gen_vstss(&range1, [14]);
    let inp = gen_inp_stream(&range2, [14]);
    let one_before = OneBeforeFlag::from_bool(false);
    let stream = RangeFilter2::new(inp, range1, one_before);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        // eprintln!("{:?}", tss_items);
        assert_eq!(tss_items, vtss_exp);
        Ok::<_, Error>(())
    };
    run_test(fut).unwrap();
}

#[test]
fn test_prune_low() {
    let range1 = NanoRange::from_ms_u64(10, 20);
    let range2 = NanoRange::from_ms_u64(8, 18);
    let range3 = NanoRange::from_ms_u64(10, 18);
    let inp = gen_inp_stream(&range2, [14]);
    let vtss_exp = gen_vstss(&range3, [14]);
    let one_before = OneBeforeFlag::from_bool(false);
    let stream = RangeFilter2::new(inp, range1, one_before);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        // eprintln!("{:?}", tss_items);
        assert_eq!(tss_items, vtss_exp);
        Ok::<_, Error>(())
    };
    run_test(fut).unwrap();
}

// #[test]
// fn test_cut_before_00() {
//     let ms = 1_000_000;
//     let beg = TsNano::from_ms(1000 * 10);
//     let end = TsNano::from_ms(1000 * 20);
//     let mut items = Vec::new();
//     {
//         let mut item = ContainerEvents::<f32>::empty();
//         item.push_back(beg.ns() - 1, 0, 2.9);
//         let w: Box<dyn Events> = Box::new(item.clone());
//         let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
//         items.push(e);
//     }
//     {
//         let mut item = ContainerEvents::<f32>::empty();
//         item.push_back(beg.ns() + 0 * ms, 0, 3.);
//         item.push_back(beg.ns() + 1 * ms, 0, 3.1);
//         item.push_back(beg.ns() + 2 * ms, 0, 3.2);
//         item.push_back(beg.ns() + 3 * ms, 0, 3.3);
//         item.push_back(beg.ns() + 4 * ms, 0, 3.4);
//         item.push_back(end.ns() - 1, 0, 4.0);
//         item.push_back(end.ns() + 0, 0, 4.1);
//         item.push_back(end.ns() + 1, 0, 4.1);
//         let w: Box<dyn Events> = Box::new(item.clone());
//         let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
//         items.push(e);
//     }
//     let inp = futures_util::stream::iter(items);
//     let one_before_range = false;
//     let range = NanoRange::from((beg.ns(), end.ns()));
//     let stream = RangeFilter2::new(inp, range, one_before_range);
//     let fut = async move {
//         let tss_items = fetch_into_tss_items(stream).await;
//         let exp: &[&[u64]] = &[
//             // TODO in the future this empty may be discarded
//             &[],
//             &[
//                 beg.ns() + 0 * ms,
//                 beg.ns() + 1 * ms,
//                 beg.ns() + 2 * ms,
//                 beg.ns() + 3 * ms,
//                 beg.ns() + 4 * ms,
//                 end.ns() - 1,
//             ],
//         ];
//         assert_eq!(&tss_items, &exp);
//         Ok::<_, Error>(())
//     };
//     run_test(fut).unwrap();
// }

// #[test]
// fn test_one_before_00() {
//     let ms = 1_000_000;
//     let beg = TsNano::from_ms(1000 * 10);
//     let end = TsNano::from_ms(1000 * 20);
//     let mut items = Vec::new();
//     {
//         let mut item = ContainerEvents::<f32>::empty();
//         item.push_back(beg.ns() - 1, 0, 2.9);
//         let w: Box<dyn Events> = Box::new(item.clone());
//         let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
//         items.push(e);
//     }
//     {
//         let mut item = ContainerEvents::<f32>::empty();
//         item.push_back(beg.ns() + 0 * ms, 0, 3.);
//         item.push_back(beg.ns() + 1 * ms, 0, 3.1);
//         item.push_back(beg.ns() + 2 * ms, 0, 3.2);
//         item.push_back(beg.ns() + 3 * ms, 0, 3.3);
//         item.push_back(beg.ns() + 4 * ms, 0, 3.4);
//         item.push_back(end.ns() - 1, 0, 4.0);
//         item.push_back(end.ns() + 0, 0, 4.1);
//         item.push_back(end.ns() + 1, 0, 4.1);
//         let w: Box<dyn Events> = Box::new(item.clone());
//         let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
//         items.push(e);
//     }
//     let inp = futures_util::stream::iter(items);
//     let one_before_range = true;
//     let range = NanoRange::from((beg.ns(), end.ns()));
//     let stream = RangeFilter2::new(inp, range, one_before_range);
//     let fut = async move {
//         let tss_items = fetch_into_tss_items(stream).await;
//         let exp: &[&[u64]] = &[
//             // TODO in the future this empty may be discarded
//             &[],
//             &[
//                 //
//                 beg.ns() - 1,
//             ],
//             &[
//                 beg.ns() + 0 * ms,
//                 beg.ns() + 1 * ms,
//                 beg.ns() + 2 * ms,
//                 beg.ns() + 3 * ms,
//                 beg.ns() + 4 * ms,
//                 end.ns() - 1,
//             ],
//         ];
//         assert_eq!(&tss_items, &exp);
//         Ok::<_, Error>(())
//     };
//     taskrun::run(fut).unwrap();
// }

// #[test]
// fn test_one_before_01() {
//     use items_0::Empty;
//     use items_2::eventsdim0::EventsDim0;
//     let ms = 1_000_000;
//     let beg = TsNano::from_ms(1000 * 10);
//     let end = TsNano::from_ms(1000 * 20);
//     let mut items = Vec::new();
//     {
//         let mut item = EventsDim0::<f32>::empty();
//         item.push_back(beg.ns() - 1, 0, 2.9);
//         item.push_back(beg.ns() + 0 * ms, 0, 3.);
//         let w: Box<dyn Events> = Box::new(item.clone());
//         let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
//         items.push(e);
//     }
//     {
//         let mut item = EventsDim0::<f32>::empty();
//         item.push_back(beg.ns() + 1 * ms, 0, 3.1);
//         item.push_back(beg.ns() + 2 * ms, 0, 3.2);
//         item.push_back(beg.ns() + 3 * ms, 0, 3.3);
//         item.push_back(beg.ns() + 4 * ms, 0, 3.4);
//         item.push_back(end.ns() - 1, 0, 4.0);
//         item.push_back(end.ns() + 0, 0, 4.1);
//         item.push_back(end.ns() + 1, 0, 4.1);
//         let w: Box<dyn Events> = Box::new(item.clone());
//         let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
//         items.push(e);
//     }
//     let inp = futures_util::stream::iter(items);
//     let one_before_range = true;
//     let range = NanoRange::from((beg.ns(), end.ns()));
//     let stream = RangeFilter2::new(inp, range, one_before_range);
//     let fut = async move {
//         let tss_items = fetch_into_tss_items(stream).await;
//         let exp: &[&[u64]] = &[
//             // TODO in the future this empty may be discarded
//             // &[],
//             &[
//                 //
//                 beg.ns() - 1,
//                 beg.ns() + 0 * ms,
//             ],
//             &[
//                 beg.ns() + 1 * ms,
//                 beg.ns() + 2 * ms,
//                 beg.ns() + 3 * ms,
//                 beg.ns() + 4 * ms,
//                 end.ns() - 1,
//             ],
//         ];
//         assert_eq!(&tss_items, &exp);
//         Ok::<_, Error>(())
//     };
//     taskrun::run(fut).unwrap();
// }

// #[test]
// fn test_one_before_only() {
//     use items_0::Empty;
//     use items_2::eventsdim0::EventsDim0;
//     let _ms = 1_000_000;
//     let beg = TsNano::from_ms(1000 * 10);
//     let end = TsNano::from_ms(1000 * 20);
//     let mut items = Vec::new();
//     {
//         let mut item = EventsDim0::<f32>::empty();
//         item.push_back(beg.ns() - 1, 0, 2.9);
//         let w: Box<dyn Events> = Box::new(item.clone());
//         let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
//         items.push(e);
//     }
//     let inp = futures_util::stream::iter(items);
//     let one_before_range = true;
//     let range = NanoRange::from((beg.ns(), end.ns()));
//     let stream = RangeFilter2::new(inp, range, one_before_range);
//     let fut = async move {
//         let tss_items = fetch_into_tss_items(stream).await;
//         let exp: &[&[u64]] = &[
//             // TODO in the future this empty may be discarded
//             &[],
//             &[
//                 //
//                 beg.ns() - 1,
//             ],
//         ];
//         assert_eq!(&tss_items, &exp);
//         Ok::<_, Error>(())
//     };
//     taskrun::run(fut).unwrap();
// }
