use crate::rangefilter2::RangeFilter2;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::Events;
use netpod::range::evrange::NanoRange;
use netpod::TsNano;
use std::collections::VecDeque;

#[test]
fn test_00() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut item1 = EventsDim0::<f32>::empty();
    item1.push_back(beg.ns() + 0 * ms, 0, 3.);
    item1.push_back(beg.ns() + 1 * ms, 0, 3.1);
    item1.push_back(beg.ns() + 2 * ms, 0, 3.2);
    item1.push_back(beg.ns() + 3 * ms, 0, 3.3);
    item1.push_back(beg.ns() + 4 * ms, 0, 3.4);
    item1.push_back(end.ns() - 1, 0, 4.0);
    item1.push_back(end.ns() + 0, 0, 4.1);
    item1.push_back(end.ns() + 1, 0, 4.1);
    let w1: Box<dyn Events> = Box::new(item1.clone());
    let e1 = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w1)));
    let inp = futures_util::stream::iter([e1]);
    let one_before_range = false;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[&[
            beg.ns() + 0 * ms,
            beg.ns() + 1 * ms,
            beg.ns() + 2 * ms,
            beg.ns() + 3 * ms,
            beg.ns() + 4 * ms,
            end.ns() - 1,
        ]];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_cut_before_00() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() + 0 * ms, 0, 3.);
        item.push_back(beg.ns() + 1 * ms, 0, 3.1);
        item.push_back(beg.ns() + 2 * ms, 0, 3.2);
        item.push_back(beg.ns() + 3 * ms, 0, 3.3);
        item.push_back(beg.ns() + 4 * ms, 0, 3.4);
        item.push_back(end.ns() - 1, 0, 4.0);
        item.push_back(end.ns() + 0, 0, 4.1);
        item.push_back(end.ns() + 1, 0, 4.1);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = false;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            &[],
            &[
                beg.ns() + 0 * ms,
                beg.ns() + 1 * ms,
                beg.ns() + 2 * ms,
                beg.ns() + 3 * ms,
                beg.ns() + 4 * ms,
                end.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_one_before_00() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() + 0 * ms, 0, 3.);
        item.push_back(beg.ns() + 1 * ms, 0, 3.1);
        item.push_back(beg.ns() + 2 * ms, 0, 3.2);
        item.push_back(beg.ns() + 3 * ms, 0, 3.3);
        item.push_back(beg.ns() + 4 * ms, 0, 3.4);
        item.push_back(end.ns() - 1, 0, 4.0);
        item.push_back(end.ns() + 0, 0, 4.1);
        item.push_back(end.ns() + 1, 0, 4.1);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = true;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            &[],
            &[
                //
                beg.ns() - 1,
            ],
            &[
                beg.ns() + 0 * ms,
                beg.ns() + 1 * ms,
                beg.ns() + 2 * ms,
                beg.ns() + 3 * ms,
                beg.ns() + 4 * ms,
                end.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_one_before_01() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        item.push_back(beg.ns() + 0 * ms, 0, 3.);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() + 1 * ms, 0, 3.1);
        item.push_back(beg.ns() + 2 * ms, 0, 3.2);
        item.push_back(beg.ns() + 3 * ms, 0, 3.3);
        item.push_back(beg.ns() + 4 * ms, 0, 3.4);
        item.push_back(end.ns() - 1, 0, 4.0);
        item.push_back(end.ns() + 0, 0, 4.1);
        item.push_back(end.ns() + 1, 0, 4.1);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = true;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            // &[],
            &[
                //
                beg.ns() - 1,
                beg.ns() + 0 * ms,
            ],
            &[
                beg.ns() + 1 * ms,
                beg.ns() + 2 * ms,
                beg.ns() + 3 * ms,
                beg.ns() + 4 * ms,
                end.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[test]
fn test_one_before_only() {
    use items_0::Empty;
    use items_2::eventsdim0::EventsDim0;
    let _ms = 1_000_000;
    let beg = TsNano::from_ms(1000 * 10);
    let end = TsNano::from_ms(1000 * 20);
    let mut items = Vec::new();
    {
        let mut item = EventsDim0::<f32>::empty();
        item.push_back(beg.ns() - 1, 0, 2.9);
        let w: Box<dyn Events> = Box::new(item.clone());
        let e: Sitemty<_> = Ok(StreamItem::DataItem(RangeCompletableItem::Data(w)));
        items.push(e);
    }
    let inp = futures_util::stream::iter(items);
    let one_before_range = true;
    let range = NanoRange::from((beg.ns(), end.ns()));
    let stream = RangeFilter2::new(inp, range, one_before_range);
    let fut = async move {
        let tss_items = fetch_into_tss_items(stream).await;
        let exp: &[&[u64]] = &[
            // TODO in the future this empty may be discarded
            &[],
            &[
                //
                beg.ns() - 1,
            ],
        ];
        assert_eq!(&tss_items, &exp);
        Ok::<_, Error>(())
    };
    taskrun::run(fut).unwrap();
}

#[cfg(test)]
async fn fetch_into_tss_items<INP>(mut inp: INP) -> VecDeque<VecDeque<u64>>
where
    INP: Stream<Item = Sitemty<Box<dyn Events>>> + Unpin,
{
    let mut tss_items = VecDeque::new();
    while let Some(e) = inp.next().await {
        if let Ok(StreamItem::DataItem(RangeCompletableItem::Data(evs))) = e {
            eprintln!("{:?}", evs);
            tss_items.push_back(Events::tss(&evs).clone());
        } else {
            eprintln!("other item ----------: {:?}", e);
        }
    }
    tss_items
}
