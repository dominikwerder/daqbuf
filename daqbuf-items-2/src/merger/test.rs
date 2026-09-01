use super::MergeInp;
use super::Merger;
use crate::binning::container_bins::compare_boxed_f32;
use crate::binning::container_events::ContainerEvents;
use crate::log;
use futures_util::Future;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::Sitemty;
use items_0::streamitem::sitem_data;
use netpod::TsNano;
use netpod::timeunits::SEC;
use std::pin::Pin;

macro_rules! error { ($($arg:expr),*) => ( if true { log::error!($($arg),*); } ) }

macro_rules! trace { ($($arg:expr),*) => ( if true { log::trace!($($arg),*); } ) }

fn run_test<F>(fut: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(fut)
}

async fn merger_00_inner() {
    let mut evs0 = ContainerEvents::<f32>::new();
    evs0.push_back(TsNano::from_ns(9), 9.0);
    let mut evs1 = ContainerEvents::<f32>::new();
    evs1.push_back(TsNano::from_ns(11), 11.0);
    let inp0: MergeInp<_> = Box::pin(futures_util::stream::iter([sitem_data(evs0), sitem_data(evs1)]));
    let inps = vec![inp0];
    let mut merger = Merger::new(inps, None);
    while let Some(x) = merger.next().await {
        log::trace!("{:?}", x);
    }
    log::trace!("DONE");
}

#[test]
fn merger_00() {
    run_test(merger_00_inner());
}

fn make_container(off: usize, step: usize, cnt: usize) -> ContainerEvents<f32> {
    let mut ret = ContainerEvents::new();
    let mut ts = off;
    for i in 0..cnt {
        ret.push_back(TsNano::from_ns(SEC * ts as u64), ts as f32);
        ts += step;
    }
    ret
}

fn make_stream_items(
    off: usize,
    step: usize,
    cnt_stream: usize,
    cnt_item: usize,
) -> Vec<Sitemty<ContainerEvents<f32>>> {
    let mut ret = Vec::new();
    let mut cnt_todo = cnt_stream;
    let mut off = off;
    while cnt_todo > 0 {
        let cnt = if cnt_todo > cnt_item {
            let x = cnt_item;
            cnt_todo -= x;
            x
        } else {
            let x = cnt_todo;
            cnt_todo = 0;
            x
        };
        let c = make_container(off, step, cnt);
        ret.push(sitem_data(c));
        off += cnt * step;
    }
    ret
}

fn make_stream(
    off: usize,
    step: usize,
    cnt_stream: usize,
    cnt_item: usize,
) -> Pin<Box<dyn Stream<Item = Sitemty<ContainerEvents<f32>>> + Send>> {
    let items = make_stream_items(off, step, cnt_stream, cnt_item);
    trace!("items {:?}", items);
    let st = futures_util::stream::iter(items);
    Box::pin(st)
}

fn make_streams_exactly_alternating(
    off: usize,
    n_streams: usize,
    cnt_total: usize,
    cnt_per_item: usize,
) -> Vec<Pin<Box<dyn Stream<Item = Sitemty<ContainerEvents<f32>>> + Send>>> {
    let mut ret = Vec::new();
    let mut cnt_todo = cnt_total;
    let cnt_per_stream = cnt_total / n_streams;
    for i in 0..n_streams {
        let cnt = if cnt_todo > cnt_per_stream {
            let x = cnt_per_stream;
            cnt_todo -= x;
            x
        } else {
            let x = cnt_todo;
            cnt_todo = 0;
            x
        };
        let st = make_stream(off + i, n_streams, cnt, cnt_per_item);
        ret.push(st);
    }
    ret
}

fn make_streams_from_pattern<P, J, K>(
    pattern: P,
) -> Vec<Pin<Box<dyn Stream<Item = Sitemty<ContainerEvents<f32>>> + Send>>>
where
    P: AsRef<[J]>,
    J: AsRef<[K]>,
    K: AsRef<[usize]>,
{
    let mut streams = Vec::new();
    for pt1 in pattern.as_ref().iter() {
        let mut conts = Vec::new();
        for pt2 in pt1.as_ref().iter() {
            let mut c = ContainerEvents::new();
            for &pt3 in pt2.as_ref().iter() {
                c.push_back(TsNano::from_ns(SEC * pt3 as u64), pt3 as f32);
            }
            trace!("made pattern {:?}", c);
            conts.push(sitem_data(c));
        }
        let st = futures_util::stream::iter(conts);
        streams.push(Box::pin(st) as Pin<Box<dyn Stream<Item = Sitemty<ContainerEvents<f32>>> + Send>>);
    }
    streams
}

async fn merger_alternating_00_inner() {
    let exp_01 = make_container(400, 1, 100);
    let inps = make_streams_exactly_alternating(400, 3, 100, 7);
    let mut merger = Merger::new(inps, None);
    let mut bad = false;
    while let Some(x) = merger.next().await {
        trace!("MERGER OUTPUT {:?}\n\n", x);
        let _ = on_sitemty_data!(x, |x| {
            match ContainerEvents::testing_cmp(&x, &exp_01) {
                Ok(()) => {}
                Err(msg) => {
                    bad = true;
                    error!("cmp {}", msg);
                }
            };
            sitem_data(x)
        });
    }
    if bad {
        panic!("bad result");
    }
}

#[test]
fn merger_alternating_00() {
    run_test(merger_alternating_00_inner());
}

macro_rules! a {
    ($val:expr) => {
        &($val[..])
    };
}

async fn merger_overlap_00_inner() {
    let pattern = [
        &[a!([400, 405, 408, 410, 414]), a!([416, 417, 418, 419])][..],
        &[a!([402]), a!([404, 406, 411, 412, 413])][..],
        &[a!([401]), a!([403]), a!([406, 407, 409]), a!([413, 414, 415])][..],
    ];
    let inps = make_streams_from_pattern(pattern);
    let exp_00 = make_container(400, 1, 20);
    let mut merger = Merger::new(inps, None);
    let mut merged = Vec::new();
    while let Some(x) = merger.next().await {
        trace!("MERGER OUTPUT {:?}\n\n", x);
        let _ = on_sitemty_data!(x, |x| {
            merged.push(x);
            sitem_data(ContainerEvents::<f32>::new())
        });
    }
    let mut bad = false;
    match ContainerEvents::testing_cmp(&merged[0], &exp_00) {
        Ok(()) => {}
        Err(msg) => {
            bad = true;
            error!("cmp {}", msg);
        }
    };
    if bad {
        panic!("bad result");
    }
}

#[test]
fn merger_overlap_00() {
    run_test(merger_overlap_00_inner());
}
