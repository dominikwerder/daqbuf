use crate::binning::container_events::ContainerEvents;
use netpod::TsNano;
use netpod::range::evrange::NanoRange;

pub fn new_events_gen_dim0_f32_v00(range: NanoRange) -> impl Iterator<Item = ContainerEvents<f32>> {
    let dt = 1000 * 1000 * 10;
    let beg = range.beg();
    let end = range.end();
    let mut ts = beg - dt;
    std::iter::repeat(0)
        .map(move |_| {
            type T = f32;
            let mut c = ContainerEvents::new();
            loop {
                let ts1 = TsNano::from_ns(ts);
                if ts1.ns() >= end {
                    break;
                }
                let val = (ts / 1_000_000) as T + 0.1;
                c.push_back(ts1, val);
                ts += dt;
                if c.len() >= 8 {
                    break;
                }
            }
            c
        })
        .take_while(|c| c.len() != 0)
}

pub fn new_events_gen_dim1_f32_v00(
    range: NanoRange,
) -> impl Iterator<Item = ContainerEvents<Vec<f32>>> {
    let dt = 1000 * 1000 * 10;
    let beg = range.beg();
    let end = range.end();
    let mut ts = beg - dt;
    std::iter::repeat(0)
        .map(move |_| {
            type T = f32;
            let mut c = ContainerEvents::new();
            loop {
                let ts1 = TsNano::from_ns(ts);
                ts += dt;
                if ts1.ns() >= end {
                    break;
                }
                let val = (ts / 1_000_000) as T;
                let val = vec![val; 16];
                c.push_back(ts1, val);
                if c.len() >= 8 {
                    break;
                }
            }
            c
        })
        .take_while(|c| c.len() != 0)
}
