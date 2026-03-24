use crate::events3::MSP_A_00;
use netpod::DtMs;
use netpod::TsMs;
use std::collections::VecDeque;

pub fn pred_ms_range(range: crate::range::ScyllaSeriesRange) -> impl Fn(&TsMs) -> bool {
    move |ms| range.beg() <= ms.ns() && ms.ns() < range.end()
}

pub fn series_a_msps() -> VecDeque<TsMs> {
    let dt = DtMs::from_ms_u64(1000 * 60 * 60);
    let mut v = MSP_A_00;
    (0..48)
        .into_iter()
        .map(|_| {
            let x = v;
            v = v.add_dt_ms(dt);
            x
        })
        .collect()
}
