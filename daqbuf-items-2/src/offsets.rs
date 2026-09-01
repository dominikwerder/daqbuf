use netpod::TsNano;
use netpod::timeunits::MS;
use netpod::timeunits::SEC;
use std::collections::VecDeque;

pub fn ts_offs_from_abs_with_anchor(ts_anchor_sec: u64, tss: &VecDeque<TsNano>) -> (VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_ns = ts_anchor_sec * SEC;
    let ts_off_ms: VecDeque<_> = tss.iter().map(|&k| (k.ns() - ts_anchor_ns) / MS).collect();
    let ts_off_ns = tss
        .iter()
        .zip(ts_off_ms.iter().map(|&k| k * MS))
        .map(|(&j, k)| j.ns() - ts_anchor_ns - k)
        .collect();
    (ts_off_ms, ts_off_ns)
}

pub fn ts_offs_from_abs(tss: &VecDeque<TsNano>) -> (u64, VecDeque<u64>, VecDeque<u64>) {
    let ts_anchor_sec = tss.front().map_or(TsNano::from_ns(0), |&k| k).ns() / SEC;
    let (ts_off_ms, ts_off_ns) = ts_offs_from_abs_with_anchor(ts_anchor_sec, tss);
    (ts_anchor_sec, ts_off_ms, ts_off_ns)
}

pub fn pulse_offs_from_abs(pulses: &VecDeque<u64>) -> (u64, VecDeque<u64>) {
    let pulse_anchor = pulses.front().map_or(0, |&k| k) / 10000 * 10000;
    let pulse_off = pulses.iter().map(|&k| k - pulse_anchor).collect();
    (pulse_anchor, pulse_off)
}
