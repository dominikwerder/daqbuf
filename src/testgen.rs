use crate::eventsdim0::EventsDim0;
use crate::Events;
use items_0::Appendable;
use items_0::Empty;

#[allow(unused)]
fn xorshift32(state: u32) -> u32 {
    let mut x = state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    x
}

pub fn make_some_boxed_d0_f32(n: usize, t0: u64, tstep: u64, tmask: u64, seed: u32) -> Box<dyn Events> {
    let mut vstate = seed;
    let mut events = EventsDim0::empty();
    for i in 0..n {
        vstate = xorshift32(vstate);
        let ts = t0 + i as u64 * tstep + (vstate as u64 & tmask);
        let value = i as f32 * 100. + vstate as f32 / u32::MAX as f32 / 10.;
        events.push(ts, ts, value);
    }
    Box::new(events)
}
