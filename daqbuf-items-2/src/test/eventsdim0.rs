use crate::eventsdim0::EventsDim0;
use items_0::Appendable;
use items_0::Empty;
use items_0::Events;

#[test]
fn collect_s_00() {
    let mut evs = EventsDim0::empty();
    evs.push(123, 4, 1.00f32);
    evs.push(124, 5, 1.01);
    let mut coll = evs.as_collectable_mut().new_collector();
    coll.ingest(&mut evs);
    assert_eq!(coll.len(), 2);
}

#[test]
fn collect_c_00() {
    let mut evs = EventsDim0::empty();
    evs.push(123, 4, 1.00f32);
    evs.push(124, 5, 1.01);
    let mut coll = evs.as_collectable_with_default_ref().new_collector();
    coll.ingest(&mut evs);
    assert_eq!(coll.len(), 2);
}
