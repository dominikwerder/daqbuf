use std::sync::Arc;
use std::task::Context;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

struct Data {
    ix: u32,
}

impl Data {
    fn new() -> Self {
        Self { ix: 0 }
    }
}

fn rw_clone(ptr: *const ()) -> RawWaker {
    unsafe { Arc::increment_strong_count(ptr as *const Data) };
    RawWaker::new(ptr, &RWVT)
}

fn rw_wake(ptr: *const ()) {
    let data = unsafe { Arc::from_raw(ptr as *const Data) };
    drop(data);
}

fn rw_wake_by_ref(ptr: *const ()) {
    unsafe { Arc::increment_strong_count(ptr as *const Data) };
    let data = unsafe { Arc::from_raw(ptr as *const Data) };
    drop(data);
}

fn rw_drop(ptr: *const ()) {
    let data = unsafe { Arc::from_raw(ptr as *const Data) };
    drop(data);
}

const RWVT: RawWakerVTable = RawWakerVTable::new(rw_clone, rw_wake, rw_wake_by_ref, rw_drop);

#[test]
fn test_sync_waker_00() {
    let data = Arc::new(Data::new());
    let ptr = Arc::into_raw(data);
    let raw_waker = RawWaker::new(ptr as *const (), &RWVT);
    let waker = unsafe { Waker::from_raw(raw_waker) };
    let cx = Context::from_waker(&waker);
    let _ = &cx;
    unsafe { Arc::increment_strong_count(ptr) };
    let data = unsafe { Arc::from_raw(ptr) };
    assert_eq!(data.ix, 0);
}
