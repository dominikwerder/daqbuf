use std::sync::Arc;
use std::sync::atomic::AtomicU32;
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
    unsafe { Arc::increment_strong_count(ptr) };
    todo!("rw_clone")
}

fn rw_wake(x: *const ()) {
    todo!("rw_wake")
}

fn rw_wake_by_ref(x: *const ()) {
    todo!("rw_wake_by_ref")
}

fn rw_drop(ptr: *const ()) {
    eprintln!("ptr {:?}", ptr);
    todo!("rw_drop")
}

const RWVT: RawWakerVTable = RawWakerVTable::new(rw_clone, rw_wake, rw_wake_by_ref, rw_drop);

#[test]
fn test_sync_waker_00() {
    let data = Data::new();
    let data = Arc::new(data);
    let ptr = Arc::into_raw(data);
    eprintln!("ptr {:?}", ptr);
    let raw_waker = RawWaker::new(ptr as _, &RWVT);
    let waker = unsafe { Waker::from_raw(raw_waker) };
    let cx = Context::from_waker(&waker);
    let data = unsafe { Arc::from_raw(ptr) };
}
