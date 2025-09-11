use futures_util::Future;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;

struct AssertFits<F, const N: usize>(PhantomData<F>);

impl<F, const N: usize> AssertFits<F, N> {
    const GOOD: bool = {
        let a = std::mem::size_of::<F>() <= N && std::mem::align_of::<F>() <= 16;
        assert!(a, "future too large or bad alignment");
        a
    };
}

#[repr(C)]
#[repr(align(16))]
struct ErasedFuture<T, const SIZE: usize> {
    futbuf: MaybeUninit<[u8; SIZE]>,
    poll_fn: fn(slf: Pin<&mut Self>, cx: &mut Context) -> Poll<T>,
    drop_fn: fn(slf: &mut Self),
}

impl<T, const SIZE: usize> ErasedFuture<T, SIZE> {
    fn new<F>(fut: F) -> Self
    where
        F: Future<Output = T>,
    {
        if false {
            let n = std::mem::size_of::<F>();
            eprintln!("future  F {n}  S {SIZE}");
        }
        let _a = AssertFits::<F, SIZE>::GOOD;
        let mut futbuf = MaybeUninit::uninit();
        unsafe {
            (futbuf.as_mut_ptr() as *mut F).write(fut);
        }
        Self {
            futbuf,
            poll_fn: Self::poll_inner::<F>,
            drop_fn: Self::drop_inner::<F>,
        }
    }

    fn as_mut_ptr<F>(&mut self) -> *mut F {
        self.futbuf.as_mut_ptr() as *mut F
    }

    fn as_pin_mut<F>(self: Pin<&mut Self>) -> Pin<&mut F> {
        unsafe { self.map_unchecked_mut(|x| &mut *x.as_mut_ptr()) }
    }

    fn poll_inner<F>(self: Pin<&mut Self>, cx: &mut Context) -> Poll<F::Output>
    where
        F: Future,
    {
        self.as_pin_mut::<F>().poll(cx)
    }

    fn drop_inner<F>(&mut self) {
        unsafe {
            std::ptr::drop_in_place(self.as_mut_ptr::<F>());
        }
    }
}

impl<T, const SIZE: usize> Drop for ErasedFuture<T, SIZE> {
    fn drop(&mut self) {
        (self.drop_fn)(self)
    }
}

impl<T, const SIZE: usize> Future for ErasedFuture<T, SIZE> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        (self.poll_fn)(self, cx)
    }
}

async fn dummy_fut_1(inp: &str) -> String {
    let ret = format!("{inp}");
    tokio::time::sleep(Duration::from_millis(1)).await;
    ret
}

#[test]
fn test_00() {
    let fut = dummy_fut_1("a");
    let fut = ErasedFuture::<_, 296>::new(fut);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(fut);
}

#[allow(unused)]
pub fn _dummy_export() {
    let fut = dummy_fut_1("a");
    let _x = ErasedFuture::<_, 296>::new(fut);
}
