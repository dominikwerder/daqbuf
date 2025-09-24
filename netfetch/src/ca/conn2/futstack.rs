use futures_util::Future;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;

struct AssertFits<F, const N: usize>(PhantomData<F>);

const CHARTAB: [u8; 16] = [
    b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'a', b'b', b'c', b'd', b'e', b'f',
];

macro_rules! format_u32 {
    ($n:expr,$b:expr) => {{
        let n: u32 = $n;
        let b: &mut [u8; 8] = $b;
        let c = n.to_le_bytes();
        b[0] = CHARTAB[(c[3] >> 4) as usize];
        b[1] = CHARTAB[(c[3] & 0xf) as usize];
        b[2] = CHARTAB[(c[2] >> 4) as usize];
        b[3] = CHARTAB[(c[2] & 0xf) as usize];
        b[4] = CHARTAB[(c[1] >> 4) as usize];
        b[5] = CHARTAB[(c[1] & 0xf) as usize];
        b[6] = CHARTAB[(c[0] >> 4) as usize];
        b[7] = CHARTAB[(c[0] & 0xf) as usize];
        if let Ok(x) = str::from_utf8(b) { x } else { "" }
    }};
}

impl<F, const N: usize> AssertFits<F, N> {
    const GOOD: bool = {
        let size = std::mem::size_of::<F>() as u32;
        let _align = std::mem::align_of::<F>();
        let a = std::mem::size_of::<F>() <= N && std::mem::align_of::<F>() <= 8;
        let mut b1 = [0u8; 8];
        let s1 = format_u32!(size as u32, &mut b1);
        if a == false {
            assert!(a, "{}", s1);
        }
        a
    };
}

#[repr(C)]
#[repr(align(16))]
pub struct ErasedFuture<T, const SIZE: usize> {
    futbuf: MaybeUninit<[u8; SIZE]>,
    poll_fn: fn(slf: Pin<&mut Self>, cx: &mut Context) -> Poll<T>,
    drop_fn: fn(slf: &mut Self),
}

impl<T, const SIZE: usize> fmt::Debug for ErasedFuture<T, SIZE> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ErasedFuture").field("SIZE", &SIZE).finish()
    }
}

impl<T, const SIZE: usize> ErasedFuture<T, SIZE> {
    pub fn new<F>(fut: F) -> Self
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
