use libc::{c_int, size_t};

extern "C" {
    pub fn bshuf_compress_lz4(
        inp: *const u8,
        out: *const u8,
        size: size_t,
        elem_size: size_t,
        block_size: size_t,
    ) -> i64;

    pub fn bshuf_decompress_lz4(
        inp: *const u8,
        out: *const u8,
        size: size_t,
        elem_size: size_t,
        block_size: size_t,
    ) -> i64;

    pub fn LZ4_decompress_safe(
        source: *const u8,
        dest: *mut u8,
        compressedSize: c_int,
        maxDecompressedSize: c_int,
    ) -> c_int;
}

pub fn bitshuffle_compress(
    inp: &[u8],
    out: &mut [u8],
    size: usize,
    elem_size: usize,
    block_size: usize,
) -> Result<usize, isize> {
    unsafe {
        let n = bshuf_compress_lz4(inp.as_ptr(), out.as_mut_ptr(), size, elem_size, block_size);
        if n >= 0 {
            Ok(n as usize)
        } else {
            Err(n as isize)
        }
    }
}

pub fn bitshuffle_decompress(
    inp: &[u8],
    out: &mut [u8],
    size: usize,
    elem_size: usize,
    block_size: usize,
) -> Result<usize, isize> {
    unsafe {
        let n = bshuf_decompress_lz4(inp.as_ptr(), out.as_mut_ptr(), size, elem_size, block_size);
        if n >= 0 {
            Ok(n as usize)
        } else {
            Err(n as isize)
        }
    }
}

pub fn lz4_decompress(inp: &[u8], out: &mut [u8]) -> Result<usize, isize> {
    let max_out = out.len() as _;
    let ec = unsafe { LZ4_decompress_safe(inp.as_ptr(), out.as_mut_ptr(), inp.len() as _, max_out) };
    if ec < 0 {
        Err(ec as _)
    } else {
        Ok(ec as _)
    }
}

#[cfg(test)]
mod _simd {
    use std::arch::x86_64::_mm_loadu_si128;
    use std::arch::x86_64::_mm_shuffle_epi32;
    use std::ptr::null;

    #[test]
    fn simd_test() {
        if is_x86_feature_detected!("sse") {
            eprintln!("have sse 1");
        }
        if is_x86_feature_detected!("sse2") {
            eprintln!("have sse 2");
            unsafe { simd_sse_2() };
        }
        if is_x86_feature_detected!("sse3") {
            eprintln!("have sse 3");
            unsafe { simd_sse_3() };
        }
        if is_x86_feature_detected!("sse4.1") {
            eprintln!("have sse 4.1");
            unsafe { simd_sse_4_1() };
        }
        if is_x86_feature_detected!("sse4.2") {
            eprintln!("have sse 4.2");
            unsafe { simd_sse_4_2() };
        }
        if is_x86_feature_detected!("sse4a") {
            eprintln!("have sse 4 a");
        }
        if is_x86_feature_detected!("avx") {
            eprintln!("have avx 1");
        }
        if is_x86_feature_detected!("avx2") {
            eprintln!("have avx 2");
        }
    }

    #[target_feature(enable = "sse2")]
    unsafe fn simd_sse_2() {
        // _mm_loadu_si128(null());
        eprintln!("sse 2 done");
    }

    #[target_feature(enable = "sse3")]
    unsafe fn simd_sse_3() {
        // core::arch::asm!();
        // core::arch::global_asm!();
        let a = core::arch::x86_64::_mm256_setzero_si256();
        let b = core::arch::x86_64::_mm256_set_epi32(7, 3, 9, 11, 17, 13, 19, 21);
        let x = core::arch::x86_64::_mm256_xor_si256(a, b);
        core::arch::x86_64::_mm256_loadu_si256(&x as *const _);
        // core::arch::x86_64::vpl!();
        // core::arch::x86_64::vps!();
        // let c = core::arch::x86_64::_mm256_shuffle_i32x4(a, b);
        eprintln!("sse 3 done");
    }

    #[target_feature(enable = "sse4.1")]
    unsafe fn simd_sse_4_1() {
        // _mm_loadu_si128(null());
        eprintln!("sse 4.1 done");
    }

    #[target_feature(enable = "sse4.2")]
    unsafe fn simd_sse_4_2() {
        // _mm_loadu_si128(null());
        eprintln!("sse 4.2 done");
    }
}
