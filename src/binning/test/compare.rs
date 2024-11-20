use crate::binning::container_bins::ContainerBins;
use std::collections::VecDeque;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "Compare")]
pub enum Error {
    AssertMsg(String),
}

pub(super) trait IntoVecDequeU64 {
    fn into_vec_deque_u64(self) -> VecDeque<u64>;
}

impl IntoVecDequeU64 for &str {
    fn into_vec_deque_u64(self) -> VecDeque<u64> {
        self.split_ascii_whitespace()
            .map(|x| x.parse().unwrap())
            .collect()
    }
}

pub(super) trait IntoVecDequeF32 {
    fn into_vec_deque_f32(self) -> VecDeque<f32>;
}

impl IntoVecDequeF32 for &str {
    fn into_vec_deque_f32(self) -> VecDeque<f32> {
        self.split_ascii_whitespace()
            .map(|x| x.parse().unwrap())
            .collect()
    }
}

fn exp_u64<'a>(
    vals: impl Iterator<Item = &'a u64>,
    exps: impl Iterator<Item = &'a u64>,
    tag: &str,
) -> Result<(), Error> {
    let mut it_a = vals;
    let mut it_b = exps;
    let mut i = 0;
    loop {
        let a = it_a.next();
        let b = it_b.next();
        if a.is_none() && b.is_none() {
            break;
        }
        if let (Some(&val), Some(&exp)) = (a, b) {
            if val != exp {
                return Err(Error::AssertMsg(format!(
                    "{tag}  val {}  exp {}  i {}",
                    val, exp, i
                )));
            }
        } else {
            return Err(Error::AssertMsg(format!("{tag} len mismatch")));
        }
        i += 1;
    }
    Ok(())
}

fn exp_f32<'a>(
    vals: impl Iterator<Item = &'a f32>,
    exps: impl Iterator<Item = &'a f32>,
    tag: &str,
) -> Result<(), Error> {
    let mut it_a = vals;
    let mut it_b = exps;
    let mut i = 0;
    loop {
        let a = it_a.next();
        let b = it_b.next();
        if a.is_none() && b.is_none() {
            break;
        }
        if let (Some(&val), Some(&exp)) = (a, b) {
            if netpod::f32_close(val, exp) == false {
                return Err(Error::AssertMsg(format!(
                    "{tag}  val {}  exp {}  i {}",
                    val, exp, i
                )));
            }
        } else {
            return Err(Error::AssertMsg(format!("{tag} len mismatch")));
        }
        i += 1;
    }
    Ok(())
}

pub(super) fn exp_cnts(
    bins: &ContainerBins<f32, f32>,
    exps: impl IntoVecDequeU64,
) -> Result<(), Error> {
    exp_u64(
        bins.cnts_iter(),
        exps.into_vec_deque_u64().iter(),
        "exp_cnts",
    )
}

pub(super) fn exp_mins(
    bins: &ContainerBins<f32, f32>,
    exps: impl IntoVecDequeF32,
) -> Result<(), Error> {
    exp_f32(
        bins.mins_iter(),
        exps.into_vec_deque_f32().iter(),
        "exp_mins",
    )
}

pub(super) fn exp_maxs(
    bins: &ContainerBins<f32, f32>,
    exps: impl IntoVecDequeF32,
) -> Result<(), Error> {
    exp_f32(
        bins.maxs_iter(),
        exps.into_vec_deque_f32().iter(),
        "exp_maxs",
    )
}

pub(super) fn exp_avgs(
    bins: &ContainerBins<f32, f32>,
    exps: impl IntoVecDequeF32,
) -> Result<(), Error> {
    let exps = exps.into_vec_deque_f32();
    let mut it_a = bins.iter_debug();
    let mut it_b = exps.iter();
    let mut i = 0;
    loop {
        let a = it_a.next();
        let b = it_b.next();
        if a.is_none() && b.is_none() {
            break;
        }
        if let (Some(a), Some(&exp)) = (a, b) {
            let val = *a.agg as f32;
            if netpod::f32_close(val, exp) == false {
                return Err(Error::AssertMsg(format!(
                    "exp_avgs  val {}  exp {}  i {}",
                    val, exp, i
                )));
            }
        } else {
            return Err(Error::AssertMsg(format!(
                "len mismatch  {}  vs  {}",
                bins.len(),
                exps.len()
            )));
        }
        i += 1;
    }
    Ok(())
}
