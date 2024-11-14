use crate::binning::container_bins::ContainerBins;
use crate::binning::container_events::ContainerEvents;
use crate::binning::timeweight::timeweight_events::BinnedEventsTimeweight;
use daqbuf_err as err;
use err::thiserror;
use err::ThisError;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRange;
use netpod::DtMs;
use netpod::EnumVariant;
use netpod::TsNano;
use std::collections::VecDeque;

#[derive(Debug, ThisError)]
#[cstm(name = "Error")]
enum Error {
    Timeweight(#[from] crate::binning::timeweight::timeweight_events::Error),
    AssertMsg(String),
}

// fn prepare_data_with_cuts(beg_ms: u64, cuts: VecDeque<u64>) -> VecDeque<ContainerEvents<f32>> {
//     let beg = TsNano::from_ms(beg_ms);
//     let end = TsNano::from_ms(120);
//     let mut cut_next = cuts.pop_front().unwrap_or(u64::MAX);
//     let mut ret = VecDeque::new();
//     let ivl = DtMs::from_ms_u64(x)
// }

fn pu(c: &mut ContainerEvents<f32>, ts_ms: u64, val: f32)
// where
// C: AsMut<ContainerEvents<f32>>,
// C: std::borrow::BorrowMut<ContainerEvents<f32>>,
{
    c.push_back(TsNano::from_ms(ts_ms), val);
}

trait IntoVecDequeU64 {
    fn into_vec_deque_u64(self) -> VecDeque<u64>;
}

impl IntoVecDequeU64 for &str {
    fn into_vec_deque_u64(self) -> VecDeque<u64> {
        self.split_ascii_whitespace()
            .map(|x| x.parse().unwrap())
            .collect()
    }
}
trait IntoVecDequeF32 {
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

#[cfg(test)]
fn exp_cnts(bins: &ContainerBins<f32>, exps: impl IntoVecDequeU64) -> Result<(), Error> {
    exp_u64(
        bins.cnts_iter(),
        exps.into_vec_deque_u64().iter(),
        "exp_cnts",
    )
}

#[cfg(test)]
fn exp_mins(bins: &ContainerBins<f32>, exps: impl IntoVecDequeF32) -> Result<(), Error> {
    exp_f32(
        bins.mins_iter(),
        exps.into_vec_deque_f32().iter(),
        "exp_mins",
    )
}

#[cfg(test)]
fn exp_maxs(bins: &ContainerBins<f32>, exps: impl IntoVecDequeF32) -> Result<(), Error> {
    exp_f32(
        bins.maxs_iter(),
        exps.into_vec_deque_f32().iter(),
        "exp_maxs",
    )
}

fn exp_avgs(bins: &ContainerBins<f32>, exps: impl IntoVecDequeF32) -> Result<(), Error> {
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
            let val = *a.avg as f32;
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

#[test]
fn test_bin_events_f32_simple_with_before_00() -> Result<(), Error> {
    let beg = TsNano::from_ms(110);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    evs.push_back(TsNano::from_ms(103), 2.0);
    binner.ingest(&evs)?;
    binner.input_done_range_final()?;
    let bins = binner.output();
    exp_cnts(&bins, "0")?;
    exp_mins(&bins, "2.")?;
    exp_maxs(&bins, "2.")?;
    exp_avgs(&bins, "2.")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_simple_with_before_01_range_final() -> Result<(), Error> {
    let beg = TsNano::from_ms(110);
    let end = TsNano::from_ms(130);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 103, 2.0);
    binner.ingest(&evs)?;
    binner.input_done_range_final()?;
    let bins = binner.output();
    exp_cnts(&bins, "0  0")?;
    exp_mins(&bins, "2.  2.")?;
    exp_maxs(&bins, "2.  2.")?;
    exp_avgs(&bins, "2.  2.")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_simple_00() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 100, 2.0);
    pu(em, 104, 2.4);
    binner.ingest(&evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 111, 1.0);
    pu(em, 112, 1.2);
    pu(em, 113, 1.4);
    binner.ingest(&evs)?;
    binner.input_done_range_open()?;
    let bins = binner.output();
    for b in bins.iter_debug() {
        trace!("{b:?}");
    }
    exp_cnts(&bins, "2     3")?;
    exp_mins(&bins, "2.    1.")?;
    exp_maxs(&bins, "2.4   2.4")?;
    exp_avgs(&bins, "2.24  1.5333")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_simple_01() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 102, 2.0);
    pu(em, 104, 2.4);
    binner.ingest(&evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 111, 1.0);
    pu(em, 112, 1.2);
    pu(em, 113, 1.4);
    binner.ingest(&evs)?;
    binner.input_done_range_open()?;
    let bins = binner.output();
    for b in bins.iter_debug() {
        trace!("{b:?}");
    }
    assert_eq!(bins.len(), 2);
    exp_cnts(&bins, "2     3")?;
    exp_mins(&bins, "2.    1.")?;
    exp_maxs(&bins, "2.4   2.4")?;
    exp_avgs(&bins, "2.30  1.5333")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_small_range_final() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 102, 2.0);
    pu(em, 104, 2.4);
    binner.ingest(&evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 111, 1.0);
    pu(em, 112, 1.2);
    pu(em, 113, 1.4);
    binner.ingest(&evs)?;
    binner.input_done_range_final()?;
    let bins = binner.output();
    for b in bins.iter_debug() {
        trace!("{b:?}");
    }
    assert_eq!(bins.len(), 2);
    exp_cnts(&bins, "2  3")?;
    exp_mins(&bins, "2.    1.")?;
    exp_maxs(&bins, "2.4   2.4")?;
    exp_avgs(&bins, "2.30  1.44")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_small_intermittent_silence_range_open() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(150);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 102, 2.0);
    pu(em, 104, 2.4);
    binner.ingest(&evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 111, 1.0);
    pu(em, 112, 1.2);
    binner.ingest(&evs)?;
    // TODO take bins already here and assert.
    // TODO combine all bins together for combined assert.
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 113, 1.4);
    pu(em, 146, 1.3);
    pu(em, 148, 1.2);
    binner.ingest(&evs)?;
    binner.input_done_range_open()?;
    let bins = binner.output();
    for b in bins.iter_debug() {
        trace!("{b:?}");
    }
    assert_eq!(bins.len(), 5);
    exp_cnts(&bins, "2  3  0  0  2")?;
    exp_mins(&bins, "2.0   1.0   1.4   1.4   1.2")?;
    exp_maxs(&bins, "2.4   2.4   1.4   1.4   1.4")?;
    exp_avgs(&bins, "2.30  1.44  1.4   1.4   1.375")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_small_intermittent_silence_range_final() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(150);
    let bin_len = DtMs::from_ms_u64(10);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, bin_len);
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 102, 2.0);
    pu(em, 104, 2.4);
    binner.ingest(&evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 111, 1.0);
    pu(em, 112, 1.2);
    binner.ingest(&evs)?;
    // TODO take bins already here and assert.
    // TODO combine all bins together for combined assert.
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 113, 1.4);
    pu(em, 146, 1.3);
    pu(em, 148, 1.2);
    binner.ingest(&evs)?;
    binner.input_done_range_final()?;
    let bins = binner.output();
    for b in bins.iter_debug() {
        trace!("{b:?}");
    }
    exp_cnts(&bins, "2  3  0  0  2")?;
    exp_mins(&bins, "2.0   1.0   1.4   1.4   1.2")?;
    exp_maxs(&bins, "2.4   2.4   1.4   1.4   1.4")?;
    exp_avgs(&bins, "2.30  1.44  1.4  1.4  1.34")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_small_intermittent_silence_minmax_no_edge_range_final() -> Result<(), Error>
{
    let beg = TsNano::from_ms(110);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 109, 50.);
    binner.ingest(&evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 111, 40.);
    // pu(em, 112, 1.2);
    // binner.ingest(evs)?;
    // let mut evs = ContainerEvents::<f32>::new();
    // let em = &mut evs;
    // pu(em, 113, 1.4);
    // pu(em, 120, 1.4);
    // pu(em, 146, 1.3);
    // pu(em, 148, 1.2);
    binner.ingest(&evs)?;
    binner.input_done_range_final()?;
    let bins = binner.output();
    for b in bins.iter_debug() {
        trace!("{b:?}");
    }
    exp_cnts(&bins, "1")?;
    exp_mins(&bins, "40.")?;
    exp_maxs(&bins, "50.")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_f32_small_intermittent_silence_minmax_edge_range_final() -> Result<(), Error> {
    let beg = TsNano::from_ms(110);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 109, 50.);
    binner.ingest(&evs)?;
    let mut evs = ContainerEvents::<f32>::new();
    let em = &mut evs;
    pu(em, 110, 40.);
    // pu(em, 112, 1.2);
    // binner.ingest(evs)?;
    // let mut evs = ContainerEvents::<f32>::new();
    // let em = &mut evs;
    // pu(em, 113, 1.4);
    // pu(em, 120, 1.4);
    // pu(em, 146, 1.3);
    // pu(em, 148, 1.2);
    binner.ingest(&evs)?;
    binner.input_done_range_final()?;
    let bins = binner.output();
    for b in bins.iter_debug() {
        trace!("{b:?}");
    }
    exp_cnts(&bins, "1")?;
    exp_mins(&bins, "40.")?;
    exp_maxs(&bins, "40.")?;
    let bins = binner.output();
    assert_eq!(bins.len(), 0);
    Ok(())
}

#[test]
fn test_bin_events_enum_simple_range_final() -> Result<(), Error> {
    let beg = TsNano::from_ms(100);
    let end = TsNano::from_ms(120);
    let nano_range = NanoRange {
        beg: beg.ns(),
        end: end.ns(),
    };
    let range = BinnedRange::from_nano_range(nano_range, DtMs::from_ms_u64(10));
    let mut binner = BinnedEventsTimeweight::new(range);
    let mut evs = ContainerEvents::new();
    evs.push_back(TsNano::from_ms(103), EnumVariant::new(1, "one"));
    evs.push_back(TsNano::from_ms(104), EnumVariant::new(2, "two"));
    binner.ingest(&evs)?;
    binner.input_done_range_final()?;
    let bins = binner.output();
    Ok(())
}
