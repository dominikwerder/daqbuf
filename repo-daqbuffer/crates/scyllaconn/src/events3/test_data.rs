use crate::events3::MSP_A_00;
use crate::events3::msplsp::LspEv;
use crate::events3::msplsp::MspEv;
use netpod::DtMs;
use netpod::DtNano;
use netpod::TsMs;
use netpod::TsNano;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::Rng;
use rand_xoshiro::rand_core::SeedableRng;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::OnceLock;

#[allow(unused)]
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }

pub fn pred_ms_range(begj: TsNano, begk: TsNano, end: TsNano) -> impl Fn(&MspEv) -> bool {
    move |ms| (begj == ms.to_ms().ns() || begk < ms.to_ms().ns()) && ms.to_ms().ns() < end
}

#[allow(unused)]
pub fn series_a_msps() -> VecDeque<TsMs> {
    let dt = DtMs::from_ms_u64(1000 * 60 * 60);
    let mut v = MSP_A_00;
    (0..48)
        .into_iter()
        .map(|_| {
            let x = v;
            v = v.add_dt_ms(dt);
            x
        })
        .collect()
}

/*
import datetime
utc = datetime.UTC
datetime.datetime.fromtimestamp(1773841020, utc)
datetime.datetime(2026, 3, 18, 13, 37, tzinfo=datetime.timezone.utc)
*/

pub fn create_msp_lsp_stream(beg: TsNano) -> impl Iterator<Item = (TsNano, MspEv, LspEv, u32, u8)> {
    trace!("beg {beg}  {h}", h = beg.ms() / 1000);
    // TODO use this as the first event timestamp, not the first msp.
    // Derive the msp from the timestamp.
    let t0 = MSP_A_00.ns();
    trace!("msA {MSP_A_00}  {}", MSP_A_00.ms() / 1000);
    trace!("t0  {t0}  {}", t0.ms() / 1000);
    let ivl = DtNano::from_ms(2000);
    let mut i0 = 0;
    if beg > t0 {
        i0 = ((beg.ns() - t0.ns()) / ivl.ns()) as u64;
    }
    trace!("i0 {i0}");
    trace!("allocate");
    let mut rnd1 = vec![0u8; 1024 * 1024 * 20];
    trace!("seed");
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(89163);
    trace!("generate");
    rng.fill_bytes(&mut rnd1);
    let mut msps = BTreeMap::new();
    {
        // produce a random distribution of msp.
        trace!("alloc msp_off_secs");
        let mut msp_off_ms = vec![0u32; 100];
        let a = &mut msp_off_ms;
        let h = size_of_val(&a[0]);
        assert_eq!(h, 4);
        let src = rnd1[1024 * 700..].as_ptr();
        let dst = a.as_mut_ptr() as *mut u8;
        trace!("memcpy");
        unsafe {
            std::ptr::copy_nonoverlapping(src, dst, a.len() * h);
        }
        for x in a.iter().take(0) {
            trace!("msp_off_ms {x}");
        }
        trace!("converting...");
        for x in a.iter() {
            let x = *x as u64;
            let dt = DtMs::from_ms_u64((x % 200000000) / 1000 * 1000);
            let msp = TsMs::from_ms_u64(MSP_A_00.ms() - 1000 * 60 * 60 * 4).add_dt_ms(dt);
            msps.insert(msp, 0u32);
        }
        for x in msps.keys().take(0) {
            trace!("msp {x}");
        }
    }
    let msps_slice: Vec<_> = msps.keys().cloned().collect();
    (i0..u64::MAX).into_iter().map(move |i| {
        let ts = t0.add_dt_nano(ivl.mul_u64(i));
        let tsms = ts.to_ts_ms();
        let n1 = msps_slice.partition_point(|x| *x <= tsms);
        let nb = ((rnd1[i as usize] as u32).pow(2) / (256 * 256 / 2)) as u8;
        let n2 = n1 - 1 - (nb as usize).min(n1 - 1);
        let msp = msps_slice[n2];
        let msp = MspEv::from(msp);
        let lsp = msp.lsp(ts).unwrap();
        (ts, msp, lsp, i as u32, nb)
    })
}

#[test]
fn test_assign() {
    for (ts, msp, lsp, val, nb) in create_msp_lsp_stream("2026-03-18T13:37:10.000Z".parse().unwrap()).take(8000) {
        let _ = lsp;
        let _ = nb;
        info!("{ts}  {msp}  {val:9}");
    }
}

pub struct FullEventSet {
    pub by_msp: BTreeMap<MspEv, Vec<(TsNano, LspEv, u32, u8)>>,
}

static FULL_EVENT_SET: OnceLock<FullEventSet> = OnceLock::new();

pub fn produce_full_event_set() -> &'static FullEventSet {
    FULL_EVENT_SET.get_or_init(|| FullEventSet {
        by_msp: {
            let beg: TsNano = "2026-03-18T13:37:10.000Z".parse().unwrap();
            let end: TsNano = "2026-03-24T13:37:10.000Z".parse().unwrap();
            let mut by_msp = BTreeMap::<_, Vec<(TsNano, LspEv, u32, u8)>>::new();
            create_msp_lsp_stream(beg)
                .take_while(|(ts, ..)| *ts < end)
                .for_each(|(ts, msp, lsp, val, nb)| {
                    by_msp
                        .entry(msp)
                        .and_modify(|e| {
                            e.push((ts, lsp, val, nb));
                        })
                        .or_insert(vec![(ts, lsp, val, nb)]);
                });
            by_msp
        },
    })
}

#[test]
fn test_full_event_set() {
    let _beg: TsNano = "2026-03-18T00:00:00.000Z".parse().unwrap();
    let _end: TsNano = "2026-03-24T16:00:00.000Z".parse().unwrap();
    let evs = produce_full_event_set();
    for (msp, v) in evs.by_msp.iter() {
        for (ts, lsp, val, nb) in v {
            let s = format!("{ts}  {msp}  {lsp}  {val:9}  {nb:2}");
            assert!(s.len() > 7);
        }
    }
}
