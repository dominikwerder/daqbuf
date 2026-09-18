use super::Error;
use super::field::PvaStruct;
use super::value::PvaDelta;
use super::value::PvaStructValue;
use super::value::PvaValue;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct PvaMonitorMerge {
    cur: PvaStructValue,
    seen: u64,
}

impl PvaMonitorMerge {
    pub fn new(ty: Arc<PvaStruct>) -> Self {
        Self {
            cur: PvaStructValue::zeroed(&ty),
            seen: 0,
        }
    }

    pub fn ty(&self) -> &Arc<PvaStruct> {
        self.cur.ty()
    }

    pub fn applied_count(&self) -> u64 {
        self.seen
    }

    pub fn has_value(&self) -> bool {
        self.seen > 0
    }

    pub fn apply(&mut self, delta: &PvaDelta) -> Result<(), Error> {
        if !Arc::ptr_eq(delta.ty(), self.cur.ty()) && delta.ty() != self.cur.ty() {
            return Err(Error::DeltaTypeMismatch);
        }
        for (off, v) in delta.entries() {
            if *off == 0 {
                match v {
                    PvaValue::Struct(x) => self.cur = x.clone(),
                    _ => return Err(Error::DeltaTypeMismatch),
                }
            } else {
                let slot = self.cur.node_mut(*off).ok_or(Error::BadNodeOffset(*off))?;
                *slot = v.clone();
            }
        }
        self.seen += 1;
        Ok(())
    }

    pub fn value(&self) -> &PvaStructValue {
        &self.cur
    }

    pub fn into_value(self) -> PvaStructValue {
        self.cur
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pva::field::IntroRegistry;
    use crate::pva::field::test::nt_scalar_f64;
    use crate::pva::pvdata::BitSet;
    use crate::pva::pvdata::Endian;
    use crate::pva::pvdata::Reader;
    use crate::pva::pvdata::Writer;

    fn enc(f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut buf = Vec::new();
        f(&mut Writer::new(&mut buf, Endian::Little));
        buf
    }

    fn delta_of(ty: &Arc<PvaStruct>, paths: &[&str], b: &[u8]) -> PvaDelta {
        let mut changed = BitSet::new();
        for p in paths {
            if p.is_empty() {
                changed.set(0);
            } else {
                changed.set(ty.offset_of_path(p).unwrap());
            }
        }
        let mut r = Reader::new(b, Endian::Little);
        let mut reg = IntroRegistry::new();
        let d = PvaStructValue::decode_partial(ty, &changed, &mut r, &mut reg, usize::MAX).unwrap();
        assert_eq!(r.remaining(), 0);
        d
    }

    #[test]
    fn merge_starts_zeroed() {
        let ty = nt_scalar_f64();
        let m = PvaMonitorMerge::new(ty.clone());
        assert!(!m.has_value());
        assert_eq!(m.applied_count(), 0);
        assert_eq!(m.value().get("value"), Some(&PvaValue::F64(0.)));
        assert_eq!(m.value().get("alarm.message"), Some(&PvaValue::String(String::new())));
        assert!(Arc::ptr_eq(m.ty(), &ty));
    }

    #[test]
    fn merge_full_then_deltas() {
        let ty = nt_scalar_f64();
        let mut m = PvaMonitorMerge::new(ty.clone());
        let full = delta_of(
            &ty,
            &[""],
            &enc(|w| {
                w.f64(1.5);
                w.i32(1);
                w.i32(2);
                w.string("hi");
                w.i64(100);
                w.i32(200);
                w.i32(0);
            }),
        );
        assert!(full.is_full());
        m.apply(&full).unwrap();
        assert!(m.has_value());
        assert_eq!(m.value().get("alarm.message"), Some(&PvaValue::String("hi".into())));

        let d = delta_of(
            &ty,
            &["value", "timeStamp"],
            &enc(|w| {
                w.f64(2.5);
                w.i64(101);
                w.i32(0);
                w.i32(0);
            }),
        );
        m.apply(&d).unwrap();
        assert_eq!(m.value().get("value"), Some(&PvaValue::F64(2.5)));
        assert_eq!(m.value().get("timeStamp.secondsPastEpoch"), Some(&PvaValue::I64(101)));
        assert_eq!(m.value().get("alarm.message"), Some(&PvaValue::String("hi".into())));
        assert_eq!(m.value().get("alarm.severity"), Some(&PvaValue::I32(1)));

        let d = delta_of(&ty, &["alarm.severity"], &enc(|w| w.i32(3)));
        m.apply(&d).unwrap();
        assert_eq!(m.value().get("alarm.severity"), Some(&PvaValue::I32(3)));
        assert_eq!(m.value().get("alarm.status"), Some(&PvaValue::I32(2)));
        assert_eq!(m.value().get("value"), Some(&PvaValue::F64(2.5)));
        assert_eq!(m.applied_count(), 3);
    }

    #[test]
    fn merge_rejects_foreign_type() {
        let ty = nt_scalar_f64();
        let other = Arc::new(PvaStruct::new(
            "other:1.0".into(),
            vec!["x".into()],
            vec![crate::pva::field::PvaField::Scalar(
                crate::pva::field::PvaScalarType::I32,
            )],
        ));
        let d = delta_of(&other, &["x"], &enc(|w| w.i32(1)));
        let mut m = PvaMonitorMerge::new(ty);
        assert!(matches!(m.apply(&d), Err(Error::DeltaTypeMismatch)));
    }

    #[test]
    fn merge_accepts_structurally_equal_type() {
        let d = delta_of(&nt_scalar_f64(), &["value"], &enc(|w| w.f64(4.5)));
        let mut m = PvaMonitorMerge::new(nt_scalar_f64());
        m.apply(&d).unwrap();
        assert_eq!(m.into_value().get("value"), Some(&PvaValue::F64(4.5)));
    }
}
