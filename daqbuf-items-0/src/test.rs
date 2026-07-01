pub fn f32_cmp_near(x: f32, y: f32, abs: f32, rel: f32) -> bool {
    /*let x = {
        let mut a = x.to_le_bytes();
        a[0] &= 0xf0;
        f32::from_ne_bytes(a)
    };
    let y = {
        let mut a = y.to_le_bytes();
        a[0] &= 0xf0;
        f32::from_ne_bytes(a)
    };
    x == y*/
    let ad = (x - y).abs();
    ad <= abs || (ad / y).abs() <= rel
}

pub fn f64_cmp_near(x: f64, y: f64, abs: f64, rel: f64) -> bool {
    /*let x = {
        let mut a = x.to_le_bytes();
        a[0] &= 0x00;
        a[1] &= 0x00;
        f64::from_ne_bytes(a)
    };
    let y = {
        let mut a = y.to_le_bytes();
        a[0] &= 0x00;
        a[1] &= 0x00;
        f64::from_ne_bytes(a)
    };
    x == y*/
    let ad = (x - y).abs();
    ad <= abs || (ad / y).abs() <= rel
}

pub fn f32_iter_cmp_near<A, B>(a: A, b: B, abs: f32, rel: f32) -> bool
where
    A: IntoIterator<Item = f32>,
    B: IntoIterator<Item = f32>,
{
    let mut a = a.into_iter();
    let mut b = b.into_iter();
    loop {
        let x = a.next();
        let y = b.next();
        if let (Some(x), Some(y)) = (x, y) {
            if !f32_cmp_near(x, y, abs, rel) {
                return false;
            }
        } else if x.is_some() || y.is_some() {
            return false;
        } else {
            return true;
        }
    }
}

pub fn f64_iter_cmp_near<A, B>(a: A, b: B, abs: f64, rel: f64) -> bool
where
    A: IntoIterator<Item = f64>,
    B: IntoIterator<Item = f64>,
{
    let mut a = a.into_iter();
    let mut b = b.into_iter();
    loop {
        let x = a.next();
        let y = b.next();
        if let (Some(x), Some(y)) = (x, y) {
            if !f64_cmp_near(x, y, abs, rel) {
                return false;
            }
        } else if x.is_some() || y.is_some() {
            return false;
        } else {
            return true;
        }
    }
}

#[test]
fn test_f32_iter_cmp_near() {
    let a = [-127.553e17];
    let b = [-127.554e17];
    assert_eq!(f32_iter_cmp_near(a, b, 0.000001, 0.000001), false);
    let a = [-127.55300e17];
    let b = [-127.55301e17];
    assert_eq!(f32_iter_cmp_near(a, b, 0.000001, 0.000001), true);
}
