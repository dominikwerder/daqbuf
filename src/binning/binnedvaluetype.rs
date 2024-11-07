pub trait BinnedValueType {}

pub struct BinnedNumericValue<EVT> {
    avg: f32,
    _t: Option<EVT>,
}

impl<EVT> BinnedValueType for BinnedNumericValue<EVT> {}
