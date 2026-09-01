use serde::Serialize;

/// Produce a serializable snapshot of a component which is itself not serializable.
///
/// The associated type is what lets the `ToSerde` derive nest components without knowing
/// any field's concrete type: a nested field's mirror type is written as
/// `<Child as ToSerde>::Serde` and resolved by the compiler.
///
/// Derived values (time in state, buffer fill, rates) are computed inside `to_serde`, so
/// they are only paid for when a snapshot is actually taken.
pub trait ToSerde {
    type Serde: Serialize;

    fn to_serde(&self) -> Self::Serde;
}

/// Fill level of a bounded buffer.
///
/// Mirrors the `{len, cap}` shape that the existing `dump_state_poll` trees already emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LenCap {
    pub len: usize,
    pub cap: usize,
}

impl LenCap {
    pub fn new(len: usize, cap: usize) -> Self {
        Self { len, cap }
    }
}

/// Anything with `len` and `capacity`, so `#[to_serde(len)]` works on `VecDeque`, `Vec`,
/// `String`, and on our own buffer types without extra impls.
pub trait HasLenCap {
    fn len_cap(&self) -> LenCap;
}

impl<T> HasLenCap for std::collections::VecDeque<T> {
    fn len_cap(&self) -> LenCap {
        LenCap::new(self.len(), self.capacity())
    }
}

impl<T> HasLenCap for Vec<T> {
    fn len_cap(&self) -> LenCap {
        LenCap::new(self.len(), self.capacity())
    }
}

impl HasLenCap for String {
    fn len_cap(&self) -> LenCap {
        LenCap::new(self.len(), self.capacity())
    }
}

/// Snapshot through a reference-like wrapper.
impl<T> ToSerde for &T
where
    T: ToSerde,
{
    type Serde = T::Serde;

    fn to_serde(&self) -> Self::Serde {
        (**self).to_serde()
    }
}

impl<T> ToSerde for Option<T>
where
    T: ToSerde,
{
    type Serde = Option<T::Serde>;

    fn to_serde(&self) -> Self::Serde {
        self.as_ref().map(|x| x.to_serde())
    }
}

impl<T> ToSerde for Box<T>
where
    T: ToSerde,
{
    type Serde = T::Serde;

    fn to_serde(&self) -> Self::Serde {
        (**self).to_serde()
    }
}
