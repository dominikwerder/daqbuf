use core::fmt;
use std::collections::VecDeque;

pub struct PreviewCell<'a, T> {
    pub a: Option<&'a T>,
    pub b: Option<&'a T>,
}

impl<'a, T> fmt::Debug for PreviewCell<'a, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match (self.a.as_ref(), self.b.as_ref()) {
            (Some(a), Some(b)) => write!(fmt, "{:?} .. {:?}", a, b),
            (Some(a), None) => write!(fmt, "{:?}", a),
            _ => write!(fmt, "(empty)"),
        }
    }
}

pub trait PreviewRange {
    fn preview<'a>(&'a self) -> Box<dyn fmt::Debug + 'a>;
}

impl<T> PreviewRange for VecDeque<T>
where
    T: fmt::Debug,
{
    fn preview<'a>(&'a self) -> Box<dyn fmt::Debug + 'a> {
        let ret = PreviewCell {
            a: self.front(),
            b: if self.len() <= 1 { None } else { self.back() },
        };
        Box::new(ret)
    }
}

pub struct VecPreview<'a> {
    c: &'a dyn PreviewRange,
}

impl<'a> VecPreview<'a> {
    pub fn new(c: &'a dyn PreviewRange) -> Self {
        Self { c }
    }
}

impl<'a> fmt::Debug for VecPreview<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self.c.preview())
    }
}
