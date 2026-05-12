use serde::Serialize;
use serde::ser::SerializeStruct;
use std::collections::VecDeque;
use std::fmt;
use std::time::Instant;

#[derive(Debug)]
enum Kind {
    Hit,
    Ready,
    Some,
    None,
    Pending,
    Skip,
    Gone,
}

impl Serialize for Kind {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Kind::Hit => ser.serialize_str("Hit"),
            Kind::Ready => ser.serialize_str("Ready"),
            Kind::Some => ser.serialize_str("Some"),
            Kind::None => ser.serialize_str("None"),
            Kind::Pending => ser.serialize_str("Pending"),
            Kind::Skip => ser.serialize_str("Skip"),
            Kind::Gone => ser.serialize_str("Gone"),
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, fmt)
    }
}

#[derive(Debug)]
pub struct TsMark {
    name: String,
    tss: VecDeque<(Instant, Kind)>,
}

impl TsMark {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tss: VecDeque::new(),
        }
    }

    fn hit(&mut self, kind: Kind) {
        if self.tss.len() >= 10 {
            self.tss.pop_front();
        }
        self.tss.push_back((Instant::now(), kind));
    }

    pub fn hit_ready(&mut self) {
        self.hit(Kind::Ready);
    }

    pub fn hit_some(&mut self) {
        self.hit(Kind::Some);
    }

    pub fn hit_none(&mut self) {
        self.hit(Kind::None);
    }

    pub fn hit_pending(&mut self) {
        self.hit(Kind::Pending);
    }

    pub fn hit_skip(&mut self) {
        self.hit(Kind::Skip);
    }

    pub fn hit_gone(&mut self) {
        self.hit(Kind::Gone);
    }
}

impl Serialize for TsMark {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // SerializeStruct::skip_field(&mut self, key)
        let mut a = ser.serialize_struct("TsMark", 2)?;
        a.serialize_field("name", &self.name)?;
        let mut a2 = Vec::new();
        for (ts, kind) in self.tss.iter().map(|(ts, kind)| (ts.elapsed(), kind)) {
            let dt = (1e3 * ts.as_secs_f32()) as u32;
            a2.push(format!("{dt:5}  {kind}"));
        }
        a.serialize_field("tss", &a2)?;
        a.end()
    }
}

#[must_use]
pub enum PushRes<T> {
    First,
    Done,
    Full(T),
}

impl<T> PushRes<T> {
    pub fn is_fail(&self) -> bool {
        match self {
            PushRes::First => false,
            PushRes::Done => false,
            PushRes::Full(_) => true,
        }
    }
}

#[derive(Debug)]
pub struct AsynBuf<T> {
    buf: VecDeque<T>,
}

impl<T> AsynBuf<T> {
    pub fn new(cap: usize) -> Self {
        Self {
            buf: VecDeque::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn cap(&self) -> usize {
        self.buf.capacity()
    }

    pub fn is_space(&self) -> bool {
        self.len() < self.cap()
    }

    pub fn push_back(&mut self, x: T) -> PushRes<T> {
        if self.buf.len() == 0 {
            self.buf.push_back(x);
            PushRes::First
        } else if self.buf.len() < self.buf.capacity() {
            self.buf.push_back(x);
            PushRes::Done
        } else {
            PushRes::Full(x)
        }
    }

    pub fn push_front(&mut self, x: T) {
        self.buf.push_front(x)
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.buf.pop_front()
    }
}
