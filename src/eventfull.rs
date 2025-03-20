use crate::framable::FrameType;
use bytes::BytesMut;
use core::ops::Range;
use daqbuf_err as err;
use err::ThisError;
use err::thiserror;
use items_0::Empty;
use items_0::WithLen;
use items_0::container::ByteEstimate;
use items_0::framable::FrameTypeInnerStatic;
use items_0::merge::DrainIntoDstResult;
use items_0::merge::DrainIntoNewResult;
use items_0::merge::MergeableTy;
use items_0::streamitem::EVENT_FULL_FRAME_TYPE_ID;
use netpod::ScalarType;
use netpod::Shape;
use netpod::TsNano;
#[allow(unused)]
use netpod::log::*;
use parse::channelconfig::CompressionMethod;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::time::Instant;

macro_rules! trace2 { ($($arg:tt)*) => ( trace!($($arg)*); ) }

#[derive(Debug, Serialize, Deserialize)]
pub struct EventFull {
    pub tss: VecDeque<u64>,
    pub pulses: VecDeque<u64>,
    pub blobs: VecDeque<Vec<u8>>,
    //#[serde(with = "decomps_serde")]
    pub scalar_types: VecDeque<ScalarType>,
    pub be: VecDeque<bool>,
    pub shapes: VecDeque<Shape>,
    pub comps: VecDeque<Option<CompressionMethod>>,
    pub entry_payload_max: u64,
}

#[allow(unused)]
mod decomps_serde {
    use super::*;

    pub fn serialize<S>(t: &VecDeque<Option<BytesMut>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let a: Vec<_> = t
            .iter()
            .map(|k| match k {
                None => None,
                Some(j) => Some(j[..].to_vec()),
            })
            .collect();
        Serialize::serialize(&a, s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<VecDeque<Option<BytesMut>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let a: Vec<Option<Vec<u8>>> = Deserialize::deserialize(d)?;
        let a = a
            .iter()
            .map(|k| match k {
                None => None,
                Some(j) => {
                    let mut a = BytesMut::new();
                    a.extend_from_slice(&j);
                    Some(a)
                }
            })
            .collect();
        Ok(a)
    }
}

impl EventFull {
    pub fn push(
        &mut self,
        ts: u64,
        pulse: u64,
        blob: Vec<u8>,
        scalar_type: ScalarType,
        be: bool,
        shape: Shape,
        comp: Option<CompressionMethod>,
    ) {
        let m1 = blob.len();
        self.entry_payload_max = self.entry_payload_max.max(m1 as u64);
        self.tss.push_back(ts);
        self.pulses.push_back(pulse);
        self.blobs.push_back(blob);
        self.scalar_types.push_back(scalar_type);
        self.be.push_back(be);
        self.shapes.push_back(shape);
        self.comps.push_back(comp);
    }

    // TODO possible to get rid of this?
    pub fn truncate_ts(&mut self, end: u64) {
        let mut nkeep = usize::MAX;
        for (i, &ts) in self.tss.iter().enumerate() {
            if ts >= end {
                nkeep = i;
                break;
            }
        }
        self.tss.truncate(nkeep);
        self.pulses.truncate(nkeep);
        self.blobs.truncate(nkeep);
        self.scalar_types.truncate(nkeep);
        self.be.truncate(nkeep);
        self.shapes.truncate(nkeep);
        self.comps.truncate(nkeep);
    }

    // NOTE needed because the databuffer actually doesn't write the correct shape per event.
    pub fn overwrite_all_shapes(&mut self, shape: &Shape) {
        for u in &mut self.shapes {
            *u = shape.clone();
        }
    }

    pub fn pop_back(&mut self) {
        self.tss.pop_back();
        self.pulses.pop_back();
        self.blobs.pop_back();
        self.scalar_types.pop_back();
        self.be.pop_back();
        self.shapes.pop_back();
        self.comps.pop_back();
    }

    pub fn keep_ixs(&mut self, ixs: &[bool]) {
        fn inner<T>(v: &mut VecDeque<T>, ixs: &[bool]) {
            let mut it = ixs.iter();
            v.retain_mut(move |_| it.next().map(Clone::clone).unwrap_or(false));
        }
        inner(&mut self.tss, ixs);
        inner(&mut self.pulses, ixs);
        inner(&mut self.blobs, ixs);
        inner(&mut self.scalar_types, ixs);
        inner(&mut self.be, ixs);
        inner(&mut self.shapes, ixs);
        inner(&mut self.comps, ixs);
    }
}

impl FrameTypeInnerStatic for EventFull {
    const FRAME_TYPE_ID: u32 = EVENT_FULL_FRAME_TYPE_ID;
}

impl FrameType for EventFull {
    fn frame_type_id(&self) -> u32 {
        <Self as FrameTypeInnerStatic>::FRAME_TYPE_ID
    }
}

impl Empty for EventFull {
    fn empty() -> Self {
        Self {
            tss: VecDeque::new(),
            pulses: VecDeque::new(),
            blobs: VecDeque::new(),
            scalar_types: VecDeque::new(),
            be: VecDeque::new(),
            shapes: VecDeque::new(),
            comps: VecDeque::new(),
            entry_payload_max: 0,
        }
    }
}

impl WithLen for EventFull {
    fn len(&self) -> usize {
        self.tss.len()
    }
}

impl ByteEstimate for EventFull {
    fn byte_estimate(&self) -> u32 {
        self.len() as u32 * (64 + self.entry_payload_max as u32)
    }
}

impl MergeableTy for EventFull {
    fn ts_min(&self) -> Option<TsNano> {
        self.tss.front().map(|&x| TsNano::from_ns(x))
    }

    fn ts_max(&self) -> Option<TsNano> {
        self.tss.back().map(|&x| TsNano::from_ns(x))
    }

    fn drain_into(&mut self, dst: &mut Self, range: Range<usize>) -> DrainIntoDstResult {
        // TODO make it harder to forget new members when the struct may get modified in the future
        let r = range;
        let mut max = dst.entry_payload_max;
        for i in r.clone() {
            max = max.max(self.blobs[i].len() as _);
        }
        dst.entry_payload_max = max;
        dst.tss.extend(self.tss.drain(r.clone()));
        dst.pulses.extend(self.pulses.drain(r.clone()));
        dst.blobs.extend(self.blobs.drain(r.clone()));
        dst.scalar_types.extend(self.scalar_types.drain(r.clone()));
        dst.be.extend(self.be.drain(r.clone()));
        dst.shapes.extend(self.shapes.drain(r.clone()));
        dst.comps.extend(self.comps.drain(r.clone()));
        DrainIntoDstResult::Done
    }

    fn drain_into_new(&mut self, range: Range<usize>) -> DrainIntoNewResult<Self> {
        let mut dst = Self::empty();
        match self.drain_into(&mut dst, range) {
            DrainIntoDstResult::Done => DrainIntoNewResult::Done(dst),
            DrainIntoDstResult::Partial => DrainIntoNewResult::Partial(dst),
            DrainIntoDstResult::NotCompatible => DrainIntoNewResult::NotCompatible,
        }
    }

    fn find_lowest_index_gt(&self, ts: TsNano) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m > ts.ns() {
                return Some(i);
            }
        }
        None
    }

    fn find_lowest_index_ge(&self, ts: TsNano) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate() {
            if m >= ts.ns() {
                return Some(i);
            }
        }
        None
    }

    fn find_highest_index_lt(&self, ts: TsNano) -> Option<usize> {
        for (i, &m) in self.tss.iter().enumerate().rev() {
            if m < ts.ns() {
                return Some(i);
            }
        }
        None
    }

    fn tss_for_testing(&self) -> VecDeque<TsNano> {
        self.tss.iter().map(|&x| TsNano::from_ns(x)).collect()
    }

    fn is_strict_monotonic(&self) -> bool {
        let mut mono = true;
        let n = self.tss.len();
        for (&ts_a, &ts_b) in self.tss.iter().zip(self.tss.range(n.min(1)..n)) {
            if ts_a >= ts_b {
                mono = false;
                error!("non-monotonic event data  ts1 {}  ts2 {}", ts_a, ts_b);
                break;
            }
        }
        mono
    }

    fn is_consistent(&self) -> bool {
        let mut good = true;
        good &= MergeableTy::is_strict_monotonic(self);
        good
    }
}

#[derive(Debug, ThisError, Serialize, Deserialize)]
#[cstm(name = "Decompress")]
pub enum DecompError {
    TooLittleInput,
    BadCompresionBlockSize,
    UnusedBytes,
    BitshuffleError,
    ShapeMakesNoSense,
    UnexpectedCompressedScalarValue,
}

fn decompress(databuf: &[u8], type_size: u32) -> Result<Vec<u8>, DecompError> {
    // TODO collect decompression stats
    let ts1 = Instant::now();
    if databuf.len() < 12 {
        return Err(DecompError::TooLittleInput);
    }
    let value_bytes = u64::from_be_bytes(databuf[0..8].try_into().unwrap());
    let block_size = u32::from_be_bytes(databuf[8..12].try_into().unwrap());
    trace2!(
        "decompress  len {}  value_bytes {}  block_size {}",
        databuf.len(),
        value_bytes,
        block_size
    );
    if block_size > 1024 * 32 {
        return Err(DecompError::BadCompresionBlockSize);
    }
    let ele_count = value_bytes / type_size as u64;
    trace2!("ele_count {}", ele_count);
    let mut decomp: Vec<u8> = Vec::with_capacity(type_size as usize * ele_count as usize);
    unsafe {
        decomp.set_len(decomp.capacity());
    }
    match bitshuffle::bitshuffle_decompress(
        &databuf[12..],
        &mut decomp,
        ele_count as _,
        type_size as _,
        0,
    ) {
        Ok(c1) => {
            if 12 + c1 != databuf.len() {
                Err(DecompError::UnusedBytes)
            } else {
                let ts2 = Instant::now();
                let _dt = ts2.duration_since(ts1);
                // TODO analyze the histo
                //self.decomp_dt_histo.ingest(dt.as_secs() as u32 + dt.subsec_micros());
                Ok(decomp)
            }
        }
        Err(_) => Err(DecompError::BitshuffleError),
    }
    // todo!("bitshuffle not available")
}

impl EventFull {
    /// Tries to infer the actual shape of the event from what's on disk and what we expect.
    /// The event data on disk usually always indicate "scalar" even for waveforms.
    /// If the data is compressed via bslz4 then we can infer the number of elements
    /// but we still don't know whether that's an image or a waveform.
    /// Therefore, the function accepts the expected shape to at least make an assumption
    /// about whether this is an image or a waveform.
    pub fn shape_derived(
        &self,
        i: usize,
        scalar_type_exp: &ScalarType,
        shape_exp: &Shape,
    ) -> Result<Shape, DecompError> {
        match shape_exp {
            Shape::Scalar => match &self.comps[i] {
                Some(_) => match scalar_type_exp {
                    ScalarType::STRING => Ok(Shape::Scalar),
                    _ => Err(DecompError::UnexpectedCompressedScalarValue),
                },
                None => Ok(Shape::Scalar),
            },
            Shape::Wave(_) => match &self.shapes[i] {
                Shape::Scalar => match &self.comps[i] {
                    Some(comp) => match comp {
                        CompressionMethod::BitshuffleLZ4 => {
                            let type_size = self.scalar_types[i].bytes() as u32;
                            match self.blobs[i][0..8].try_into() {
                                Ok(a) => {
                                    let value_bytes = u64::from_be_bytes(a);
                                    let value_bytes = value_bytes as u32;
                                    if value_bytes % type_size != 0 {
                                        Err(DecompError::ShapeMakesNoSense)
                                    } else {
                                        let n = value_bytes / type_size;
                                        // Here we still can't know whether the disk contains a waveform or image
                                        // so we assume that the user input is correct:
                                        Ok(Shape::Wave(n))
                                    }
                                }
                                Err(_) => Err(DecompError::ShapeMakesNoSense),
                            }
                        }
                    },
                    None => Err(DecompError::ShapeMakesNoSense),
                },
                Shape::Wave(s) => Ok(Shape::Wave(s.clone())),
                Shape::Image(_, _) => Err(DecompError::ShapeMakesNoSense),
            },
            Shape::Image(a, b) => match &self.shapes[i] {
                Shape::Scalar => match &self.comps[i] {
                    Some(comp) => match comp {
                        CompressionMethod::BitshuffleLZ4 => {
                            let type_size = self.scalar_types[i].bytes() as u32;
                            match self.blobs[i][0..8].try_into() {
                                Ok(vb) => {
                                    let value_bytes = u64::from_be_bytes(vb);
                                    let value_bytes = value_bytes as u32;
                                    if value_bytes % type_size != 0 {
                                        Err(DecompError::ShapeMakesNoSense)
                                    } else {
                                        let n = value_bytes / type_size;
                                        // Here we still can't know whether the disk contains a waveform or image
                                        // so we assume that the user input is correct.
                                        // NOTE
                                        // We only know the number of pixels from the compressed blob but we can't
                                        // know the actual shape.
                                        // Can only rely on user input and check that total number of pixels agree.
                                        if *a * *b != n {
                                            Err(DecompError::ShapeMakesNoSense)
                                        } else {
                                            Ok(Shape::Image(*a, *b))
                                        }
                                    }
                                }
                                Err(_) => Err(DecompError::ShapeMakesNoSense),
                            }
                        }
                    },
                    None => Err(DecompError::ShapeMakesNoSense),
                },
                Shape::Wave(_) => Err(DecompError::ShapeMakesNoSense),
                Shape::Image(a, b) => Ok(Shape::Image(*a, *b)),
            },
        }
    }

    pub fn data_raw(&self, i: usize) -> &[u8] {
        &self.blobs[i]
    }

    pub fn data_decompressed(&self, i: usize) -> Result<Cow<[u8]>, DecompError> {
        if let Some(comp) = &self.comps[i] {
            match comp {
                CompressionMethod::BitshuffleLZ4 => {
                    // NOTE the event data on databuffer disk seems to contain the correct scalar type
                    // but the shape of the event record seems always "scalar" even for waveforms
                    // so we must derive the shape of the compressed data from the length of the
                    // uncompressed byte blob and the byte size of the scalar type.
                    let type_size = self.scalar_types[i].bytes() as u32;
                    let data = decompress(&self.blobs[i], type_size)?;
                    Ok(Cow::Owned(data))
                }
            }
        } else {
            let data = &self.blobs[i];
            Ok(Cow::Borrowed(data.as_slice()))
        }
    }
}
