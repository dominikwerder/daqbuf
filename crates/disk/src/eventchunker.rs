use bytes::Buf;
use bytes::BytesMut;
use daqbuf_err as err;
use err::Error;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::LogItem;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::StatsItem;
use items_0::streamitem::StreamItem;
use items_0::Empty;
use items_0::WithLen;
use items_2::eventfull::EventFull;
use netpod::histo::HistoLog2;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::SEC;
use netpod::ByteSize;
use netpod::EventDataReadStats;
use netpod::ScalarType;
use netpod::SfChFetchInfo;
use netpod::Shape;
use parse::channelconfig::CompressionMethod;
use std::collections::VecDeque;
use std::io::Cursor;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use streams::dtflags::*;
use streams::filechunkread::FileChunkRead;
use streams::needminbuffer::NeedMinBuffer;

#[allow(unused)]
macro_rules! trace_parse_buf { ($($arg:tt)*) => ( if false { trace!($($arg)*); }) }

autoerr::create_error_v1!(
    name(DataParseError, "DatabufferDataParse"),
    enum variants {
        DataFrameLengthMismatch,
        FileHeaderTooShort,
        BadVersionTag,
        HeaderTooLarge,
        Utf8Error,
        EventTooShort,
        EventTooLong(Shape, u32),
        TooManyBeforeRange,
        EventWithOptional,
        BadTypeIndex,
        WaveShapeWithoutEventArray,
        ShapedWithoutDims,
        TooManyDims,
        UnknownCompression,
        BadCompresionBlockSize,
    },
);

pub struct EventChunker {
    inp: NeedMinBuffer,
    state: DataFileState,
    need_min: u32,
    fetch_info: SfChFetchInfo,
    need_min_max: u32,
    errored: bool,
    completed: bool,
    range: NanoRange,
    stats_conf: EventChunkerConf,
    seen_beyond_range: bool,
    sent_beyond_range: bool,
    data_emit_complete: bool,
    final_stats_sent: bool,
    parsed_bytes: u64,
    node_ix: usize,
    dbg_path: PathBuf,
    last_ts: u64,
    item_len_emit_histo: HistoLog2,
    seen_before_range_count: usize,
    seen_after_range_count: usize,
    seen_events: usize,
    unordered_count: usize,
    repeated_ts_count: usize,
    discard_count_range: usize,
    discard_count_scalar_type: usize,
    discard_count_shape: usize,
    discard_count_shape_derived: usize,
    discard_count_shape_derived_err: usize,
    log_items: VecDeque<LogItem>,
}

impl Drop for EventChunker {
    fn drop(&mut self) {
        // TODO collect somewhere
        debug!(
            concat!(
                "EventChunker-stats {{ node_ix: {}, seen_events: {}, discard_count_range: {},",
                " discard_count_scalar_type: {}, discard_count_shape: {},",
                " discard_count_shape_derived: {}, discard_count_shape_derived_err: {},",
                " item_len_emit_histo: {:?} }}",
            ),
            self.node_ix,
            self.seen_events,
            self.discard_count_range,
            self.discard_count_scalar_type,
            self.discard_count_shape,
            self.discard_count_shape_derived,
            self.discard_count_shape_derived_err,
            self.item_len_emit_histo
        );
    }
}

enum DataFileState {
    FileHeader,
    Event,
}

struct ParseResult {
    events: EventFull,
    parsed_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct EventChunkerConf {
    pub disk_stats_every: ByteSize,
}

impl EventChunkerConf {
    pub fn new(disk_stats_every: ByteSize) -> Self {
        Self { disk_stats_every }
    }
}

impl EventChunker {
    pub fn self_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    // TODO   `expand` flag usage
    pub fn from_start(
        inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, items_0::streamitem::SitemErrTy>> + Send>>,
        fetch_info: SfChFetchInfo,
        range: NanoRange,
        stats_conf: EventChunkerConf,
        node_ix: usize,
        dbg_path: PathBuf,
    ) -> Self {
        debug!("{}::{}  node {}", Self::self_name(), "from_start", node_ix);
        let need_min_max = match fetch_info.shape() {
            Shape::Scalar => 1024 * 8,
            Shape::Wave(_) => 1024 * 32,
            Shape::Image(_, _) => 1024 * 1024 * 80,
        };
        let mut inp = NeedMinBuffer::new(inp);
        inp.set_need_min(6);
        Self {
            inp,
            state: DataFileState::FileHeader,
            need_min: 6,
            need_min_max,
            fetch_info,
            errored: false,
            completed: false,
            range,
            stats_conf,
            seen_beyond_range: false,
            sent_beyond_range: false,
            data_emit_complete: false,
            final_stats_sent: false,
            parsed_bytes: 0,
            dbg_path,
            node_ix,
            last_ts: 0,
            item_len_emit_histo: HistoLog2::new(0),
            seen_before_range_count: 0,
            seen_after_range_count: 0,
            seen_events: 0,
            unordered_count: 0,
            repeated_ts_count: 0,
            discard_count_range: 0,
            discard_count_scalar_type: 0,
            discard_count_shape: 0,
            discard_count_shape_derived: 0,
            discard_count_shape_derived_err: 0,
            log_items: VecDeque::new(),
        }
    }

    pub fn from_event_boundary(
        inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, Error>> + Send>>,
        fetch_info: SfChFetchInfo,
        range: NanoRange,
        stats_conf: EventChunkerConf,
        node_ix: usize,
        dbg_path: PathBuf,
    ) -> Self {
        debug!("{}::{}  node {}", Self::self_name(), "from_event_boundary", node_ix);
        let mut ret = Self::from_start(inp, fetch_info, range, stats_conf, node_ix, dbg_path);
        ret.state = DataFileState::Event;
        ret.need_min = 4;
        ret.inp.set_need_min(4);
        ret
    }

    fn parse_buf(&mut self, buf: &mut BytesMut) -> Result<(ParseResult, Vec<LogItem>), Error> {
        span!(Level::INFO, "EventChunker::parse_buf")
            .in_scope(|| self.parse_buf_inner(buf))
            .map_err(|e| Error::with_msg_no_trace(format!("{e:?}")))
    }

    fn parse_buf_inner(&mut self, buf: &mut BytesMut) -> Result<(ParseResult, Vec<LogItem>), DataParseError> {
        use byteorder::ReadBytesExt;
        use byteorder::BE;
        trace_parse_buf!("parse_buf_inner  buf len {}", buf.len());
        let mut ret = EventFull::empty();
        let mut log_items = Vec::new();
        let mut parsed_bytes = 0;
        loop {
            if (buf.len() as u32) < self.need_min {
                break;
            }
            match self.state {
                DataFileState::FileHeader => {
                    if buf.len() < 6 {
                        return Err(DataParseError::FileHeaderTooShort);
                    }
                    let mut sl = Cursor::new(buf.as_ref());
                    let fver = sl.read_i16::<BE>().unwrap();
                    if fver != 0 {
                        return Err(DataParseError::BadVersionTag);
                    }
                    let len = sl.read_i32::<BE>().unwrap();
                    if len <= 0 || len >= 512 {
                        return Err(DataParseError::HeaderTooLarge);
                    }
                    let totlen = len as usize + 2;
                    if buf.len() < totlen {
                        self.need_min = totlen as u32;
                        break;
                    } else {
                        sl.advance(len as usize - 8);
                        let len2 = sl.read_i32::<BE>().unwrap();
                        if len != len2 {
                            return Err(DataParseError::DataFrameLengthMismatch);
                        }
                        let _ = String::from_utf8(buf.as_ref()[6..(len as usize + 6 - 8)].to_vec())
                            .map_err(|_| DataParseError::Utf8Error);
                        self.state = DataFileState::Event;
                        self.need_min = 4;
                        buf.advance(totlen);
                        parsed_bytes += totlen as u64;
                    }
                }
                DataFileState::Event => {
                    let p0 = 0;
                    let mut sl = Cursor::new(buf.as_ref());
                    let len = sl.read_i32::<BE>().unwrap();
                    if len < 20 {
                        return Err(DataParseError::EventTooShort);
                    }
                    let shape = self.fetch_info.shape();
                    match shape {
                        Shape::Scalar if len > 1024 * 64 => {
                            return Err(DataParseError::EventTooLong(shape.clone(), len as _))
                        }
                        Shape::Wave(_) if len > 1024 * 1024 * 32 => {
                            return Err(DataParseError::EventTooLong(shape.clone(), len as _))
                        }
                        Shape::Image(_, _) if len > 1024 * 1024 * 200 => {
                            return Err(DataParseError::EventTooLong(shape.clone(), len as _))
                        }
                        _ => {}
                    }
                    let len = len as u32;
                    if (buf.len() as u32) < len {
                        self.need_min = len as u32;
                        break;
                    } else {
                        self.seen_events += 1;
                        let mut discard = false;
                        let _ttl = sl.read_i64::<BE>().unwrap();
                        let ts = sl.read_i64::<BE>().unwrap() as u64;
                        let pulse = sl.read_i64::<BE>().unwrap() as u64;
                        if ts == self.last_ts {
                            self.repeated_ts_count += 1;
                            if self.repeated_ts_count < 20 {
                                let msg = format!(
                                    "EventChunker  repeated event ts ix {}  ts {}.{}  last_ts {}.{}  config {:?}  path {:?}",
                                    self.repeated_ts_count,
                                    ts / SEC,
                                    ts % SEC,
                                    self.last_ts / SEC,
                                    self.last_ts % SEC,
                                    self.fetch_info.shape(),
                                    self.dbg_path
                                );
                                warn!("{}", msg);
                            }
                        }
                        if ts < self.last_ts {
                            discard = true;
                            self.discard_count_range += 1;
                            self.unordered_count += 1;
                            if self.unordered_count < 20 {
                                let msg = format!(
                                    "EventChunker  unordered event ix {}  ts {}.{}  last_ts {}.{}  config {:?}  path {:?}",
                                    self.unordered_count,
                                    ts / SEC,
                                    ts % SEC,
                                    self.last_ts / SEC,
                                    self.last_ts % SEC,
                                    self.fetch_info.shape(),
                                    self.dbg_path
                                );
                                warn!("{}", msg);
                            }
                        }
                        self.last_ts = ts;
                        if ts >= self.range.end {
                            discard = true;
                            self.discard_count_range += 1;
                            self.seen_after_range_count += 1;
                            self.seen_beyond_range = true;
                            if self.seen_after_range_count >= 2 {
                                self.data_emit_complete = true;
                                break;
                            }
                        }
                        if ts < self.range.beg {
                            // discard = true;
                            // self.discard_count_range += 1;
                            self.seen_before_range_count += 1;
                            if self.seen_before_range_count < 20 {
                                let msg = format!(
                                    "seen before range: {} event ts {}.{}  range beg {}.{}  range end {}.{}  pulse {}  config {:?}  path {:?}",
                                    self.seen_before_range_count,
                                    ts / SEC,
                                    ts % SEC,
                                    self.range.beg / SEC,
                                    self.range.beg % SEC,
                                    self.range.end / SEC,
                                    self.range.end % SEC,
                                    pulse,
                                    self.fetch_info.shape(),
                                    self.dbg_path
                                );
                                warn!("{}", msg);
                                let item = LogItem::from_node(Level::INFO, msg);
                                self.log_items.push_back(item);
                            }
                            if self.seen_before_range_count > 100 {
                                let msg = format!(
                                    "too many seen before range: {} event ts {}.{}  range beg {}.{}  range end {}.{}  pulse {}  config {:?}  path {:?}",
                                    self.seen_before_range_count,
                                    ts / SEC,
                                    ts % SEC,
                                    self.range.beg / SEC,
                                    self.range.beg % SEC,
                                    self.range.end / SEC,
                                    self.range.end % SEC,
                                    pulse,
                                    self.fetch_info.shape(),
                                    self.dbg_path
                                );
                                error!("{}", msg);
                                return Err(DataParseError::TooManyBeforeRange);
                            }
                        }
                        let _ioc_ts = sl.read_i64::<BE>().unwrap();
                        let status = sl.read_i8().unwrap();
                        let severity = sl.read_i8().unwrap();
                        let optional = sl.read_i32::<BE>().unwrap();
                        if status != 0 {
                            // return Err(DataParseError::UnexpectedStatus);
                            // TODO count
                        }
                        if severity != 0 {
                            // return Err(DataParseError::TooManyBeforeRange);
                            // TODO count
                        }
                        if optional != -1 {
                            return Err(DataParseError::EventWithOptional);
                        }
                        let type_flags = sl.read_u8().unwrap();
                        let type_index = sl.read_u8().unwrap();
                        if type_index > 13 {
                            return Err(DataParseError::BadTypeIndex);
                        }
                        let scalar_type =
                            ScalarType::from_dtype_index(type_index).map_err(|_| DataParseError::BadTypeIndex)?;
                        let is_compressed = type_flags & COMPRESSION != 0;
                        let is_array = type_flags & ARRAY != 0;
                        let is_big_endian = type_flags & BIG_ENDIAN != 0;
                        let is_shaped = type_flags & SHAPE != 0;
                        if let Shape::Wave(_) = self.fetch_info.shape() {
                            if !is_array {
                                return Err(DataParseError::WaveShapeWithoutEventArray);
                            }
                        }
                        let compression_method = if is_compressed { sl.read_u8().unwrap() } else { 0 };
                        let shape_dim = if is_shaped { sl.read_u8().unwrap() } else { 0 };
                        let mut shape_lens = [0, 0, 0, 0];
                        for i1 in 0..shape_dim {
                            shape_lens[i1 as usize] = sl.read_u32::<BE>().unwrap();
                        }
                        // NOTE the databuffer does not fill in this correctly, the data on disk
                        // contains often just "scalar" even though it is a waveform.
                        let shape_this = {
                            if is_shaped {
                                if shape_dim == 1 {
                                    Shape::Wave(shape_lens[0])
                                } else if shape_dim == 2 {
                                    Shape::Image(shape_lens[0], shape_lens[1])
                                } else if shape_dim == 0 {
                                    discard = true;
                                    self.discard_count_shape += 1;
                                    // return Err(DataParseError::ShapedWithoutDims);
                                    Shape::Scalar
                                } else {
                                    discard = true;
                                    self.discard_count_shape += 1;
                                    // return Err(DataParseError::TooManyDims);
                                    Shape::Scalar
                                }
                            } else {
                                Shape::Scalar
                            }
                        };
                        let comp_this = if is_compressed {
                            if compression_method == 0 {
                                Some(CompressionMethod::BitshuffleLZ4)
                            } else {
                                return Err(DataParseError::UnknownCompression);
                            }
                        } else {
                            None
                        };
                        if self.fetch_info.scalar_type().ne(&scalar_type) {
                            discard = true;
                            self.discard_count_scalar_type += 1;
                            let msg = format!(
                                "scalar_type mismatch  {:?}  {:?}",
                                scalar_type,
                                self.fetch_info.scalar_type(),
                            );
                            let item = LogItem::from_node(Level::WARN, msg);
                            log_items.push(item);
                        }
                        if false {
                            // Unfortunately the shape stored by databuffer recording on disk is not reliable
                            // especially for waveforms it will wrongly indicate scalar. So this is unusable.
                            if self.fetch_info.shape().ne(&shape_this) {
                                discard = true;
                                self.discard_count_shape += 1;
                                let msg = format!("shape mismatch  {:?}  {:?}", shape_this, self.fetch_info.shape(),);
                                let item = LogItem::from_node(Level::WARN, msg);
                                log_items.push(item);
                            }
                        }
                        let p1 = sl.position();
                        let n1 = p1 - p0;
                        let n2 = len as u64 - n1 - 4;
                        let databuf = buf[p1 as usize..(p1 as usize + n2 as usize)].as_ref();
                        if discard {
                        } else {
                            ret.push(
                                ts,
                                pulse,
                                databuf.to_vec(),
                                scalar_type,
                                is_big_endian,
                                shape_this,
                                comp_this,
                            );
                            match ret.shape_derived(
                                ret.len() - 1,
                                self.fetch_info.scalar_type(),
                                self.fetch_info.shape(),
                            ) {
                                Ok(sh) => {
                                    if sh.ne(self.fetch_info.shape()) {
                                        self.discard_count_shape_derived += 1;
                                        ret.pop_back();
                                        let msg = format!(
                                            "EventChunker  shape_derived mismatch  {:?}  {:?}  {:?}",
                                            self.fetch_info.scalar_type(),
                                            self.fetch_info.shape(),
                                            sh,
                                        );
                                        let item = LogItem::from_node(Level::WARN, msg);
                                        log_items.push(item);
                                    }
                                }
                                Err(e) => {
                                    self.discard_count_shape_derived_err += 1;
                                    ret.pop_back();
                                    let msg = format!(
                                        "EventChunker  shape_derived error  {}  {:?}  {:?}",
                                        e,
                                        self.fetch_info.scalar_type(),
                                        self.fetch_info.shape(),
                                    );
                                    let item = LogItem::from_node(Level::WARN, msg);
                                    log_items.push(item);
                                }
                            }
                        }
                        buf.advance(len as usize);
                        parsed_bytes += len as u64;
                        self.need_min = 4;
                    }
                }
            }
        }
        let ret = ParseResult {
            events: ret,
            parsed_bytes,
        };
        Ok((ret, log_items))
    }
}

impl Stream for EventChunker {
    type Item = Result<StreamItem<RangeCompletableItem<EventFull>>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("EventChunker poll_next on completed");
            } else if let Some(item) = self.log_items.pop_front() {
                Ready(Some(Ok(StreamItem::Log(item))))
            } else if self.errored {
                self.completed = true;
                Ready(None)
            } else if self.parsed_bytes >= self.stats_conf.disk_stats_every.bytes() as u64 {
                let item = EventDataReadStats {
                    parsed_bytes: self.parsed_bytes,
                };
                self.parsed_bytes = 0;
                let ret = StreamItem::Stats(StatsItem::EventDataReadStats(item));
                Ready(Some(Ok(ret)))
            } else if self.sent_beyond_range {
                self.completed = true;
                Ready(None)
            } else if self.final_stats_sent {
                self.sent_beyond_range = true;
                trace!("sent_beyond_range");
                if self.seen_beyond_range {
                    trace!("sent_beyond_range  RangeComplete");
                    Ready(Some(Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete))))
                } else {
                    trace!("sent_beyond_range  non-complete");
                    continue;
                }
            } else if self.data_emit_complete {
                let item = EventDataReadStats {
                    parsed_bytes: self.parsed_bytes,
                };
                self.parsed_bytes = 0;
                let ret = StreamItem::Stats(StatsItem::EventDataReadStats(item));
                self.final_stats_sent = true;
                Ready(Some(Ok(ret)))
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(mut fcr))) => {
                        if false {
                            // TODO collect for stats:
                            info!(
                                "file read  bytes {}  ms {}",
                                fcr.buf().len(),
                                fcr.duration().as_millis()
                            );
                        }
                        match self.parse_buf(fcr.buf_mut()) {
                            Ok((res, log_items)) => {
                                for item in log_items {
                                    self.log_items.push_back(item);
                                }
                                self.parsed_bytes += res.parsed_bytes;
                                if fcr.buf().len() > 0 {
                                    // TODO gather stats about this:
                                    self.inp.put_back(fcr);
                                }
                                if self.need_min > self.need_min_max {
                                    let msg = format!(
                                        "spurious EventChunker asks for need_min {}  max {}",
                                        self.need_min, self.need_min_max
                                    );
                                    self.errored = true;
                                    return Ready(Some(Err(Error::with_msg(msg))));
                                }
                                let x = self.need_min;
                                self.inp.set_need_min(x);
                                if false {
                                    info!(
                                        "EventChunker  emits {} events  tss {:?}",
                                        res.events.len(),
                                        res.events.tss
                                    );
                                };
                                self.item_len_emit_histo.ingest(res.events.len() as u32);
                                let ret = StreamItem::DataItem(RangeCompletableItem::Data(res.events));
                                Ready(Some(Ok(ret)))
                            }
                            Err(e) => {
                                self.errored = true;
                                Ready(Some(Err(e.into())))
                            }
                        }
                    }
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e)))
                    }
                    Ready(None) => {
                        self.data_emit_complete = true;
                        continue;
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
