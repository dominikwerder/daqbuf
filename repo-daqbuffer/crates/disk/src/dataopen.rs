use super::paths;
use bytes::BytesMut;
use daqbuf_err as err;
use err::ErrStr;
use err::Error;
use futures_util::StreamExt;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::Node;
use netpod::SfChFetchInfo;
use netpod::TsNano;
use std::fmt;
use std::path::PathBuf;
use std::time::Instant;
use taskrun::tokio;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::ErrorKind;
use tokio::io::SeekFrom;
use tracing::Instrument;

#[cfg(test)]
const BACKEND: &str = "testbackend-00";

pub struct Positioned {
    pub file: OpenedFile,
    pub found: bool,
}

pub async fn position_file_for_test(
    path: &PathBuf,
    range: &NanoRange,
    expand_left: bool,
    expand_right: bool,
) -> Result<Positioned, Error> {
    position_file(path, range, expand_left, expand_right).await
}

async fn position_file(
    path: &PathBuf,
    range: &NanoRange,
    expand_left: bool,
    expand_right: bool,
) -> Result<Positioned, Error> {
    trace!("position_file  called  expand_left {expand_left}  expand_right {expand_right}  {range:?}  {path:?}");
    assert_eq!(expand_left && expand_right, false);
    match OpenOptions::new().read(true).open(&path).await {
        Ok(file) => {
            let index_path = PathBuf::from(format!("{}_Index", path.to_str().unwrap()));
            match OpenOptions::new().read(true).open(&index_path).await {
                Ok(mut index_file) => {
                    let meta = index_file.metadata().await?;
                    if meta.len() > 1024 * 1024 * 500 {
                        let msg = format!("too large index file  {} bytes  for {:?}", meta.len(), index_path);
                        error!("{}", msg);
                        return Err(Error::with_msg(msg));
                    } else if meta.len() > 1024 * 1024 * 200 {
                        let msg = format!("very large index file  {} bytes  for {:?}", meta.len(), index_path);
                        warn!("{}", msg);
                    } else if meta.len() > 1024 * 1024 * 80 {
                        let msg = format!("very large index file  {} bytes  for {:?}", meta.len(), index_path);
                        warn!("{}", msg);
                    } else if meta.len() > 1024 * 1024 * 20 {
                        let msg = format!("large index file  {} bytes  for {:?}", meta.len(), index_path);
                        info!("{}", msg);
                    }
                    if meta.len() < 2 {
                        return Err(Error::with_msg(format!(
                            "bad meta len {}  for {:?}",
                            meta.len(),
                            index_path
                        )));
                    }
                    if meta.len() % 16 != 2 {
                        return Err(Error::with_msg(format!(
                            "bad meta len {}  for {:?}",
                            meta.len(),
                            index_path
                        )));
                    }
                    let mut buf = BytesMut::with_capacity(meta.len() as usize);
                    buf.resize(buf.capacity(), 0);
                    index_file.read_exact(&mut buf).await?;
                    let gg = if expand_left {
                        super::index::find_largest_smaller_than(range.clone(), expand_right, &buf[2..])
                    } else {
                        super::index::find_ge(range.clone(), expand_right, &buf[2..])
                    };
                    let gg = match gg {
                        Ok(x) => x,
                        Err(e) => {
                            error!(
                                "can not position file for  range {range:?}  expand_right {expand_right:?}  buflen {}",
                                buf.len()
                            );
                            return Err(e);
                        }
                    };
                    match gg {
                        Some(o) => {
                            let mut file = file;
                            file.seek(SeekFrom::Start(o.1)).await?;
                            //info!("position_file  case A  {:?}", path);
                            let g = OpenedFile {
                                file: Some(file),
                                path: path.clone(),
                                positioned: true,
                                index: true,
                                nreads: 0,
                                pos: o.1,
                            };
                            return Ok(Positioned { file: g, found: true });
                        }
                        None => {
                            //info!("position_file  case B  {:?}", path);
                            let g = OpenedFile {
                                file: Some(file),
                                path: path.clone(),
                                positioned: false,
                                index: true,
                                nreads: 0,
                                pos: 0,
                            };
                            return Ok(Positioned { file: g, found: false });
                        }
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => {
                        let ts1 = Instant::now();
                        let res = if expand_left {
                            super::index::position_static_len_datafile_at_largest_smaller_than(
                                file,
                                range.clone(),
                                expand_right,
                            )
                            .await?
                        } else {
                            super::index::position_static_len_datafile(file, range.clone(), expand_right).await?
                        };
                        let ts2 = Instant::now();
                        if false {
                            // TODO collect for stats:
                            let dur = ts2.duration_since(ts1);
                            info!("position_static_len_datafile took  ms {}", dur.as_millis());
                        }
                        let file = res.0;
                        if res.1 {
                            //info!("position_file  case C  {:?}", path);
                            let g = OpenedFile {
                                file: Some(file),
                                path: path.clone(),
                                positioned: true,
                                index: false,
                                nreads: res.2,
                                pos: res.3,
                            };
                            return Ok(Positioned { file: g, found: true });
                        } else {
                            //info!("position_file  case D  {:?}", path);
                            let g = OpenedFile {
                                file: Some(file),
                                path: path.clone(),
                                positioned: false,
                                index: false,
                                nreads: res.2,
                                pos: 0,
                            };
                            return Ok(Positioned { file: g, found: false });
                        }
                    }
                    _ => Err(e)?,
                },
            }
        }
        Err(e) => {
            warn!("can not open {:?}  error {:?}", path, e);
            let g = OpenedFile {
                file: None,
                path: path.clone(),
                positioned: false,
                index: true,
                nreads: 0,
                pos: 0,
            };
            return Ok(Positioned { file: g, found: false });
        }
    }
}

pub struct OpenedFile {
    pub pos: u64,
    pub path: PathBuf,
    pub file: Option<File>,
    pub positioned: bool,
    pub index: bool,
    pub nreads: u32,
}

impl fmt::Debug for OpenedFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("OpenedFile")
            .field("pos", &self.pos)
            .field("path", &self.path)
            .field("file", &self.file.is_some())
            .field("positioned", &self.positioned)
            .field("index", &self.index)
            .field("nreads", &self.nreads)
            .finish()
    }
}

#[derive(Debug)]
pub struct OpenedFileSet {
    pub timebin: u64,
    pub files: Vec<OpenedFile>,
}

pub fn open_files(
    range: &NanoRange,
    fetch_info: &SfChFetchInfo,
    reqid: &str,
    node: Node,
) -> async_channel::Receiver<Result<OpenedFileSet, Error>> {
    let span = tracing::span!(tracing::Level::DEBUG, "open_files", reqid);
    let (chtx, chrx) = async_channel::bounded(2);
    let range = range.clone();
    let fetch_info = fetch_info.clone();
    tokio::spawn(
        async move {
            match open_files_inner(&chtx, &range, &fetch_info, node).await {
                Ok(_) => {}
                Err(e) => {
                    let e = e.add_public_msg(format!(
                        "Can not open file for channel: {fetch_info:?}  range: {range:?}"
                    ));
                    match chtx.send(Err(e.into())).await {
                        Ok(_) => {}
                        Err(e) => {
                            // This case is fine.
                            debug!("open_files  channel send error {:?}", e);
                        }
                    }
                }
            }
        }
        .instrument(span),
    );
    chrx
}

async fn open_files_inner(
    chtx: &async_channel::Sender<Result<OpenedFileSet, Error>>,
    range: &NanoRange,
    fetch_info: &SfChFetchInfo,
    node: Node,
) -> Result<(), Error> {
    let fetch_info = fetch_info.clone();
    let timebins = get_timebins(&fetch_info, node.clone()).await?;
    if timebins.len() == 0 {
        return Ok(());
    }
    for &tb in &timebins {
        let ts_bin = TsNano::from_ns(tb * fetch_info.bs().ns());
        if ts_bin.ns() >= range.end {
            continue;
        }
        if ts_bin.ns() + fetch_info.bs().ns() <= range.beg {
            continue;
        }
        let mut a = Vec::new();
        for path in paths::datapaths_for_timebin(tb, &fetch_info, &node).await? {
            let w = position_file(&path, range, false, false).await?;
            if w.found {
                a.push(w.file);
            }
        }
        let h = OpenedFileSet { timebin: tb, files: a };
        debug!(
            "----- open_files_inner  giving OpenedFileSet with {} files",
            h.files.len()
        );
        chtx.send(Ok(h)).await.errstr()?;
    }
    Ok(())
}

/**
Provide the stream of positioned data files which are relevant for the given parameters.

Expanded to one event before and after the requested range, if exists.
*/
pub fn open_expanded_files(
    range: &NanoRange,
    fetch_info: &SfChFetchInfo,
    node: Node,
) -> async_channel::Receiver<Result<OpenedFileSet, Error>> {
    let (chtx, chrx) = async_channel::bounded(2);
    let range = range.clone();
    let fetch_info = fetch_info.clone();
    tokio::spawn(async move {
        match open_expanded_files_inner(&chtx, &range, &fetch_info, node).await {
            Ok(_) => {}
            Err(e) => match chtx.send(Err(e.into())).await {
                Ok(_) => {}
                Err(e) => {
                    // To be expected
                    debug!("open_expanded_files  channel send error {:?}", e);
                }
            },
        }
    });
    chrx
}

async fn get_timebins(fetch_info: &SfChFetchInfo, node: Node) -> Result<Vec<u64>, Error> {
    let mut timebins = Vec::new();
    let p0 = paths::channel_timebins_dir_path(&fetch_info, &node)?;
    match tokio::fs::read_dir(&p0).await {
        Ok(rd) => {
            let mut rd = tokio_stream::wrappers::ReadDirStream::new(rd);
            while let Some(e) = rd.next().await {
                let e = e?;
                let dn = e
                    .file_name()
                    .into_string()
                    .map_err(|e| Error::with_msg(format!("Bad OS path {:?}", e)))?;
                if dn.len() != 19 {
                    warn!("get_timebins  weird directory {:?}  p0 {:?}", e.path(), p0);
                }
                let vv = dn.chars().fold(0, |a, x| if x.is_digit(10) { a + 1 } else { a });
                if vv == 19 {
                    timebins.push(dn.parse::<u64>()?);
                }
            }
            timebins.sort_unstable();
            Ok(timebins)
        }
        Err(e) => {
            debug!("get_timebins  no timebins for {:?}  {:?}  p0 {:?}", fetch_info, e, p0);
            Ok(Vec::new())
        }
    }
}

async fn open_expanded_files_inner(
    chtx: &async_channel::Sender<Result<OpenedFileSet, Error>>,
    range: &NanoRange,
    fetch_info: &SfChFetchInfo,
    node: Node,
) -> Result<(), Error> {
    let fetch_info = fetch_info.clone();
    let timebins = get_timebins(&fetch_info, node.clone()).await?;
    debug!("timebins {timebins:?}");
    if timebins.len() == 0 {
        return Ok(());
    }
    let mut p1 = None;
    for (i, tb) in timebins.iter().enumerate().rev() {
        let ts_bin = TsNano::from_ns(tb * fetch_info.bs().ns());
        if ts_bin.ns() <= range.beg {
            p1 = Some(i);
            break;
        }
    }
    let mut p1 = if let Some(i) = p1 { i } else { 0 };
    if p1 >= timebins.len() {
        return Err(Error::with_msg(format!(
            "logic error p1 {}  range {:?}  fetch_info {:?}",
            p1, range, fetch_info
        )));
    }
    let mut found_pre = false;
    loop {
        let tb = timebins[p1];
        let mut a = Vec::new();
        for path in paths::datapaths_for_timebin(tb, &fetch_info, &node).await? {
            let w = position_file(&path, range, true, false).await?;
            if w.found {
                debug!("----- open_expanded_files_inner  FOUND  tb {:?}  path {:?}", tb, path);
                a.push(w.file);
                found_pre = true;
            } else {
                debug!("----- open_expanded_files_inner  UNFND  tb {:?}  path {:?}", tb, path);
            }
        }
        let h = OpenedFileSet { timebin: tb, files: a };
        debug!(
            "----- open_expanded_files_inner  giving OpenedFileSet with {} files",
            h.files.len()
        );
        chtx.send(Ok(h)).await.errstr()?;
        if found_pre {
            p1 += 1;
            break;
        } else if p1 == 0 {
            break;
        } else {
            p1 -= 1;
        }
    }
    if found_pre {
        // Append all following positioned files.
        while p1 < timebins.len() {
            let tb = timebins[p1];
            let mut a = Vec::new();
            for path in paths::datapaths_for_timebin(tb, &fetch_info, &node).await? {
                let w = position_file(&path, range, false, true).await?;
                if w.found {
                    a.push(w.file);
                }
            }
            let h = OpenedFileSet { timebin: tb, files: a };
            chtx.send(Ok(h)).await.errstr()?;
            p1 += 1;
        }
    } else {
        // TODO emit statsfor this or log somewhere?
        debug!("Could not find some event before the requested range, fall back to standard file list.");
        // Try to locate files according to non-expand-algorithm.
        open_files_inner(chtx, range, &fetch_info, node).await?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use err::Error;
    use netpod::range::evrange::NanoRange;
    use netpod::test_data_base_path_databuffer;
    use netpod::timeunits::*;
    use std::path::PathBuf;
    use tokio::fs::OpenOptions;

    fn scalar_file_path() -> PathBuf {
        test_data_base_path_databuffer()
            .join("node00/ks_2/byTime/scalar-i32-be")
            .join("0000000000000000001/0000000000/0000000000086400000_00000_Data")
    }

    fn wave_file_path() -> PathBuf {
        test_data_base_path_databuffer()
            .join("node00/ks_3/byTime/wave-f64-be-n21")
            .join("0000000000000000001/0000000000/0000000000086400000_00000_Data")
    }

    #[test]
    fn position_basic_file_at_begin() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY,
                end: DAY + MS * 20000,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 23);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_file_for_empty_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY + MS * 80000,
                end: DAY + MS * 80000,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_file_at_begin_for_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY,
                end: DAY + MS * 300000,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 23);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_file_at_inner() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY + MS * 4000,
                end: DAY + MS * 7000,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 179);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    // TODO add same test for WAVE
    #[test]
    fn position_basic_file_at_inner_for_too_small_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY + MS * 1501,
                end: DAY + MS * 1502,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    // TODO add same test for WAVE
    #[test]
    fn position_basic_file_starts_after_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: HOUR * 22,
                end: HOUR * 23,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_file_ends_before_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY * 2,
                end: DAY * 2 + HOUR,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_index() -> Result<(), Error> {
        let fut = async {
            let path = wave_file_path();
            let range = NanoRange {
                beg: DAY + MS * 4000,
                end: DAY + MS * 90000,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, true);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 184);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_index_too_small_range() -> Result<(), Error> {
        let fut = async {
            let path = wave_file_path();
            let range = NanoRange {
                beg: DAY + MS * 3100,
                end: DAY + MS * 3200,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, true);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_index_starts_after_range() -> Result<(), Error> {
        let fut = async {
            let path = wave_file_path();
            let range = NanoRange {
                beg: HOUR * 10,
                end: HOUR * 12,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, true);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_basic_index_ends_before_range() -> Result<(), Error> {
        let fut = async {
            let path = wave_file_path();
            let range = NanoRange {
                beg: DAY * 2,
                end: DAY * 2 + MS * 40000,
            };
            let res = position_file(&path, &range, false, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, true);
            assert_eq!(res.file.positioned, false);
            assert_eq!(res.file.pos, 0);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    //
    // --------------     Expanded   -----------------------------------
    //

    #[test]
    fn position_expand_file_at_begin_no_fallback() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY + MS * 3000,
                end: DAY + MS * 40000,
            };
            let file = OpenOptions::new().read(true).open(path).await?;
            let res =
                super::super::index::position_static_len_datafile_at_largest_smaller_than(file, range.clone(), true)
                    .await?;
            assert_eq!(res.1, true);
            assert_eq!(res.3, 75);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok::<_, Error>(())
    }

    #[test]
    fn position_expand_left_file_at_evts_file_begin() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY,
                end: DAY + MS * 40000,
            };
            let res = position_file(&path, &range, true, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_expand_right_file_at_evts_file_begin() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY,
                end: DAY + MS * 40000,
            };
            let res = position_file(&path, &range, false, true).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 23);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn position_expand_left_file_at_evts_file_within() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY + MS * 3000,
                end: DAY + MS * 40000,
            };
            let res = position_file(&path, &range, true, false).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 75);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    // -------    TODO do the same with Wave (index)
    #[test]
    fn position_expand_left_file_ends_before_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY * 2,
                end: DAY * 2 + MS * 40000,
            };
            let res = position_file(&path, &range, true, false).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 2995171);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    // -------    TODO do the same with Wave (index)
    #[test]
    fn position_expand_left_file_begins_exactly_after_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: HOUR * 23,
                end: DAY,
            };
            let res = position_file(&path, &range, true, false).await?;
            assert_eq!(res.found, false);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, false);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    // -------    TODO do the same with Wave (index)
    #[test]
    fn position_expand_right_file_begins_exactly_after_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: HOUR * 23,
                end: DAY,
            };
            let res = position_file(&path, &range, false, true).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 23);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    // TODO add same test for indexed
    #[test]
    fn position_expand_left_basic_file_at_inner_for_too_small_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY + MS * 1501,
                end: DAY + MS * 1502,
            };
            let res = position_file(&path, &range, true, false).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 75);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    // TODO add same test for indexed
    #[test]
    fn position_expand_right_basic_file_at_inner_for_too_small_range() -> Result<(), Error> {
        let fut = async {
            let path = scalar_file_path();
            let range = NanoRange {
                beg: DAY + MS * 1501,
                end: DAY + MS * 1502,
            };
            let res = position_file(&path, &range, false, true).await?;
            assert_eq!(res.found, true);
            assert_eq!(res.file.index, false);
            assert_eq!(res.file.positioned, true);
            assert_eq!(res.file.pos, 127);
            Ok::<_, Error>(())
        };
        taskrun::run(fut)?;
        Ok(())
    }

    #[test]
    fn expanded_file_list() {
        let range = NanoRange {
            beg: DAY + HOUR * 5,
            end: DAY + HOUR * 8,
        };
        let _chn = netpod::SfDbChannel::from_name(BACKEND, "scalar-i32-be");
        // TODO read config from disk? Or expose the config from data generator?
        let fetch_info = err::todoval();
        // let fetch_info = SfChFetchInfo {
        //     channel: chn,
        //     keyspace: 2,
        //     time_bin_size: TsNano(DAY),
        //     scalar_type: netpod::ScalarType::I32,
        //     byte_order: netpod::ByteOrder::Big,
        //     shape: netpod::Shape::Scalar,
        //     array: false,
        //     compression: false,
        // };
        let cluster = netpod::test_cluster();
        let task = async move {
            let mut paths = Vec::new();
            let files = open_expanded_files(&range, &fetch_info, cluster.nodes[0].clone());
            let mut files = Box::pin(files);
            while let Some(file) = files.next().await {
                match file {
                    Ok(k) => {
                        debug!("opened file: {:?}", k);
                        paths.push(k.files);
                    }
                    Err(e) => {
                        error!("error while trying to open {:?}", e);
                        break;
                    }
                }
            }
            if paths.len() != 2 {
                return Err(Error::with_msg_no_trace(format!(
                    "expected 2 files got {n}",
                    n = paths.len()
                )));
            }
            Ok::<_, Error>(())
        };
        taskrun::run(task).unwrap();
    }
}
