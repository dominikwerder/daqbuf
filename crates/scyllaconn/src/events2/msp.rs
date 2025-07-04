use crate::events2::prepare::StmtsEvents;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaQueue;
use daqbuf_series::SeriesId;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::TryStreamExt;
use netpod::TsMs;
use netpod::TsMsVecFmt;
use netpod::log;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! trace_emit { ($det:expr, $($arg:tt)*) => ( if $det { log::trace!($($arg)*); } ) }

macro_rules! trace_msp { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

macro_rules! log_fetch_result { ($($arg:tt)*) => { if true { log::trace!("fetch  {}", format_args!($($arg)*)); } }; }

autoerr::create_error_v1!(
    name(Error, "EventsMsp"),
    enum variants {
        Logic,
        Worker(Box<crate::worker::Error>),
        ScyllaRow(#[from] scylla::errors::NextRowError),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        TooManyRows,
    },
);

impl From<crate::worker::Error> for Error {
    fn from(value: crate::worker::Error) -> Self {
        Self::Worker(Box::new(value))
    }
}

enum Resolvable<F>
where
    F: Future,
{
    Future(F),
    Output(<F as Future>::Output),
    Taken,
}

impl<F> Resolvable<F>
where
    F: Future,
{
    fn take(&mut self) -> Option<<F as Future>::Output> {
        let x = std::mem::replace(self, Resolvable::Taken);
        match x {
            Resolvable::Future(_) => None,
            Resolvable::Output(x) => Some(x),
            Resolvable::Taken => None,
        }
    }

    fn is_taken(&self) -> bool {
        if let Self::Taken = self { true } else { false }
    }
}

struct BckAndFirstFwd {
    fut_bck: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
    fut_fwd: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
}

struct Fwd {
    fut_fwd: Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>>,
}

enum State {
    BckAndFirstFwd(BckAndFirstFwd),
    Fwd(Fwd),
}

#[pin_project::pin_project]
pub struct MspStreamRt {
    rt: RetentionTime,
    series: SeriesId,
    range: ScyllaSeriesRange,
    no_more: bool,
    #[pin]
    state: State,
    out: VecDeque<TsMs>,
    scyqueue: ScyllaQueue,
    do_trace_detail: bool,
}

impl MspStreamRt {
    pub fn new(rt: RetentionTime, series: SeriesId, range: ScyllaSeriesRange, scyqueue: ScyllaQueue) -> Self {
        let fut_bck = {
            let scyqueue = scyqueue.clone();
            let rt = rt.clone();
            let series = series.clone();
            let range = range.clone();
            async move { scyqueue.find_ts_msp(rt, series.id(), range, true).await }
        };
        let fut_fwd = {
            let scyqueue = scyqueue.clone();
            let rt = rt.clone();
            let series = series.clone();
            let range = range.clone();
            async move { scyqueue.find_ts_msp(rt, series.id(), range, false).await }
        };
        let do_trace_detail = daqbuf_series::dbg::dbg_series(series.clone());
        trace_emit!(do_trace_detail, "-------------------------------------  TEST TRACE");
        Self {
            rt,
            series,
            range,
            no_more: false,
            state: State::BckAndFirstFwd(BckAndFirstFwd {
                fut_bck: Resolvable::Future(Box::pin(fut_bck)),
                fut_fwd: Resolvable::Future(Box::pin(fut_fwd)),
            }),
            out: VecDeque::new(),
            scyqueue,
            do_trace_detail,
        }
    }

    fn next_fwd_fut(
        &mut self,
    ) -> Resolvable<Pin<Box<dyn Future<Output = Result<VecDeque<TsMs>, crate::worker::Error>> + Send>>> {
        let range = if let Some(msp) = self.out.back() {
            let x = ScyllaSeriesRange::new(msp.bump_epsilon().ns(), self.range.end());
            trace_emit!(self.do_trace_detail, "next_fwd_fut  {}", x.fmt());
            x
        } else {
            // should not get here
            let x = ScyllaSeriesRange::new(self.range.end(), self.range.end());
            trace_emit!(self.do_trace_detail, "next_fwd_fut  NOTHING IN BUFFER  {}", x.fmt());
            x
        };
        let fut_fwd = {
            let scyqueue = self.scyqueue.clone();
            let rt = self.rt.clone();
            let series = self.series.clone();
            async move { scyqueue.find_ts_msp(rt, series.id(), range, false).await }
        };
        Resolvable::Future(Box::pin(fut_fwd))
    }
}

impl Stream for MspStreamRt {
    type Item = Result<TsMs, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let trdet = self.do_trace_detail;
        loop {
            break match &mut self.state {
                State::BckAndFirstFwd(st) => {
                    let mut have_pending = false;
                    let rsv = &mut st.fut_bck;
                    match rsv {
                        Resolvable::Future(fut) => match fut.poll_unpin(cx) {
                            Ready(x) => {
                                trace_emit!(trdet, "bck resolved {x:?}");
                                *rsv = Resolvable::Output(x);
                            }
                            Pending => {
                                have_pending = true;
                            }
                        },
                        _ => {}
                    }
                    let rsv = &mut st.fut_fwd;
                    match rsv {
                        Resolvable::Future(fut) => match fut.poll_unpin(cx) {
                            Ready(x) => {
                                trace_emit!(trdet, "fwd resolved {x:?}");
                                *rsv = Resolvable::Output(x);
                            }
                            Pending => {
                                have_pending = true;
                            }
                        },
                        _ => {}
                    }
                    if have_pending {
                        Pending
                    } else {
                        let taken_bck = st.fut_bck.take();
                        let taken_fwd = st.fut_fwd.take();
                        if let (Some(taken_bck), Some(taken_fwd)) = (taken_bck, taken_fwd) {
                            match taken_bck {
                                Ok(v1) => match taken_fwd {
                                    Ok(v2) => {
                                        for e in v1 {
                                            self.out.push_back(e)
                                        }
                                        for e in v2 {
                                            self.out.push_back(e)
                                        }
                                        trace_emit!(trdet, "ready out {}", TsMsVecFmt(self.out.iter()));
                                        self.state = State::Fwd(Fwd {
                                            fut_fwd: self.next_fwd_fut(),
                                        });
                                        continue;
                                    }
                                    Err(e) => Ready(Some(Err(e.into()))),
                                },
                                Err(e) => Ready(Some(Err(e.into()))),
                            }
                        } else {
                            Ready(Some(Err(Error::Logic)))
                        }
                    }
                }
                State::Fwd(st) => {
                    // TODO check if more input is coming
                    let mut have_pending = false;
                    let mut have_progress = false;
                    let rsv = &mut st.fut_fwd;
                    match rsv {
                        Resolvable::Future(fut) => match fut.poll_unpin(cx) {
                            Ready(Ok(x)) => {
                                *rsv = Resolvable::Taken;
                                trace_emit!(trdet, "bulk fwd resolved {x:?}");
                                if x.len() == 0 {
                                    self.no_more = true;
                                }
                                for e in x {
                                    self.out.push_back(e)
                                }
                            }
                            Ready(Err(e)) => {
                                trace_emit!(trdet, "bulk fwd error {e}");
                                *rsv = Resolvable::Taken;
                                log::error!("{e}");
                            }
                            Pending => {
                                trace_emit!(trdet, "bulk fwd Pending");
                                have_pending = true;
                            }
                        },
                        Resolvable::Output(..) => {
                            trace_emit!(trdet, "bulk fwd Output");
                        }
                        Resolvable::Taken => {
                            trace_emit!(trdet, "bulk fwd Taken");
                        }
                    }
                    let is_taken = if let State::Fwd(st2) = &self.state {
                        st2.fut_fwd.is_taken()
                    } else {
                        panic!("logic");
                    };
                    if self.out.len() < 3 && self.no_more == false && is_taken {
                        trace_emit!(trdet, "bulk fwd  low in out  make next fut   ++++++++++++++++++++");
                        self.state = State::Fwd(Fwd {
                            fut_fwd: self.next_fwd_fut(),
                        });
                        have_progress = true;
                    }
                    if let Some(x) = self.out.pop_front() {
                        trace_emit!(trdet, "State::Fwd => Some  emit {}", x.fmt());
                        Ready(Some(Ok(x)))
                    } else if have_progress {
                        continue;
                    } else if have_pending {
                        trace_emit!(trdet, "State::Fwd => Pending      !!!!!!!!!!!!!!!!!!!!!!!");
                        Pending
                    } else {
                        trace_emit!(trdet, "State::Fwd => None       !!!!!!!!!!!!!!!!!!!!!!");
                        Ready(None)
                    }
                }
            };
        }
    }
}

fn trait_assert<T>(_: T)
where
    T: Stream + Unpin + Send,
{
}

#[allow(unused)]
fn trait_assert_try() {
    let x: MspStreamRt = phantomval();
    trait_assert(x);
}

fn phantomval<T>() -> T {
    panic!()
}

pub async fn find_ts_msp(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    bck: bool,
    stmts: &StmtsEvents,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    trace_msp!(
        "find_ts_msp  series  {:?}  {:?}  {}  bck {}",
        rt,
        series,
        range.fmt(),
        bck
    );
    if bck {
        find_ts_msp_bck_workaround(rt, series, range, stmts, scy).await
    } else {
        find_ts_msp_fwd(rt, series, range, stmts, scy).await
    }
}

async fn find_ts_msp_fwd(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    stmts: &StmtsEvents,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let selfname = "find_ts_msp_fwd";
    let mut ret = VecDeque::new();
    // TODO time range truncation can be handled better
    let params = (series as i64, range.beg().ms() as i64, 1 + range.end().ms() as i64);
    log_fetch_result!("{selfname}  {:?}", params);
    let mut res = scy
        .execute_iter(stmts.rt(rt).ts_msp_fwd().clone(), params)
        .await?
        .rows_stream::<(i64,)>()?;
    while let Some(row) = res.try_next().await? {
        let ts = TsMs::from_ms_u64(row.0 as u64);
        log_fetch_result!("{selfname}  {params:?}  {ts}");
        ret.push_back(ts);
    }
    Ok(ret)
}

async fn find_ts_msp_bck(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    stmts: &StmtsEvents,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let selfname = "find_ts_msp_bck";
    let mut ret = VecDeque::new();
    let params = (series as i64, range.beg().ms() as i64);
    log_fetch_result!("{selfname}  {:?}", params);
    let mut res = scy
        .execute_iter(stmts.rt(rt).ts_msp_bck().clone(), params)
        .await?
        .rows_stream::<(i64,)>()?;
    while let Some(row) = res.try_next().await? {
        let ts = TsMs::from_ms_u64(row.0 as u64);
        log_fetch_result!("{selfname}  {params:?}  {ts}");
        ret.push_front(ts);
    }
    Ok(ret)
}

// Workaround because scylla's order by desc is broken at the moment.
async fn find_ts_msp_bck_workaround(
    rt: &RetentionTime,
    series: u64,
    range: ScyllaSeriesRange,
    stmts: &StmtsEvents,
    scy: &Session,
) -> Result<VecDeque<TsMs>, Error> {
    let selfname = "find_ts_msp_bck_workaround";
    let mut ret = Vec::new();
    // let params = (series as i64, 0 as i64, range.beg().ms() as i64);
    let params = (series as i64, 0 as i64, i64::MAX);
    log_fetch_result!("{selfname}  {:?}", params);
    let mut res = scy
        .execute_iter(stmts.rt(rt).ts_msp_bck_workaround().clone(), params)
        .await?
        .rows_stream::<(i64,)>()?;
    let mut c = 0;
    while let Some(row) = res.try_next().await? {
        c += 1;
        let ts = TsMs::from_ms_u64(row.0 as u64);
        if ts >= range.beg().to_ts_ms() {
            log_fetch_result!("{selfname}  {params:?}  {ts}  DISCARD AFTER RANGE");
        } else {
            if ret.len() > 1024 * 1024 {
                return Err(Error::TooManyRows);
            } else {
                log_fetch_result!("{selfname}  {params:?}  {ts}  USE");
                ret.push(ts);
            }
        }
    }
    log_fetch_result!("{selfname}  {params:?}  considered msp {c}");
    if ret.len() > 1024 * 2 {
        log::info!("quite many ts_msp values in reverse lookup  len {}", ret.len());
    }
    if ret.len() > 1024 * 64 {
        log::warn!("quite many ts_msp values in reverse lookup  len {}", ret.len());
    }
    let m = ret.len().max(2) - 2;
    log_fetch_result!("{selfname}  {params:?}  {ret:?}  {m}");
    let ret = ret.into_iter().skip(m).collect();
    log_fetch_result!("{selfname}  {params:?}  {ret:?}");
    Ok(ret)
}
