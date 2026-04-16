use netpod::log;
use std::fmt;
use std::time::Instant;
use taskrun::tracing;

autoerr::create_error_v1!(
    name(Error, "ScyllaEvents"),
    enum variants {
        Worker(Box<crate::worker::Error>),
        Msp(#[from] crate::events2::msp::Error),
        Unordered,
        OutOfRange,
        BadBatch,
        ReadQueueEmptyBck,
        ReadQueueEmptyFwd,
        Logic,
        TruncateLogic,
        AlreadyTaken,
        DrainFailure,
        RangeEndOverflow,
        NotTokenAware,
        Prepare(#[from] crate::events2::prepare::Error),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaWorker(Box<crate::worker::Error>),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        UserTriggered,
    },
);

impl From<crate::worker::Error> for Error {
    fn from(e: crate::worker::Error) -> Self {
        Error::Worker(Box::new(e))
    }
}

#[derive(Debug)]
pub enum ReadEventKind {
    Create,
    FutgenCallingReadNextValues,
    FutgenFutureCreated,
    CallExecuteIter,
    ScyllaReadRow(u32),
    ScyllaReadRowDone(u32),
    ReadNextValuesFutureDone,
    EventsStreamRtSees(u32),
    ReadEventsLspAllDone,
}

#[derive(Debug)]
pub struct ReadJobTrace {
    jobid: u64,
    ts0: Instant,
    events: Vec<(Instant, ReadEventKind)>,
}

impl ReadJobTrace {
    pub fn new() -> Self {
        static JOBID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self {
            jobid: JOBID.fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            ts0: Instant::now(),
            events: Vec::with_capacity(128),
        }
    }

    pub fn add_event_now(&mut self, kind: ReadEventKind) {
        self.events.push((Instant::now(), kind))
    }
}

impl fmt::Display for ReadJobTrace {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "ReadJobTrace  jobid {jid}", jid = self.jobid)?;
        for (ts, kind) in &self.events {
            let dt = 1e3 * ts.saturating_duration_since(self.ts0).as_secs_f32();
            write!(fmt, "\njobid {jid:4}  {dt:7.2}  {kind:?}", jid = self.jobid)?;
        }
        Ok(())
    }
}

// TODO
async fn __use_log_and_instrument_code() {
    let level = taskrun::query_log_level();
    let futgen = move || {
        let fut = async move {
            let logspan = if level == log::Level::DEBUG {
                tracing::span!(log::Level::INFO, "log_span_debug")
            } else if level == log::Level::TRACE {
                tracing::span!(log::Level::INFO, "log_span_trace")
            } else {
                tracing::Span::none()
            };
            let fut = async { 0u8 };
            let fut = tracing::Instrument::instrument(fut, logspan);
            fut.await
        };
        fut
    };
    let _ = futgen;
    todo!()
}
