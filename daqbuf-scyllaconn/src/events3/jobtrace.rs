use std::fmt;
use std::time::Instant;

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
