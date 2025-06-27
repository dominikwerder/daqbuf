use crate::errconv::ErrConv;
use daqbuf_err as err;
use err::Error;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use futures_util::TryStreamExt;
use items_0::Empty;
use items_0::Extendable;
use items_0::WithLen;
use items_2::accounting::AccountingEvents;
use netpod::EMIT_ACCOUNTING_SNAP;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits;
use scylla::statement::prepared::PreparedStatement;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

type ScySession = scylla::client::session::Session;

async fn read_next(
    ts_msp: u64,
    fwd: bool,
    qu: PreparedStatement,
    scy: Arc<ScySession>,
) -> Result<AccountingEvents, Error> {
    type RowType = (i64, i64, i64);
    let mut ret = AccountingEvents::empty();
    let mut tot_count = 0;
    let mut tot_bytes = 0;
    for part in 0..255_u32 {
        let mut res = if fwd {
            scy.execute_iter(qu.clone(), (part as i32, ts_msp as i64))
                .await
                .err_conv()?
                .rows_stream::<RowType>()
                .err_conv()?
        } else {
            return Err(Error::with_msg_no_trace("no backward support"));
        };
        while let Some(row) = res.try_next().await.err_conv()? {
            let _ts = ts_msp;
            let _series = row.0 as u64;
            let count = row.1 as u64;
            let bytes = row.2 as u64;
            tot_count += count;
            tot_bytes += bytes;
        }
    }
    ret.tss.push_back(ts_msp);
    ret.count.push_back(tot_count);
    ret.bytes.push_back(tot_bytes);
    Ok(ret)
}

struct ReadValues {
    #[allow(unused)]
    range: NanoRange,
    ts_msps: VecDeque<u64>,
    fwd: bool,
    #[allow(unused)]
    do_one_before_range: bool,
    fut: Pin<Box<dyn Future<Output = Result<AccountingEvents, Error>> + Send>>,
    scy: Arc<ScySession>,
    qu: PreparedStatement,
}

impl ReadValues {
    fn new(
        range: NanoRange,
        ts_msps: VecDeque<u64>,
        fwd: bool,
        do_one_before_range: bool,
        scy: Arc<ScySession>,
        qu: PreparedStatement,
    ) -> Self {
        let mut ret = Self {
            range,
            ts_msps,
            fwd,
            do_one_before_range,
            fut: Box::pin(futures_util::future::ready(Err(Error::with_msg_no_trace(
                "future not initialized",
            )))),
            scy,
            qu,
        };
        ret.next();
        ret
    }

    fn next(&mut self) -> bool {
        if let Some(ts_msp) = self.ts_msps.pop_front() {
            self.fut = self.make_fut(ts_msp);
            true
        } else {
            false
        }
    }

    fn make_fut(&mut self, ts_msp: u64) -> Pin<Box<dyn Future<Output = Result<AccountingEvents, Error>> + Send>> {
        let fut = read_next(ts_msp, self.fwd, self.qu.clone(), self.scy.clone());
        Box::pin(fut)
    }
}

enum FrState {
    New,
    Prepare(PrepFut),
    Start,
    ReadValues(ReadValues),
    Done,
}

type PrepFut = Pin<Box<dyn Future<Output = Result<PreparedStatement, Error>> + Send>>;

pub struct AccountingStreamScylla {
    state: FrState,
    range: NanoRange,
    scy: Arc<ScySession>,
    qu_select: Option<PreparedStatement>,
    outbuf: AccountingEvents,
    poll_count: u32,
}

impl AccountingStreamScylla {
    pub fn new(range: NanoRange, scy: Arc<ScySession>) -> Self {
        Self {
            state: FrState::New,
            range,
            scy,
            qu_select: None,
            outbuf: AccountingEvents::empty(),
            poll_count: 0,
        }
    }
}

async fn prep(cql: &str, scy: Arc<ScySession>) -> Result<PreparedStatement, Error> {
    scy.prepare(cql)
        .await
        .map_err(|e| Error::with_msg_no_trace(format!("cql error {e}")))
}

impl Stream for AccountingStreamScylla {
    type Item = Result<AccountingEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        // debug!("poll {}", self.poll_count);
        self.poll_count += 1;
        if self.poll_count > 200000 {
            debug!("abort high poll count");
            return Ready(None);
        }
        let span = tracing::span!(tracing::Level::TRACE, "poll_next");
        let _spg = span.enter();
        loop {
            if self.outbuf.len() > 0 {
                let item = std::mem::replace(&mut self.outbuf, AccountingEvents::empty());
                break Ready(Some(Ok(item)));
            }
            break match &mut self.state {
                FrState::New => {
                    let cql =
                        concat!("select series, count, bytes from account_00 where part = ? and ts = ? bypass cache");
                    let fut = prep(cql, self.scy.clone());
                    let fut: PrepFut = Box::pin(fut);
                    self.state = FrState::Prepare(fut);
                    continue;
                }
                FrState::Prepare(fut) => match fut.poll_unpin(cx) {
                    Ready(Ok(x)) => {
                        self.qu_select = Some(x);
                        self.state = FrState::Start;
                        continue;
                    }
                    Ready(Err(e)) => {
                        error!("{e}");
                        Ready(Some(Err(Error::with_msg_no_trace("cql error"))))
                    }
                    Pending => Pending,
                },
                FrState::Start => {
                    let mut ts_msps = VecDeque::new();
                    let snap = EMIT_ACCOUNTING_SNAP.ms() / 1000;
                    let mut ts = self.range.beg / timeunits::SEC / snap * snap;
                    let ts_e = self.range.end / timeunits::SEC / snap * snap;
                    while ts < ts_e {
                        if ts_msps.len() >= 100 {
                            debug!("too large time range requested");
                            break;
                        }
                        ts_msps.push_back(ts);
                        ts += snap;
                    }
                    if ts_msps.len() == 0 {
                        self.state = FrState::Done;
                        continue;
                    } else {
                        let fwd = true;
                        let do_one_before_range = false;
                        let st = ReadValues::new(
                            self.range.clone(),
                            ts_msps,
                            fwd,
                            do_one_before_range,
                            self.scy.clone(),
                            self.qu_select.as_ref().unwrap().clone(),
                        );
                        self.state = FrState::ReadValues(st);
                        continue;
                    }
                }
                FrState::ReadValues(st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(mut item)) => {
                        if !st.next() {
                            self.state = FrState::Done;
                        }
                        self.outbuf.extend_from(&mut item);
                        continue;
                    }
                    Ready(Err(e)) => {
                        error!("{e}");
                        Ready(Some(Err(e)))
                    }
                    Pending => Pending,
                },
                FrState::Done => Ready(None),
            };
        }
    }
}
