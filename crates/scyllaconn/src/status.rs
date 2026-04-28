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
use items_0::isodate::IsoDateTime;
use items_2::channelevents::ChannelStatusEvents;
use items_2::channelevents::ChannelStatusPubApi;
use netpod::CONNECTION_STATUS_DIV;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::timeunits::MS;
use scylla::client::session::Session as ScySession;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

async fn read_next_status_events(
    series: u64,
    ts_msp: u64,
    range: NanoRange,
    fwd: bool,
    do_one_before: bool,
    scy: Arc<ScySession>,
) -> Result<ChannelStatusEvents, Error> {
    if ts_msp >= range.end {
        warn!(
            "given ts_msp {}  >=  range.end {}  not necessary to read this",
            ts_msp, range.end
        );
    }
    if range.end > i64::MAX as u64 {
        return Err(Error::with_msg_no_trace(format!("range.end overflows i64")));
    }
    let res = if fwd {
        let ts_lsp_min = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
        let ts_lsp_max = if ts_msp < range.end { range.end - ts_msp } else { 0 };
        trace!(
            "FWD  ts_msp {}  ts_lsp_min {}  ts_lsp_max {}  beg {}  end {}",
            ts_msp, ts_lsp_min, ts_lsp_max, range.beg, range.end
        );
        // TODO use prepared!
        let cql = concat!(
            "select ts_lsp, kind from channel_status where series = ? and ts_msp = ? and ts_lsp >= ? and ts_lsp < ?"
        );
        scy.query_iter(
            cql,
            (series as i64, ts_msp as i64, ts_lsp_min as i64, ts_lsp_max as i64),
        )
        .await
        .err_conv()?
    } else {
        let ts_lsp_max = if ts_msp < range.beg { range.beg - ts_msp } else { 0 };
        debug!(
            "BCK  ts_msp {}  ts_lsp_max {}  beg {}  end {}  {}",
            ts_msp,
            ts_lsp_max,
            range.beg,
            range.end,
            stringify!($fname)
        );
        // TODO use prepared!
        let cql = concat!(
            "select ts_lsp, kind from channel_status where series = ? and ts_msp = ? and ts_lsp < ? order by ts_lsp desc limit 1"
        );
        scy.query_iter(cql, (series as i64, ts_msp as i64, ts_lsp_max as i64))
            .await
            .err_conv()?
    };
    let mut last_before = None;
    let mut ret = ChannelStatusEvents::empty();
    let mut it = res.rows_stream::<(i64, i32)>().err_conv()?;
    while let Some(row) = it.try_next().await.err_conv()? {
        let ts = ts_msp + row.0 as u64;
        let kind = row.1 as u32;
        let datetime = IsoDateTime::from_unix_millis(ts / MS);
        let status = ChannelStatusPubApi::from_ca_ingest_status_kind(kind);
        if ts >= range.end {
        } else if ts >= range.beg {
            ret.tss.push_back(ts);
            ret.datetimes.push_back(datetime);
            ret.statuses.push_back(status);
        } else {
            last_before = Some((ts, datetime, status));
        }
    }
    if do_one_before {
        if let Some((ts, datetime, status)) = last_before {
            ret.tss.push_front(ts);
            ret.datetimes.push_front(datetime);
            ret.statuses.push_front(status);
        }
    }
    trace!("found in total {} events  ts_msp {}", ret.len(), ts_msp);
    Ok(ret)
}

struct ReadValues {
    series: u64,
    range: NanoRange,
    ts_msps: VecDeque<u64>,
    fwd: bool,
    do_one_before_range: bool,
    fut: Pin<Box<dyn Future<Output = Result<ChannelStatusEvents, Error>> + Send>>,
    scy: Arc<ScySession>,
}

impl ReadValues {
    fn new(
        series: u64,
        range: NanoRange,
        ts_msps: VecDeque<u64>,
        fwd: bool,
        do_one_before_range: bool,
        scy: Arc<ScySession>,
    ) -> Self {
        let mut ret = Self {
            series,
            range,
            ts_msps,
            fwd,
            do_one_before_range,
            fut: Box::pin(futures_util::future::ready(Err(Error::with_msg_no_trace(
                "future not initialized",
            )))),
            scy,
        };
        ret.next();
        ret
    }

    fn next(&mut self) -> bool {
        if let Some(ts_msp) = self.ts_msps.pop_front() {
            self.fut = self.make_fut(ts_msp);
            true
        } else {
            debug!("no more msp");
            false
        }
    }

    fn make_fut(&mut self, ts_msp: u64) -> Pin<Box<dyn Future<Output = Result<ChannelStatusEvents, Error>> + Send>> {
        debug!("make fut for {ts_msp}");
        let fut = read_next_status_events(
            self.series,
            ts_msp,
            self.range.clone(),
            self.fwd,
            self.do_one_before_range,
            self.scy.clone(),
        );
        Box::pin(fut)
    }
}

enum FrState {
    New,
    ReadValues(ReadValues),
    Done,
}

pub struct StatusStreamScylla {
    state: FrState,
    series: u64,
    range: NanoRange,
    do_one_before_range: bool,
    scy: Arc<ScySession>,
    outbuf: ChannelStatusEvents,
}

impl StatusStreamScylla {
    pub fn new(series: u64, range: NanoRange, do_one_before_range: bool, scy: Arc<ScySession>) -> Self {
        Self {
            state: FrState::New,
            series,
            range,
            do_one_before_range,
            scy,
            outbuf: ChannelStatusEvents::empty(),
        }
    }
}

impl Stream for StatusStreamScylla {
    type Item = Result<ChannelStatusEvents, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        let span = tracing::span!(tracing::Level::TRACE, "poll_next");
        let _spg = span.enter();
        loop {
            if self.outbuf.len() > 0 {
                let item = std::mem::replace(&mut self.outbuf, ChannelStatusEvents::empty());
                break Ready(Some(Ok(item)));
            }
            break match self.state {
                FrState::New => {
                    let mut ts_msps = VecDeque::new();
                    let snap = CONNECTION_STATUS_DIV.ms() * 1000000;
                    let mut ts = self.range.beg / snap * snap;
                    while ts < self.range.end {
                        debug!("Use ts {ts}");
                        ts_msps.push_back(ts);
                        ts += snap;
                    }
                    let st = ReadValues::new(
                        self.series,
                        self.range.clone(),
                        ts_msps,
                        true,
                        self.do_one_before_range,
                        self.scy.clone(),
                    );
                    self.state = FrState::ReadValues(st);
                    continue;
                }
                FrState::ReadValues(ref mut st) => match st.fut.poll_unpin(cx) {
                    Ready(Ok(mut item)) => {
                        if !st.next() {
                            debug!("ReadValues exhausted");
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
