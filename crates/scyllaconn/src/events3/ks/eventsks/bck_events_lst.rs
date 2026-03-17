use crate::events3::SeriesInfo;
use crate::events3::msplsp::LspEv;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::Stream;
use items_0::streamitem::Sitemty2;
use items_2::channelevents::ChannelEvents;
use netpod::TsMs;
use netpod::futdbg::FutDbg;
use netpod::futdbg::FutDbgBox;
use serde::Serialize;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ) }
macro_rules! warn { ($($arg:tt)*) => ( if true { log::warn!($($arg)*); } ) }
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ) }
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ) }
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ) }

autoerr::create_error_v1!(
    name(Error, "BckEventsLst"),
    enum variants {
        LspLst(#[from] crate::events3::lsplst::Error),
    },
);

#[derive(Debug, Serialize)]
pub struct Res1 {
    lsps_a: VecDeque<Option<LspEv>>,
}

#[derive(Debug)]
pub struct BckEventsLst {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    scyqu: ScyllaQueueCluster,
    range: ScyllaSeriesRange,
    fut: FutDbg<Result<Res1, Error>>,
}

impl BckEventsLst {
    async fn fetch(
        series_info: SeriesInfo,
        ks: KeyspaceId,
        msps: VecDeque<MspEv>,
        scyqu: ScyllaQueueCluster,
    ) -> Result<Res1, Error> {
        let series = series_info.id();
        let end = LspEv::max();
        let mut lsps_a = VecDeque::new();
        for msp in msps.iter() {
            let x = scyqu
                .read_03_lsp_lst(ks.clone(), series_info.clone(), msp.clone(), None)
                .await?;
            lsps_a.push_back(x);
        }
        let ret = Res1 { lsps_a };
        Ok(ret)
    }

    pub fn new(
        series_info: SeriesInfo,
        ks: KeyspaceId,
        range: ScyllaSeriesRange,
        msps: VecDeque<MspEv>,
        scyqu: ScyllaQueueCluster,
    ) -> Self {
        // TODO fetch the latest lsp without value for each msp
        let fut = Self::fetch(series_info.clone(), ks.clone(), msps, scyqu.clone()).box2();
        // TODO if lsps after range begin, backwards search again for the latest before.
        // TODO determine the latest before.
        // TODO record effort information.
        Self {
            series_info,
            ks,
            scyqu,
            range,
            fut,
        }
    }
}

impl Future for BckEventsLst {
    type Output = Result<Res1, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use Poll::*;
        match self.fut.poll_unpin(cx) {
            Ready(x) => match x {
                Ok(x) => Ready(Ok(x)),
                Err(e) => Ready(Err(e)),
            },
            Pending => Pending,
        }
    }
}
