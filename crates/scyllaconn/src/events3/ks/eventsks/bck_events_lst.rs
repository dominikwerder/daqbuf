use crate::events3::SeriesInfo;
use crate::events3::msplsp::LspEv;
use crate::events3::msplsp::MspEv;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaOptsSubmit;
use crate::worker::ScyllaQueueCluster;
use futures_util::Future;
use futures_util::FutureExt;
use netpod::TsNano;
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

fn _keep() {
    error!("");
    warn!("");
    info!("");
    debug!("");
    trace!("");
}

autoerr::create_error_v1!(
    name(Error, "BckLspLst"),
    enum variants {
        LspLst(#[from] crate::events3::lsplst::Error),
        LspImpossible,
    },
);

#[derive(Debug, Serialize)]
pub struct Res1 {
    lsps_a: VecDeque<(MspEv, Option<LspEv>)>,
}

impl Res1 {
    pub fn lsps(&self) -> &VecDeque<(MspEv, Option<LspEv>)> {
        &self.lsps_a
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct BckLspLst {
    series_info: SeriesInfo,
    ks: KeyspaceId,
    scyopts: ScyllaOptsSubmit,
    scyqu: ScyllaQueueCluster,
    fut: FutDbg<Result<Res1, Error>>,
}

impl BckLspLst {
    pub fn new(
        series_info: SeriesInfo,
        ks: KeyspaceId,
        msps: VecDeque<MspEv>,
        end: Option<TsNano>,
        scyopts: ScyllaOptsSubmit,
        scyqu: ScyllaQueueCluster,
    ) -> Self {
        // TODO fetch the latest lsp without value for each msp
        let fut = Self::fetch(
            series_info.clone(),
            ks.clone(),
            msps,
            end,
            scyopts.clone(),
            scyqu.clone(),
        )
        .box2();
        // TODO if lsps after range begin, backwards search again for the latest before.
        // TODO determine the latest before.
        // TODO record effort information.
        Self {
            series_info,
            ks,
            scyopts,
            scyqu,
            fut,
        }
    }

    async fn fetch(
        series_info: SeriesInfo,
        ks: KeyspaceId,
        msps: VecDeque<MspEv>,
        end: Option<TsNano>,
        scyopts: ScyllaOptsSubmit,
        scyqu: ScyllaQueueCluster,
    ) -> Result<Res1, Error> {
        let selfname = std::any::type_name::<Self>();
        let clt = scyqu.tag();
        let kst = ks.name();
        let mut lsps_a = VecDeque::new();
        let scyopts2 = scyopts.resolve(scyqu.scyopts());
        for msp in msps.iter() {
            let end = if let Some(end) = end {
                let x = if let Some(x) = msp.lsp(end) {
                    x
                } else {
                    return Err(Error::LspImpossible);
                };
                Some(x)
            } else {
                None
            };
            if scyopts2.avoid_order_desc {
                let lsps = scyqu
                    .read_03_lsp_only(ks.clone(), series_info.clone(), msp.clone(), scyopts.clone())
                    .await?;
                debug!("{selfname}  {clt}  {kst}  lsps len {}", lsps.len());
                let i = if let Some(end) = end {
                    lsps.partition_point(|x| *x < end)
                } else {
                    lsps.len()
                };
                if i > lsps.len() {
                    warn!("{selfname}  {clt}  {kst}  bad partition point");
                }
                lsps_a.push_back((*msp, lsps.get(i - 1).cloned()));
            } else {
                let x = scyqu
                    .read_03_lsp_lst(ks.clone(), series_info.clone(), msp.clone(), end, scyopts.clone())
                    .await?;
                lsps_a.push_back((*msp, x));
            }
        }
        let ret = Res1 { lsps_a };
        Ok(ret)
    }
}

impl Future for BckLspLst {
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
