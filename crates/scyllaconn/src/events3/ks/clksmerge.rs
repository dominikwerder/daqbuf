use crate::events3::SeriesInfo;
use crate::events3::ks::eventsks::bck_events_lst::BckLspLst;
use crate::events3::msplsp::MspEv;
use crate::range::ScyllaSeriesRange;
use crate::worker::ScyllaOptsSubmit;
use crate::worker::ScyllaQueue;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use netpod::OneBeforeFlag;
use netpod::range::evrange::SeriesRange;
use netpod::ttl::RetentionTime;
use std::collections::VecDeque;
use streams::print_first_ts::PrintFirstTs;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }
macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ClKsMerged"),
    enum variants {
        Msg(#[from] String),
        ExpectTimeRange,
        Read03MspBck(#[from] crate::events3::mspbck::Error),
        BckLspLst(#[from] crate::events3::ks::eventsks::bck_events_lst::Error),
    },
);

const MSP_LIMIT_DEF: u32 = 10;
const LSP_LIMIT_DEF: u32 = 800;
const MSP_RESERVE_MIN: usize = 20;
const MSP_PREOPEN_MIN: usize = 6;
const LSP_SINGLE_BUF_MAX: usize = 5;

pub async fn cl_ks_merged(
    series_info: SeriesInfo,
    range: SeriesRange,
    one_before: OneBeforeFlag,
    filter_rts: Option<Vec<RetentionTime>>,
    scyqu: ScyllaQueue,
    scyopts: ScyllaOptsSubmit,
) -> Result<impl Stream<Item = Sitemty<ChannelEvents>> + Send, Error> {
    let range = range.to_time().ok_or(Error::ExpectTimeRange)?;
    let range = ScyllaSeriesRange::new(range.beg_ts(), range.end_ts());
    let mut inps = Vec::new();
    for cl in scyqu.clusters() {
        for ks in cl.keyspaces() {
            if filter_rts.as_ref().map_or(true, |fs| fs.contains(&ks.rt())) {
                let opts = crate::events3::ks::lsp_fwd_msp_multi::Opts::new(
                    MSP_LIMIT_DEF,
                    LSP_LIMIT_DEF,
                    MSP_RESERVE_MIN,
                    MSP_PREOPEN_MIN,
                    LSP_SINGLE_BUF_MAX,
                );
                let msps: VecDeque<_> = crate::events3::mspbck::msp_bck(
                    ks.clone(),
                    series_info.clone(),
                    range.beg(),
                    cl.as_ref().clone(),
                    scyopts.clone(),
                )
                .await?
                .into_iter()
                .map(MspEv::from)
                .collect();
                let scan_range = if one_before.as_bool() {
                    let res = BckLspLst::new(
                        series_info.clone(),
                        ks.clone(),
                        msps.clone(),
                        Some(range.beg()),
                        scyopts.clone(),
                        cl.as_ref().clone(),
                    )
                    .await?;
                    let range_beg_before = res
                        .lsps()
                        .iter()
                        .filter_map(|(msp, lsp)| lsp.as_ref().map(|lsp| msp.to_ts(*lsp)))
                        .filter(|ts| *ts < range.beg())
                        .max()
                        .unwrap_or(range.beg());

                    // TODO check log what timestamp we find for each RT
                    info!(
                        "cl_ks_merged  build  one_before  {:6}  {:6}  range_beg_before {}  range.beg {}",
                        cl.tag(),
                        ks.rt(),
                        range_beg_before,
                        range.beg()
                    );

                    ScyllaSeriesRange::new(range_beg_before, range.end())
                } else {
                    range.clone()
                };
                let stream = crate::events3::ks::lsp_fwd_msp_multi::LspFwdMspMulti::new(
                    ks.clone(),
                    series_info.clone(),
                    scan_range,
                    opts,
                    scyopts.clone(),
                    cl.as_ref().clone(),
                    msps,
                );
                let stream = streams::withlenhisto::WithLenHisto::new(
                    stream,
                    format!(
                        "after-LspFwdMspMulti-{}-{}-{}",
                        cl.tag(),
                        ks.name(),
                        ks.rt().debug_tag()
                    ),
                );
                let stream = PrintFirstTs::new(stream, format!("{}-{}", cl.tag(), ks.name()));
                inps.push(stream);
            }
        }
    }
    let stream = crate::events3::ks::lspmerge::LspMerge::new(inps, LSP_LIMIT_DEF);
    let stream = streams::dedup::Dedup::new(stream).map(|x| x);
    let stream = stream.map(|x| match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(x) => Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                    ChannelEvents::Events(x),
                ))),
                RangeCompletableItem::RangeComplete => Ok(StreamItem::DataItem(RangeCompletableItem::RangeComplete)),
            },
            StreamItem::Log(x) => Ok(StreamItem::Log(x)),
            StreamItem::Stats(x) => Ok(StreamItem::Stats(x)),
        },
        Err(e) => Err(daqbuf_err::Error::from_string(format!("{e}"))),
    });
    Ok(stream)
}
