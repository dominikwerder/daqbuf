use crate::events3::SeriesInfo;
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

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "ClKsMerged"),
    enum variants {
        Msg(#[from] String),
        ExpectTimeRange,
        Read03MspBck(#[from] crate::events3::mspbck::Error),
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
    scyqu: ScyllaQueue,
    scyopts: ScyllaOptsSubmit,
) -> Result<impl Stream<Item = Sitemty<ChannelEvents>> + Send, Error> {
    let range = range.to_time().ok_or(Error::ExpectTimeRange)?;
    let range = ScyllaSeriesRange::new(range.beg_ts(), range.end_ts());
    let mut inps = Vec::new();
    for cl in scyqu.clusters() {
        for ks in cl.keyspaces() {
            let opts = crate::events3::ks::lsp_fwd_msp_multi::Opts::new(
                MSP_LIMIT_DEF,
                LSP_LIMIT_DEF,
                MSP_RESERVE_MIN,
                MSP_PREOPEN_MIN,
                LSP_SINGLE_BUF_MAX,
            );
            let msps = crate::events3::mspbck::msp_bck(
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
            let stream = crate::events3::ks::lsp_fwd_msp_multi::LspFwdMspMulti::new(
                ks.clone(),
                series_info.clone(),
                range.clone(),
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
            inps.push(stream);
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
