use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use netpod::range::evrange::NanoRange;
use netpod::ChannelTypeConfigGen;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;
use scyllaconn::events3::ks::clksmerge::cl_ks_merged;
use scyllaconn::events3::SeriesInfo;
use scyllaconn::worker::ScyllaOptsSubmit;
use scyllaconn::worker::ScyllaQueue;
use series::SeriesId;
use streams::rangefilter2::RangeFilter2;
use streams::tcprawclient::container_stream_from_bytes_stream;
use streams::tcprawclient::make_sub_query;
use streams::tcprawclient::OpenBoxedBytesStreamsBox;
use streams::ChannelEventsStream;

macro_rules! debug { ($($arg:tt)*) => { if true { log::debug!($($arg)*); } }; }
macro_rules! trace { ($($arg:tt)*) => { if true { log::trace!($($arg)*); } }; }

fn _keep() {
    debug!("");
}

autoerr::create_error_v1!(
    name(Error, "DynEventsStream"),
    enum variants {
        NanoRangeFromSeriesRange,
        TcpRawClient(#[from] streams::tcprawclient::Error),
        MissingScylla,
        ClKsMerged(#[from] scyllaconn::events3::ks::clksmerge::Error),
    },
);

pub async fn dyn_events_stream(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
    scyqu: Option<ScyllaQueue>,
) -> Result<ChannelEventsStream, Error> {
    let selfname = "dyn_events_stream";
    trace!("{selfname}  {}", evq.summary_short());
    use query::api4::events::EventsSubQuerySettings;
    let stream = if let Ok(chconf) = ch_conf.to_scylla() {
        let scyqu = scyqu.ok_or(Error::MissingScylla)?;
        let series_info = SeriesInfo::new(
            SeriesId::new(chconf.series()),
            chconf.scalar_type().clone(),
            chconf.shape().clone(),
        );
        let range = evq.range().clone();
        let stream = cl_ks_merged(series_info, range, scyqu, ScyllaOptsSubmit::no_choice()).await?;
        Box::pin(stream) as ChannelEventsStream
    } else {
        let subq = make_sub_query(
            ch_conf,
            evq.range().clone(),
            evq.one_before_range(),
            evq.transform().clone(),
            EventsSubQuerySettings::from(evq),
            evq.log_level().into(),
            ctx,
        );
        let inmem_bufcap = subq.inmem_bufcap();
        let bytes_streams = open_bytes.open(subq, ctx.clone()).await?;
        let mut inps = Vec::new();
        for s in bytes_streams {
            let s = container_stream_from_bytes_stream::<ChannelEvents>(s, inmem_bufcap.clone(), "TODOdbgdesc".into())?;
            // let s = Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;
            let s = Box::pin(s) as ChannelEventsStream;
            inps.push(s);
        }
        // TODO make sure the empty container arrives over the network.
        // TODO propagate also the max-buf-len for the first stage event reader.
        // TODO use a mixture of count and byte-size as threshold.
        let stream = Merger::new(inps, evq.merger_out_len_max());
        let range_ty2 = match NanoRange::try_from(evq.range()) {
            Ok(x) => x,
            Err(_e) => return Err(Error::NanoRangeFromSeriesRange),
        };
        let stream = RangeFilter2::new(stream, range_ty2, evq.one_before_range());
        Box::pin(stream) as ChannelEventsStream
    };
    if let Some(wasmname) = evq.test_do_wasm() {
        let _ = wasmname;
        // let stream = transform_wasm::<_, items_0::streamitem::SitemErrTy>(stream, wasmname, ctx).await?;
        Ok(Box::pin(stream) as ChannelEventsStream)
    } else {
        Ok(Box::pin(stream) as ChannelEventsStream)
    }
}
