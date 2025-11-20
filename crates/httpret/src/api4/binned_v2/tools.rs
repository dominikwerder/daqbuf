use crate::channelconfig::ch_conf_from_binned;
use daqbuf_err as err;
use dbconn::worker::PgQueue;
use netpod::log;
use netpod::ChannelTypeConfigGen;
use netpod::NodeConfigCached;
use netpod::ReqCtx;
use netpod::UseScylla6Workarounds;
use nodenet::client::OpenBoxedBytesViaHttp;
use nodenet::scylla::ScyllaEventReadProvider;
use query::api4::binned::BinnedQuery;
use scyllaconn::worker::ScyllaQueue;
use std::pin::Pin;
use std::sync::Arc;
use streams::eventsplainreader::DummyCacheReadProvider;
use streams::eventsplainreader::SfDatabufferEventReadProvider;
use streams::streamtimeout::StreamTimeout2;
use streams::timebin::cached::reader::EventsReadProvider;
use streams::timebin::CacheReadProvider;
use tracing::Span;
use url::Url;

#[allow(unused)]
macro_rules! error { ($($arg:tt)*) => ( if true { log::error!($($arg)*); } ); }
#[allow(unused)]
macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }
#[allow(unused)]
macro_rules! debug { ($($arg:tt)*) => ( if true { log::debug!($($arg)*); } ); }
#[allow(unused)]
macro_rules! trace { ($($arg:tt)*) => ( if true { log::trace!($($arg)*); } ); }
#[allow(unused)]
macro_rules! log_query { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }

autoerr::create_error_v1!(
    name(Error, "BinnedV2Tools"),
    enum variants {
        ChannelNotFound,
        BadQuery(String),
        HttpLib(#[from] http::Error),
        ChannelConfig(crate::channelconfig::Error),
        Retrieval(#[from] crate::RetrievalError),
        EventsCbor(#[from] streams::plaineventscbor::Error),
        EventsJson(#[from] streams::plaineventsjson::Error),
        ServerError,
        BinnedStream(err::Error),
        TimebinnedJson(#[from] streams::timebinnedjson::Error),
        ReadAllCoarse(#[from] scyllaconn::binwriteindex::read_all_coarse::Error),
        Binned2FromBinned(#[from] scyllaconn::binned2::frombinned::Error),
        BinnedQuery(#[from] query::api4::binned::Error),
        BadRange,
        Msg(String),
    },
);

impl From<crate::channelconfig::Error> for Error {
    fn from(value: crate::channelconfig::Error) -> Self {
        use crate::channelconfig::Error::*;
        match value {
            NotFound(_) => Self::ChannelNotFound,
            _ => Self::ChannelConfig(value),
        }
    }
}

impl From<Error> for crate::RetrievalError {
    fn from(value: Error) -> Self {
        crate::RetrievalError::TextError(value.to_string())
    }
}

pub struct HandleRes2<'a> {
    pub logspan: Span,
    pub url: Url,
    pub query: BinnedQuery,
    pub ch_conf: ChannelTypeConfigGen,
    pub events_read_provider: Arc<dyn EventsReadProvider>,
    pub cache_read_provider: Arc<dyn CacheReadProvider>,
    pub timeout_provider: Arc<dyn StreamTimeout2>,
    pub pgqueue: &'a PgQueue,
    pub scyqueue: Option<ScyllaQueue>,
    pub use_scylla6_workarounds: UseScylla6Workarounds,
}

impl<'a> HandleRes2<'a> {
    pub async fn new(
        ctx: &ReqCtx,
        logspan: Span,
        url: Url,
        query: BinnedQuery,
        pgqueue: &'a PgQueue,
        scyqueue: Option<ScyllaQueue>,
        ncc: &NodeConfigCached,
    ) -> Result<Self, Error> {
        let use_scylla6_workarounds = query
            .use_scylla6_workarounds()
            .map(|x| {
                if x == 0 {
                    UseScylla6Workarounds::no_workarounds()
                } else {
                    UseScylla6Workarounds::with_workarounds()
                }
            })
            .unwrap_or(ncc.node_config.cluster.use_scylla6_workarounds());
        log_query!("HandleRes2::new  {:?}  {:?}", query, use_scylla6_workarounds);
        let ch_conf = ch_conf_from_binned(&query, ctx, pgqueue, ncc)
            .await?
            .ok_or_else(|| Error::ChannelNotFound)?;
        let open_bytes = Arc::pin(OpenBoxedBytesViaHttp::new(ncc.node_config.cluster.clone()));
        let (events_read_provider, cache_read_provider) = make_read_provider(
            ch_conf.name(),
            use_scylla6_workarounds.clone(),
            scyqueue.clone(),
            open_bytes,
            ctx,
            ncc,
        );
        let timeout_provider = streamio::streamtimeout::StreamTimeout::arced();
        let ret = Self {
            logspan,
            url,
            query,
            ch_conf,
            events_read_provider,
            cache_read_provider,
            timeout_provider,
            pgqueue,
            scyqueue,
            use_scylla6_workarounds,
        };
        Ok(ret)
    }
}

fn make_read_provider(
    chname: &str,
    use_scylla6_workarounds: UseScylla6Workarounds,
    scyqueue: Option<ScyllaQueue>,
    open_bytes: Pin<Arc<OpenBoxedBytesViaHttp>>,
    ctx: &ReqCtx,
    ncc: &NodeConfigCached,
) -> (Arc<dyn EventsReadProvider>, Arc<dyn CacheReadProvider>) {
    let events_read_provider = if chname.starts_with("unittest") {
        let x = streams::teststream::UnitTestStream::new();
        Arc::new(x)
    } else if ncc.node_config.cluster.scylla_lt().is_some() {
        scyqueue
            .clone()
            .map(|qu| ScyllaEventReadProvider::new(qu, use_scylla6_workarounds.clone()))
            .map(|x| Arc::new(x) as Arc<dyn EventsReadProvider>)
            .expect("scylla queue")
    } else if ncc.node.sf_databuffer.is_some() {
        // TODO do not clone the request. Pass an Arc up to here.
        let x = SfDatabufferEventReadProvider::new(Arc::new(ctx.clone()), open_bytes);
        Arc::new(x)
    } else {
        panic!("unexpected backend")
    };
    let cache_read_provider = if ncc.node_config.cluster.scylla_lt().is_some() {
        scyqueue
            .clone()
            .map(|qu| scyllaconn::bincache::ScyllaPrebinnedReadProvider::new(use_scylla6_workarounds, qu))
            .map(|x| Arc::new(x) as Arc<dyn CacheReadProvider>)
            .expect("scylla queue")
    } else if ncc.node.sf_databuffer.is_some() {
        let x = DummyCacheReadProvider::new();
        Arc::new(x)
    } else {
        panic!("unexpected backend")
    };
    (events_read_provider, cache_read_provider)
}
