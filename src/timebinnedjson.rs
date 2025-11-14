use crate::cbor_stream::CborStream;
use crate::collect::Collect;
use crate::collect::CollectResult;
use crate::json_stream::JsonStream;
use crate::rangefilter2::RangeFilter2;
use crate::streamtimeout::StreamTimeout2;
use crate::streamtimeout::TimeoutableStream;
use crate::tcprawclient::container_stream_from_bytes_stream;
use crate::tcprawclient::make_sub_query;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use crate::timebin::cached::reader::CacheReadProvider;
use crate::timebin::cached::reader::EventsReadProvider;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use items_0::collect_s::CollectableDyn;
use items_0::on_sitemty_data;
use items_0::streamitem::sitem_err2_from_string;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_2::channelevents::ChannelEvents;
use items_2::jsonbytes::CborBytes;
use items_2::jsonbytes::JsonBytes;
use items_2::merger::Merger;
use netpod::log;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use netpod::BinnedRangeEnum;
use netpod::ChannelTypeConfigGen;
use netpod::DtMs;
use netpod::ReqCtx;
use query::api4::binned::BinnedQuery;
use query::api4::events::EventsSubQuerySettings;
use query::transform::TransformQuery;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

autoerr::create_error_v1!(
    name(Error, "TimebinnedJson"),
    enum variants {
        Query(#[from] query::api4::binned::Error),
        FromLayers(#[from] super::timebin::fromlayers::Error),
        TcpRawClient(#[from] crate::tcprawclient::Error),
        Collect(#[from] crate::collect::Error),
        Json(#[from] serde_json::Error),
        Msg(String),
        BadRange,
    },
);

struct ErrMsg<E>(E)
where
    E: ToString;

impl<E> From<ErrMsg<E>> for Error
where
    E: ToString,
{
    fn from(value: ErrMsg<E>) -> Self {
        Self::Msg(value.0.to_string())
    }
}

#[allow(unused)]
fn assert_stream_send<'u, R>(
    stream: impl 'u + Send + Stream<Item = R>,
) -> impl 'u + Send + Stream<Item = R> {
    stream
}

pub async fn timebinnable_stream_sf_databuffer_channelevents(
    range: NanoRange,
    one_before_range: bool,
    ch_conf: ChannelTypeConfigGen,
    transform_query: TransformQuery,
    sub: EventsSubQuerySettings,
    log_level: String,
    ctx: Arc<ReqCtx>,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<impl Stream<Item = Sitemty<ChannelEvents>>, Error> {
    let subq = make_sub_query(
        ch_conf,
        range.clone().into(),
        one_before_range,
        transform_query,
        sub.clone(),
        log_level.clone(),
        &ctx,
    );
    let inmem_bufcap = subq.inmem_bufcap();
    let _wasm1 = subq.wasm1().map(ToString::to_string);
    let bytes_streams = open_bytes.open(subq, ctx.as_ref().clone()).await?;
    let mut inps = Vec::new();
    for s in bytes_streams {
        let s = container_stream_from_bytes_stream::<ChannelEvents>(
            s,
            inmem_bufcap.clone(),
            "TODOdbgdesc".into(),
        )?;
        let s = Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;
        inps.push(s);
    }
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, sub.merger_out_len_max());
    let stream = RangeFilter2::new(stream, range, one_before_range);
    let stream = stream.map(move |k: Sitemty<ChannelEvents>| {
        use ChannelEvents;
        use RangeCompletableItem::*;
        use StreamItem::*;
        match k {
            Ok(DataItem(Data(ChannelEvents::Events(k)))) => {
                // let k = k.to_dim0_f32_for_binning();
                Ok(StreamItem::DataItem(RangeCompletableItem::Data(
                    ChannelEvents::Events(k),
                )))
            }
            _ => k,
        }
    });

    #[cfg(feature = "wasm_transform")]
    let stream = if let Some(wasmname) = wasm1 {
        debug!("make wasm transform");
        use httpclient::url::Url;
        use wasmer::Value;
        use wasmer::WasmSlice;
        let t = httpclient::http_get(
            Url::parse(&format!("http://data-api.psi.ch/distri/{}", wasmname)).unwrap(),
            "*/*",
            ctx,
        )
        .await
        .unwrap();
        let wasm = t.body;
        // let wasm = include_bytes!("dummy.wasm");
        let mut store = wasmer::Store::default();
        let module = wasmer::Module::new(&store, wasm).unwrap();
        // TODO assert that memory is large enough
        let memory =
            wasmer::Memory::new(&mut store, wasmer::MemoryType::new(10, Some(30), false)).unwrap();
        let import_object = wasmer::imports! {
            "env" => {
                "memory" => memory.clone(),
            }
        };
        let instance = wasmer::Instance::new(&mut store, &module, &import_object).unwrap();
        let get_buffer_ptr = instance.exports.get_function("get_buffer_ptr").unwrap();
        let buffer_ptr = get_buffer_ptr.call(&mut store, &[]).unwrap();
        let buffer_ptr = buffer_ptr[0].i32().unwrap();
        let stream = stream.map(move |x| {
            let memory = memory.clone();
            let item = on_sitemty_data!(x, |mut evs: Box<dyn TodoUseType>| {
                let x = {
                    use items_0::AsAnyMut;
                    if true {
                        let r1 = evs
                            .as_any_mut()
                            .downcast_mut::<ContainerEvents<f64>>()
                            .is_some();
                        let r2 = evs
                            .as_any_mut()
                            .downcast_mut::<Box<ContainerEvents<f64>>>()
                            .is_some();
                        let r3 = evs
                            .as_mut()
                            .as_any_mut()
                            .downcast_mut::<ChannelEvents>()
                            .is_some();
                        let r4 = evs
                            .as_mut()
                            .as_any_mut()
                            .downcast_mut::<Box<ChannelEvents>>()
                            .is_some();
                        debug!("wasm  castings:  {r1}  {r2}  {r3}  {r4}");
                    }
                    if let Some(evs) = evs.as_any_mut().downcast_mut::<ChannelEvents>() {
                        match evs {
                            ChannelEvents::Events(evs) => {
                                if let Some(evs) =
                                    evs.as_any_mut().downcast_mut::<ContainerEvents<f64>>()
                                {
                                    use items_0::WithLen;
                                    if evs.len() == 0 {
                                        debug!("wasm  empty EventsDim0<f64>");
                                    } else {
                                        debug!("wasm  see EventsDim0<f64>  len {}", evs.len());
                                        let max_len_needed = 16000;
                                        let dummy1 =
                                            instance.exports.get_function("dummy1").unwrap();
                                        let s = evs.values.as_mut_slices();
                                        for sl in [s.0, s.1] {
                                            if sl.len() > max_len_needed as _ {
                                                // TODO cause error
                                                panic!();
                                            }
                                            let wmemoff = buffer_ptr as u64;
                                            let view = memory.view(&store);
                                            // TODO is the offset bytes or elements?
                                            let wsl = WasmSlice::<f64>::new(
                                                &view,
                                                wmemoff,
                                                sl.len() as _,
                                            )
                                            .unwrap();
                                            // debug!("wasm pages {:?}  data size {:?}", view.size(), view.data_size());
                                            wsl.write_slice(&sl).unwrap();
                                            let ptr = wsl.as_ptr32();
                                            debug!("ptr {:?}  offset {}", ptr, ptr.offset());
                                            let params = [
                                                Value::I32(ptr.offset() as _),
                                                Value::I32(sl.len() as _),
                                            ];
                                            let res = dummy1.call(&mut store, &params).unwrap();
                                            match res[0] {
                                                Value::I32(x) => {
                                                    debug!("wasm  dummy1 returned: {x:?}");
                                                    if x != 1 {
                                                        error!("unexpected return value {res:?}");
                                                    }
                                                }
                                                _ => {
                                                    error!("unexpected return type {res:?}");
                                                }
                                            }
                                            // Init the slice again because we need to drop ownership for the function call.
                                            let view = memory.view(&store);
                                            let wsl = WasmSlice::<f64>::new(
                                                &view,
                                                wmemoff,
                                                sl.len() as _,
                                            )
                                            .unwrap();
                                            wsl.read_slice(sl).unwrap();
                                        }
                                    }
                                } else {
                                    debug!("wasm  not EventsDim0<f64>");
                                }
                            }
                            ChannelEvents::Status(_) => {}
                        }
                    } else {
                        debug!("wasm  not ChannelEvents");
                    }
                    evs
                };
                Ok(StreamItem::DataItem(RangeCompletableItem::Data(x)))
            });
            // Box::new(item) as Box<dyn Framable + Send>
            item
        });
        Box::pin(stream) as Pin<Box<dyn Stream<Item = Sitemty<Box<dyn TodoUseType>>> + Send>>
    } else {
        let stream = stream.map(|x| x);
        Box::pin(stream)
    };
    Ok(stream)
}

async fn timebinned_stream(
    query: BinnedQuery,
    binned_range: BinnedRangeEnum,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    events_read_provider: Arc<dyn EventsReadProvider>,
) -> Result<Pin<Box<dyn Stream<Item = Sitemty<Box<dyn CollectableDyn>>> + Send>>, Error> {
    let do_time_weight = true;
    let bin_len_layers = if let Some(subgrids) = query.subgrids() {
        subgrids
            .iter()
            .map(|&x| DtMs::from_ms_u64(1000 * x.as_secs()))
            .collect()
    } else {
        netpod::time_bin_len_cache_opts().to_vec()
    };
    let stream = crate::timebin::fromlayers::TimeBinnedFromLayers::new(
        ch_conf,
        (&query).into(),
        query.cache_usage().unwrap_or(Default::default()),
        query.transform().clone(),
        EventsSubQuerySettings::from(&query),
        query.log_level().into(),
        Arc::new(ctx.clone()),
        binned_range
            .binned_range_time()
            .ok_or_else(|| Error::BadRange)?,
        do_time_weight,
        bin_len_layers,
        cache_read_provider,
        events_read_provider,
    )?;
    let stream = stream.map(|item| {
        use items_0::timebin::BinsBoxed;
        on_sitemty_data!(item, |mut x: BinsBoxed| {
            x.fix_numerics();
            let ret = x.boxed_into_collectable_box();
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(ret)))
        })
    });
    let stream = Box::pin(stream);
    Ok(stream)
}

pub async fn timebinned_json(
    query: BinnedQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    events_read_provider: Arc<dyn EventsReadProvider>,
    timeout_provider: Arc<dyn StreamTimeout2>,
) -> Result<CollectResult<JsonBytes>, Error> {
    let deadline = Instant::now()
        + query
            .timeout_content()
            .unwrap_or(Duration::from_millis(3000))
            .min(Duration::from_millis(5000))
            .max(Duration::from_millis(200));
    let binned_range = query.covering_range()?;
    // TODO derive better values, from query
    let collect_max = 10000;
    let bytes_max = 100 * collect_max;
    let stream = timebinned_stream(
        query.clone(),
        binned_range.clone(),
        ch_conf,
        ctx,
        cache_read_provider,
        events_read_provider,
    )
    .await?;
    let collected = Collect::new(stream, deadline, collect_max, bytes_max, timeout_provider);
    let collected: BoxFuture<_> = Box::pin(collected);
    let collres = collected.await?;
    match collres {
        CollectResult::Some(res) => {
            let val = res.into_user_facing_api_type_box().into_serializable_json();
            let jsval = serde_json::to_string(&val)?;
            Ok(CollectResult::Some(JsonBytes::new(jsval)))
        }
        CollectResult::Empty => Ok(CollectResult::Empty),
        CollectResult::Timeout => Ok(CollectResult::Timeout),
    }
}

fn take_collector_result_json(
    coll: &mut Box<dyn items_0::collect_s::CollectorDyn>,
) -> Option<JsonBytes> {
    match coll.result() {
        Ok(collres) => {
            let x = collres.into_user_facing_api_type_box();
            let val = x.into_serializable_json();
            match serde_json::to_string(&val) {
                Ok(jsval) => Some(JsonBytes::new(jsval)),
                Err(e) => {
                    error!("{}", e);
                    Some(JsonBytes::new("{\"ERROR\":true}"))
                }
            }
        }
        Err(e) => {
            error!("{}", e);
            Some(JsonBytes::new("{\"ERROR\":true}"))
        }
    }
}

fn take_collector_result_cbor(
    coll: &mut Box<dyn items_0::collect_s::CollectorDyn>,
) -> Option<CborBytes> {
    match coll.result() {
        Ok(collres) => {
            trace!("take_collector_result_cbor  len {}", collres.len());
            let x = collres.into_user_facing_api_type_box();
            let val = x.into_serializable_normal();
            let mut buf = Vec::with_capacity(1024);
            ciborium::into_writer(&val, &mut buf).expect("cbor serialize");
            let bytes = Bytes::from(buf);
            let item = CborBytes::new(bytes);
            Some(item)
        }
        Err(e) => {
            error!("{}", e);
            use ciborium::cbor;
            let val = cbor!({
                "ERROR" => true,
            })
            .unwrap();
            let mut buf = Vec::with_capacity(1024);
            ciborium::into_writer(&val, &mut buf).expect("cbor serialize");
            let bytes = Bytes::from(buf);
            let item = CborBytes::new(bytes);
            Some(item)
        }
    }
}

pub fn timeoutable_collectable_stream_to_json_bytes(
    stream: Pin<Box<dyn Stream<Item = Option<Option<Sitemty<Box<dyn CollectableDyn>>>>> + Send>>,
    timeout_content_2: Duration,
    emit_log_items: bool,
) -> Pin<Box<dyn Stream<Item = Result<JsonBytes, crate::json_stream::Error>> + Send>> {
    let mut coll = None;
    let mut last_emit = Instant::now();
    let stream = stream.map(move |x| {
        match x {
            Some(x) => match x {
                Some(x) => match x {
                    Ok(x) => match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(mut item) => {
                                let coll = coll.get_or_insert_with(|| item.new_collector());
                                coll.ingest(&mut item);
                                if coll.len() >= 128 || last_emit.elapsed() >= timeout_content_2 {
                                    last_emit = Instant::now();
                                    take_collector_result_json(coll).map(|x| Ok(x))
                                } else {
                                    // Some(serde_json::Value::String(format!("coll len {}", coll.len())))
                                    None
                                }
                            }
                            RangeCompletableItem::RangeComplete => None,
                        },
                        StreamItem::Log(x) => {
                            if emit_log_items {
                                let obj = serde_json::json!({"type": "log", "obj": x});
                                match serde_json::to_string(&obj) {
                                    Ok(json) => Some(Ok(JsonBytes::new(json))),
                                    Err(e) => Some(Err(sitem_err2_from_string(e))),
                                }
                            } else {
                                match x.level() {
                                    Level::ERROR => {
                                        error!("{}", x.display_log_file());
                                    }
                                    Level::WARN => {
                                        warn!("{}", x.display_log_file());
                                    }
                                    Level::INFO => {
                                        info!("{}", x.display_log_file());
                                    }
                                    Level::DEBUG => {
                                        debug!("{}", x.display_log_file());
                                    }
                                    Level::TRACE => {
                                        trace!("{}", x.display_log_file());
                                    }
                                }
                                None
                            }
                        }
                        StreamItem::Stats(x) => {
                            debug!("{x:?}");
                            // Some(serde_json::Value::String(format!("{x:?}")))
                            None
                        }
                    },
                    Err(e) => Some(Err(e)),
                },
                None => {
                    if let Some(coll) = coll.as_mut() {
                        last_emit = Instant::now();
                        take_collector_result_json(coll).map(|x| Ok(x))
                    } else {
                        // Some(serde_json::Value::String(format!(
                        //     "end of input but no collector to take something from"
                        // )))
                        None
                    }
                }
            },
            None => {
                if let Some(coll) = coll.as_mut() {
                    if coll.len() != 0 {
                        last_emit = Instant::now();
                        take_collector_result_json(coll).map(|x| Ok(x))
                    } else {
                        // Some(serde_json::Value::String(format!("timeout but nothing to do")))
                        None
                    }
                } else {
                    // Some(serde_json::Value::String(format!("timeout but no collector")))
                    None
                }
            }
        }
    });
    let stream = stream.filter_map(|x| futures_util::future::ready(x));
    let stream = stream.map_err(|e| crate::json_stream::Error::Msg(e.to_string()));
    let stream: Pin<Box<dyn Stream<Item = Result<JsonBytes, crate::json_stream::Error>> + Send>> =
        Box::pin(stream);
    stream
}

pub async fn timebinned_json_framed(
    query: BinnedQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    events_read_provider: Arc<dyn EventsReadProvider>,
    timeout_provider: Arc<dyn StreamTimeout2>,
) -> Result<JsonStream, Error> {
    let binned_range = query.covering_range()?;
    // TODO derive better values, from query
    let stream = timebinned_stream(
        query.clone(),
        binned_range.clone(),
        ch_conf,
        ctx,
        cache_read_provider,
        events_read_provider,
    )
    .await?;
    // let stream = timebinned_to_collectable(stream);
    // TODO create a custom Stream adapter.
    // Want to timeout only on data items: the user wants to wait for bins only a maximum time.
    // But also, I want to coalesce.
    let timeout_content_base = query
        .timeout_content()
        .unwrap_or(Duration::from_millis(1000))
        .min(Duration::from_millis(5000))
        .max(Duration::from_millis(100));
    let timeout_content_2 = timeout_content_base * 2 / 3;
    let stream = stream
        .map(|x| Some(x))
        .chain(futures_util::stream::iter([None]));
    let stream = TimeoutableStream::new(timeout_content_base, timeout_provider, stream);
    let stream = Box::pin(stream);
    let stream = timeoutable_collectable_stream_to_json_bytes(stream, timeout_content_2, false);
    Ok(stream)
}

pub async fn timebinned_cbor_framed(
    query: BinnedQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    cache_read_provider: Arc<dyn CacheReadProvider>,
    events_read_provider: Arc<dyn EventsReadProvider>,
    timeout_provider: Arc<dyn StreamTimeout2>,
) -> Result<CborStream, Error> {
    let binned_range = query.covering_range()?;
    // TODO derive better values, from query
    let stream = timebinned_stream(
        query.clone(),
        binned_range.clone(),
        ch_conf,
        ctx,
        cache_read_provider,
        events_read_provider,
    )
    .await?;
    let timeout_content_base = query
        .timeout_content()
        .unwrap_or(Duration::from_millis(1000))
        .min(Duration::from_millis(5000))
        .max(Duration::from_millis(100));
    let timeout_content_2 = timeout_content_base * 2 / 3;
    let mut coll = None;
    let mut last_emit = Instant::now();
    let log_items_level = match query.log_items() {
        "trace" => log::Level::TRACE,
        "debug" => log::Level::DEBUG,
        "info" => log::Level::INFO,
        "warn" => log::Level::WARN,
        _ => log::Level::ERROR,
    };
    let stats_items = match query.stats_items() {
        Some(_) => {
            // TODO ?
            true
        }
        None => false,
    };
    let stream = stream
        .map(|x| Some(x))
        .chain(futures_util::stream::iter([None]));
    let stream = TimeoutableStream::new(timeout_content_base, timeout_provider, stream);
    let stream = stream.map(move |x| {
        match x {
            Some(x) => match x {
                Some(x) => match x {
                    Ok(x) => match x {
                        StreamItem::DataItem(x) => match x {
                            RangeCompletableItem::Data(mut item) => {
                                let tsnow = Instant::now();
                                let coll = coll.get_or_insert_with(|| item.new_collector());
                                coll.ingest(&mut item);
                                if coll.len() >= 128 || tsnow >= last_emit + timeout_content_2 {
                                    last_emit = tsnow;
                                    take_collector_result_cbor(coll).map(|x| Ok(x))
                                } else {
                                    None
                                }
                            }
                            RangeCompletableItem::RangeComplete => None,
                        },
                        StreamItem::Log(x) => {
                            if x.level() <= log_items_level {
                                let mut buf = Vec::with_capacity(1024);
                                ciborium::into_writer(&x, &mut buf).expect("cbor serialize");
                                let bytes = Bytes::from(buf);
                                let item = CborBytes::new(bytes);
                                Some(Ok(item))
                            } else {
                                None
                            }
                        }
                        StreamItem::Stats(x) => {
                            if stats_items {
                                let mut buf = Vec::with_capacity(1024);
                                ciborium::into_writer(&x, &mut buf).expect("cbor serialize");
                                let bytes = Bytes::from(buf);
                                let item = CborBytes::new(bytes);
                                Some(Ok(item))
                            } else {
                                None
                            }
                        }
                    },
                    Err(e) => Some(Err(e)),
                },
                None => {
                    if let Some(coll) = coll.as_mut() {
                        last_emit = Instant::now();
                        take_collector_result_cbor(coll).map(|x| Ok(x))
                    } else {
                        // Some(serde_json::Value::String(format!(
                        //     "end of input but no collector to take something from"
                        // )))
                        None
                    }
                }
            },
            None => {
                if let Some(coll) = coll.as_mut() {
                    if coll.len() != 0 {
                        last_emit = Instant::now();
                        take_collector_result_cbor(coll).map(|x| Ok(x))
                    } else {
                        // Some(serde_json::Value::String(format!("timeout but nothing to do")))
                        None
                    }
                } else {
                    // Some(serde_json::Value::String(format!("timeout but no collector")))
                    None
                }
            }
        }
    });
    let stream = stream.filter_map(|x| futures_util::future::ready(x));
    let stream = stream.map_err(|e| crate::cbor_stream::Error::Msg(e.to_string()));
    Ok(Box::pin(stream))
}
