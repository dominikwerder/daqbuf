use crate::tcprawclient::container_stream_from_bytes_stream;
use crate::tcprawclient::make_sub_query;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
use crate::transform::build_merged_event_transform;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::on_sitemty_data;
use items_0::streamitem::Sitemty;
use items_0::Events;
use items_2::channelevents::ChannelEvents;
use items_2::merger::Merger;
use netpod::log::*;
use netpod::ChannelTypeConfigGen;
use netpod::ReqCtx;
use query::api4::events::PlainEventsQuery;
use std::pin::Pin;

#[derive(Debug, thiserror::Error)]
#[cstm(name = "PlainEventsStream")]
pub enum Error {
    Netpod(#[from] netpod::NetpodError),
    Transform(#[from] crate::transform::Error),
    TcpRawClient(#[from] crate::tcprawclient::Error),
}

pub type DynEventsStream = Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Events>>> + Send>>;

pub async fn dyn_events_stream(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<DynEventsStream, Error> {
    trace!("dyn_events_stream  {}", evq.summary_short());
    use query::api4::events::EventsSubQuerySettings;
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
    let mut tr = build_merged_event_transform(evq.transform())?;
    let bytes_streams = open_bytes.open(subq, ctx.clone()).await?;
    let mut inps = Vec::new();
    for s in bytes_streams {
        let s = container_stream_from_bytes_stream::<ChannelEvents>(s, inmem_bufcap.clone(), "TODOdbgdesc".into())?;
        let s = Box::pin(s) as Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;
        inps.push(s);
    }
    // TODO make sure the empty container arrives over the network.
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, evq.merger_out_len_max());

    // let stream = stream.map(|item| {
    //     info!("item after merge: {item:?}");
    //     item
    // });

    let stream = crate::rangefilter2::RangeFilter2::new(stream, evq.range().try_into()?, evq.one_before_range());

    // let stream = stream.map(|item| {
    //     info!("item after rangefilter: {item:?}");
    //     item
    // });

    let stream = stream.map(move |k| {
        on_sitemty_data!(k, |k| {
            let k: Box<dyn Events> = Box::new(k);
            // trace!("got len {}", k.len());
            let k = tr.0.transform(k);
            Ok(StreamItem::DataItem(RangeCompletableItem::Data(k)))
        })
    });

    if let Some(wasmname) = evq.test_do_wasm() {
        let stream = transform_wasm::<_, items_0::streamitem::SitemErrTy>(stream, wasmname, ctx).await?;
        Ok(Box::pin(stream))
    } else {
        Ok(Box::pin(stream))
    }
}

#[cfg(not(feature = "wasm_transform"))]
async fn transform_wasm<INP, ETS>(
    stream: INP,
    _wasmname: &str,
    _ctx: &ReqCtx,
) -> Result<impl Stream<Item = Sitemty<Box<dyn Events>>> + Send, Error>
where
    INP: Stream<Item = Sitemty<Box<dyn Events>>> + Send + 'static,
{
    let ret: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Events>>> + Send>> = Box::pin(stream);
    Ok(ret)
}

#[cfg(feature = "wasm_transform")]
async fn transform_wasm<INP>(
    stream: INP,
    wasmname: &str,
    ctx: &ReqCtx,
) -> Result<impl Stream<Item = Sitemty<Box<dyn Events>>> + Send, Error>
where
    INP: Stream<Item = Sitemty<Box<dyn Events>>> + Send + 'static,
{
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
    let memory = wasmer::Memory::new(&mut store, wasmer::MemoryType::new(10, Some(30), false)).unwrap();
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
        let item = on_sitemty_data!(x, |mut evs: Box<dyn Events>| {
            let x = {
                use items_0::AsAnyMut;
                if true {
                    let r1 = evs
                        .as_any_mut()
                        .downcast_mut::<items_2::eventsdim0::EventsDim0<f64>>()
                        .is_some();
                    let r2 = evs
                        .as_mut()
                        .as_any_mut()
                        .downcast_mut::<items_2::eventsdim0::EventsDim0<f64>>()
                        .is_some();
                    let r3 = evs
                        .as_any_mut()
                        .downcast_mut::<Box<items_2::eventsdim0::EventsDim0<f64>>>()
                        .is_some();
                    let r4 = evs
                        .as_mut()
                        .as_any_mut()
                        .downcast_mut::<Box<items_2::eventsdim0::EventsDim0<f64>>>()
                        .is_some();
                    let r5 = evs.as_mut().as_any_mut().downcast_mut::<ChannelEvents>().is_some();
                    let r6 = evs.as_mut().as_any_mut().downcast_mut::<Box<ChannelEvents>>().is_some();
                    debug!("wasm  castings:  {r1}  {r2}  {r3}  {r4}  {r5}  {r6}");
                }
                if let Some(evs) = evs.as_any_mut().downcast_mut::<ChannelEvents>() {
                    match evs {
                        ChannelEvents::Events(evs) => {
                            if let Some(evs) = evs.as_any_mut().downcast_mut::<items_2::eventsdim0::EventsDim0<f64>>() {
                                use items_0::WithLen;
                                if evs.len() == 0 {
                                    debug!("wasm  empty EventsDim0<f64>");
                                } else {
                                    debug!("wasm  see EventsDim0<f64>");
                                    let max_len_needed = 16000;
                                    let dummy1 = instance.exports.get_function("dummy1").unwrap();
                                    let s = evs.values.as_mut_slices();
                                    for sl in [s.0, s.1] {
                                        if sl.len() > max_len_needed as _ {
                                            // TODO cause error
                                            panic!();
                                        }
                                        let wmemoff = buffer_ptr as u64;
                                        let view = memory.view(&store);
                                        // TODO is the offset bytes or elements?
                                        let wsl = WasmSlice::<f64>::new(&view, wmemoff, sl.len() as _).unwrap();
                                        // debug!("wasm pages {:?}  data size {:?}", view.size(), view.data_size());
                                        wsl.write_slice(&sl).unwrap();
                                        let ptr = wsl.as_ptr32();
                                        debug!("ptr {:?}  offset {}", ptr, ptr.offset());
                                        let params = [Value::I32(ptr.offset() as _), Value::I32(sl.len() as _)];
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
                                        let wsl = WasmSlice::<f64>::new(&view, wmemoff, sl.len() as _).unwrap();
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
    let ret: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Events>>> + Send>> = Box::pin(stream);
    Ok(ret)
}
