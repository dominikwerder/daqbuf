use crate::rangefilter2::RangeFilter2;
use crate::tcprawclient::container_stream_from_bytes_stream;
use crate::tcprawclient::make_sub_query;
use crate::tcprawclient::OpenBoxedBytesStreamsBox;
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

pub type ChannelEventsStream = Pin<Box<dyn Stream<Item = Sitemty<ChannelEvents>> + Send>>;

pub async fn dyn_events_stream(
    evq: &PlainEventsQuery,
    ch_conf: ChannelTypeConfigGen,
    ctx: &ReqCtx,
    open_bytes: OpenBoxedBytesStreamsBox,
) -> Result<ChannelEventsStream, Error> {
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
    let bytes_streams = open_bytes.open(subq, ctx.clone()).await?;
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
    // TODO make sure the empty container arrives over the network.
    // TODO propagate also the max-buf-len for the first stage event reader.
    // TODO use a mixture of count and byte-size as threshold.
    let stream = Merger::new(inps, evq.merger_out_len_max());
    let stream = stream.inspect(|x| {
        if true {
            use items_0::streamitem::RangeCompletableItem::*;
            use items_0::streamitem::StreamItem::*;
            use items_0::WithLen;
            use items_2::channelevents::ChannelEvents;
            match x {
                Ok(DataItem(Data(ChannelEvents::Events(x)))) => {
                    trace!("after MERGE  yields item len {}", x.len());
                }
                _ => {
                    trace!("after MERGE  yields item {:?}", x);
                }
            }
        }
    });
    let stream = RangeFilter2::new(stream, evq.range().try_into()?, evq.one_before_range());
    let stream = stream.inspect(|x| {
        if true {
            use items_0::streamitem::RangeCompletableItem::*;
            use items_0::streamitem::StreamItem::*;
            use items_0::WithLen;
            use items_2::channelevents::ChannelEvents;
            match x {
                Ok(DataItem(Data(ChannelEvents::Events(x)))) => {
                    trace!("after merge and filter  yields item len {}", x.len());
                }
                _ => {
                    trace!("after merge and filter  yields item {:?}", x);
                }
            }
        }
    });
    if let Some(wasmname) = evq.test_do_wasm() {
        let stream =
            transform_wasm::<_, items_0::streamitem::SitemErrTy>(stream, wasmname, ctx).await?;
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
) -> Result<impl Stream<Item = Sitemty<ChannelEvents>> + Send, Error>
where
    INP: Stream<Item = Sitemty<ChannelEvents>> + Send + 'static,
{
    let ret = Box::pin(stream);
    Ok(ret)
}

#[cfg(feature = "wasm_transform")]
async fn transform_wasm<INP>(
    stream: INP,
    wasmname: &str,
    ctx: &ReqCtx,
) -> Result<impl Stream<Item = Sitemty<ChannelEvents>> + Send, Error>
where
    INP: Stream<Item = Sitemty<ChannelEvents>> + Send + 'static,
{
    debug!("make wasm transform");
    use httpclient::url::Url;
    use items_2::binning::container_events::ContainerEvents;
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
        let item = on_sitemty_data!(x, |mut evs: Box<dyn Events>| {
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
                                    debug!("wasm  empty");
                                } else {
                                    debug!("wasm  see");
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
                                        let wsl =
                                            WasmSlice::<f64>::new(&view, wmemoff, sl.len() as _)
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
                                        let wsl =
                                            WasmSlice::<f64>::new(&view, wmemoff, sl.len() as _)
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
        item
    });
    let ret: Pin<Box<dyn Stream<Item = Sitemty<Box<dyn Events>>> + Send>> = Box::pin(stream);
    Ok(ret)
}
