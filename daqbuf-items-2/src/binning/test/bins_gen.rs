use crate::binning::container_bins::ContainerBins;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinningggContainerBinsDyn;
use netpod::DtNano;
use netpod::TsNano;
use std::pin::Pin;

pub(super) fn boxed_conts<S>(inp: S) -> Pin<Box<dyn Stream<Item = <S as Stream>::Item> + Send>>
where
    S: Stream + Send + 'static,
{
    Box::pin(inp)
}

pub(super) fn bins_gen_dim0_f32_v00() -> impl Stream<Item = Sitemty<Box<dyn BinningggContainerBinsDyn>>> {
    futures_util::stream::iter((9u64..100).into_iter())
        .map(|x| {
            let mut c = ContainerBins::<f32, f32>::new();
            let bl = DtNano::from_ms(10);
            let ts1 = TsNano::from_ms(bl.ms_u64() * x);
            let ts2 = ts1.add_dt_nano(bl);
            let cnt = 8;
            let min = 2.;
            let max = 4.;
            let agg = 2.2;
            let lst = 2.4;
            let fnl = true;
            c.push_back(ts1, ts2, cnt, min, max, agg, lst, fnl);
            Box::new(c) as Box<dyn BinningggContainerBinsDyn>
        })
        .map(|x| Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))))
}
