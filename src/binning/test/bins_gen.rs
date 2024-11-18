use crate::binning::container_bins::ContainerBins;
use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinningggContainerBinsDyn;
use std::pin::Pin;

pub(super) fn boxed_conts<S>(inp: S) -> Pin<Box<dyn Stream<Item = <S as Stream>::Item> + Send>>
where
    S: Stream + Send + 'static,
{
    Box::pin(inp)
}

pub(super) fn bins_gen_dim0_f32_v00(
) -> impl Stream<Item = Sitemty<Box<dyn BinningggContainerBinsDyn>>> {
    futures_util::stream::iter((0usize..1000).into_iter())
        .map(|x| {
            let c = ContainerBins::<f32>::new();
            Box::new(c) as Box<dyn BinningggContainerBinsDyn>
        })
        .map(|x| Ok(StreamItem::DataItem(RangeCompletableItem::Data(x))))
}
