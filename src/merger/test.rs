use super::MergeInp;
use super::Merger;
use crate::binning::container_events::ContainerEvents;
use crate::log::*;
use futures_util::StreamExt;
use items_0::streamitem::sitem_data;
use netpod::TsNano;

async fn merger_00_inner() {
    let mut evs0 = ContainerEvents::<f32>::new();
    evs0.push_back(TsNano::from_ns(9), 9.0);
    let mut evs1 = ContainerEvents::<f32>::new();
    evs1.push_back(TsNano::from_ns(11), 11.0);
    let inp0: MergeInp<_> = Box::pin(futures_util::stream::iter([
        sitem_data(evs0),
        sitem_data(evs1),
    ]));
    let inps = vec![inp0];
    let mut merger = Merger::new(inps, None);
    while let Some(x) = merger.next().await {
        trace!("{:?}", x);
    }
    trace!("DONE");
}

#[test]
fn merger_00() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(merger_00_inner());
}
