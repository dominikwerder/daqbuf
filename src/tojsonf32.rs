use futures_util::Stream;
use futures_util::StreamExt;
use items_0::streamitem::RangeCompletableItem;
use items_0::streamitem::Sitemty2;
use items_0::streamitem::StreamItem;
use items_0::timebin::BinningggContainerEventsDyn;
use items_2::binning::container_events::ContainerEvents;
use serde_json::json;

pub fn to_json_f32<T, E>(
    stream: impl Stream<Item = Sitemty2<T, E>>,
) -> impl Stream<Item = serde_json::Value>
where
    T: AsRef<dyn BinningggContainerEventsDyn>,
    E: ToString,
{
    stream.map(|x| match x {
        Ok(x) => match x {
            StreamItem::DataItem(x) => match x {
                RangeCompletableItem::Data(x) => {
                    let x = x.as_ref().to_f32_for_binning_v01();
                    if let Some(x) = x.as_any_ref().downcast_ref::<ContainerEvents<f32>>() {
                        let (tss, vals) = x.iter_zip().fold(
                            (Vec::new(), Vec::new()),
                            |(mut tss, mut vals), (ts, val)| {
                                tss.push(ts.ms());
                                vals.push(val);
                                (tss, vals)
                            },
                        );
                        json!({
                            "type": "events",
                            "tss": tss,
                            "vals": vals,
                        })
                    } else {
                        json!({
                            "type": "error",
                            "error": "can not downcast",
                        })
                    }
                }
                RangeCompletableItem::RangeComplete => {
                    json!({
                        "type": "RangeFinal",
                    })
                }
            },
            StreamItem::Log(x) => {
                json!({
                    "type": "log",
                    "log": x,
                })
            }
            StreamItem::Stats(x) => {
                json!({
                    "type": "stats",
                    "stats": x,
                })
            }
        },
        Err(e) => json!({
            "type": "error",
            "error": e.to_string(),
        }),
    })
}
