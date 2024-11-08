use std::fmt;
use std::time::Duration;
use tracing::Instrument;

async fn collect_in_span<T, S>(
    stream: S,
    deadline: Instant,
    events_max: u64,
    range: Option<SeriesRange>,
    binrange: Option<BinnedRangeEnum>,
) -> Result<Box<dyn CollectedDyn>, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: CollectableDyn,
{
    info!("collect  events_max {events_max}  deadline {deadline:?}");
    let mut collector: Option<Box<dyn CollectorDyn>> = None;
    let mut stream = stream;
    let deadline = deadline.into();
    let mut range_complete = false;
    let mut timed_out = false;
    let mut total_duration = Duration::ZERO;
    loop {
        let item = match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(k)) => k,
            Ok(None) => break,
            Err(_e) => {
                warn!("collect timeout");
                timed_out = true;
                if let Some(coll) = collector.as_mut() {
                    info!("collect_in_span  call  set_timed_out");
                    coll.set_timed_out();
                } else {
                    warn!("collect_in_span  collect timeout but no collector yet");
                }
                break;
            }
        };
        match item {
            Ok(item) => match item {
                StreamItem::DataItem(item) => match item {
                    RangeCompletableItem::RangeComplete => {
                        range_complete = true;
                        if let Some(coll) = collector.as_mut() {
                            coll.set_range_complete();
                        } else {
                            warn!("collect_in_span  received RangeComplete but no collector yet");
                        }
                    }
                    RangeCompletableItem::Data(mut item) => {
                        trace!("collect_in_span  sees len {}", item.len());
                        if collector.is_none() {
                            let c = item.new_collector();
                            collector = Some(c);
                        }
                        let coll = collector.as_mut().unwrap();
                        coll.ingest(&mut item);
                        if coll.len() as u64 >= events_max {
                            warn!("span reached events_max {}", events_max);
                            info!("collect_in_span  call  set_continue_at_here");
                            coll.set_continue_at_here();
                            break;
                        }
                    }
                },
                StreamItem::Log(item) => {
                    trace!("collect_in_span  log {:?}", item);
                }
                StreamItem::Stats(item) => {
                    trace!("collect_in_span  stats {:?}", item);
                    match item {
                        // TODO factor and simplify the stats collection:
                        StatsItem::EventDataReadStats(_) => {}
                        StatsItem::RangeFilterStats(_) => {}
                        StatsItem::DiskStats(item) => match item {
                            DiskStats::OpenStats(k) => {
                                total_duration += k.duration;
                            }
                            DiskStats::SeekStats(k) => {
                                total_duration += k.duration;
                            }
                            DiskStats::ReadStats(k) => {
                                total_duration += k.duration;
                            }
                            DiskStats::ReadExactStats(k) => {
                                total_duration += k.duration;
                            }
                        },
                        _ => {}
                    }
                }
            },
            Err(e) => {
                // TODO  Need to use some flags to get good enough error message for remote user.
                return Err(ErrMsg(e).into());
            }
        }
    }
    let _ = range_complete;
    let _ = timed_out;
    let res = collector
        .ok_or_else(|| Error::NoResultNoCollector)?
        .result(range, binrange)
        .map_err(ErrMsg)?;
    info!("collect_in_span  stats total duration: {:?}", total_duration);
    Ok(res)
}

async fn collect<T, S>(
    stream: S,
    deadline: Instant,
    events_max: u64,
    range: Option<SeriesRange>,
    binrange: Option<BinnedRangeEnum>,
) -> Result<Box<dyn CollectedDyn>, Error>
where
    S: Stream<Item = Sitemty<T>> + Unpin,
    T: CollectableDyn + WithLen + fmt::Debug,
{
    let span = span!(Level::INFO, "collect");
    collect_in_span(stream, deadline, events_max, range, binrange)
        .instrument(span)
        .await
}
