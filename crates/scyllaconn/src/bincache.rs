use crate::events2::prepare::StmtsEvents;
use crate::worker::ScyllaQueue;
use daqbuf_series::msp::PrebinnedPartitioning;
use futures_util::TryStreamExt;
use items_0::merge::MergeableTy;
use items_2::binning::container_bins::ContainerBins;
use netpod::DtMs;
use netpod::TsNano;
use netpod::UseScylla6Workarounds;
use netpod::ttl::RetentionTime;
use std::ops::Range;
use streams::timebin::cached::reader::BinsReadRes;

type ScySession = scylla::client::session::Session;

async fn scylla_read_prebinned_f32(
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: Range<u32>,
    use_scylla6_workarounds: UseScylla6Workarounds,
    scyqueue: ScyllaQueue,
) -> BinsReadRes {
    let rts = [RetentionTime::Short, RetentionTime::Medium, RetentionTime::Long];
    let mut res = Vec::new();
    for rt in rts {
        let x = scyqueue
            .read_prebinned_f32(rt, series, bin_len, msp, offs.clone(), use_scylla6_workarounds.clone())
            .await?;
        res.push(x);
    }
    let mut out = ContainerBins::new();
    let mut dmp = ContainerBins::new();
    loop {
        // TODO count for metrics when duplicates are found, or other issues.
        let mins: Vec<_> = res.iter().map(|x| MergeableTy::ts_min(x)).collect();
        let mut ix = None;
        let mut min2 = None;
        for (i, min) in mins.iter().map(|x| x.clone()).enumerate() {
            if let Some(min) = min {
                if let Some(min2a) = min2 {
                    if min < min2a {
                        ix = Some(i);
                        min2 = Some(min);
                    }
                } else {
                    ix = Some(i);
                    min2 = Some(min);
                }
            }
        }
        if let Some(ix) = ix {
            let min2 = min2.unwrap();
            res[ix].drain_into(&mut out, 0..1);
            let pps: Vec<_> = res.iter().map(|x| MergeableTy::find_lowest_index_gt(x, min2)).collect();
            for (i, pp) in pps.into_iter().enumerate() {
                if let Some(pp) = pp {
                    MergeableTy::drain_into(res.get_mut(i).unwrap(), &mut dmp, 0..pp);
                }
            }
        } else {
            break;
        }
    }
    if out.len() == 0 {
        Ok(None)
    } else {
        Ok(Some(Box::new(out)))
    }
}

pub struct ScyllaPrebinnedReadProvider {
    use_scylla6_workarounds: UseScylla6Workarounds,
    scyqueue: ScyllaQueue,
}

impl ScyllaPrebinnedReadProvider {
    pub fn new(use_scylla6_workarounds: UseScylla6Workarounds, scyqueue: ScyllaQueue) -> Self {
        Self {
            use_scylla6_workarounds,
            scyqueue,
        }
    }
}

impl streams::timebin::CacheReadProvider for ScyllaPrebinnedReadProvider {
    fn read(
        &self,
        series: u64,
        bin_len: DtMs,
        msp: u64,
        offs: Range<u32>,
    ) -> streams::timebin::cached::reader::CacheReading {
        // let fut = async { todo!("TODO impl scylla cache read") };
        let fut = scylla_read_prebinned_f32(
            series,
            bin_len,
            msp,
            offs,
            self.use_scylla6_workarounds.clone(),
            self.scyqueue.clone(),
        );
        streams::timebin::cached::reader::CacheReading::new(Box::pin(fut))
    }
}

// TODO remove?
pub async fn worker_read(
    rt: RetentionTime,
    series: u64,
    bin_len: DtMs,
    msp: u64,
    offs: core::ops::Range<u32>,
    use_scylla6_workarounds: UseScylla6Workarounds,
    stmts: &StmtsEvents,
    scy: &ScySession,
) -> Result<ContainerBins<f32, f32>, streams::timebin::cached::reader::Error> {
    let partt = PrebinnedPartitioning::try_from(bin_len)?;
    let div = partt.patch_dt();
    let params = (
        series as i64,
        bin_len.ms() as i32,
        msp as i64,
        offs.start as i32,
        offs.end as i32,
    );
    let res = scy
        .execute_iter(
            stmts
                .cache_bypass(use_scylla6_workarounds.get())
                .rt(&rt)
                .prebinned_f32()
                .clone(),
            params,
        )
        .await
        .map_err(|e| streams::timebin::cached::reader::Error::Scylla(e.to_string()))?;
    let mut it = res
        .rows_stream::<(i32, i64, f32, f32, f32, f32)>()
        .map_err(|e| streams::timebin::cached::reader::Error::Scylla(e.to_string()))?;
    let mut bins = ContainerBins::new();
    while let Some(row) = it
        .try_next()
        .await
        .map_err(|e| streams::timebin::cached::reader::Error::Scylla(e.to_string()))?
    {
        let off = row.0 as u64;
        let cnt = row.1 as u64;
        let min = row.2;
        let max = row.3;
        let avg = row.4;
        let lst = row.5;
        let ts1 = TsNano::from_ns(bin_len.ns() * off + div.ns() * msp);
        let ts2 = TsNano::from_ns(ts1.ns() + bin_len.ns());
        // By assumption, bins which got written to storage are considered final
        let fnl = true;
        bins.push_back(ts1, ts2, cnt, min, max, avg, lst, fnl);
    }
    Ok(bins)
}
