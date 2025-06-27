use crate::log::*;
use futures_util::TryStreamExt;
use netpod::EMIT_ACCOUNTING_SNAP;
use netpod::TsMs;
use netpod::ttl::RetentionTime;
use scylla::client::session::Session as ScySession;
use scylla::statement::prepared::PreparedStatement;

autoerr::create_error_v1!(
    name(Error, "AccountingToplist"),
    enum variants {
        ScyllaPrepare(#[from] scylla::errors::PrepareError),
        ScyllaPagerExecution(#[from] scylla::errors::PagerExecutionError),
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaTypeCheck(#[from] scylla::deserialize::TypeCheckError),
        UsageDataMalformed,
    },
);

#[derive(Debug)]
pub struct UsageData {
    ts: TsMs,
    series: Vec<u64>,
    counts: Vec<u64>,
    bytes: Vec<u64>,
}

impl UsageData {
    pub fn new(ts: TsMs) -> Self {
        Self {
            ts,
            series: Vec::new(),
            counts: Vec::new(),
            bytes: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.series.len()
    }

    pub fn ts(&self) -> TsMs {
        self.ts
    }

    pub fn series(&self) -> &[u64] {
        &self.series
    }

    pub fn counts(&self) -> &[u64] {
        &self.counts
    }

    pub fn bytes(&self) -> &[u64] {
        &self.bytes
    }

    pub fn sort_by_counts(&mut self) {
        let mut tmp: Vec<_> = self
            .counts
            .iter()
            .map(|&x| x)
            .enumerate()
            .map(|(i, x)| (x, i))
            .collect();
        tmp.sort_unstable();
        let tmp: Vec<_> = tmp.into_iter().rev().map(|x| x.1).collect();
        self.reorder_by_index_list(&tmp);
    }

    pub fn sort_by_bytes(&mut self) {
        let mut tmp: Vec<_> = self.bytes.iter().map(|&x| x).enumerate().map(|(i, x)| (x, i)).collect();
        tmp.sort_unstable();
        let tmp: Vec<_> = tmp.into_iter().rev().map(|x| x.1).collect();
        self.reorder_by_index_list(&tmp);
    }

    fn reorder_by_index_list(&mut self, tmp: &[usize]) {
        self.series = tmp.iter().map(|&x| self.series[x]).collect();
        self.counts = tmp.iter().map(|&x| self.counts[x]).collect();
        self.bytes = tmp.iter().map(|&x| self.bytes[x]).collect();
    }

    fn verify(&self) -> Result<(), Error> {
        if self.counts.len() != self.series.len() {
            Err(Error::UsageDataMalformed)
        } else if self.bytes.len() != self.series.len() {
            Err(Error::UsageDataMalformed)
        } else {
            Ok(())
        }
    }
}

pub async fn read_ts(ks: &str, rt: RetentionTime, ts: TsMs, scy: &ScySession) -> Result<UsageData, Error> {
    // TODO  toplist::read_ts  refactor
    let snap = EMIT_ACCOUNTING_SNAP.ms();
    let ts = TsMs::from_ms_u64(ts.ms() / snap * snap);
    let ret = read_ts_inner(ks, rt, ts, scy).await?;
    Ok(ret)
}

async fn read_ts_inner(ks: &str, rt: RetentionTime, ts: TsMs, scy: &ScySession) -> Result<UsageData, Error> {
    trace!("read_ts_inner");
    type RowType = (i64, i64, i64);
    let cql = format!(
        concat!(
            "select series, count, bytes",
            " from {}.{}account_00",
            " where part = ? and ts = ?",
            " bypass cache"
        ),
        ks,
        rt.table_prefix()
    );
    let qu = scy.prepare(cql).await?;
    let ts_sec = ts.ms() as i64 / 1000;
    let mut ret = UsageData::new(ts);
    for part in 0..255_u32 {
        let mut res = scy
            .execute_iter(qu.clone(), (part as i32, ts_sec))
            .await?
            .rows_stream::<RowType>()?;
        while let Some(row) = res.try_next().await? {
            let series = row.0 as u64;
            let count = row.1 as u64;
            let bytes = row.2 as u64;
            ret.series.push(series);
            ret.counts.push(count);
            ret.bytes.push(bytes);
        }
    }
    ret.verify()?;
    Ok(ret)
}
