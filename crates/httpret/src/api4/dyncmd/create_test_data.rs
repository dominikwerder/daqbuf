use netpod::ttl::RetentionTime;
use scyllaconn::worker::KeyspaceId;
use scyllaconn::worker::ScyllaQueue;
use scyllaconn::worker::ScyllaQueueCluster;
use serde_json::json;
use series::SeriesId;

macro_rules! error { ($($arg:tt)*) => { if true { log::error!($($arg)*); } }; }
macro_rules! info { ($($arg:tt)*) => { if true { log::info!($($arg)*); } }; }

autoerr::create_error_v1!(
    name(Error, "CreateTestData"),
    enum variants {
        ScyllaWorker(#[from] scyllaconn::worker::Error),
    },
);

// Wed Mar 18 02:37:00 PM CET 2026
// 1773841020

const SERIES_ID_A: u64 = 291;
const MSP_A_00: u64 = 1773841020000;

async fn create_test_data_1(ksid: &KeyspaceId, scyqu: &ScyllaQueueCluster) -> Result<(), Error> {
    info!("create_test_data_1");
    let rt = ksid.rt();
    let ks = ksid.name();
    let tpre = rt.table_prefix();
    {
        let cql = format!(
            "{}{}{}",
            format_args!("insert into {ks}.{tpre}ts_msp"),
            format_args!(" (series, ts_msp)"),
            format_args!(" values (?, ?)")
        );
        let stmt = scyqu.prepare(ksid.clone(), cql).await?;
        let _ = scyqu
            .execute(ksid.clone(), stmt, Box::new((SERIES_ID_A as i64, MSP_A_00 as i64)))
            .await?;
    }
    let cql = format!(
        "{}{}{}",
        format_args!("insert into {ks}.{tpre}events_scalar_f32"),
        format_args!(" (series, ts_msp, ts_lsp, value)"),
        format_args!(" values (?, ?, ?, ?)")
    );
    let stmt = scyqu.prepare(ksid.clone(), cql).await?;
    Ok(())
}

pub async fn create_test_data(scyqu: &ScyllaQueue) -> Result<serde_json::Value, Error> {
    info!("create_test_data");
    for c in scyqu.clusters().iter() {
        if c.tag() == "cl1" {
            for k in c.keyspaces().iter() {
                if k.rt() == RetentionTime::Short {
                    create_test_data_1(k, c).await?;
                    let ret = json!({});
                    return Ok(ret);
                }
            }
        }
    }
    let ret = json!({
        "error": "No suitable cluster/keyspace found",
    });
    return Ok(ret);
}
