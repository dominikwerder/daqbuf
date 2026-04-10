use crate::events3;
use crate::events3::MSP_A_00;
use crate::events3::SERIES_ID_A;
use crate::events3::SeriesInfo;
use crate::events3::ks::eventsks::EventsKs;
use crate::range::ScyllaSeriesRange;
use crate::worker::KeyspaceId;
use crate::worker::ScyllaQueueCluster;
use futures_util::StreamExt;
use netpod::DtMs;
use netpod::DtNano;
use netpod::RangeExcl;
use netpod::ScalarType;
use netpod::Shape;
use netpod::ttl::RetentionTime;
use taskrun::tokio;

macro_rules! info { ($($arg:tt)*) => ( if true { log::info!($($arg)*); } ); }

async fn read_msp_fwd_00_async() {
    // TODO the backwards read should probably be based on multiple sub queries.
    // I want to mock as little as possible, only the actual queries. Other logic must be used as tested.
    let scyqu = ScyllaQueueCluster::new_mock("mock1".into(), [].into()).unwrap();
    let ks = KeyspaceId::new("short".into(), RetentionTime::Short);
    let series = SERIES_ID_A;
    let min = DtMs::from_ms_u64(1000 * 60);
    let range = ScyllaSeriesRange::new(MSP_A_00.ns(), MSP_A_00.add_dt_ms(min.mul(60 * 3)).ns());
    let e0 = scyqu
        .read_msp_03_fwd(ks, series, range, RangeExcl::None, 1)
        .await
        .unwrap();
    let n = e0.len();
    for x in e0 {
        info!("msp {}", x.fmt());
    }
    assert_eq!(n, 3);
}

#[test]
fn read_msp_fwd_00() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(read_msp_fwd_00_async());
}

async fn read_msp_bck_00_async() {
    let scyqu = ScyllaQueueCluster::new_mock("mock1".into(), [].into()).unwrap();
    let ks = KeyspaceId::new("short".into(), RetentionTime::Short);
    let series = SERIES_ID_A;
    let min = DtMs::from_ms_u64(1000 * 60);
    let range = ScyllaSeriesRange::new(
        MSP_A_00.add_dt_ms(min.mul(60 * 2)).ns(),
        MSP_A_00.add_dt_ms(min.mul(60 * 3)).ns(),
    );
    let win = ks.rt().msp_rollover_ivl_on_read();
    let beg = range.beg().sub(DtNano::from_ms(1000 * win.as_secs()));
    let end = range.beg();
    let range_bck = ScyllaSeriesRange::new(beg, end);
    let mut s = events3::mspfwd::ReadMspFwdStream::new(ks, series, range_bck, RangeExcl::None, 1, scyqu);
    while let Some(x) = s.next().await {
        let e0 = x.unwrap();
        let n = e0.len();
        info!("n {n}");
        for x in e0 {
            info!("msp {}", x.fmt());
        }
    }
}

#[test]
fn read_msp_bck_00() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(read_msp_bck_00_async());
}

async fn read_ks_async() {
    let series_info = SeriesInfo::new(SERIES_ID_A, ScalarType::U32, Shape::Scalar);
    let scyqu = ScyllaQueueCluster::new_mock(
        "mock1".into(),
        [KeyspaceId::new("mock1".into(), RetentionTime::Short)].into(),
    )
    .unwrap();
    let ks = scyqu.keyspaces()[0].clone();
    // TODO compare with window back
    let range = ScyllaSeriesRange::new(
        "2026-03-20T13:37:10.000Z".parse().unwrap(),
        "2026-03-20T13:40:00.000Z".parse().unwrap(),
    );
    let mut evsks = EventsKs::new(series_info, ks, scyqu, range, events3::ks::eventsks::Opts::new());
    while let Some(e) = evsks.next().await {
        info!("{e:?}");
    }
}

#[test]
fn read_ks() {
    tokio::runtime::Runtime::new().unwrap().block_on(read_ks_async());
}
