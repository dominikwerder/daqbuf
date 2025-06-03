use autoerr::dbgdisplay::Dbg;
use chrono::DateTime;
use chrono::Utc;
use netpod::ChConf;
use netpod::ScalarType;
use netpod::SeriesKind;
use netpod::SfDbChannel;
use netpod::Shape;
use netpod::TsMs;
use netpod::log::*;
use netpod::range::evrange::NanoRange;
use std::time::Duration;
use tokio_postgres::Client;

autoerr::create_error_v1!(
    name(Error, "DbChannelConfig"),
    enum variants {
        Pg(#[from] tokio_postgres::Error),
        NotFound(SfDbChannel, NanoRange),
        SeriesNotFound(String, u64),
        BadScalarType(i32),
        BadShape(Dbg<Vec<i32>>),
        BadKind(i16),
        NoInput,
    },
);

/// It is an unsolved question as to how we want to uniquely address channels.
/// Currently, the usual (backend, channelname) works in 99% of the cases, but the edge-cases
/// are not solved. At the same time, it is desirable to avoid to complicate things for users.
/// Current state:
/// If the series id is given, we take that.
/// Otherwise we try to uniquely identify the series id from the given information.
/// In the future, we can even try to involve time range information for that, but backends like
/// old archivers and sf databuffer do not support such lookup.
pub(super) async fn chconf_best_matching_for_name_and_range(
    channel: SfDbChannel,
    range: NanoRange,
    pg: &Client,
) -> Result<ChConf, Error> {
    debug!("chconf_best_matching_for_name_and_range  {channel:?}  {range:?}");
    let sql = concat!(
        "select unnest(tscs) as tsc, series, scalar_type, shape_dims",
        " from series_by_channel",
        " where facility = $1",
        " and channel = $2",
        " and kind = $3",
        " order by tsc",
    );
    let res = pg
        .query(sql, &[&channel.backend(), &channel.name(), &channel.kind().to_db_i16()])
        .await?;
    if res.len() == 0 {
        let e = Error::NotFound(channel, range);
        warn!("{e}");
        Err(e)
    } else if res.len() > 1 {
        let mut rows = Vec::new();
        for r in res {
            let tsc: DateTime<Utc> = r.get(0);
            let series: i64 = r.get(1);
            let scalar_type: i32 = r.get(2);
            // TODO can I get a slice from psql driver?
            let shape_dims: Vec<i32> = r.get(3);
            let series = series as u64;
            let _scalar_type =
                ScalarType::from_scylla_i32(scalar_type).map_err(|_| Error::BadScalarType(scalar_type))?;
            let _shape = Shape::from_scylla_shape_dims(&shape_dims).map_err(|_| Error::BadShape(Dbg(shape_dims)))?;
            let tsms = tsc.signed_duration_since(DateTime::UNIX_EPOCH).num_milliseconds() as u64;
            let ts = TsMs::from_ms_u64(tsms);
            rows.push((ts, series));
        }
        let tsmss: Vec<_> = rows.iter().map(|x| x.0.clone()).collect();
        let range = (TsMs(range.beg / 1000), TsMs(range.end / 1000));
        let res = decide_best_matching_index(range, &tsmss)?;
        let backend = channel.backend().into();
        let ch_conf = chconf_for_series(backend, rows[res].1, pg).await?;
        Ok(ch_conf)
    } else {
        let r = res.first().unwrap();
        let _tsc: DateTime<Utc> = r.get(0);
        let series: i64 = r.get(1);
        let scalar_type: i32 = r.get(2);
        // TODO can I get a slice from psql driver?
        let shape_dims: Vec<i32> = r.get(3);
        let series = series as u64;
        let kind = channel.kind();
        let scalar_type = ScalarType::from_scylla_i32(scalar_type).map_err(|_| Error::BadScalarType(scalar_type))?;
        let shape = Shape::from_scylla_shape_dims(&shape_dims).map_err(|_| Error::BadShape(Dbg(shape_dims)))?;
        let ret = ChConf::new(channel.backend(), series, kind, scalar_type, shape, channel.name());
        Ok(ret)
    }
}

fn decide_best_matching_index(range: (TsMs, TsMs), rows: &[TsMs]) -> Result<usize, Error> {
    if rows.len() < 1 {
        let e = Error::NoInput;
        warn!("{e}");
        Err(e)
    } else {
        let rows: Vec<_> = rows
            .iter()
            .map(Clone::clone)
            .zip(rows[1..].iter().map(Clone::clone).chain([TsMs(u64::MAX)]))
            .map(|x| (x.0, x.1))
            .collect();
        let rows: Vec<_> = rows
            .into_iter()
            .map(|x| {
                let dur = if x.1 <= range.0 {
                    Duration::from_millis(0)
                } else if x.0 >= range.1 {
                    Duration::from_millis(0)
                } else if x.0 <= range.0 {
                    if x.1 >= range.1 {
                        Duration::from_millis((range.1.clone() - range.0.clone()).ms())
                    } else {
                        Duration::from_millis((x.1.clone() - range.0.clone()).ms())
                    }
                } else {
                    if x.1 >= range.1 {
                        Duration::from_millis((range.1.clone() - x.0.clone()).ms())
                    } else {
                        Duration::from_millis((x.1.clone() - x.0.clone()).ms())
                    }
                };
                dur
            })
            .collect();
        let mut max = Duration::ZERO;
        // By default, return the last
        let mut found = rows.len() - 1;
        for (i, r) in rows.into_iter().enumerate() {
            if r >= max {
                max = r;
                found = i;
            }
        }
        Ok(found)
    }
}

#[test]
fn test_decide_best_matching_index_before_00() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 0);
}

#[test]
fn test_decide_best_matching_index_before_01() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 1);
}

#[test]
fn test_decide_best_matching_index_before_02() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30), TsMs(70)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 1);
}

#[test]
fn test_decide_best_matching_index_overlap_00() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30), TsMs(42), TsMs(65)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 2);
}

#[test]
fn test_decide_best_matching_index_overlap_01() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(10), TsMs(30), TsMs(52), TsMs(58), TsMs(60)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 1);
}

#[test]
fn test_decide_best_matching_index_after_00() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(60)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 0);
}

#[test]
fn test_decide_best_matching_index_after_01() {
    let range = (TsMs(40), TsMs(60));
    let rows = &vec![TsMs(70)];
    let i = decide_best_matching_index(range, rows).unwrap();
    assert_eq!(i, 0);
}

pub(super) async fn chconf_for_series(backend: &str, series: u64, pg: &Client) -> Result<ChConf, Error> {
    let res = pg
        .query(
            "select channel, scalar_type, shape_dims, kind from series_by_channel where facility = $1 and series = $2",
            &[&backend, &(series as i64)],
        )
        .await?;
    if res.len() < 1 {
        let e = Error::SeriesNotFound(backend.into(), series);
        warn!("{e}");
        Err(e)
    } else {
        let row = res.first().unwrap();
        let name: String = row.get(0);
        let scalar_type = row.get::<_, i32>(1);
        let scalar_type =
            ScalarType::from_dtype_index(scalar_type as _).map_err(|_| Error::BadScalarType(scalar_type))?;
        // TODO can I get a slice from psql driver?
        let shape = row.get::<_, Vec<i32>>(2);
        let shape = Shape::from_scylla_shape_dims(&shape).map_err(|_| Error::BadShape(Dbg(shape)))?;
        let kind: i16 = row.get(3);
        let kind = SeriesKind::from_db_i16(kind).map_err(|_| Error::BadKind(kind))?;
        let ret = ChConf::new(backend, series, kind, scalar_type, shape, name);
        Ok(ret)
    }
}
