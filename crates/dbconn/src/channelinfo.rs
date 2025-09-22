use netpod::ScalarType;
use netpod::Shape;
use tokio_postgres::Client;

autoerr::create_error_v1!(
    name(Error, "ChannelInfo"),
    enum variants {
        Postgres(#[from] crate::pg::Error),
        BadValue,
    },
);

pub struct ChannelInfo {
    pub series: u64,
    pub backend: String,
    pub name: String,
    pub scalar_type: ScalarType,
    pub shape: Shape,
    pub kind: u16,
}

pub async fn info_for_series_ids(series_ids: &[u64], pg: &Client) -> Result<Vec<Option<ChannelInfo>>, Error> {
    let (ord, seriess) = series_ids
        .iter()
        .enumerate()
        .fold((Vec::new(), Vec::new()), |mut a, (i, &series)| {
            a.0.push(i as i32);
            a.1.push(series as i64);
            a
        });
    let sql = concat!(
        "with q1 as (",
        " select * from unnest($1, $2) as inp (ord, series)",
        ")",
        "select q1.ord, q1.series, t.facility, t.channel, t.scalar_type, t.shape_dims, t.kind",
        " from q1",
        " left join series_by_channel t",
        " on t.series = q1.series",
        " and t.kind = 2",
        " order by q1.ord",
    );
    use crate::pg::Type as PgType;
    let st = pg.prepare_typed(sql, &[PgType::INT4_ARRAY, PgType::INT8_ARRAY]).await?;
    let rows = pg.query(&st, &[&ord, &seriess]).await?;
    let mut ret = Vec::new();
    for row in rows {
        let series = row.get::<_, i64>(1) as u64;
        let backend: Option<String> = row.get(2);
        if let Some(backend) = backend {
            let channel: String = row.get(3);
            let scalar_type: i32 = row.get(4);
            let shape_dims: Vec<i32> = row.get(5);
            let kind: i16 = row.get(6);
            let scalar_type = ScalarType::from_scylla_i32(scalar_type).map_err(|_| Error::BadValue)?;
            let shape = Shape::from_scylla_shape_dims(&shape_dims).map_err(|_| Error::BadValue)?;
            let kind = kind as u16;
            let e = ChannelInfo {
                series,
                backend,
                name: channel,
                scalar_type,
                shape,
                kind,
            };
            ret.push(Some(e));
        } else {
            ret.push(None);
        }
    }
    Ok(ret)
}
