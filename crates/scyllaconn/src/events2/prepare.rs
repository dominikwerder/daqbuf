use netpod::ttl::RetentionTime;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

macro_rules! info_prepare {
    ($($arg:tt)*) => { log::info!("prepare cql  {}", format_args!($($arg)*)); };
}

autoerr::create_error_v1!(
    name(Error, "ScyllaPrepare"),
    enum variants {
        ScyllaNextRow(#[from] scylla::errors::NextRowError),
        ScyllaWorker(Box<crate::worker::Error>),
        ScyllaPrepare(#[from] scylla::errors::PrepareError),
        MissingQuery(String),
        RangeEndOverflow,
        InvalidFuture,
        TestError(String),
    },
);

#[derive(Debug)]
pub struct StmtsLspShape {
    u8: PreparedStatement,
    u16: PreparedStatement,
    u32: PreparedStatement,
    u64: PreparedStatement,
    i8: PreparedStatement,
    i16: PreparedStatement,
    i32: PreparedStatement,
    i64: PreparedStatement,
    f32: PreparedStatement,
    f64: PreparedStatement,
    bool: PreparedStatement,
    string: PreparedStatement,
    enumvals: PreparedStatement,
}

impl StmtsLspShape {
    pub fn st(&self, stname: &str) -> Result<&PreparedStatement, Error> {
        let ret = match stname {
            "u8" => &self.u8,
            "u16" => &self.u16,
            "u32" => &self.u32,
            "u64" => &self.u64,
            "i8" => &self.i8,
            "i16" => &self.i16,
            "i32" => &self.i32,
            "i64" => &self.i64,
            "f32" => &self.f32,
            "f64" => &self.f64,
            "bool" => &self.bool,
            "string" => &self.string,
            "enum" => &self.enumvals,
            _ => return Err(Error::MissingQuery(format!("no query for stname {stname}"))),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
pub struct StmtsLspDir {
    scalar: StmtsLspShape,
    array: StmtsLspShape,
}

impl StmtsLspDir {
    pub fn shape(&self, array: bool) -> &StmtsLspShape {
        if array { &self.array } else { &self.scalar }
    }
}

#[derive(Debug)]
pub struct StmtsEventsRt {
    ts_msp_fwd: PreparedStatement,
    ts_msp_bck: PreparedStatement,
    lsp_fwd_val: StmtsLspDir,
    lsp_bck_val: StmtsLspDir,
    lsp_fwd_ts: StmtsLspDir,
    lsp_bck_ts: StmtsLspDir,
    prebinned_f32: PreparedStatement,
    bin_write_index_read: PreparedStatement,
}

impl StmtsEventsRt {
    pub fn ts_msp_fwd(&self) -> &PreparedStatement {
        &self.ts_msp_fwd
    }

    pub fn ts_msp_bck(&self) -> &PreparedStatement {
        &self.ts_msp_bck
    }

    pub fn lsp(&self, bck: bool, val: bool) -> &StmtsLspDir {
        if bck {
            if val { &self.lsp_bck_val } else { &self.lsp_bck_ts }
        } else {
            if val { &self.lsp_fwd_val } else { &self.lsp_fwd_ts }
        }
    }

    pub fn prebinned_f32(&self) -> &PreparedStatement {
        &self.prebinned_f32
    }

    pub fn bin_write_index_read(&self) -> &PreparedStatement {
        &self.bin_write_index_read
    }
}

async fn make_msp_dir(
    ks: &str,
    rt: &RetentionTime,
    bck: bool,
    query_opts: &str,
    scy: &Session,
) -> Result<PreparedStatement, Error> {
    let table_name = "ts_msp";
    let select_cond = if bck {
        "ts_msp < ? order by ts_msp desc limit 2"
    } else {
        "ts_msp >= ? and ts_msp < ? limit 20"
    };
    let cql = format!(
        "select ts_msp from {}.{}{} where series = ? and {} {}",
        ks,
        rt.table_prefix(),
        table_name,
        select_cond,
        query_opts
    );
    info_prepare!("{ks} {rt} {cql}");
    let qu = scy.prepare(cql).await?;
    Ok(qu)
}

async fn make_lsp(
    ks: &str,
    rt: &RetentionTime,
    shapepre: &str,
    stname: &str,
    values: &str,
    bck: bool,
    query_opts: &str,
    scy: &Session,
) -> Result<PreparedStatement, Error> {
    let select_cond = if bck {
        "ts_lsp < ? order by ts_lsp desc limit 1"
    } else {
        "ts_lsp >= ? and ts_lsp < ?"
    };
    let cql = format!(
        concat!(
            "select {} from {}.{}events_{}_{}",
            " where series = ? and ts_msp = ? and {} {}"
        ),
        values,
        ks,
        rt.table_prefix(),
        shapepre,
        stname,
        select_cond,
        query_opts
    );
    info_prepare!("{ks} {rt} {cql}");
    let qu = scy.prepare(cql).await?;
    Ok(qu)
}

async fn make_lsp_shape(
    ks: &str,
    rt: &RetentionTime,
    shapepre: &str,
    values: &str,
    bck: bool,
    query_opts: &str,
    scy: &Session,
) -> Result<StmtsLspShape, Error> {
    let values = if shapepre.contains("array") {
        values.replace("value", "valueblob")
    } else {
        values.into()
    };
    let values = &values;
    let maker = |stname| make_lsp(ks, rt, shapepre, stname, values, bck, query_opts, scy);
    let ret = StmtsLspShape {
        u8: maker("u8").await?,
        u16: maker("u16").await?,
        u32: maker("u32").await?,
        u64: maker("u64").await?,
        i8: maker("i8").await?,
        i16: maker("i16").await?,
        i32: maker("i32").await?,
        i64: maker("i64").await?,
        f32: maker("f32").await?,
        f64: maker("f64").await?,
        bool: maker("bool").await?,
        string: maker("string").await?,
        enumvals: if shapepre == "scalar" {
            make_lsp(
                ks,
                rt,
                shapepre,
                "enum",
                "ts_lsp, value, valuestr",
                bck,
                query_opts,
                scy,
            )
            .await?
        } else {
            // exists only for scalar, therefore produce some dummy here
            let table_name = "ts_msp";
            let cql = format!(
                "select ts_msp from {}.{}{} limit 1 {}",
                ks,
                rt.table_prefix(),
                table_name,
                query_opts
            );
            info_prepare!("{ks} {rt} {cql}");
            let qu = scy.prepare(cql).await?;
            qu
        },
    };
    Ok(ret)
}

async fn make_lsp_dir(
    ks: &str,
    rt: &RetentionTime,
    values: &str,
    bck: bool,
    query_opts: &str,
    scy: &Session,
) -> Result<StmtsLspDir, Error> {
    let ret = StmtsLspDir {
        scalar: make_lsp_shape(ks, rt, "scalar", values, bck, query_opts, scy).await?,
        array: make_lsp_shape(ks, rt, "array", values, bck, query_opts, scy).await?,
    };
    Ok(ret)
}

async fn make_prebinned_f32(
    ks: &str,
    rt: &RetentionTime,
    query_opts: &str,
    scy: &Session,
) -> Result<PreparedStatement, Error> {
    let cql = format!(
        concat!(
            "select off, cnt, min, max, avg, lst from {}.{}binned_scalar_f32_v02",
            " where series = ? and binlen = ? and msp = ?",
            " and off >= ? and off < ?",
            " {}"
        ),
        ks,
        rt.table_prefix(),
        query_opts
    );
    info_prepare!("{ks} {rt} {cql}");
    let qu = scy.prepare(cql).await?;
    Ok(qu)
}

async fn make_bin_write_index_read(
    ks: &str,
    rt: &RetentionTime,
    query_opts: &str,
    scy: &Session,
) -> Result<PreparedStatement, Error> {
    let cql = format!(
        concat!(
            "select lsp, binlen",
            " from {}.{}bin_write_index_v04",
            " where series = ? and pbp = ? and msp = ?",
            " and lsp >= ? and lsp < ?",
            " {}"
        ),
        ks,
        rt.table_prefix(),
        query_opts
    );
    info_prepare!("{ks} {rt} {cql}");
    let qu = scy.prepare(cql).await?;
    Ok(qu)
}

async fn make_rt(ks: &str, rt: &RetentionTime, query_opts: &str, scy: &Session) -> Result<StmtsEventsRt, Error> {
    let ret = StmtsEventsRt {
        ts_msp_fwd: make_msp_dir(ks, rt, false, query_opts, scy).await?,
        ts_msp_bck: make_msp_dir(ks, rt, true, query_opts, scy).await?,
        lsp_fwd_val: make_lsp_dir(ks, rt, "ts_lsp, value", false, query_opts, scy).await?,
        lsp_bck_val: make_lsp_dir(ks, rt, "ts_lsp, value", true, query_opts, scy).await?,
        lsp_fwd_ts: make_lsp_dir(ks, rt, "ts_lsp", false, query_opts, scy).await?,
        lsp_bck_ts: make_lsp_dir(ks, rt, "ts_lsp", true, query_opts, scy).await?,
        prebinned_f32: make_prebinned_f32(ks, rt, query_opts, scy).await?,
        bin_write_index_read: make_bin_write_index_read(ks, rt, query_opts, scy).await?,
    };
    Ok(ret)
}

#[derive(Debug)]
pub struct StmtsEvents {
    st: StmtsEventsRt,
    mt: StmtsEventsRt,
    lt: StmtsEventsRt,
}

impl StmtsEvents {
    pub async fn new(ks: [&str; 3], bypass_cache: bool, scy: &Session) -> Result<Self, Error> {
        let query_opts = if bypass_cache { "bypass cache" } else { "" };
        let ret = StmtsEvents {
            st: make_rt(ks[0], &RetentionTime::Short, query_opts, scy).await?,
            mt: make_rt(ks[1], &RetentionTime::Medium, query_opts, scy).await?,
            lt: make_rt(ks[2], &RetentionTime::Long, query_opts, scy).await?,
        };
        Ok(ret)
    }

    pub fn rt(&self, rt: &RetentionTime) -> &StmtsEventsRt {
        match rt {
            RetentionTime::Short => &self.st,
            RetentionTime::Medium => &self.mt,
            RetentionTime::Long => &&self.lt,
        }
    }
}
