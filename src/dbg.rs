use crate::SeriesId;

pub fn dbg_chn(chn: &str) -> bool {
    if true {
        let chns = [
            // "SATUN21-MQUA080:I-SET",
            // "SATUN21-MQUA080:I-SET-ARCH",
            // "SATUN21-MQUA080:I-READ",
            // "ARS05-RAMP-0060:COUNTER",
            // "SSL2-LENC-MF05:H_SCALE",
            // "SINEG01:QE-B1-OP",
            // "STSRD01-TCWL-STX02:AMP1CURR",
            // "SATMA01-DBPM150:EST-Q1-SUM",
            "TEST:SLOWPAUSE:SCALAR:F32:000000",
            "TEST:SLOWPAUSE:SCALAR:F32:000001",
        ];
        chns.contains(&chn)
    } else {
        false
    }
}

pub fn dbg_series(series: SeriesId) -> bool {
    if true {
        let seriess = [
            // SATMA01-DBPM150:EST-Q1-SUM
            // 2968634857399905951,
        ];
        seriess.contains(&series.id())
    } else {
        false
    }
}
