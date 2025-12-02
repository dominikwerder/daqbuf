use crate::SeriesId;

const CHNS: [(u64, &'static str); 3] = [
    (1001110436017869811, "SF-STAT-AR-USER:UT"),
    (4244984666146442200, "X06DA-ES-BS:TRX1.OFF"),
    (7055133662199761613, "X06DA-ES-HFM:TRYDW.RBV"),
];

pub fn dbg_chn(chn: &str) -> bool {
    if chn.contains("daqbuftest") {
        true
    } else if true {
        CHNS.iter().any(|x| x.1 == chn)
    } else {
        false
    }
}

pub fn dbg_series(series: SeriesId) -> bool {
    if series.id() < 100 {
        // unit test series
        true
    } else if true {
        CHNS.iter().any(|x| x.0 == series.id())
    } else {
        false
    }
}

pub fn dbg_check_scy6(series: SeriesId) -> bool {
    CHNS.iter().any(|x| x.0 == series.id())
}
