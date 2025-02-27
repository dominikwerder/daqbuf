pub fn dbg_chn(chn: &str) -> bool {
    if true {
        let chns = [
            "ARS05-RAMP-0060:COUNTER",
            "SSL2-LENC-MF05:H_SCALE",
            "SINEG01:QE-B1-OP",
            "STSRD01-TCWL-STX02:AMP1CURR",
        ];
        chns.contains(&chn)
    } else {
        false
    }
}
