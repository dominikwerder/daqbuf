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

pub fn binwrite2_enable(chn: &str) -> bool {
    let chns = [
        // "ARS05-RAMP-0230:PLC-COUNT-MON",
        // "ARS09-R3HC-0150:TUN1-MOTOR-TEMP",
        // "ARS09-R3HC-0150:HOM27-ATT-TEMP",
        // "ARIDI-BLM01:LOSS7",
    ];
    chns.contains(&chn)
}
