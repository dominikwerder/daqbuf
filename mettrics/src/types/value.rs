use crate::rexport::serde;

#[derive(Debug, serde::Serialize)]
pub struct ValueU32 {
    inc: u64,
    dec: u64,
    lst: u32,
}

impl ValueU32 {
    pub fn new() -> Self {
        Self { inc: 0, dec: 0, lst: 0 }
    }

    #[inline]
    pub fn set(&mut self, v: u32) {
        if v > self.lst {
            let diff = v - self.lst;
            self.lst = v;
            self.inc = self.inc.wrapping_add(diff as u64);
        } else if v < self.lst {
            let diff = self.lst - v;
            self.lst = v;
            self.dec = self.dec.wrapping_add(diff as u64);
        }
    }

    #[inline(always)]
    pub fn ingest(&mut self, inp: Self) {
        self.inc = self.inc.wrapping_add(inp.inc);
        self.dec = self.dec.wrapping_add(inp.dec);
    }

    #[inline(always)]
    pub fn take_from(&mut self, from: &mut Self) {
        self.inc = self.inc.wrapping_add(from.inc);
        self.dec = self.dec.wrapping_add(from.dec);
        from.inc = 0;
        from.dec = 0;
    }

    fn to_prometheus_field(name: &str, suff: &str, ty: &str, val: u64, out: &mut Vec<String>) {
        // s.push_str("# HELP ");
        // s.push_str(name);
        // s.push_str(" help-text-missing\n");
        out.push(format!("# TYPE {}{} {}", name, suff, ty));
        out.push(format!("{}{} {}", name, suff, val));
    }

    pub fn to_flatten_prometheus(&self, name: &str) -> Vec<String> {
        // https://prometheus.io/docs/instrumenting/exposition_formats/
        let mut ret = Vec::new();
        let gauge = self.inc.wrapping_sub(self.dec);
        Self::to_prometheus_field(name, "", "gauge", gauge, &mut ret);
        Self::to_prometheus_field(name, "_inc", "counter", self.inc, &mut ret);
        Self::to_prometheus_field(name, "_dec", "counter", self.dec, &mut ret);
        ret
    }
}
