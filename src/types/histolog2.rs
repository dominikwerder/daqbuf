use std::fmt;
use std::time::Duration;

use crate::rexport::serde;

#[derive(Debug, serde::Serialize)]
pub struct HistoLog2 {
    sum: u64,
    histo: Vec<u32>,
}

#[allow(unused)]
macro_rules! rep20 {
    ([$x:expr]) => {
        [
            $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x, $x,
        ]
    };
}

impl HistoLog2 {
    pub fn new(n: u16) -> Self {
        let histo = vec![0; n as usize];
        Self { sum: 0, histo }
    }

    #[inline]
    pub fn push_val(&mut self, v: u32) {
        self.sum = self.sum.wrapping_add(v as u64);
        let w = 32 - v.leading_zeros();
        let i = w.min(self.histo.len() as u32 - 1);
        let h = &mut self.histo[i as usize];
        *h = h.wrapping_add(1);
    }

    #[inline]
    pub fn push_dur_10us(&mut self, dt: Duration) {
        let v = 1000000 * dt.as_secs() as u32 + dt.subsec_micros();
        let v = v / 10;
        self.push_val(v);
    }

    #[inline]
    pub fn push_dur_100us(&mut self, dt: Duration) {
        let v = 1000000 * dt.as_secs() as u32 + dt.subsec_micros();
        let v = v / 100;
        self.push_val(v);
    }

    #[inline]
    pub fn push_dur_1ms(&mut self, dt: Duration) {
        let v = 1000000 * dt.as_secs() as u32 + dt.subsec_micros();
        let v = v / 1000;
        self.push_val(v);
    }

    #[inline]
    pub fn push_dur_10ms(&mut self, dt: Duration) {
        let v = 1000000 * dt.as_secs() as u32 + dt.subsec_micros();
        let v = v / 10000;
        self.push_val(v);
    }

    pub fn ingest_upstream(&mut self, inp: Self) {
        self.sum += inp.sum;
        for (x, y) in inp.histo.into_iter().zip(self.histo.iter_mut()) {
            *y += x;
        }
    }

    pub fn to_flatten_prometheus(&self, name: &str) -> Vec<String> {
        // https://prometheus.io/docs/instrumenting/exposition_formats/
        let mut ret = String::with_capacity(2048);
        ret.push_str("# HELP ");
        ret.push_str(name);
        ret.push_str(" help-text-missing\n");
        ret.push_str("# TYPE ");
        ret.push_str(name);
        ret.push_str(" histogram\n");
        let mut cnt = 0;
        let lastix = (self.histo.len() - 1) as u32;
        for (i, &v) in self.histo.iter().enumerate() {
            let i = i as u32;
            let le = if i == 0 {
                0
            } else {
                u32::MAX >> (u32::BITS - i)
            };
            cnt += v;
            ret.push_str(name);
            ret.push_str("_bucket{le=\"");
            if i == lastix {
                ret.push_str("+Inf");
            } else {
                ret.push_str(&le.to_string());
            }
            ret.push_str("\"} ");
            ret.push_str(&cnt.to_string());
            ret.push_str("\n");
        }
        ret.push_str(name);
        ret.push_str("_count ");
        ret.push_str(&cnt.to_string());
        ret.push_str("\n");

        let sum = self.sum;
        ret.push_str(name);
        ret.push_str("_sum ");
        ret.push_str(&sum.to_string());
        let ret = vec![ret];
        ret
    }

    pub fn to_display(&self) -> HistoLog2Display {
        HistoLog2Display { inner: self }
    }
}

pub struct HistoLog2Display<'a> {
    inner: &'a HistoLog2,
}

impl<'a> fmt::Display for HistoLog2Display<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let histo: Vec<_> = self.inner.histo.iter().map(|&x| x).collect();
        write!(
            fmt,
            "HistoLog2 {{ histo: {:?}, sum: {:?} }}",
            histo, self.inner.sum
        )
    }
}

#[test]
fn histo_00() {
    let mut histo = HistoLog2::new(20);
    // histo.ingest(0);
    // histo.ingest(1);
    // histo.ingest(2);
    histo.push_val(3);
    histo.push_val(4);
    histo.push_val(262143);
    histo.push_val(262144);
    let s = histo.to_flatten_prometheus("the_metric");
    // eprintln!("{s}");
    let exp = r##"# HELP the_metric help-text-missing
# TYPE the_metric histogram
the_metric_bucket{le="0"} 0
the_metric_bucket{le="1"} 0
the_metric_bucket{le="3"} 1
the_metric_bucket{le="7"} 2
the_metric_bucket{le="15"} 2
the_metric_bucket{le="31"} 2
the_metric_bucket{le="63"} 2
the_metric_bucket{le="127"} 2
the_metric_bucket{le="255"} 2
the_metric_bucket{le="511"} 2
the_metric_bucket{le="1023"} 2
the_metric_bucket{le="2047"} 2
the_metric_bucket{le="4095"} 2
the_metric_bucket{le="8191"} 2
the_metric_bucket{le="16383"} 2
the_metric_bucket{le="32767"} 2
the_metric_bucket{le="65535"} 2
the_metric_bucket{le="131071"} 2
the_metric_bucket{le="262143"} 3
the_metric_bucket{le="+Inf"} 4
the_metric_count 4
the_metric_sum 524294"##;
    assert_eq!(s.join("\n"), exp);
}
