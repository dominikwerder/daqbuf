use netpod::TsNano;
use series::SeriesId;

#[derive(Debug)]
pub struct ChannelEventValue {
    series: SeriesId,
    ts: TsNano,
    val_f32: f32,
}

impl ChannelEventValue {
    pub fn new(series: SeriesId, ts: TsNano, val_f32: f32) -> Self {
        Self { series, ts, val_f32 }
    }

    pub fn series(&self) -> SeriesId {
        self.series
    }

    pub fn ts(&self) -> TsNano {
        self.ts
    }

    pub fn val_f32(&self) -> f32 {
        self.val_f32
    }
}
