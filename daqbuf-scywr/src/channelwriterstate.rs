use crate::asynchan;
use series::SeriesId;

autoerr::create_error_v1!(
    name(Error, "WriterStateQuery"),
    enum variants {
        Logic,
    },
);

pub type Ret = Result<Vec<u8>, Error>;

#[derive(Debug, Clone)]
pub struct WriterStateQuery {
    series: SeriesId,
    tx: asynchan::Sender<Ret>,
}

impl WriterStateQuery {
    pub fn new(series: SeriesId) -> (Self, asynchan::Receiver<Ret>) {
        let (tx, rx) = asynchan::bounded(10, "WriterStateQuery");
        (Self { series, tx }, rx)
    }

    pub fn series(&self) -> SeriesId {
        self.series
    }
}
