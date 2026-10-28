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
pub struct WriterStateSelect {
    series: SeriesId,
    tx: asynchan::Sender<Ret>,
}

impl WriterStateSelect {
    pub fn new(series: SeriesId) -> (Self, asynchan::Receiver<Ret>) {
        let (tx, rx) = asynchan::bounded(10, "WriterStateSelect");
        (Self { series, tx }, rx)
    }

    pub fn series(&self) -> SeriesId {
        self.series
    }

    pub fn tx(&mut self) -> &mut asynchan::Sender<Ret> {
        &mut self.tx
    }
}

#[derive(Debug, Clone)]
pub struct WriterStateInsert {
    series: SeriesId,
    state_00: Vec<u8>,
    tx: asynchan::Sender<Result<(), Error>>,
}

impl WriterStateInsert {
    pub fn new(series: SeriesId, state_00: Vec<u8>) -> (Self, asynchan::Receiver<Result<(), Error>>) {
        let (tx, rx) = asynchan::bounded(10, "WriterStateInsert");
        (Self { series, state_00, tx }, rx)
    }

    pub fn series(&self) -> SeriesId {
        self.series
    }

    pub fn state_00(&self) -> &[u8] {
        &self.state_00
    }

    pub fn tx(&mut self) -> &mut asynchan::Sender<Result<(), Error>> {
        &mut self.tx
    }
}
