use crate::filechunkread::FileChunkRead;
use futures_util::Stream;
use futures_util::StreamExt;
use netpod::histo::HistoLog2;
use netpod::log::*;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

autoerr::create_error_v1!(
    name(Error, "NeedMinBuffer"),
    enum variants {
        Dummy,
    },
);

pub struct NeedMinBuffer {
    inp: Pin<Box<dyn Stream<Item = Result<FileChunkRead, items_0::streamitem::SitemErrTy>> + Send>>,
    need_min: u32,
    left: Option<FileChunkRead>,
    buf_len_histo: HistoLog2,
    errored: bool,
    completed: bool,
}

impl NeedMinBuffer {
    pub fn new(
        inp: Pin<
            Box<dyn Stream<Item = Result<FileChunkRead, items_0::streamitem::SitemErrTy>> + Send>,
        >,
    ) -> Self {
        Self {
            inp,
            need_min: 1,
            left: None,
            buf_len_histo: HistoLog2::new(8),
            errored: false,
            completed: false,
        }
    }

    pub fn put_back(&mut self, buf: FileChunkRead) {
        assert!(self.left.is_none());
        self.left = Some(buf);
    }

    pub fn set_need_min(&mut self, need_min: u32) {
        self.need_min = need_min;
    }
}

// TODO collect somewhere else
impl Drop for NeedMinBuffer {
    fn drop(&mut self) {
        debug!(
            "NeedMinBuffer-drop {{ buf_len_histo: {:?} }}",
            self.buf_len_histo
        );
    }
}

impl Stream for NeedMinBuffer {
    type Item = Result<FileChunkRead, items_0::streamitem::SitemErrTy>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break if self.completed {
                panic!("NeedMinBuffer  poll_next on completed");
            } else if self.errored {
                self.completed = true;
                return Ready(None);
            } else {
                match self.inp.poll_next_unpin(cx) {
                    Ready(Some(Ok(mut fcr))) => {
                        self.buf_len_histo.ingest(fcr.buf().len() as u32);
                        //info!("NeedMinBuffer got buf  len {}", fcr.buf.len());
                        match self.left.take() {
                            Some(mut lfcr) => {
                                // TODO measure:
                                lfcr.buf_mut().unsplit(fcr.buf_take());
                                *lfcr.duration_mut() += *fcr.duration();
                                let fcr = lfcr;
                                if fcr.buf().len() as u32 >= self.need_min {
                                    //info!("with left ready  len {}  need_min {}", buf.len(), self.need_min);
                                    Ready(Some(Ok(fcr)))
                                } else {
                                    //info!("with left not enough  len {}  need_min {}", buf.len(), self.need_min);
                                    self.left.replace(fcr);
                                    continue;
                                }
                            }
                            None => {
                                if fcr.buf().len() as u32 >= self.need_min {
                                    //info!("simply ready  len {}  need_min {}", buf.len(), self.need_min);
                                    Ready(Some(Ok(fcr)))
                                } else {
                                    //info!("no previous leftover, need more  len {}  need_min {}", buf.len(), self.need_min);
                                    self.left.replace(fcr);
                                    continue;
                                }
                            }
                        }
                    }
                    Ready(Some(Err(e))) => {
                        self.errored = true;
                        Ready(Some(Err(e.into())))
                    }
                    Ready(None) => {
                        // TODO collect somewhere
                        debug!("NeedMinBuffer  histo: {:?}", self.buf_len_histo);
                        Ready(None)
                    }
                    Pending => Pending,
                }
            };
        }
    }
}
