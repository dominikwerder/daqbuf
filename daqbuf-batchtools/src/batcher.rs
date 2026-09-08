use async_channel::Receiver;
use async_channel::Sender;
use futures::Future;
use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use taskrun::tokio;

pub fn batch<T>(
    batch_limit: usize,
    timeout: Duration,
    outcap: usize,
    rx: Receiver<T>,
) -> (Receiver<Vec<T>>, tokio::task::JoinHandle<()>)
where
    T: Send + 'static,
{
    let (batch_tx, batch_rx) = async_channel::bounded(outcap);
    (batch_rx, tokio::spawn(run_batcher(rx, batch_tx, batch_limit, timeout)))
}

async fn run_batcher<T>(rx: Receiver<T>, batch_tx: Sender<Vec<T>>, batch_limit: usize, timeout: Duration) {
    let mut all = Vec::new();
    let mut do_emit = false;
    loop {
        use tokio::time::error::Elapsed;
        if do_emit {
            let batch = std::mem::replace(&mut all, Vec::new());
            match batch_tx.send(batch).await {
                Ok(()) => {
                    do_emit = false;
                }
                Err(_) => {
                    log::error!("batch send error, channel closed");
                    break;
                }
            }
        } else {
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(k) => match k {
                    Ok(item) => {
                        all.push(item);
                        if all.len() >= batch_limit {
                            do_emit = true;
                        }
                    }
                    Err(_) => {
                        let batch = std::mem::replace(&mut all, Vec::new());
                        match batch_tx.send(batch).await {
                            Ok(()) => {}
                            Err(_) => {
                                log::error!("can not send batch");
                            }
                        }
                        break;
                    }
                },
                Err(e) => {
                    let _: Elapsed = e;
                    if all.len() > 0 {
                        do_emit = true;
                    }
                }
            }
        }
    }
    log::debug!("batcher done");
}

pub struct Batcher2<S, T> {
    inp: Option<S>,
    buf: VecDeque<T>,
    interval: Duration,
    timeout_fut: tokio::time::Sleep,
}

impl<S, T> Batcher2<S, T> {
    pub fn new(inp: S, interval: Duration, limit_len: usize) -> Self {
        let timeout_fut = tokio::time::sleep(interval);
        Self {
            inp: Some(inp),
            buf: VecDeque::with_capacity(limit_len),
            interval,
            timeout_fut,
        }
    }
}

impl<S, T> Stream for Batcher2<S, T>
where
    S: Stream<Item = T>,
{
    type Item = VecDeque<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            let mut prog = false;
            let mut pend = false;
            let inp = unsafe { &mut self.as_mut().get_unchecked_mut().inp };
            if let Some(inp) = inp.as_mut() {
                let inp = unsafe { Pin::new_unchecked(inp) };
                match inp.poll_next(cx) {
                    Ready(x) => match x {
                        Some(item) => {
                            let self2 = unsafe { self.as_mut().get_unchecked_mut() };
                            self2.buf.push_back(item);
                            prog = true;
                            if self2.buf.len() >= self2.buf.capacity() {
                                let buf = std::mem::replace(&mut self2.buf, VecDeque::new());
                                break Ready(Some(buf));
                            }
                        }
                        None => {
                            let self2 = unsafe { self.as_mut().get_unchecked_mut() };
                            self2.inp = None;
                            let n = self2.buf.len();
                            log::debug!("BATCHER SEES INPUT EOS  n {n}");
                            if n > 0 {
                                let buf = std::mem::replace(&mut self2.buf, VecDeque::new());
                                break Ready(Some(buf));
                            }
                        }
                    },
                    Pending => {
                        pend = true;
                    }
                }
                let mut timeout_fut = unsafe { Pin::new_unchecked(&mut self.as_mut().get_unchecked_mut().timeout_fut) };
                match timeout_fut.as_mut().poll(cx) {
                    Ready(()) => {
                        let self2 = unsafe { self.as_mut().get_unchecked_mut() };
                        self2.timeout_fut = tokio::time::sleep(self2.interval);
                        if self2.buf.len() == 0 {
                            prog = true;
                        } else {
                            let buf = std::mem::replace(&mut self2.buf, VecDeque::new());
                            break Ready(Some(buf));
                        }
                    }
                    Pending => {
                        pend = true;
                    }
                }
            }
            break if prog {
                continue;
            } else if pend {
                Pending
            } else {
                log::debug!("BATCHER EMITS EOS {:?}", &self as *const _);
                Ready(None)
            };
        }
    }
}

#[cfg(test)]
mod test {
    use super::batch;
    use std::time::Duration;
    use taskrun::tokio;

    /// The batcher task must end when the input is closed, otherwise the consumers of the
    /// batch output never see the end of stream either.
    #[test]
    fn ends_on_input_closed() {
        let rt = taskrun::get_runtime();
        rt.block_on(async {
            let (tx, rx) = async_channel::bounded::<u32>(8);
            let (batch_rx, jh) = batch(4, Duration::from_millis(20), 4, rx);
            tx.send(1).await.unwrap();
            // A clone stays alive, so only the explicit close can end the batcher.
            let tx2 = tx.clone();
            tx.close();
            let mut seen = 0;
            while let Ok(b) = batch_rx.recv().await {
                seen += b.len();
            }
            assert_eq!(seen, 1);
            tokio::time::timeout(Duration::from_millis(2000), jh)
                .await
                .expect("batcher did not end")
                .unwrap();
            drop(tx2);
        });
    }

    /// Same, but the input closes because all senders got dropped.
    #[test]
    fn ends_on_senders_dropped() {
        let rt = taskrun::get_runtime();
        rt.block_on(async {
            let (tx, rx) = async_channel::bounded::<u32>(8);
            let (batch_rx, jh) = batch(4, Duration::from_millis(20), 4, rx);
            drop(tx);
            while let Ok(_) = batch_rx.recv().await {}
            tokio::time::timeout(Duration::from_millis(2000), jh)
                .await
                .expect("batcher did not end")
                .unwrap();
        });
    }
}
