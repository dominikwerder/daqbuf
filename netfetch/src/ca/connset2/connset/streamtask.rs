use crate::ca::conn2::asynchan;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use taskrun::tokio;
use tokio::task::JoinHandle;

enum StateRx<T>
where
    T: Unpin + Send,
{
    Run(asynchan::Receiver<T>, JoinHandle<()>),
    Join(JoinHandle<()>),
    Done,
}

pub struct StreamTaskRx<T>
where
    T: Unpin + Send,
{
    state: StateRx<T>,
}

impl<T> StreamTaskRx<T>
where
    T: Unpin + Send,
{
    fn new(rx: asynchan::Receiver<T>, jh: JoinHandle<()>) -> Self {
        StreamTaskRx {
            state: StateRx::Run(rx, jh),
        }
    }
}

impl<T> Stream for StreamTaskRx<T>
where
    T: Unpin + Send + 'static,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        loop {
            break match &mut self.state {
                StateRx::Run(rx, jh) => match rx.poll_next_unpin(cx) {
                    Ready(x) => match x {
                        Some(v) => Ready(Some(v)),
                        None => {
                            let state = std::mem::replace(&mut self.state, StateRx::Done);
                            if let StateRx::Run(_, jh) = state {
                                self.state = StateRx::Join(jh);
                                continue;
                            } else {
                                panic!("logic");
                            }
                        }
                    },
                    Pending => Pending,
                },
                StateRx::Join(jh) => match jh.poll_unpin(cx) {
                    Ready(x) => {
                        // TODO forward error
                        self.state = StateRx::Done;
                        continue;
                    }
                    Pending => Pending,
                },
                StateRx::Done => Ready(None),
            };
        }
    }
}

enum StateTx<S, T>
where
    S: Stream<Item = T>,
    T: Unpin + Send,
{
    Run(S, asynchan::Sender<T>, JoinHandle<()>),
    Join(asynchan::Sender<T>, JoinHandle<()>),
    Done,
}

pub struct StreamTaskTx<S, T>
where
    S: Stream<Item = T>,
    T: Unpin + Send,
{
    // state: StateTx<S, T>,
    stream: S,
    tx: asynchan::Sender<T>,
}

impl<S, T> StreamTaskTx<S, T>
where
    S: Stream<Item = T> + Unpin,
    T: Unpin + Send + 'static,
{
    fn new(stream: S, tx: asynchan::Sender<T>) -> Self {
        StreamTaskTx { stream, tx }
    }

    async fn pull_and_send(mut self) {
        while let Some(item) = self.stream.next().await {
            // TODO handle send error?
            self.tx.send(item).await;
        }
    }
}

pub fn run_in_task<S, T>(stream: S) -> StreamTaskRx<T>
where
    S: Stream<Item = T> + Unpin + Send + 'static,
    T: Unpin + Send + 'static,
{
    let (tx, rx) = asynchan::bounded(24, "run_in_task");
    let stx = StreamTaskTx::new(stream, tx);
    let task = taskrun::tokio::spawn(stx.pull_and_send());
    let srx = StreamTaskRx::new(rx, task);
    srx
}
