use async_channel::Sender;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use std::collections::VecDeque;
use std::mem;

pub struct ScyWriteQueue {
    tx: Sender<VecDeque<QueryItem>>,
    qu: VecDeque<QueryItem>,
}

impl ScyWriteQueue {
    pub fn new(tx: Sender<VecDeque<QueryItem>>) -> Self {
        Self {
            tx,
            qu: VecDeque::new(),
        }
    }

    pub async fn push(&mut self, item: QueryItem) {
        if self.qu.len() >= 1000 {
            self.flush().await;
        }
        self.qu.push_back(item);
    }

    pub async fn flush(&mut self) {
        if !self.qu.is_empty() {
            let qu = mem::replace(&mut self.qu, VecDeque::new());
            let _ = self.tx.send(qu).await;
        }
    }
}

pub struct ScyWriteQueues {
    st_rf1: ScyWriteQueue,
    st_rf3: ScyWriteQueue,
    mt_rf3: ScyWriteQueue,
    lt_rf3: ScyWriteQueue,
}

impl ScyWriteQueues {}
