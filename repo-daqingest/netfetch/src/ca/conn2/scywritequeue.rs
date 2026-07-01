use async_channel::Sender;
use scywr::insertqueues::InsertQueuesTx;
use scywr::iteminsertqueue::QueryItem;
use std::collections::VecDeque;
use std::fmt;
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

impl fmt::Debug for ScyWriteQueue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("ScyWriteQueue")
            .field("tx", &"Sender")
            .field("qu.len", &self.qu.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct ScyWriteQueues {
    st_rf1: ScyWriteQueue,
    st_rf3: ScyWriteQueue,
    mt_rf3: ScyWriteQueue,
    lt_rf3: ScyWriteQueue,
}

impl ScyWriteQueues {
    pub fn lt(&mut self) -> &mut ScyWriteQueue {
        &mut self.lt_rf3
    }
}
