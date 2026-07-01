use items_0::streamitem::LogItem;
use netpod::log::*;
use std::collections::VecDeque;

pub struct Streamlog {
    items: VecDeque<LogItem>,
    #[allow(unused)]
    node_ix: u32,
}

impl Streamlog {
    pub fn new(node_ix: u32) -> Self {
        Self {
            items: VecDeque::new(),
            node_ix,
        }
    }

    pub fn append(&mut self, level: Level, msg: String) {
        let item = LogItem::level_msg(level, msg);
        self.items.push_back(item);
    }

    pub fn pop(&mut self) -> Option<LogItem> {
        self.items.pop_back()
    }

    pub fn emit(item: &LogItem) {
        match item.level() {
            Level::ERROR => {
                error!("StreamLog  {}", item.display_log_file());
            }
            Level::WARN => {
                warn!("StreamLog  {}", item.display_log_file());
            }
            Level::INFO => {
                info!("StreamLog  {}", item.display_log_file());
            }
            Level::DEBUG => {
                debug!("StreamLog  {}", item.display_log_file());
            }
            Level::TRACE => {
                trace!("StreamLog  {}", item.display_log_file());
            }
        }
    }
}
