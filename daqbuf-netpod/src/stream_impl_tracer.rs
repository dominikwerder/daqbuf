use crate::log::*;

pub struct StreamImplTracer {
    name: String,
    npoll_cnt: usize,
    npoll_max: usize,
    loop_cnt: usize,
    loop_max: usize,
}

impl StreamImplTracer {
    pub fn new(name: String, npoll_max: usize, loop_max: usize) -> Self {
        Self {
            name,
            npoll_cnt: 0,
            npoll_max,
            loop_cnt: 0,
            loop_max,
        }
    }

    pub fn poll_enter(&mut self) -> bool {
        self.npoll_cnt += 1;
        if self.npoll_cnt >= self.npoll_max {
            trace!("{}  poll {} reached limit", self.name, self.npoll_cnt);
            true
        } else {
            trace!("{}  poll {}", self.name, self.npoll_cnt);
            false
        }
    }

    pub fn loop_enter(&mut self) -> bool {
        self.loop_cnt += 1;
        if self.loop_cnt >= self.loop_max {
            trace!("{}  loop {} reached limit", self.name, self.loop_cnt);
            true
        } else {
            trace!("{}  loop {}", self.name, self.loop_cnt);
            false
        }
    }
}
