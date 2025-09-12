use crate::ca::conn2::scywritequeue::ScyWriteQueues;
use std::cell::RefCell;
use std::cell::RefMut;
use std::rc::Rc;

#[derive(Debug)]
pub struct StateResources1 {
    scy_wr_qus: ScyWriteQueues,
}

impl StateResources1 {
    pub fn new(scy_wr_qus: ScyWriteQueues) -> Self {
        Self { scy_wr_qus }
    }

    pub fn scy_wr_qus(&mut self) -> &mut ScyWriteQueues {
        &mut self.scy_wr_qus
    }
}

#[derive(Debug, Clone)]
pub struct StateRessShr1 {
    ress: Rc<RefCell<StateResources1>>,
}

impl StateRessShr1 {
    pub fn borrow_mut(&self) -> RefMut<StateResources1> {
        self.ress.borrow_mut()
    }
}
