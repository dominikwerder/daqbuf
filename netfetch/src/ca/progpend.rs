pub struct HaveProgressPending {
    have_progress: bool,
    have_pending: bool,
}

impl HaveProgressPending {
    pub fn new() -> Self {
        Self {
            have_progress: false,
            have_pending: false,
        }
    }

    pub fn mark_progress(&mut self) {
        self.have_progress = true;
    }

    pub fn mark_pending(&mut self) {
        self.have_pending = true;
    }

    pub fn have_progress(&self) -> bool {
        self.have_progress
    }

    pub fn have_pending(&self) -> bool {
        self.have_pending
    }
}
