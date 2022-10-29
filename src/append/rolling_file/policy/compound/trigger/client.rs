//! The client trigger.
//!
//! Requires the `client_trigger` feature.

use crate::append::rolling_file::{policy::compound::trigger::Trigger, LogFile};

/// A trigger which rolls the log when requested by a client.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct ClientTrigger {
    latch: std::sync::atomic::AtomicBool,
}

impl ClientTrigger {
    /// Returns a new trigger which rolls the log whenever signalled by the client.
    pub fn new() -> Self {
        Self { latch: std::sync::atomic::AtomicBool::new(false) }
    }
}

impl Trigger for ClientTrigger {
    fn trigger(&self, file: &LogFile) -> anyhow::Result<bool> {
        let latch = self.latch.swap(false, std::sync::atomic::Ordering::AcqRel);
        Ok(latch)
    }
}

impl ClientTrigger {
    pub fn rotate_on_next_append(&self) {
        self.latch.swap(true, std::sync::atomic::Ordering::AcqRel);
    }
}
