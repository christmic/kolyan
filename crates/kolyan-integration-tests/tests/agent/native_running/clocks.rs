//! Fixture clocks never alter a production tool's authority or deadline.

use std::time::Duration;
use tokio::time::Instant;

use super::Dataset;

pub(super) struct Clocks {
    pub model_deadline: Instant,
    phase_duration: Duration,
    cleanup_duration: Duration,
}

impl Clocks {
    pub fn new(dataset: &Dataset, started: Instant) -> Self {
        Self {
            model_deadline: started + Duration::from_millis(dataset.model_timeout_ms),
            phase_duration: Duration::from_millis(dataset.gate_timeout_ms),
            cleanup_duration: Duration::from_millis(dataset.cleanup_timeout_ms),
        }
    }

    pub fn phase_deadline(&self, captured: Instant) -> Instant {
        captured + self.phase_duration
    }

    pub fn cleanup_deadline(&self, requested: Instant) -> Instant {
        requested + self.cleanup_duration
    }
}

#[cfg(test)]
mod tests;
