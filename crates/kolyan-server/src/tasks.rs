//! Durable task coordination above Session execution.
//!
//! This domain validates journal facts on admission and replay. It neither runs
//! models nor restores tool permissions; the host verifies stopped execution
//! evidence before submitting observations. CAS is the only write boundary.

mod coordinator;
mod reducer;
mod types;

pub use coordinator::TaskCoordinator;
pub use types::*;

#[cfg(test)]
mod tests;
