//! Shared execution coordinates used across policy, Runtime and Server.

use serde::{Deserialize, Serialize};

/// Exact host-owned execution identity. Step and Agent bindings are layered on
/// this key; model-generated tool call identifiers do not identify executions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionKey {
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: String,
}
