//! Portable tool completion and external-wait values. These values do not prove
//! durable admission: the host must verify receipts before supplying resolutions.

use kolyan_model::ToolResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::checkpoint::{CheckpointError, identifier};

/// Maximum encoded opaque wait binding, independent of tool output limits.
pub const MAX_EXTERNAL_BINDING_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ToolOutcome {
    Completed(ToolResult),
    AwaitingExternal(ExternalWait),
}

/// Host-generated identity and exact opaque admission binding. Core deliberately
/// does not interpret the kind; Runtime must reject unsupported critical kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalWait {
    pub wait_id: String,
    pub kind: String,
    pub schema_version: u32,
    #[serde(deserialize_with = "super::checkpoint::exact")]
    pub binding: Value,
}

impl ExternalWait {
    pub fn validate(&self) -> Result<(), CheckpointError> {
        identifier(&self.wait_id)?;
        identifier(&self.kind)?;
        if self.schema_version == 0 || !self.binding.is_object() {
            return Err(CheckpointError::Invalid(
                "invalid external wait schema or binding".into(),
            ));
        }
        if serde_json::to_vec(&self.binding)?.len() > MAX_EXTERNAL_BINDING_BYTES {
            return Err(CheckpointError::Invalid(
                "external binding exceeds byte limit".into(),
            ));
        }
        Ok(())
    }
}

/// An already host-verified result, not a request to execute or poll a child.
/// The saved wait and issued authority are checked again during pure merging.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResolution {
    pub call_id: String,
    pub wait: ExternalWait,
    #[serde(deserialize_with = "super::checkpoint::exact")]
    pub result: ToolResult,
}
