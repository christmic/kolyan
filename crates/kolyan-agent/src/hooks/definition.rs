//! Validated static script ceilings, not installation or dynamic authority.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use kolyan_policy::{Capability, Effect};

use super::{HookError, id};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookKey {
    pub id: String,
    pub revision: String,
}
impl HookKey {
    pub fn validate(&self) -> Result<(), HookError> {
        id(&self.id)?;
        id(&self.revision)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPhase {
    BeforeModel,
    BeforeTool,
    AfterTool,
}

/// Host-authored definitions allow only read-only native execution. Resource
/// access is independently enforced by NativeHookHost's exact-file sandbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookManifest {
    pub key: HookKey,
    pub phases: BTreeSet<HookPhase>,
    pub tool_names: BTreeSet<String>,
    pub capabilities: BTreeSet<Capability>,
    pub effects: BTreeSet<Effect>,
    pub timeout_ms: u64,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
}
impl HookManifest {
    pub fn validate(&self) -> Result<(), HookError> {
        self.key.validate()?;
        if self.phases.is_empty() || self.tool_names.len() > 32 {
            return Err(HookError::Invalid("invalid event selection".into()));
        }
        for name in &self.tool_names {
            id(name)?;
        }
        if self.capabilities
            != BTreeSet::from([Capability::ProcessExecute, Capability::FilesystemRead])
            || self.effects != BTreeSet::from([Effect::Execute, Effect::Read])
        {
            return Err(HookError::Permission);
        }
        if !(1..=5000).contains(&self.timeout_ms)
            || !(1..=256 * 1024).contains(&self.max_input_bytes)
            || !(1..=64 * 1024).contains(&self.max_output_bytes)
        {
            return Err(HookError::Capacity);
        }
        Ok(())
    }
    pub(super) fn matches(&self, phase: HookPhase, name: Option<&str>) -> bool {
        self.phases.contains(&phase)
            && (phase == HookPhase::BeforeModel
                || self.tool_names.is_empty()
                || name.is_some_and(|n| self.tool_names.contains(n)))
    }
}
