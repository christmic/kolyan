//! Host correlation for optional process telemetry, never execution authority.

use kolyan_policy::{PreparedCall, ToolExecutionScope};
use kolyan_sandbox::SandboxProcessObservationSender;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToolProcessObservationContext {
    pub tool_name: String,
    pub scope: ToolExecutionScope,
    pub prepared_digest: String,
}

pub(crate) fn bind_observer(
    sender: Option<&SandboxProcessObservationSender>,
    prepared: &PreparedCall,
    scope: &ToolExecutionScope,
) -> Option<SandboxProcessObservationSender> {
    let sender = sender?;
    let context = ToolProcessObservationContext {
        tool_name: prepared.call().name.clone(),
        scope: scope.clone(),
        prepared_digest: prepared.digest().into(),
    };
    match serde_json::to_string(&context) {
        Ok(context) => sender.bind_context(context),
        Err(_) => {
            sender.record_loss();
            None
        }
    }
}
