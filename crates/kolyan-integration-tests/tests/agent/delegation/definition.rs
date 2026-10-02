//! Compare validated semantic definitions without changing the recorded wire input.

mod tests;

use kolyan_agent::{AgentDefinition, AgentDefinitionInput};
use serde_json::Value;

pub(super) fn validated(value: Value) -> Result<AgentDefinition, String> {
    let input: AgentDefinitionInput =
        serde_json::from_value(value).map_err(|error| error.to_string())?;
    AgentDefinition::new(input).map_err(|error| error.to_string())
}
