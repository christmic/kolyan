//! Case inputs and expectations are data, shared by scripted and network runs.

use std::collections::BTreeMap;

use kolyan_model::{ContentBlock, TokenUsage};
use kolyan_policy::ToolManifest;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub schema_version: u32,
    pub configured_combinations: usize,
    pub selectors: Vec<String>,
    pub instructions: String,
    pub inspection_window_assumption_tokens: u32,
    pub output_reserve_tokens: u32,
    pub max_steps: usize,
    pub max_tool_calls: usize,
    pub policy_scope: String,
    pub shell_policy_scope: String,
    pub policy: Vec<ToolManifest>,
    pub turns: Vec<Turn>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Turn {
    pub id: String,
    pub input: String,
    pub script: Vec<Frame>,
    pub expected: Expected,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frame {
    pub reasoning: String,
    pub content: Vec<ContentBlock>,
    pub usage: TokenUsage,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expected {
    pub file: BTreeMap<String, String>,
    pub tool_minimums: BTreeMap<String, usize>,
    pub outcome: String,
    pub history_contains: Vec<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

pub fn dataset() -> Dataset {
    serde_json::from_str(include_str!("../fixtures/agent/root.json")).unwrap()
}
