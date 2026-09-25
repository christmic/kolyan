//! Progress policy uses completed invocation facts, not token counts or call IDs.

use super::{Effect, PolicyEngine};
use kolyan_model::{ContentBlock, Message, ToolCall};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressPolicy {
    pub repeat_limit: usize,
    pub polling_tools: BTreeSet<String>,
}

impl PolicyEngine {
    pub fn with_progress_policy(mut self, policy: ProgressPolicy) -> Result<Self, String> {
        if policy.repeat_limit == 0 {
            return Err("progress repeat_limit must be positive".into());
        }
        for name in &policy.polling_tools {
            let manifest = self
                .manifests
                .get(name)
                .ok_or("polling tool has no manifest")?;
            if manifest.effects.is_empty()
                || manifest
                    .effects
                    .iter()
                    .any(|effect| *effect != Effect::Read)
            {
                return Err("only read-only tools may bypass repetition checks".into());
            }
        }
        self.progress = Some(policy);
        Ok(self)
    }

    pub fn has_no_progress(&self, next: &ToolCall, messages: &[Message]) -> bool {
        let Some(policy) = &self.progress else {
            return false;
        };
        if policy.polling_tools.contains(&next.name) {
            return false;
        }
        let mut calls = HashMap::new();
        let mut last = None;
        let mut repeats = 0;
        for message in messages {
            for block in &message.content {
                match block {
                    ContentBlock::ToolCall { call } => {
                        calls.insert(&call.id, call);
                    }
                    ContentBlock::ToolResult { result } => {
                        let Some(call) = calls.remove(&result.call_id) else {
                            continue;
                        };
                        let signature = (
                            &call.name,
                            &call.arguments,
                            &result.content,
                            result.is_error,
                        );
                        if last == Some(signature) {
                            repeats += 1;
                        } else {
                            last = Some(signature);
                            repeats = 1;
                        }
                    }
                    _ => {}
                }
            }
        }
        repeats >= policy.repeat_limit
            && last.is_some_and(|(name, args, _, _)| *name == next.name && *args == next.arguments)
    }
}

#[cfg(test)]
mod tests;
