//! Bounded encoding and neutral message invariants; no provider payload rewriting.

use std::collections::BTreeSet;
use std::io::{self, Write};

use kolyan_model::{CacheBreakpoint, ContentBlock, MessageRole, ModelRequest};
use sha2::{Digest, Sha256};

use super::{ContextError, ContextPolicy};

pub(super) fn policy(value: &ContextPolicy) -> Result<(), ContextError> {
    crate::identity(&value.id).map_err(|e| ContextError::Invalid(e.to_string()))?;
    crate::identity(&value.revision).map_err(|e| ContextError::Invalid(e.to_string()))?;
    if value.max_serialized_bytes == 0
        || value.max_serialized_bytes > 16 * 1024 * 1024
        || value.max_messages == 0
        || value.max_messages > 4096
        || value.max_content_blocks == 0
        || value.max_content_blocks > 16384
        || value.context_limit_tokens == Some(0)
        || value.output_reserve_tokens == 0
    {
        return Err(ContextError::Invalid(
            "invalid bounded context policy".into(),
        ));
    }
    Ok(())
}

pub(super) fn serialize(request: &ModelRequest, limit: usize) -> Result<Vec<u8>, ContextError> {
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    if let Err(error) = serde_json::to_writer(&mut writer, request) {
        return Err(if writer.exceeded {
            ContextError::SizeLimit
        } else {
            ContextError::Invalid(error.to_string())
        });
    }
    Ok(writer.bytes)
}

pub(super) fn bytes_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"kolyan.context.request.v1\0");
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub(super) fn messages(request: &ModelRequest, policy: &ContextPolicy) -> Result<(), ContextError> {
    if request.messages.len() > policy.max_messages {
        return invalid("message count exceeds bound");
    }
    let mut total = 0usize;
    // Model call IDs are Step-scoped. Completed results release IDs so a later
    // assistant batch can reuse them without confusing active result pairing.
    let mut pending = BTreeSet::new();
    for message in &request.messages {
        total = total
            .checked_add(message.content.len())
            .ok_or(ContextError::SizeLimit)?;
        if total > policy.max_content_blocks {
            return invalid("content block count exceeds bound");
        }
        if !pending.is_empty() && message.role != MessageRole::User {
            return invalid("assistant advanced before all tool results");
        }
        for block in &message.content {
            match block {
                ContentBlock::ToolCall { call } => {
                    if message.role != MessageRole::Assistant
                        || call.id.trim().is_empty()
                        || call.name.trim().is_empty()
                        || !pending.insert(call.id.clone())
                    {
                        return invalid("invalid or duplicate assistant tool call");
                    }
                }
                ContentBlock::ToolResult { result } => {
                    if message.role != MessageRole::User || !pending.remove(&result.call_id) {
                        return invalid("dangling or duplicate tool result");
                    }
                }
                ContentBlock::Reasoning { .. } if message.role != MessageRole::Assistant => {
                    return invalid("reasoning belongs to assistant content");
                }
                ContentBlock::Text { .. }
                | ContentBlock::Image { .. }
                | ContentBlock::Document { .. }
                    if !pending.is_empty() && message.role == MessageRole::User =>
                {
                    return invalid("non-result content interleaved with pending tool results");
                }
                _ => {}
            }
        }
    }
    if !pending.is_empty() {
        return invalid("tool calls lack completed results");
    }
    if let Some(cache) = &request.prompt_cache {
        for breakpoint in &cache.breakpoints {
            let exists = match breakpoint {
                CacheBreakpoint::System => !request.system.is_empty(),
                CacheBreakpoint::Tools => !request.tools.is_empty(),
                CacheBreakpoint::Messages => request.messages.iter().any(|m| !m.content.is_empty()),
            };
            if !exists {
                return invalid("cache breakpoint references an absent section");
            }
        }
    }
    Ok(())
}

fn invalid<T>(reason: &str) -> Result<T, ContextError> {
    Err(ContextError::Invalid(reason.into()))
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("context byte bound exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
