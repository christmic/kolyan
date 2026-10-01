//! Host-selected whole-message projection before Turn admission. Source history
//! is immutable; this module does not summarize, persist or alter Provider input.

use std::collections::BTreeMap;

use kolyan_model::{ContentBlock, MessageRole, ModelDescriptor, ModelRequest};
use serde::{Deserialize, Serialize};

use super::{
    ContextChange, ContextError, ContextPolicy, ContextTokenCounter, PreparedContext,
    RetainedMessageRange, prepare_context, validation,
};

/// Versioned host proposal bound to the exact complete source request. Ranges
/// must be ordered, nonempty and disjoint; selected messages remain verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextProjectionPlan {
    pub policy_id: String,
    pub policy_revision: String,
    pub expected_source_digest: String,
    pub retained_messages: Vec<RetainedMessageRange>,
}

/// Evidence of a deterministic selection, not a trusted token count or grant.
/// The host must persist the full source and this evidence before admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionProvenance {
    pub source_digest: String,
    pub selected_digest: String,
    pub policy_digest: String,
    pub policy_id: String,
    pub policy_revision: String,
    pub source_serialized_bytes: usize,
    pub selected_serialized_bytes: usize,
    pub retained_messages: Vec<RetainedMessageRange>,
    pub omitted_messages: Vec<RetainedMessageRange>,
}

/// Core and Provider must receive this exact prepared request. Budget provenance
/// refers to the selected request; projection provenance refers to full history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectedContext {
    pub prepared: PreparedContext,
    pub provenance: ProjectionProvenance,
}

/// Validate source semantics and bounded serialization without claiming its
/// token budget is admissible. Counting applies to the selected request only.
pub fn context_source_digest(
    source: &ModelRequest,
    source_bounds: &ContextPolicy,
) -> Result<String, ContextError> {
    Ok(validation::bytes_digest(&source_bytes(
        source,
        source_bounds,
    )?))
}

/// Select complete historical messages before Turn admission. Preserve the
/// objective anchor and every message from the latest non-result user input.
/// Reject incomplete tool pairing, changed source, implicit output normalization
/// and unknown Strict budgets. No I/O, source mutation or automatic retry occurs.
pub fn project_context(
    source: &ModelRequest,
    descriptor: &ModelDescriptor,
    source_bounds: &ContextPolicy,
    plan: &ContextProjectionPlan,
    target_policy: &ContextPolicy,
    counter: &dyn ContextTokenCounter,
) -> Result<ProjectedContext, ContextError> {
    crate::identity(&plan.policy_id).map_err(invalid)?;
    crate::identity(&plan.policy_revision).map_err(invalid)?;
    let bytes = source_bytes(source, source_bounds)?;
    let source_digest = validation::bytes_digest(&bytes);
    if plan.expected_source_digest != source_digest {
        return Err(invalid("projection source digest mismatch"));
    }
    let mut selected = source.clone();
    selected.messages.clear();
    let mut retained = vec![false; source.messages.len()];
    let mut omitted = Vec::new();
    let mut end = 0;
    if plan.retained_messages.is_empty() {
        return Err(invalid("projection has no retained message ranges"));
    }
    for range in &plan.retained_messages {
        if range.start < end || range.start >= range.end || range.end > retained.len() {
            return Err(invalid(
                "projection ranges are unordered, overlapping or invalid",
            ));
        }
        if range.start > end {
            omitted.push(RetainedMessageRange {
                start: end,
                end: range.start,
            });
        }
        selected
            .messages
            .extend_from_slice(&source.messages[range.start..range.end]);
        retained[range.start..range.end].fill(true);
        end = range.end;
    }
    if end < retained.len() {
        omitted.push(RetainedMessageRange {
            start: end,
            end: retained.len(),
        });
    }
    let anchors = source
        .messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            (message.role == MessageRole::User
                && message
                    .content
                    .iter()
                    .any(|block| !matches!(block, ContentBlock::ToolResult { .. })))
            .then_some(index)
        })
        .collect::<Vec<_>>();
    let first = *anchors
        .first()
        .ok_or_else(|| invalid("projection has no user objective anchor"))?;
    let current = *anchors.last().expect("first anchor exists");
    if !retained[first] || retained[current..].iter().any(|kept| !kept) {
        return Err(invalid(
            "projection removed objective or current input tail",
        ));
    }
    preserve_original_pairs(source, &retained)?;
    let prepared = prepare_context(&selected, descriptor, target_policy, counter)?;
    if prepared.change != ContextChange::Unchanged || prepared.request != selected {
        return Err(invalid(
            "projection requires an explicit unchanged output limit",
        ));
    }
    let provenance = ProjectionProvenance {
        source_digest,
        selected_digest: prepared.provenance.prepared_digest.clone(),
        policy_digest: crate::digest(&(
            "kolyan.context.projection.v1",
            source_bounds,
            plan,
            target_policy,
        ))
        .map_err(invalid)?,
        policy_id: plan.policy_id.clone(),
        policy_revision: plan.policy_revision.clone(),
        source_serialized_bytes: bytes.len(),
        selected_serialized_bytes: prepared.provenance.prepared_serialized_bytes,
        retained_messages: plan.retained_messages.clone(),
        omitted_messages: omitted,
    };
    Ok(ProjectedContext {
        prepared,
        provenance,
    })
}

fn source_bytes(source: &ModelRequest, bounds: &ContextPolicy) -> Result<Vec<u8>, ContextError> {
    validation::policy(bounds)?;
    let bytes = validation::serialize(source, bounds.max_serialized_bytes)?;
    validation::messages(source, bounds)?;
    Ok(bytes)
}

fn preserve_original_pairs(source: &ModelRequest, retained: &[bool]) -> Result<(), ContextError> {
    let mut pending = BTreeMap::new();
    for (index, message) in source.messages.iter().enumerate() {
        for block in &message.content {
            match block {
                ContentBlock::ToolCall { call } => {
                    pending.insert(&call.id, index);
                }
                ContentBlock::ToolResult { result } => {
                    let call_index = pending
                        .remove(&result.call_id)
                        .ok_or_else(|| invalid("source tool result has no call"))?;
                    // Step-scoped IDs may recur. Validate the original pair,
                    // not an accidental match across omitted historical Steps.
                    if retained[call_index] != retained[index] {
                        return Err(invalid("projection split an original tool pair"));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn invalid(reason: impl ToString) -> ContextError {
    ContextError::Invalid(reason.to_string())
}

#[cfg(test)]
mod tests;
