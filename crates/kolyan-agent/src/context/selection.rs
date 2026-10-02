//! Bounded deterministic proposals; admission and accounting remain host-owned.

use kolyan_model::{ContentBlock, MessageRole, ModelRequest};
use serde::{Deserialize, Serialize};

use super::{
    ContextError, ContextPolicy, ContextProjectionPlan, RetainedMessageRange,
    context_source_digest, projection::visit_original_pairs,
};

/// Source guards and versioned oldest-group omission policy, not a token budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionPolicy {
    pub id: String,
    pub revision: String,
    pub source_bounds: ContextPolicy,
    pub max_reduced_candidates: u8,
}

/// Propose full history followed by at most eight cumulative reductions.
/// Original tool pairs form contiguous closures. The first objective and latest
/// user input tail are protected. No summary, mutation, counting or I/O occurs.
/// Candidates do not guarantee fit or monotonically decreasing token counts;
/// the host must validate each selected projection and account its mapped input.
pub fn selection_candidates(
    source: &ModelRequest,
    policy: &SelectionPolicy,
) -> Result<Vec<ContextProjectionPlan>, ContextError> {
    crate::identity(&policy.id).map_err(invalid)?;
    crate::identity(&policy.revision).map_err(invalid)?;
    if policy.max_reduced_candidates > 8 {
        return Err(invalid("selection allows at most eight reduced candidates"));
    }
    let digest = context_source_digest(source, &policy.source_bounds)?;
    let mut anchors = source
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
        });
    let first = anchors
        .next()
        .ok_or_else(|| invalid("selection has no user objective anchor"))?;
    let current = anchors.next_back().unwrap_or(first);
    let length = source.messages.len();
    let mut ends: Vec<usize> = (1..=length).collect();
    visit_original_pairs(source, |call, result| {
        ends[call] = ends[call].max(result + 1);
        Ok(())
    })?;
    let mut groups = Vec::new();
    let mut start = 0;
    while start < length {
        let mut end = ends[start];
        let mut cursor = start + 1;
        while cursor < end {
            end = end.max(ends[cursor]);
            cursor += 1;
        }
        groups.push(RetainedMessageRange { start, end });
        start = end;
    }
    let optional: Vec<usize> = groups
        .iter()
        .enumerate()
        .filter_map(|(index, group)| {
            (!(group.start <= first && first < group.end) && group.end <= current).then_some(index)
        })
        .collect();
    let count = optional
        .len()
        .min(usize::from(policy.max_reduced_candidates));
    let mut plans = Vec::with_capacity(count + 1);
    let mut retained = vec![true; groups.len()];
    let mut removed = 0;
    for candidate in 0..=count {
        if candidate > 0 {
            let prefix = (candidate * optional.len()).div_ceil(count);
            for &index in &optional[removed..prefix] {
                retained[index] = false;
            }
            removed = prefix;
        }
        let mut ranges: Vec<RetainedMessageRange> = Vec::new();
        for (group, kept) in groups.iter().zip(&retained) {
            if !kept {
                continue;
            }
            if let Some(previous) = ranges.last_mut().filter(|range| range.end == group.start) {
                previous.end = group.end;
            } else {
                ranges.push(group.clone());
            }
        }
        plans.push(ContextProjectionPlan {
            policy_id: policy.id.clone(),
            policy_revision: policy.revision.clone(),
            expected_source_digest: digest.clone(),
            retained_messages: ranges,
        });
    }
    Ok(plans)
}

fn invalid(reason: impl ToString) -> ContextError {
    ContextError::Invalid(reason.to_string())
}

#[cfg(test)]
mod tests;
