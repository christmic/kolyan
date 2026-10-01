//! Lossless context preparation with semantic validation and explicit uncertainty.
//! No source content is discarded or summarized. Strict budget admission requires
//! a trusted model-specific counter supplied by the host, not byte estimates.

mod projection;
mod validation;

pub use projection::{
    ContextProjectionPlan, ProjectedContext, ProjectionProvenance, context_source_digest,
    project_context,
};

use kolyan_model::{ModelDescriptor, ModelRequest};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Strict admission rejects unknown counts; inspection may return unverified data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BudgetMode {
    Strict,
    Inspect,
}

/// Deterministic policy. Size limits count serialized UTF-8 bytes, not tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPolicy {
    pub id: String,
    pub revision: String,
    pub mode: BudgetMode,
    pub max_serialized_bytes: usize,
    pub max_messages: usize,
    pub max_content_blocks: usize,
    pub context_limit_tokens: Option<u32>,
    pub output_reserve_tokens: u32,
}

/// A trusted count must cover the full provider-mapped request, including framing,
/// tools and extensions. The caller owns counter trust and model compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenMeasurement {
    Trusted {
        input_tokens: u64,
    },
    Unknown {
        reason: String,
        estimated_input_tokens: Option<u64>,
    },
}

/// Host-owned model-specific counting port. Errors are never replaced by estimates.
pub trait ContextTokenCounter {
    fn id(&self) -> &str;
    fn revision(&self) -> &str;
    fn count(
        &self,
        request: &ModelRequest,
        serialized_utf8: &[u8],
    ) -> Result<TokenMeasurement, String>;
}

/// Diagnostic assumption only: one estimated token per serialized UTF-8 byte.
/// This is not an authoritative upper bound for any provider or model.
pub struct SerializedByteEstimator;

impl ContextTokenCounter for SerializedByteEstimator {
    fn id(&self) -> &str {
        "serialized-utf8-byte-estimate"
    }
    fn revision(&self) -> &str {
        "1"
    }
    fn count(&self, _: &ModelRequest, serialized_utf8: &[u8]) -> Result<TokenMeasurement, String> {
        Ok(TokenMeasurement::Unknown {
            reason: "Diagnostic one-token-per-serialized-byte assumption; provider token count is unknown".into(),
            estimated_input_tokens: Some(serialized_utf8.len() as u64),
        })
    }
}

/// Half-open source message indices. No ranges are omitted in this implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMessageRange {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextChange {
    Unchanged,
    OutputLimitAdded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BudgetStatus {
    Verified {
        input_tokens: u64,
    },
    Unverified {
        reason: String,
        estimated_input_tokens: Option<u64>,
    },
}

/// Source and derived digests cover the entire neutral request, not mapped wire.
/// Digests establish content identity, not trusted authorization or exact tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextProvenance {
    pub source_digest: String,
    pub prepared_digest: String,
    pub policy_digest: String,
    pub policy_id: String,
    pub policy_revision: String,
    pub counter_id: String,
    pub counter_revision: String,
    pub source_serialized_bytes: usize,
    pub prepared_serialized_bytes: usize,
    pub retained_messages: Vec<RetainedMessageRange>,
    pub system_preserved: bool,
    pub tools_preserved: bool,
    pub cache_preserved: bool,
    pub context_limit_tokens: u32,
    pub output_reserve_tokens: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedContext {
    pub request: ModelRequest,
    pub change: ContextChange,
    pub budget: BudgetStatus,
    pub provenance: ContextProvenance,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContextError {
    #[error("invalid context: {0}")]
    Invalid(String),
    #[error("serialized context exceeds the UTF-8 byte bound")]
    SizeLimit,
    #[error("model context window is unknown")]
    UnknownModelLimit,
    #[error("token budget is unknown: {reason}")]
    UnknownBudget {
        reason: String,
        estimated_input_tokens: Option<u64>,
        provenance: Box<ContextProvenance>,
    },
    #[error("input tokens {input_tokens} plus output reserve exceed the context limit")]
    Overflow {
        input_tokens: u64,
        provenance: Box<ContextProvenance>,
    },
    #[error("token counter failed: {reason}")]
    CounterFailed {
        reason: String,
        provenance: Box<ContextProvenance>,
    },
}

/// Validate semantics and bounded serialization before counting. Retain every
/// system/message/tool/reasoning/cache block verbatim; only fill a missing output
/// limit from the explicit reserve. This function performs no I/O or compaction.
pub fn prepare_context(
    request: &ModelRequest,
    descriptor: &ModelDescriptor,
    policy: &ContextPolicy,
    counter: &dyn ContextTokenCounter,
) -> Result<PreparedContext, ContextError> {
    validation::policy(policy)?;
    if request.model != descriptor.reference {
        return Err(ContextError::Invalid("descriptor model mismatch".into()));
    }
    crate::identity(counter.id()).map_err(|e| ContextError::Invalid(e.to_string()))?;
    crate::identity(counter.revision()).map_err(|e| ContextError::Invalid(e.to_string()))?;
    let window = descriptor
        .context_window
        .filter(|v| *v > 0)
        .ok_or(ContextError::UnknownModelLimit)?;
    let limit = policy
        .context_limit_tokens
        .map_or(window, |v| v.min(window));
    if policy.output_reserve_tokens >= limit
        || descriptor
            .max_output_tokens
            .is_some_and(|max| policy.output_reserve_tokens > max)
    {
        return Err(ContextError::Invalid(
            "output reserve exceeds model limits".into(),
        ));
    }
    if request
        .max_output_tokens
        .is_some_and(|max| max == 0 || max > policy.output_reserve_tokens)
        || request
            .reasoning
            .as_ref()
            .and_then(|v| v.budget_tokens)
            .is_some_and(|budget| budget > policy.output_reserve_tokens)
    {
        return Err(ContextError::Invalid(
            "output or reasoning budget exceeds reserve".into(),
        ));
    }
    let source = validation::serialize(request, policy.max_serialized_bytes)?;
    validation::messages(request, policy)?;
    let mut prepared = request.clone();
    let change = if prepared.max_output_tokens.is_none() {
        prepared.max_output_tokens = Some(policy.output_reserve_tokens);
        ContextChange::OutputLimitAdded
    } else {
        ContextChange::Unchanged
    };
    let bytes = validation::serialize(&prepared, policy.max_serialized_bytes)?;
    let provenance = ContextProvenance {
        source_digest: validation::bytes_digest(&source),
        prepared_digest: validation::bytes_digest(&bytes),
        policy_digest: crate::digest(&("kolyan.context.policy.v1", policy))
            .map_err(|e| ContextError::Invalid(e.to_string()))?,
        policy_id: policy.id.clone(),
        policy_revision: policy.revision.clone(),
        counter_id: counter.id().into(),
        counter_revision: counter.revision().into(),
        source_serialized_bytes: source.len(),
        prepared_serialized_bytes: bytes.len(),
        retained_messages: if request.messages.is_empty() {
            Vec::new()
        } else {
            vec![RetainedMessageRange {
                start: 0,
                end: request.messages.len(),
            }]
        },
        system_preserved: true,
        tools_preserved: true,
        cache_preserved: true,
        context_limit_tokens: limit,
        output_reserve_tokens: policy.output_reserve_tokens,
    };
    let measurement =
        counter
            .count(&prepared, &bytes)
            .map_err(|reason| ContextError::CounterFailed {
                reason,
                provenance: Box::new(provenance.clone()),
            })?;
    let budget = match measurement {
        TokenMeasurement::Trusted { input_tokens } => {
            if input_tokens
                .checked_add(u64::from(policy.output_reserve_tokens))
                .is_none_or(|sum| sum > u64::from(limit))
            {
                return Err(ContextError::Overflow {
                    input_tokens,
                    provenance: Box::new(provenance),
                });
            }
            BudgetStatus::Verified { input_tokens }
        }
        TokenMeasurement::Unknown {
            reason,
            estimated_input_tokens,
        } => {
            if policy.mode == BudgetMode::Strict {
                return Err(ContextError::UnknownBudget {
                    reason,
                    estimated_input_tokens,
                    provenance: Box::new(provenance),
                });
            }
            BudgetStatus::Unverified {
                reason,
                estimated_input_tokens,
            }
        }
    };
    Ok(PreparedContext {
        request: prepared,
        change,
        budget,
        provenance,
    })
}

#[cfg(test)]
mod tests;
