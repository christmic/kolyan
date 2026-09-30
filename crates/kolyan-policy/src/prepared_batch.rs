//! Batch admission using trusted preparation, without tool-name inference.

use std::collections::BTreeSet;

use crate::{
    BatchCallDecision, BatchExecutionPlan, PolicyContext, PolicyEngine, PreparedCall,
    PreparedError, execution_stages,
};

impl PolicyEngine {
    /// Duplicate invocation IDs or corrupted preparations fail admission rather
    /// than selecting an arbitrary claim. Scheduling cannot grant authority or
    /// replace execution validation.
    pub fn resolve_prepared_batch(
        &self,
        context: &PolicyContext,
        calls: &[PreparedCall],
    ) -> Result<BatchExecutionPlan, PreparedError> {
        let mut identities = BTreeSet::new();
        let mut decisions = Vec::with_capacity(calls.len());
        for call in calls {
            call.validate()?;
            if !identities.insert(call.call().id.as_str()) {
                return Err(PreparedError::Invalid("duplicate prepared call ID".into()));
            }
            decisions.push(BatchCallDecision {
                call_id: call.call().id.clone(),
                claim: call.claim().clone(),
                decision: self.decide_prepared(call, context),
            });
        }
        let stages = execution_stages(&decisions, &[]);
        Ok(BatchExecutionPlan { decisions, stages })
    }
}

#[cfg(test)]
mod tests;
