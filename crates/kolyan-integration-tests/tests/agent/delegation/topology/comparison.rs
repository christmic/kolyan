//! Export every observed child and the aggregate before any receipt assertion.

use serde_json::Value;

use super::Evidence;

pub(super) struct InvocationObservation {
    pub row: Value,
    pub file_receipts: usize,
    pub requires_receipt: bool,
}

pub(super) fn export_then_compare(
    evidence: &Evidence,
    observations: &[InvocationObservation],
    summary: Value,
) {
    for observation in observations {
        evidence.append(observation.row.clone()).unwrap();
    }
    evidence.append(summary).unwrap();
    for observation in observations {
        assert!(
            observation.file_receipts <= 1,
            "no invocation may substitute a duplicate read for another child's missing effect"
        );
        if observation.requires_receipt {
            assert_eq!(
                observation.file_receipts, 1,
                "each separately approved child requires its own receipt"
            );
        }
    }
}

#[cfg(test)]
mod tests;
