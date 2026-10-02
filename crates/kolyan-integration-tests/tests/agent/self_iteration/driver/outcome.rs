//! Observe actual typed results before enforcing the host's FinalAnswer requirement.

mod tests;

use std::error::Error;

use kolyan_core::{TurnEndReason, TurnError, TurnOutcome};
use kolyan_model::ProviderError;
use kolyan_runtime::DurableTurnResult;
use serde_json::{Value, json};

pub(in crate::self_iteration) struct Observation {
    pub record: Value,
    pub failure: Option<String>,
}

pub(in crate::self_iteration) fn observe(
    stage: &str,
    result: Result<&DurableTurnResult, &(dyn Error + 'static)>,
) -> Observation {
    let (detail, failure) = match result {
        Ok(DurableTurnResult::Completed(completed, _)) => {
            let (kind, response, reason) = match &completed.result.outcome {
                TurnOutcome::FinalAnswer { response } => ("FinalAnswer", Some(response), None),
                TurnOutcome::Refused { response } => ("Refused", Some(response), None),
                TurnOutcome::Incomplete { response } => ("Incomplete", Some(response), None),
                TurnOutcome::Rejected { reason } => ("Rejected", None, Some(reason)),
                TurnOutcome::Expired { reason } => ("Expired", None, Some(reason)),
                TurnOutcome::MaxSteps => ("MaxSteps", None, None),
            };
            (
                json!({"result":"completed","turn_id":completed.result.turn_id,
                "outcome":kind,"end_reason":end_reason(completed.result.end_reason),
                "steps":completed.result.steps.len(),"response":response,"reason":reason}),
                (kind != "FinalAnswer").then(|| {
                    format!(
                        "stage {stage} stopped with actual TurnOutcome::{kind}{}",
                        reason.map(|r| format!(": {r}")).unwrap_or_default()
                    )
                }),
            )
        }
        Ok(DurableTurnResult::Suspended { suspension, .. }) => (
            json!({"result":"suspended","suspension":suspension}),
            Some(format!(
                "stage {stage} actually suspended; no automatic approval"
            )),
        ),
        Err(error) => {
            let mut chain = Vec::new();
            let mut provider = None;
            let mut terminal = None;
            let mut current = Some(error);
            while let Some(cause) = current {
                chain.push(cause.to_string());
                if let Some(error) = cause.downcast_ref::<ProviderError>() {
                    provider = Some(
                        json!({"kind":error.kind,"phase":error.phase,"message":error.message,
                        "provider":error.provider,"status":error.status,"diagnostics":error.diagnostics}),
                    );
                }
                if let Some(error) = cause.downcast_ref::<TurnError>() {
                    terminal = Some(end_reason(error.end_reason()));
                }
                current = cause.source();
            }
            let detail = json!({"result":"error","error":error.to_string(),"error_chain":chain,
                "turn_end_reason":terminal,"provider_error":provider});
            let failure = format!("stage {stage} failed: {error}; actual error detail: {detail}");
            (detail, Some(failure))
        }
    };
    Observation {
        record: json!({"event":"self_iteration_stage_turn_outcome","stage":stage,"detail":detail}),
        failure,
    }
}

fn end_reason(reason: TurnEndReason) -> &'static str {
    match reason {
        TurnEndReason::FinalAnswer => "FinalAnswer",
        TurnEndReason::Refused => "Refused",
        TurnEndReason::Incomplete => "Incomplete",
        TurnEndReason::MaxSteps => "MaxSteps",
        TurnEndReason::Failed => "Failed",
        TurnEndReason::Cancelled => "Cancelled",
        TurnEndReason::TimedOut => "TimedOut",
        TurnEndReason::ApprovalRejected => "ApprovalRejected",
        TurnEndReason::ApprovalExpired => "ApprovalExpired",
        TurnEndReason::NoProgress => "NoProgress",
    }
}
