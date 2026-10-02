//! Failure/retention boundary only, not positive reduction acceptance.
//! Source is a real durable ModelRequested with actual previous tool results.
use super::*;
use kolyan_agent::context::{
    BudgetMode, BudgetStatus, ContextError, ContextPolicy, prepare_context,
};
use kolyan_model::ModelDescriptor;

pub(super) fn verify(source: &ModelRequest, case: &Case, evidence: &Evidence) {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/agent/long_task_context_boundary.json"
    ))
    .unwrap();
    let oracle: Value = serde_json::from_str(
        include_str!("../../../expected/agent/long_task_context_boundary.jsonl").trim(),
    )
    .unwrap();
    assert_eq!(fixture["schema_version"], 1);
    assert_eq!(
        fixture["source"],
        "final_recorded_model_request_from_single_task_continuation"
    );
    let bytes = serde_json::to_vec(source).unwrap();
    assert!(
        source
            .messages
            .iter()
            .flat_map(|m| &m.content)
            .any(|b| matches!(b, ContentBlock::ToolResult { .. })),
        "the source must include actual prior tool results"
    );
    let descriptor = ModelDescriptor {
        reference: source.model.clone(),
        context_window: Some(case.inspection_window_assumption_tokens),
        max_output_tokens: None,
        features: Default::default(),
    };
    let mut policy = ContextPolicy {
        id: "long-context-boundary".into(),
        revision: "1".into(),
        mode: BudgetMode::Inspect,
        max_serialized_bytes: fixture["inspect_limit_bytes"].as_u64().unwrap() as usize,
        max_messages: 2048,
        max_content_blocks: 8192,
        context_limit_tokens: None,
        output_reserve_tokens: case.output_reserve_tokens,
    };
    let inspected = prepare_context(source, &descriptor, &policy, &providers::UnsupportedCounter);
    evidence.append(json!({"event":"context_boundary_inspect","source":source,"result":inspected.as_ref().ok(),"error":inspected.as_ref().err().map(ToString::to_string),"positive_reduction":oracle["positive_context_reduction"]})).unwrap();
    let inspected = inspected.unwrap();
    assert!(matches!(
        inspected.budget,
        BudgetStatus::Unverified {
            estimated_input_tokens: None,
            ..
        }
    ));
    assert_eq!(inspected.request, *source);
    assert_eq!(
        inspected.provenance.source_digest,
        inspected.provenance.prepared_digest
    );
    policy.mode = BudgetMode::Strict;
    let strict = prepare_context(source, &descriptor, &policy, &providers::UnsupportedCounter);
    evidence.append(json!({"event":"context_boundary_strict","error":strict.as_ref().err().map(ToString::to_string),"expected":oracle["strict"]})).unwrap();
    assert!(matches!(
        strict,
        Err(ContextError::UnknownBudget {
            estimated_input_tokens: None,
            ..
        })
    ));
    policy.mode = BudgetMode::Inspect;
    policy.max_serialized_bytes = fixture["overflow_limit_bytes"].as_u64().unwrap() as usize;
    let overflow = prepare_context(source, &descriptor, &policy, &providers::UnsupportedCounter);
    evidence.append(json!({"event":"context_boundary_overflow","error":overflow.as_ref().err().map(ToString::to_string),"expected":oracle["overflow"],"positive_reduction":false})).unwrap();
    assert_eq!(overflow.unwrap_err(), ContextError::SizeLimit);
    assert_eq!(
        serde_json::to_vec(source).unwrap(),
        bytes,
        "never silently reduce or truncate the persisted source"
    );
}
