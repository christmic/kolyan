use std::io::Write;

use kolyan_model::{ModelDescriptor, ModelRef};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::context::{BudgetMode, SerializedByteEstimator, project_context};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    messages: Vec<Value>,
    limit: u8,
    mutation: String,
    expected: String,
    ranges: Vec<Vec<[usize; 2]>>,
    projection_error: Option<String>,
}

#[test]
fn candidates_export_full_observations_then_reload_and_compare() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-context-selection-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    println!("CONTEXT_SELECTION_TRACE={}", path.display());
    for case in &cases {
        let mut source: ModelRequest = serde_json::from_value(json!({
            "request_id":"selection-source", "model":{"provider":"test","model":"test"},
            "system":[{"text":"Preserve 系统 🦀", "cache":true}],
            "messages":case.messages,
            "tools":[{"name":"file.read","description":"Read only","input_schema":{"type":"object"}}],
            "tool_choice":"auto", "max_output_tokens":100,
            "extensions":{"opaque":{"keep":[1,"中文",null]}}
        })).unwrap();
        let mut policy = SelectionPolicy {
            id: "oldest-groups".into(),
            revision: "1".into(),
            max_reduced_candidates: case.limit,
            source_bounds: ContextPolicy {
                id: "bounded-source".into(),
                revision: "1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 65536,
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 100,
            },
        };
        match case.mutation.as_str() {
            "none" => {}
            "identity" => policy.id.clear(),
            "bytes" => policy.source_bounds.max_serialized_bytes = 1,
            "messages" => policy.source_bounds.max_messages = 1,
            "blocks" => policy.source_bounds.max_content_blocks = 1,
            "missing_output" => source.max_output_tokens = None,
            "strict" => policy.source_bounds.mode = BudgetMode::Strict,
            "cache" => {
                source.prompt_cache =
                    Some(serde_json::from_value(json!({"breakpoints":["messages"]})).unwrap())
            }
            other => panic!("unknown mutation {other}"),
        }
        let before = source.clone();
        let result = selection_candidates(&source, &policy);
        let projections: Vec<Value> = result.as_ref().map(|plans| plans.iter().map(|plan| {
            let projected = project_context(&source,&descriptor(),&policy.source_bounds,plan,&policy.source_bounds,&SerializedByteEstimator);
            json!({"plan":plan,"projected":projected.as_ref().ok(),"error":projected.as_ref().err().map(error_value)})
        }).collect()).unwrap_or_default();
        writeln!(file,"{}",json!({"case":case.id,"source_before":before,"source_after":source,
            "policy":policy,"plans":result.as_ref().ok(),"error":result.as_ref().err().map(error_value),
            "projections":projections})).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    let actual = std::fs::read_to_string(&path).unwrap();
    let rows: Vec<Value> = actual
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["source_before"], row["source_after"], "{}", case.id);
        let source: ModelRequest = serde_json::from_value(row["source_before"].clone()).unwrap();
        let policy: SelectionPolicy = serde_json::from_value(row["policy"].clone()).unwrap();
        if case.expected != "passed" {
            assert_eq!(row["error"]["kind"], case.expected, "{}", case.id);
            assert!(row["plans"].is_null());
            continue;
        }
        let plans: Vec<ContextProjectionPlan> =
            serde_json::from_value(row["plans"].clone()).unwrap();
        assert_eq!(selection_candidates(&source, &policy).unwrap(), plans);
        let ranges: Vec<Vec<[usize; 2]>> = plans
            .iter()
            .map(|plan| {
                plan.retained_messages
                    .iter()
                    .map(|range| [range.start, range.end])
                    .collect()
            })
            .collect();
        assert_eq!(ranges, case.ranges, "{}", case.id);
        assert!(plans.len() <= 9);
        for observation in row["projections"].as_array().unwrap() {
            if let Some(expected) = &case.projection_error {
                assert_eq!(observation["error"]["kind"], *expected, "{}", case.id);
                continue;
            }
            assert!(
                observation["error"].is_null(),
                "{}: {}",
                case.id,
                observation["error"]
            );
            let projected: super::super::ProjectedContext =
                serde_json::from_value(observation["projected"].clone()).unwrap();
            let plan: ContextProjectionPlan =
                serde_json::from_value(observation["plan"].clone()).unwrap();
            let mut expected = source.clone();
            expected.messages = plan
                .retained_messages
                .iter()
                .flat_map(|range| source.messages[range.start..range.end].iter().cloned())
                .collect();
            assert_eq!(projected.prepared.request, expected);
            assert_eq!(
                projected.provenance.source_digest,
                plan.expected_source_digest
            );
            assert_eq!(
                project_context(
                    &source,
                    &descriptor(),
                    &policy.source_bounds,
                    &plan,
                    &policy.source_bounds,
                    &SerializedByteEstimator
                )
                .unwrap(),
                projected
            );
        }
    }
}

fn descriptor() -> ModelDescriptor {
    ModelDescriptor {
        reference: ModelRef::new("test", "test"),
        context_window: Some(100000),
        max_output_tokens: Some(1000),
        features: Default::default(),
    }
}

fn error_value(error: &ContextError) -> Value {
    let (kind, evidence) = match error {
        ContextError::Invalid(_) => ("invalid", Value::Null),
        ContextError::SizeLimit => ("size_limit", Value::Null),
        ContextError::UnknownModelLimit => ("unknown_model_limit", Value::Null),
        ContextError::UnknownBudget {
            reason,
            estimated_input_tokens,
            provenance,
        } => (
            "unknown_budget",
            json!({"reason":reason,"estimated_input_tokens":estimated_input_tokens,"provenance":provenance}),
        ),
        ContextError::Overflow {
            input_tokens,
            provenance,
        } => (
            "overflow",
            json!({"input_tokens":input_tokens,"provenance":provenance}),
        ),
        ContextError::CounterFailed { reason, provenance } => (
            "counter_failed",
            json!({"reason":reason,"provenance":provenance}),
        ),
    };
    json!({"kind":kind,"message":error.to_string(),"evidence":evidence})
}
