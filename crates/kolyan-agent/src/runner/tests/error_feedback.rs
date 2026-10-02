//! Synthetic model/fault adapters; actual AgentRunner, Task, Session and Runtime.
//! No network or native OS-tool acceptance is claimed by these module cases.

mod adapters;
mod observe;

use std::{
    fs::File,
    io::{BufWriter, Write},
};

use kolyan_model::{ContentBlock, ModelRequest, ToolCall};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    continue_batch: bool,
    approval: bool,
    max_steps: usize,
    max_tool_calls: usize,
    responses: Vec<Vec<ToolCall>>,
    fault: Option<Fault>,
    expected: Expected,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fault {
    phase: Phase,
    error: Fatal,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Prepare,
    Execute,
}

#[derive(Clone, Copy, Deserialize)]
enum Fatal {
    Uncertain,
    TimedOut,
    Cancelled,
    InvalidBatch,
}

impl Fatal {
    fn error(self) -> kolyan_core::ToolError {
        use kolyan_core::ToolError;
        match self {
            Self::Uncertain => ToolError::Uncertain {
                message: "synthetic uncertain outcome".into(),
            },
            Self::TimedOut => ToolError::TimedOut,
            Self::Cancelled => ToolError::Cancelled,
            Self::InvalidBatch => ToolError::InvalidBatch {
                message: "synthetic invalid batch".into(),
            },
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    requests: usize,
    effects: usize,
    grants: usize,
    receipts: usize,
    feedback: usize,
    error: bool,
    outcome: Option<String>,
}

#[tokio::test]
async fn host_tool_error_policy_cross_chain_exports_all_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("error_feedback/cases.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        rows.push(observe::run(case).await);
    }
    let exported = export(&rows);
    for (case, row) in cases.iter().zip(&exported) {
        compare(case, row);
    }
}

#[tokio::test]
async fn routed_strict_invocation_parser_feedback_exports_both_before_comparison() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("error_feedback/delegation.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        rows.push(observe::run_delegation(case).await);
    }
    let exported = export(&rows);
    for (case, row) in cases.iter().zip(&exported) {
        compare(case, row);
        assert!(
            row["requests"][0]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t["name"] == "agent.invoke"),
            "{}: delegation was not advertised",
            case.id
        );
        assert_eq!(row["host_permissions"]["delegation"]["allow_self"], true);
        let failure = row["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| {
                e["kind"] == "tool_execution_failed" && e["payload"]["call_id"] == "malformed"
            })
            .unwrap();
        let error = failure["payload"]["error"].as_str().unwrap();
        assert!(
            error.contains("invalid invocation input") && error.contains("invalid type"),
            "{}: {error}",
            case.id
        );
        assert!(!error.contains("delegation not configured"), "{}", case.id);
    }
}

fn export(rows: &[Value]) -> Vec<Value> {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-runner-tool-error-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut output = BufWriter::new(File::create(&path).unwrap());
    for row in rows {
        serde_json::to_writer(&mut output, row).unwrap();
        writeln!(output).unwrap();
    }
    output.flush().unwrap();
    println!("RUNNER_TOOL_ERROR_TRACE={}", path.display());
    let exported: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(exported, rows);
    exported
}

fn compare(case: &Case, row: &Value) {
    let label = &case.id;
    let requests: Vec<ModelRequest> = serde_json::from_value(row["requests"].clone()).unwrap();
    let ledger = row["ledger"].as_array().unwrap();
    assert_eq!(requests.len(), case.expected.requests, "{label}: {row}");
    assert_eq!(
        row["effects"].as_array().unwrap().len(),
        case.expected.effects,
        "{label}"
    );
    assert_eq!(
        row["dispatched_grants"].as_array().unwrap().len(),
        case.expected.grants,
        "{label}"
    );
    assert_eq!(
        ledger
            .iter()
            .filter(|e| e["kind"] == "effect_authorized")
            .count(),
        case.expected.grants,
        "{label}"
    );
    assert_eq!(
        ledger
            .iter()
            .filter(|e| e["kind"] == "effect_receipt")
            .count(),
        case.expected.receipts,
        "{label}"
    );
    assert_eq!(
        row["error"].is_string(),
        case.expected.error,
        "{label}: {row}"
    );
    if let Some(fault) = &case.fault {
        // Runtime wraps an entered uncertain effect in its trusted-recovery
        // error; the complete original adapter cause remains its message.
        let cause = fault.error.error();
        let recorded =
            if matches!(fault.phase, Phase::Execute) && matches!(fault.error, Fatal::Uncertain) {
                kolyan_core::ToolError::Uncertain {
                    message: cause.to_string(),
                }
                .to_string()
            } else {
                cause.to_string()
            };
        assert!(
            ledger
                .iter()
                .any(|e| e["kind"] == "tool_execution_failed" && e["payload"]["error"] == recorded),
            "{label}: original fatal cause was lost"
        );
    }
    if let Some(outcome) = &case.expected.outcome {
        assert!(
            row["outcome"].as_str().unwrap().starts_with(outcome),
            "{label}"
        );
    }
    let feedback: Vec<_> = ledger
        .iter()
        .filter(|e| e["kind"] == "tool_execution_completed" && e["payload"]["is_error"] == true)
        .collect();
    assert_eq!(feedback.len(), case.expected.feedback, "{label}");
    for result in feedback {
        let payload = &result["payload"]["result"];
        let failure = ledger
            .iter()
            .find(|e| {
                e["kind"] == "tool_execution_failed"
                    && e["payload"]["call_id"] == payload["call_id"]
            })
            .unwrap();
        assert_eq!(payload["content"], failure["payload"]["error"], "{label}");
        // A final denied Step at the budget boundary is recorded but has no next request.
        if let Some(next) = requests.iter().find(|q| q.messages.iter().flat_map(|m| &m.content).any(|b| matches!(b, ContentBlock::ToolResult{result} if result.call_id == payload["call_id"]))) {
            let actual = next.messages.iter().flat_map(|m| &m.content).find_map(|b| match b { ContentBlock::ToolResult{result} if result.call_id == payload["call_id"] => Some(result), _ => None }).unwrap();
            assert_eq!(json!(actual), *payload, "{label}");
        } else { assert_eq!(case.expected.outcome.as_deref(), Some("MaxSteps"), "{label}: feedback was lost"); }
        assert!(
            !row["dispatched_grants"]
                .as_array()
                .unwrap()
                .iter()
                .any(|g| g["prepared"]["call"]["id"] == payload["call_id"]),
            "{label}: rejected call received a grant"
        );
    }
    let task = &row["task"];
    assert_eq!(
        task["invocations"].as_object().unwrap().len(),
        1,
        "{label}: denied delegation admitted a child"
    );
    assert_eq!(
        row["instance_facts"].as_array().unwrap().len(),
        1,
        "{label}: denied delegation allocated an instance"
    );
    for invocation in row["dispatched_grants"].as_array().unwrap() {
        let prepared: kolyan_policy::PreparedCall =
            serde_json::from_value(invocation["prepared"].clone()).unwrap();
        let grant: kolyan_policy::PreparedGrant =
            serde_json::from_value(invocation["grant"].clone()).unwrap();
        let scope = serde_json::from_value(invocation["scope"].clone()).unwrap();
        grant
            .validate(
                &prepared,
                invocation["policy_revision"].as_str().unwrap(),
                &scope,
            )
            .unwrap();
        assert_eq!(
            invocation["prepared"]["call"]["name"], "file.read",
            "{label}"
        );
        assert_eq!(
            invocation["prepared"]["call"]["arguments"]["path"], "input.txt",
            "{label}"
        );
        assert_eq!(
            invocation["scope"]["execution"]["execution_id"],
            format!("execution-{label}"),
            "{label}"
        );
    }
    for request in &requests {
        assert!(
            row["context_records"]
                .as_array()
                .unwrap()
                .iter()
                .any(|record| record["kind"] == "prepared"
                    && record["source"] == json!(request)
                    && record["prepared"]["request"] == json!(request)),
            "{label}: context boundary changed the actual feedback input"
        );
        assert!(
            ledger.iter().any(
                |e| e["kind"] == "model_requested" && e["payload"]["request"] == json!(request)
            ),
            "{label}: Core/Provider input differs"
        );
    }
    if case.approval {
        assert_eq!(
            row["saved_suspension"]["checkpoint"]["dispatch"]["on_error"], "ContinueBatch",
            "{label}"
        );
        assert_eq!(row["restored_host_policy"], "FailTurn", "{label}");
        assert_eq!(row["effects_before_resume"], 0, "{label}");
        assert_eq!(row["requests_before_resume"], 1, "{label}");
        assert_eq!(
            row["saved_suspension"]["checkpoint"]["budget"]["max_steps"], case.max_steps,
            "{label}"
        );
        assert_eq!(
            row["saved_suspension"]["checkpoint"]["budget"]["max_tool_calls"], case.max_tool_calls,
            "{label}"
        );
        assert!(row["resume_observation_error"].is_null(), "{label}");
    }
    if case.expected.outcome.as_deref() == Some("FinalAnswer") {
        assert_eq!(task["state"], "Completed", "{label}");
    } else {
        assert_ne!(
            task["state"], "Completed",
            "{label}: failure fabricated Task success"
        );
    }
}
