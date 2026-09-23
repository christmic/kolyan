use super::common::*;
use super::turn_resume_support::*;
use kolyan_core::*;
use kolyan_model::*;
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Deserialize)]
struct LiveCase {
    name: String,
    fixture: String,
    approval_tools: Vec<String>,
    dispatch: ToolDispatchPolicy,
    cancel_when_suspended: bool,
    expected: LiveExpected,
}

#[derive(Deserialize)]
struct LiveExpected {
    outcome: String,
    min_steps: usize,
    min_approvals: usize,
    min_multi_approval_batches: usize,
    files: BTreeMap<String, String>,
    trace: String,
}

#[tokio::test]
#[ignore = "real-network matrix: requires all configured provider keys"]
async fn resumed_turns_and_external_cancellation_across_all_models() {
    let config = load_config();
    let cases: Vec<LiveCase> =
        serde_json::from_str(include_str!("../fixtures/turn_resume_live.json")).unwrap();
    let mut rows = 0;
    for (family, cfg) in [
        ("minimax", &config.minimax_openai),
        ("qwen", &config.qwen_openai),
    ] {
        let provider = Arc::new(build_openai_provider(cfg, &require_api_key(cfg)));
        for entry in &cfg.model_matrix {
            for case in &cases {
                run(provider.clone(), family, "openai", entry, case).await;
                rows += 1;
            }
        }
    }
    for (family, cfg) in [
        ("minimax", &config.minimax_anthropic),
        ("qwen", &config.qwen_anthropic),
    ] {
        let provider = Arc::new(build_anthropic_provider(
            cfg,
            &require_api_key_anthropic(cfg),
        ));
        for entry in &cfg.model_matrix {
            for case in &cases {
                run(provider.clone(), family, "anthropic", entry, case).await;
                rows += 1;
            }
        }
    }
    eprintln!("PASS live Turn resume matrix: {rows} case/model/protocol rows, no skips");
}

async fn run<P: ModelProvider + 'static>(
    provider: Arc<P>,
    family: &str,
    protocol: &str,
    entry: &ModelMatrixEntry,
    case: &LiveCase,
) {
    let label = format!("{family}/{protocol}/{}/{}", entry.model, case.name);
    let fixture: Fixture = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(&case.fixture),
        )
        .unwrap(),
    )
    .unwrap();
    let records = Records::default();
    let gate = Arc::new(MemoryGate::new(records.clone(), None, None));
    let root_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "kolyan-resume-live-{}-{root_id}",
        std::process::id()
    ));
    std::fs::create_dir_all(root.join("safe")).unwrap();
    let policy = policy(&case.approval_tools, &[]);
    let build = || {
        TurnExecutor::with_tools(
            RecordingProvider {
                inner: provider.clone(),
                records: records.clone(),
            },
            RecordingTool {
                inner: PolicyEnforcingTool::new(RestrictedFileTool::new(&root), policy.clone()),
                records: records.clone(),
            },
        )
        .with_policy_engine(policy.clone())
        .with_boundary_control(gate.clone())
    };
    let request = TurnRequest {
        turn_id: format!("live-{protocol}-{}-{}", entry.model, case.name),
        model_request: build_request(
            family,
            &entry.model,
            &fixture,
            case.name.clone(),
            entry.max_output_tokens,
        ),
        config: TurnConfig {
            max_steps: fixture
                .turn
                .as_ref()
                .and_then(|turn| turn.max_steps)
                .unwrap(),
            ..Default::default()
        },
    };
    let initial = build().with_tool_dispatch_policy(case.dispatch);
    let mut result = initial.start_resumable(request).await;
    drop(initial);
    let mut approvals = 0;
    let mut multiple = std::collections::BTreeSet::new();
    let mut previous_batch: Option<(String, usize)> = None;
    while let Ok(ResumableTurn::AwaitingApproval(approval)) = result {
        approvals += 1;
        assert!(approvals < 30, "[{label}] unbounded approval sequence");
        let step_id = approval.continuation.steps.last().unwrap().step_id.clone();
        if !approval.continuation.approved_call_ids.is_empty() {
            multiple.insert(step_id.clone());
        }
        if let Some((previous, effects)) = &previous_batch
            && *previous == step_id
        {
            assert_eq!(
                records.count("tool_start"),
                *effects,
                "[{label}] effects before all approvals"
            );
        }
        previous_batch = Some((step_id, records.count("tool_start")));
        records.push(json!({"event":"suspended","approval":approval}));
        // Only the caller carries serialized data into the reconstructed executor.
        let encoded = serde_json::to_vec(&approval).unwrap();
        drop(approval);
        let restored: ApprovalRequest = serde_json::from_slice(&encoded).unwrap();
        records.push(json!({"event":"executor_reconstructed"}));
        if case.cancel_when_suspended {
            assert!(gate.cancel());
        }
        let id = restored.approval_id.clone();
        result = build().resume_approval(restored, &id).await;
    }
    let (outcome, steps) = match result {
        Ok(ResumableTurn::Completed(execution)) => (
            format!("{:?}", execution.result.end_reason),
            execution.result.steps.len(),
        ),
        Err(error) => {
            records.push(json!({"event":"error","message":error.to_string()}));
            (
                format!("{:?}", error.end_reason()),
                records.count("model_request"),
            )
        }
        _ => unreachable!(),
    };
    records.push(json!({"event":"outcome","reason":outcome}));
    let trace = records.write(&label);
    assert_eq!(
        outcome,
        case.expected.outcome,
        "[{label}] {}",
        trace.display()
    );
    assert!(steps >= case.expected.min_steps, "[{label}] steps={steps}");
    assert!(
        approvals >= case.expected.min_approvals,
        "[{label}] approvals={approvals}"
    );
    assert!(
        multiple.len() >= case.expected.min_multi_approval_batches,
        "[{label}] no multi-approval batch: {}",
        trace.display()
    );
    for (path, content) in &case.expected.files {
        assert_eq!(
            std::fs::read_to_string(root.join(path)).unwrap(),
            *content,
            "[{label}] {path}"
        );
    }
    if case.cancel_when_suspended {
        assert_eq!(
            records.count("tool_start"),
            0,
            "[{label}] cancelled approval executed a tool"
        );
        assert_eq!(std::fs::read_dir(root.join("safe")).unwrap().count(), 0);
    }
    let expected = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/expected/turn")
            .join(&case.expected.trace),
    )
    .unwrap();
    compare(&trace, &expected, &label);
    eprintln!(
        "PASS live {label}: {steps} steps, {approvals} approvals, {} multi-approval batches",
        multiple.len()
    );
}
