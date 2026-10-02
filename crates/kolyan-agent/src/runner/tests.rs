//! Root tests use the actual journal, Task service, Session store and Runtime.
//! Only the network Provider and external-effect adapter are test implementations.

mod error_feedback;
mod goals;
mod resume;
mod skills;
pub(super) mod support;

use std::sync::Arc;

use kolyan_core::{ToolError, ToolExecutor};
use kolyan_model::{ToolCall, ToolChoice};
use kolyan_server::{InvocationState, TaskState};
use serde_json::json;

use super::*;
use support::*;

#[tokio::test]
async fn named_and_inline_roots_execute_and_commit_verified_completion() {
    for named in [true, false] {
        let harness = Harness::new();
        let input = harness.request("one", named);
        let result = harness.runner.start(input).await.unwrap();
        assert_eq!(result.task.state, TaskState::Completed);
        assert_eq!(result.task.success_evidence.len(), 1);
        assert_eq!(result.task.usage.input_tokens, 1);
        assert_eq!(result.task.usage.output_tokens, 2);
        assert_eq!(result.task.usage.unreported_steps, 0);
        assert_eq!(
            result.task.invocations["root"].state,
            InvocationState::Completed
        );
        assert!(matches!(result.execution, DurableTurnResult::Completed(..)));
        let requests = harness.observations.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].model, *result.snapshot.definition().model());
        assert_eq!(requests[0].system[0].text, "Host instruction");
        assert_eq!(requests[0].system[1].text, "Agent instruction");
        assert_eq!(requests[0].tools.len(), 1);
        assert_eq!(requests[0].tools[0].name, "file.read");
        assert_eq!(harness.observations.records.lock().unwrap().len(), 1);
        assert_eq!(
            harness
                .bindings
                .load("one", "root", "session")
                .unwrap()
                .unwrap()
                .snapshot,
            result.snapshot
        );
        let attempt = &result.task.attempts["attempt"];
        assert_eq!(attempt.binding.constraints_digest, result.snapshot.digest());
        let session = harness
            .service
            .sessions()
            .sessions()
            .load("session")
            .unwrap();
        assert_eq!(session.turns.len(), 1);
        assert_eq!(session.messages.len(), 2);
    }
}

#[tokio::test]
async fn second_root_turn_projects_session_history_once() {
    let harness = Harness::new();
    let first = harness
        .runner
        .start(harness.request("one", true))
        .await
        .unwrap();
    let second = harness
        .runner
        .start(harness.request("two", false))
        .await
        .unwrap();
    assert_ne!(
        first.snapshot.identity().instance_id,
        second.snapshot.identity().instance_id
    );
    let requests = harness.observations.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].messages.len(), 3);
    assert_eq!(requests[1].messages[0], requests[0].messages[0]);
    assert_eq!(requests[1].max_output_tokens, Some(50));
}

#[tokio::test]
async fn exact_admission_retry_does_not_replay_model_or_replace_instance() {
    let harness = Harness::new();
    let first = harness
        .runner
        .start(harness.request("one", true))
        .await
        .unwrap();
    assert!(
        harness
            .runner
            .start(harness.request("one", true))
            .await
            .is_err()
    );
    assert_eq!(harness.observations.requests.lock().unwrap().len(), 1);
    assert_eq!(
        harness
            .bindings
            .load("one", "root", "session")
            .unwrap()
            .unwrap()
            .snapshot,
        first.snapshot
    );
}

#[tokio::test]
async fn input_authority_expansion_and_foreign_inventory_fail_before_model() {
    let harness = Harness::new();
    let mut input = harness.request("expanded", true);
    input
        .requested_permissions
        .tools
        .insert(crate::EnvironmentTool::Shell);
    assert!(matches!(
        harness.runner.start(input).await,
        Err(RunnerError::Agent(crate::AgentError::PermissionDenied))
    ));
    let mut input = harness.request("schemas", true);
    input.turn.model_request.tools = definitions();
    assert!(matches!(
        harness.runner.start(input).await,
        Err(RunnerError::Host(_))
    ));
    let mut input = harness.request("choice", true);
    input.turn.model_request.tool_choice = ToolChoice::Tool("shell".into());
    assert!(matches!(
        harness.runner.start(input).await,
        Err(RunnerError::Host(_))
    ));
    assert!(harness.observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_factory_failure_and_context_refusal_are_not_success() {
    let mut harness = Harness::new();
    Arc::get_mut(&mut harness.runner).unwrap().providers.fail = true;
    assert!(matches!(
        harness.runner.start(harness.request("factory", true)).await,
        Err(RunnerError::Host(_))
    ));
    assert!(
        harness
            .bindings
            .load("factory", "root", "session")
            .unwrap()
            .is_none()
    );
    Arc::get_mut(&mut harness.runner).unwrap().providers.fail = false;
    Arc::get_mut(&mut harness.runner)
        .unwrap()
        .providers
        .reject_context = true;
    let outcome = harness.runner.start(harness.request("context", true)).await;
    assert!(outcome.is_err());
    assert!(harness.observations.requests.lock().unwrap().is_empty());
    let task = harness.service.coordinator().snapshot("context").unwrap();
    assert_ne!(task.state, TaskState::Completed);
    assert!(task.success_evidence.is_empty());
    assert_eq!(harness.observations.records.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn omitted_output_limit_and_wrong_turn_fail_before_fact_or_model() {
    let harness = Harness::new();
    let mut input = harness.request("no-limit", false);
    input.turn.model_request.max_output_tokens = None;
    assert!(harness.runner.start(input).await.is_err());
    let mut input = harness.request("wrong-turn", false);
    input.execution.turn_id = "foreign".into();
    assert!(harness.runner.start(input).await.is_err());
    assert!(harness.observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn snapshot_execution_gate_blocks_unadvertised_tools_at_preparation() {
    let harness = Harness::new();
    let result = harness
        .runner
        .start(harness.request("one", true))
        .await
        .unwrap();
    let tools = super::tools::SnapshotTools {
        inner: kolyan_core::NoopToolExecutor,
        snapshot: result.snapshot,
        execution: execution("one"),
        policy: Arc::new(kolyan_policy::PolicyEngine::default()),
    };
    let failure = tools
        .prepare(ToolCall {
            id: "denied".into(),
            name: "shell".into(),
            arguments: json!({"command":"true"}),
        })
        .await
        .unwrap_err();
    assert!(matches!(failure, ToolError::PolicyDenied { .. }));
}

#[tokio::test]
async fn root_tool_loop_binds_snapshot_scope_and_returns_result_to_actual_next_request() {
    let mut harness = Harness::new();
    Arc::get_mut(&mut harness.runner)
        .unwrap()
        .providers
        .call_tool = true;
    let mut input = harness.request("tools", true);
    input.limits.max_steps_per_turn = 3;
    input.turn.config.max_steps = 3;
    input.limits.max_tokens = None;
    input.turn.model_request.max_output_tokens = Some(50);
    let result = harness.runner.start(input).await.unwrap();
    assert_eq!(result.task.state, TaskState::Completed);
    let effects = harness.observations.effects.lock().unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(
        effects[0].agent_snapshot_digest.as_deref(),
        Some(result.snapshot.digest())
    );
    assert_eq!(effects[0].execution.session_id, "session");
    assert_eq!(effects[0].execution.turn_id, "turn-tools");
    assert_eq!(effects[0].execution.execution_id, "execution-tools");
    let requests = harness.observations.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.iter().flat_map(|message| &message.content).any(|block| matches!(block, kolyan_model::ContentBlock::ToolResult { result } if result.call_id == "read-call" && result.content == "verified unit read" && !result.is_error)));
    assert_eq!(harness.observations.records.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn valid_grant_for_foreign_scope_cannot_cross_the_host_execution_gate() {
    let harness = Harness::new();
    let result = harness
        .runner
        .start(harness.request("scopes", true))
        .await
        .unwrap();
    let host_execution = execution("scopes");
    let set = harness
        .runner
        .tools
        .build(&result.snapshot, &host_execution)
        .unwrap();
    let gate = super::tools::SnapshotTools {
        inner: set.executor,
        snapshot: result.snapshot.clone(),
        execution: host_execution.clone(),
        policy: set.policy.clone(),
    };
    let prepared = gate
        .prepare(ToolCall {
            id: "read".into(),
            name: "file.read".into(),
            arguments: json!({"path":"input.txt"}),
        })
        .await
        .unwrap();
    let base = kolyan_policy::ToolExecutionScope {
        execution: serde_json::from_value(serde_json::to_value(&host_execution).unwrap()).unwrap(),
        step_id: "step-0".into(),
        agent_snapshot_digest: Some(result.snapshot.digest().into()),
    };
    for field in ["session", "turn", "execution", "snapshot"] {
        let mut foreign = base.clone();
        match field {
            "session" => foreign.execution.session_id = "foreign-session".into(),
            "turn" => foreign.execution.turn_id = "foreign-turn".into(),
            "execution" => foreign.execution.execution_id = "foreign-execution".into(),
            "snapshot" => foreign.agent_snapshot_digest = Some("a".repeat(64)),
            _ => unreachable!(),
        }
        let grant = kolyan_policy::PreparedGrant::issue(
            &prepared,
            set.policy.decide_prepared(&prepared, &Default::default()),
            kolyan_policy::ApprovalEvidence::NotConfirmed,
            foreign.clone(),
        )
        .unwrap();
        let failure = gate
            .execute_invocation(kolyan_core::ToolInvocation {
                prepared: prepared.clone(),
                grant,
                scope: foreign,
                policy_revision: set.policy.revision(),
                control: kolyan_core::TurnControl::default(),
                window: kolyan_core::ToolExecutionWindow::at_deadline(
                    std::time::Instant::now() + std::time::Duration::from_secs(30),
                ),
            })
            .await
            .unwrap_err();
        assert!(matches!(failure, ToolError::PolicyDenied { .. }), "{field}");
        assert!(
            harness.observations.effects.lock().unwrap().is_empty(),
            "{field}"
        );
    }
    let grant = kolyan_policy::PreparedGrant::issue(
        &prepared,
        set.policy.decide_prepared(&prepared, &Default::default()),
        kolyan_policy::ApprovalEvidence::NotConfirmed,
        base.clone(),
    )
    .unwrap();
    let success = gate
        .execute_invocation(kolyan_core::ToolInvocation {
            prepared,
            grant,
            scope: base,
            policy_revision: set.policy.revision(),
            control: kolyan_core::TurnControl::default(),
            window: kolyan_core::ToolExecutionWindow::at_deadline(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
            ),
        })
        .await
        .unwrap();
    assert!(matches!(success, kolyan_core::ToolOutcome::Completed(_)));
    assert_eq!(harness.observations.effects.lock().unwrap().len(), 1);
}
