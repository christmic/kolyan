//! Detached child approval uses durable admission, never active parent authority.

use super::*;
use kolyan_server::CancellationPolicy;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LifecycleCase {
    id: String,
    base_case: String,
    policy: CancellationPolicy,
    mutation: Mutation,
    entered: bool,
    expected_refused: bool,
    expected_requests: usize,
    expected_effects: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    HostRevoked,
    ForeignCheckpoint,
    ForeignChild,
}

async fn scenario(case: &LifecycleCase, fixture: &Fixture, row: &mut Value) -> Result<(), String> {
    let source = &fixture.request;
    let mut request = crate::RootRunRequest {
        goals: vec![],
        task_id: source.task_id.clone(),
        invocation_id: source.invocation_id.clone(),
        attempt_id: source.attempt_id.clone(),
        execution: source.execution.clone(),
        selector: source.selector.clone(),
        requested_permissions: source.requested_permissions.clone(),
        objective: source.objective.clone(),
        limits: source.limits.clone(),
        cancellation_policy: source.cancellation_policy,
        turn: source.turn.clone(),
    };
    request.cancellation_policy = case.policy;
    let started = fixture
        .runner
        .start(request)
        .await
        .map_err(|error| error.to_string())?;
    let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        return Err("root checkpoint absent".into());
    };
    let root = cursor(
        &case.id,
        started.task.attempts["attempt"].binding.clone(),
        &suspension,
    )?;
    row["root_cursor"] = json!(root);
    let children = fixture
        .runner
        .verify_agent_child_wait(root.owner.clone(), root.issued.clone(), root.wait.clone())
        .await
        .map_err(|error| error.to_string())?;
    let child = children.first().ok_or("no admitted child")?;
    let mut resume = ChildApprovalResumeRequest {
        owner: root.owner.clone(),
        issued: root.issued.clone(),
        wait: root.wait.clone(),
        child_invocation_id: child.attempt.invocation_id.clone(),
        checkpoint_id: "never-entered-checkpoint".into(),
        approval_id: "never-issued-approval".into(),
    };
    if case.entered {
        let driven = waiting(pump(&fixture.runner, &root, &fixture.template).await?)?;
        let AgentChildDriveResult::Waiting { execution, .. } = &driven[0] else {
            return Err("entered child did not wait".into());
        };
        let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = execution.as_ref()
        else {
            return Err("child suspension absent".into());
        };
        resume.checkpoint_id = suspension.checkpoint.checkpoint_id.clone();
        resume.approval_id = suspension
            .waiting
            .approvals
            .first()
            .ok_or("child approval absent")?
            .approval_id
            .clone();
    }
    fixture
        .service
        .cancel(
            &case.id,
            &format!("{}/cancel", case.id),
            "host cancels root",
        )
        .map_err(|error| error.to_string())?;
    let mut host = fixture.runner.host.clone();
    match case.mutation {
        Mutation::None => {}
        Mutation::HostRevoked => host.tools.clear(),
        Mutation::ForeignCheckpoint => resume.checkpoint_id = "foreign-checkpoint".into(),
        Mutation::ForeignChild => resume.child_invocation_id = "foreign-child".into(),
    }
    // Restore two independent hosts, retaining only durable coordinates and a
    // shared host scheduling budget, not the original execution future.
    let restored = restore_with_host(&fixture.runner, host.clone())?;
    row["resume_request"] = json!(resume);
    row["before"] = json!(
        fixture
            .service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["first"] = result(restored.resume_agent_child_approval(resume.clone()).await);
    row["after_first"] = json!(
        fixture
            .service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    let second = restore_with_host(&fixture.runner, host)?;
    row["second"] = result(second.resume_agent_child_approval(resume).await);
    row["after_second"] = json!(
        fixture
            .service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["pump_refusal"] = json!(pump(&restored, &root, &fixture.template).await.err());
    row["new_child_refusal"] = json!(
        restored
            .verify_agent_child_wait(root.owner.clone(), root.issued.clone(), root.wait.clone())
            .await
            .err()
            .map(|error| error.to_string())
    );
    let saved = fixture
        .runner
        .bindings
        .load(&case.id, &root.owner.parent.invocation_id, "session")
        .map_err(|error| error.to_string())?
        .ok_or("parent binding absent")?;
    let policy = fixture
        .runner
        .routed_tool_set(&saved.snapshot, &root.owner.parent.execution)
        .map_err(|error| error.to_string())?
        .policy;
    row["admission_refusal"] = json!(
        restored
            .admit_agent_children(
                root.owner.clone(),
                kolyan_core::ToolInvocation {
                    prepared: root.issued.prepared.clone(),
                    grant: root.issued.grant.clone(),
                    scope: root.owner.scope.clone(),
                    policy_revision: policy.revision(),
                    control: kolyan_core::TurnControl::default(),
                },
                restored
                    .delegation
                    .as_ref()
                    .ok_or("delegation missing")?
                    .limits
                    .clone(),
                policy,
            )
            .await
            .err()
            .map(|error| error.to_string())
    );
    row["after_refusals"] = json!(
        fixture
            .service
            .coordinator()
            .journal()
            .read(&case.id, 0, 1024)
            .map_err(|error| error.to_string())?
    );
    row["task"] = json!(
        fixture
            .service
            .coordinator()
            .snapshot(&case.id)
            .map_err(|error| error.to_string())?
    );
    row["slots"] = json!(restored.execution_budget.slots.available_permits());
    Ok(())
}

fn result(value: Result<AgentChildDriveResult, kolyan_core::ToolError>) -> Value {
    match value {
        Ok(AgentChildDriveResult::Terminal {
            result,
            dispatch_error,
        }) => json!({"terminal":result,"dispatch_error":dispatch_error}),
        Ok(AgentChildDriveResult::Waiting { child, .. }) => json!({"waiting":child}),
        Err(error) => json!({"error":error.to_string()}),
    }
}

#[tokio::test]
async fn detached_root_only_child_recovery_exports_before_comparison() {
    let cases: Vec<LifecycleCase> = serde_json::from_str(include_str!("root_only.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-root-only-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    println!("AGENT_ROOT_ONLY_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let mut row = json!({"id":case.id,"policy":case.policy});
        let sources: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
        let mut source = sources
            .into_iter()
            .find(|source| source.id == case.base_case)
            .unwrap();
        source.id = case.id.clone();
        match setup(&source) {
            Ok(fixture) => {
                if let Err(error) = Box::pin(scenario(case, &fixture, &mut row)).await {
                    row["error"] = json!(error);
                }
                capture(&mut row, &fixture.observations, &fixture.service, &case.id);
            }
            Err(error) => row["error"] = json!(error),
        }
        writeln!(output, "{row}").unwrap();
        output.sync_all().unwrap();
    }
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert!(row["error"].is_null(), "{}: {row}", case.id);
        assert_eq!(
            row["first"]["error"].is_string(),
            case.expected_refused,
            "{}: {row}",
            case.id
        );
        assert!(row["second"]["error"].is_string(), "{row}");
        assert_eq!(row["after_first"], row["after_second"]);
        if case.expected_refused {
            assert_eq!(row["before"], row["after_first"]);
        } else {
            assert_eq!(row["first"]["terminal"]["outcome"]["status"], "completed");
        }
        assert_eq!(row["task"]["state"], "Cancelled");
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            case.expected_requests
        );
        assert_eq!(
            row["effects"].as_array().unwrap().len(),
            case.expected_effects
        );
        assert_eq!(row["slots"], 2);
        assert!(row["pump_refusal"].is_string() && row["new_child_refusal"].is_string());
        assert!(row["admission_refusal"].is_string());
        assert_eq!(row["after_second"], row["after_refusals"]);
        assert!(
            row["facts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|fact| !matches!(
                    fact["draft"]["kind"].as_str(),
                    Some("task.result_consumed" | "task.terminal_result_consumed")
                ))
        );
    }
}
