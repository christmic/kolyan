//! Current authority and scheduling regression matrix using actual durable ports.

use super::*;
use std::sync::atomic::Ordering;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryCase {
    id: String,
    base_case: String,
    action: Action,
    slots: usize,
    delay_ms: u64,
    max_tokens: Option<u64>,
    expected_requests: usize,
    expected_effects: usize,
    expected_peak: usize,
    expected_denied: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    RevokeRoot,
    RevokeNested,
    BoundOne,
    SimultaneousApprovals,
}

async fn scenario(case: &RecoveryCase, fixture: Fixture, row: &mut Value) -> Result<(), String> {
    if matches!(case.action, Action::BoundOne) {
        let sources: Vec<Case> =
            serde_json::from_str(include_str!("cases.json")).map_err(|error| error.to_string())?;
        let mut source = sources
            .into_iter()
            .find(|source| source.id == case.base_case)
            .ok_or("missing base case")?;
        source.id = case.id.clone();
        return Box::pin(execute(&source, row, fixture)).await;
    }
    let Fixture {
        _root,
        runner,
        service,
        request,
        template,
        ..
    } = fixture;
    let started = runner
        .start(request)
        .await
        .map_err(|error| error.to_string())?;
    let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = started.execution else {
        return Err("root did not suspend".into());
    };
    let root = cursor(
        &case.id,
        started.task.attempts["attempt"].binding.clone(),
        &suspension,
    )?;
    row["root_cursor"] = json!(root);
    let children = waiting(pump(&runner, &root, &template).await?)?;
    match case.action {
        Action::RevokeRoot | Action::RevokeNested => {
            let AgentChildDriveResult::Waiting { child, execution } = &children[0] else {
                return Err("middle did not suspend".into());
            };
            let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } =
                execution.as_ref()
            else {
                return Err("middle checkpoint absent".into());
            };
            let middle = cursor(&case.id, child.attempt.clone(), suspension)?;
            if matches!(case.action, Action::RevokeRoot) {
                let AgentChildrenPumpResult::Resumed(_) = pump(&runner, &middle, &template).await?
                else {
                    return Err("middle did not finish".into());
                };
            } else {
                let driven = runner
                    .drive_agent_children(
                        middle.owner.clone(),
                        middle.issued.clone(),
                        middle.wait.clone(),
                        template.clone(),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                if !driven
                    .iter()
                    .all(|result| matches!(result, AgentChildDriveResult::Terminal { .. }))
                {
                    return Err("leaf did not finish".into());
                }
            }
            let before = service
                .coordinator()
                .journal()
                .read(&case.id, 0, 1024)
                .map_err(|error| error.to_string())?;
            let mut host = runner.host.clone();
            host.tools.clear();
            let rebuilt = restore_with_host(&runner, host)?;
            let target = if matches!(case.action, Action::RevokeRoot) {
                &root
            } else {
                &middle
            };
            row["refusal"] = json!(pump(&rebuilt, target, &template).await.err());
            row["before_refusal_facts"] = json!(before);
            row["after_refusal_facts"] = json!(
                service
                    .coordinator()
                    .journal()
                    .read(&case.id, 0, 1024)
                    .map_err(|error| error.to_string())?
            );
        }
        Action::SimultaneousApprovals => {
            let mut requests = Vec::new();
            for result in children {
                let AgentChildDriveResult::Waiting { child, execution } = result else {
                    return Err("child did not wait".into());
                };
                let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = *execution
                else {
                    return Err("child checkpoint absent".into());
                };
                requests.push(ChildApprovalResumeRequest {
                    owner: root.owner.clone(),
                    issued: root.issued.clone(),
                    wait: root.wait.clone(),
                    child_invocation_id: child.attempt.invocation_id,
                    checkpoint_id: suspension.checkpoint.checkpoint_id.clone(),
                    approval_id: suspension.waiting.approvals[0].approval_id.clone(),
                });
            }
            let rebuilt = restore(&runner)?;
            let mut jobs = tokio::task::JoinSet::new();
            for (index, request) in requests.into_iter().enumerate() {
                let runner = if index % 2 == 0 {
                    runner.clone()
                } else {
                    rebuilt.clone()
                };
                jobs.spawn(async move { runner.resume_agent_child_approval(request).await });
            }
            let mut outcomes = Vec::new();
            let mut errors = Vec::new();
            while let Some(joined) = jobs.join_next().await {
                match joined {
                    Ok(Ok(AgentChildDriveResult::Terminal {
                        result,
                        dispatch_error,
                    })) => outcomes.push(json!({"result":result,"dispatch_error":dispatch_error})),
                    Ok(Ok(_)) => errors.push("approved child unexpectedly waiting".into()),
                    Ok(Err(error)) => errors.push(error.to_string()),
                    Err(error) => errors.push(error.to_string()),
                }
            }
            row["approval_results"] = json!(outcomes);
            row["approval_errors"] = json!(errors);
            if !errors.is_empty() {
                return Err(errors.join("; "));
            }
            let AgentChildrenPumpResult::Resumed(result) = pump(&rebuilt, &root, &template).await?
            else {
                return Err("root still waiting".into());
            };
            row["final_task"] = json!(result.task);
        }
        Action::BoundOne => unreachable!(),
    }
    Ok(())
}

#[tokio::test]
async fn recovery_authority_and_all_execution_paths_export_before_assertions() {
    use std::io::Write;

    let cases: Vec<RecoveryCase> = serde_json::from_str(include_str!("recovery.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-recovery-admission-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut output = std::fs::File::create(&path).unwrap();
    let mut progress = std::fs::File::create(directory.join("progress.jsonl")).unwrap();
    println!("AGENT_RECOVERY_ADMISSION_TRACE={}", path.display());
    for case in &cases {
        // A process abort cannot be captured as a returned scenario error.
        // Preserve the last started case separately, without inventing results.
        writeln!(progress, "{}", json!({"id":case.id,"state":"started"})).unwrap();
        progress.sync_all().unwrap();
        let sources: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
        let mut source = sources
            .into_iter()
            .find(|source| source.id == case.base_case)
            .unwrap();
        source.id = case.id.clone();
        source.slots = Some(case.slots);
        source.delay_ms = case.delay_ms;
        source.max_tokens = case.max_tokens;
        let fixture = setup(&source).unwrap();
        let _root = fixture._root.clone();
        let observations = fixture.observations.clone();
        let service = fixture.service.clone();
        let budget = fixture.runner.execution_budget.clone();
        let concurrency = fixture.runner.providers.concurrency.clone();
        let mut row = json!({"id":case.id,"phases":[]});
        if let Err(error) = Box::pin(scenario(case, fixture, &mut row)).await {
            row["error"] = json!(error);
        }
        capture(&mut row, &observations, &service, &case.id);
        row["peak"] = json!(concurrency.peak.load(Ordering::SeqCst));
        row["active"] = json!(concurrency.active.load(Ordering::SeqCst));
        row["available_slots"] = json!(budget.slots.available_permits());
        writeln!(output, "{}", serde_json::to_string(&row).unwrap()).unwrap();
        output.sync_all().unwrap();
        writeln!(progress, "{}", json!({"id":case.id,"state":"captured"})).unwrap();
        progress.sync_all().unwrap();
    }
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert!(row["error"].is_null(), "{}: {row}", case.id);
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            case.expected_requests,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["effects"].as_array().unwrap().len(),
            case.expected_effects,
            "{}: {row}",
            case.id
        );
        assert_eq!(row["peak"], case.expected_peak, "{}: {row}", case.id);
        assert_eq!(row["active"], 0);
        assert_eq!(row["available_slots"], case.slots);
        assert_eq!(
            row["refusal"].is_string(),
            case.expected_denied,
            "{}: {row}",
            case.id
        );
        if case.expected_denied {
            assert_eq!(row["before_refusal_facts"], row["after_refusal_facts"]);
        } else {
            assert_eq!(row["final_task"]["state"], "Completed");
        }
    }
}
