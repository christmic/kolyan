//! Rebuilt child denials keep exact historical authority but never open factories.

use super::*;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DenialCase {
    id: String,
    revoke: bool,
    factory_refuses: bool,
    mutation: String,
    success: bool,
}

struct Factory {
    inner: provider::Factory,
    refuses: bool,
}
impl crate::ProviderFactory for Factory {
    type Provider = provider::Provider;
    fn build(
        &self,
        snapshot: &crate::AgentSnapshot,
        execution: &kolyan_server::ExecutionRef,
    ) -> Result<crate::provider::ContextPreparingProvider<Self::Provider>, crate::RunnerError> {
        if self.refuses {
            return Err(crate::RunnerError::Host("factory unavailable".into()));
        }
        self.inner.build(snapshot, execution)
    }
}

#[tokio::test]
async fn child_denial_historical_authority_without_executable_factory() {
    let cases: Vec<DenialCase> = serde_json::from_str(include_str!("denial.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-child-deny-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let mut bases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
        let mut base = bases.remove(1);
        base.id = case.id.clone();
        let mut fixture = setup(&base).unwrap();
        if case.mutation == "root_only_cancel" {
            fixture.request.cancellation_policy = kolyan_server::CancellationPolicy::RootOnly;
        }
        let started = fixture.runner.start(fixture.request).await.unwrap();
        let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = started.execution
        else {
            panic!("setup root missing suspension");
        };
        let root = cursor(
            &case.id,
            started.task.attempts["attempt"].binding.clone(),
            &suspension,
        )
        .unwrap();
        let mut children = waiting(
            pump(&fixture.runner, &root, &fixture.template)
                .await
                .unwrap(),
        )
        .unwrap();
        let AgentChildDriveResult::Waiting { child, execution } = children.remove(0) else {
            panic!("setup child missing suspension");
        };
        let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = *execution else {
            panic!("setup checkpoint absent");
        };
        let mut request = ChildApprovalResumeRequest {
            owner: root.owner,
            issued: root.issued,
            wait: root.wait,
            child_invocation_id: child.attempt.invocation_id.clone(),
            checkpoint_id: suspension.checkpoint.checkpoint_id.clone(),
            approval_id: suspension.waiting.approvals[0].approval_id.clone(),
        };
        match case.mutation.as_str() {
            "none" | "root_only_cancel" | "all_cancel" => {}
            "owner" => request.owner.logical_session_id = "foreign".into(),
            "checkpoint" => request.checkpoint_id = "foreign".into(),
            "scope" => request.owner.scope.step_id = "foreign".into(),
            other => panic!("unknown mutation {other}"),
        }
        if matches!(case.mutation.as_str(), "root_only_cancel" | "all_cancel") {
            fixture
                .service
                .cancel(
                    &case.id,
                    &format!("{}/cancel", case.id),
                    "host cancellation",
                )
                .unwrap();
        }
        let mut host = fixture.runner.host.clone();
        if case.revoke {
            host.tools.clear();
            host.delegation = Default::default();
        }
        let restored = Arc::new(
            AgentRunner::new(
                fixture.service.clone(),
                fixture.runner.instances.clone(),
                fixture.runner.bindings.clone(),
                crate::AgentCatalog::new(8).unwrap(),
                host,
                (
                    Factory {
                        inner: fixture.runner.providers.clone(),
                        refuses: case.factory_refuses,
                    },
                    fixture.runner.tools.clone(),
                ),
                fixture.runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        let before = fixture.service.coordinator().snapshot(&case.id).unwrap();
        let result = restored.deny_agent_child_approval(request.clone()).await;
        let after = fixture.service.coordinator().snapshot(&case.id).unwrap();
        let repeat = if result.is_ok() {
            Some(
                restored
                    .deny_agent_child_approval(request.clone())
                    .await
                    .is_ok(),
            )
        } else {
            None
        };
        let mut row = json!({"case_id":case.id,"request":request,"before":before,"after":after,
            "success":result.is_ok(),"terminal":match &result { Ok(AgentChildDriveResult::Terminal{result,..})=>json!(result), _=>Value::Null },
            "error":result.err().map(|e|e.to_string()),"repeat_success":repeat,"child_attempt":child.attempt});
        capture(&mut row, &fixture.observations, &fixture.service, &case.id);
        writeln!(export, "{row}").unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    eprintln!("CHILD_DENY_EVIDENCE={}", path.display());
    let actual: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual.len(), cases.len());
    for (case, row) in cases.iter().zip(actual) {
        assert_eq!(row["success"], case.success, "{row}");
        assert_eq!(row["requests"].as_array().unwrap().len(), 3, "{row}");
        assert!(row["effects"].as_array().unwrap().is_empty(), "{row}");
        if case.success {
            assert_eq!(row["repeat_success"], false, "{row}");
            let attempt = row["child_attempt"]["attempt_id"].as_str().unwrap();
            assert_eq!(
                row["after"]["attempts"][attempt]["state"], "Failed",
                "{row}"
            );
            assert!(!row["terminal"].is_null(), "{row}");
        } else {
            assert_eq!(row["before"], row["after"], "{row}");
        }
    }
}
