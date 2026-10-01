use super::*;
use serde_json::json;

#[test]
fn delegation_permission_is_independent_of_process_execution() {
    let call = ToolCall {
        id: "delegate".into(),
        name: "agent.invoke".into(),
        arguments: json!({}),
    };
    let prepared = PreparedCall::new(
        call,
        "delegation-v1".into(),
        InvocationClaim {
            tool_name: "agent.invoke".into(),
            capabilities: [Capability::AgentDelegate].into(),
            effects: [Effect::Delegate].into(),
            resource: ResourceClaim { path: None },
            idempotency: Idempotency::NonIdempotent,
        },
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 4096,
            timeout_ms: 1000,
        },
    )
    .unwrap();
    let mut engine = PolicyEngine::default();
    let mut manifest = ToolManifest::new("agent.invoke");
    manifest.capabilities.insert(Capability::ProcessExecute);
    manifest.effects.insert(Effect::Execute);
    manifest.approval = ApprovalMode::Never;
    engine.register(manifest.clone());
    assert_eq!(
        engine
            .decide_prepared(&prepared, &PolicyContext::default())
            .kind,
        PolicyDecisionKind::Deny
    );
    manifest.capabilities = [Capability::AgentDelegate].into();
    manifest.effects = [Effect::Delegate].into();
    engine.register(manifest);
    assert_eq!(
        engine
            .decide_prepared(&prepared, &PolicyContext::default())
            .kind,
        PolicyDecisionKind::Allow
    );
    let mut second = prepared.call().clone();
    second.id = "delegate-other".into();
    let second = PreparedCall::new(
        second,
        "delegation-v1".into(),
        prepared.claim().clone(),
        prepared.requirements().clone(),
    )
    .unwrap();
    let plan = engine
        .resolve_prepared_batch(&PolicyContext::default(), &[prepared, second])
        .unwrap();
    assert_eq!(plan.stages, vec![vec!["delegate"], vec!["delegate-other"]]);
}

fn write_manifest() -> ToolManifest {
    ToolManifest {
        tool_name: "file.write".into(),
        capabilities: [Capability::FilesystemWrite].into_iter().collect(),
        effects: [Effect::Update].into_iter().collect(),
        path_scopes: vec![PathScope::new("/workspace/src")],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    }
}
fn call(path: &str) -> ToolCall {
    ToolCall {
        id: "call-1".into(),
        name: "file.write".into(),
        arguments: json!({"path": path, "content": "ok"}),
    }
}

#[test]
fn static_manifest_and_dynamic_workspace_scope_are_intersected() {
    let mut engine = PolicyEngine::default();
    engine.register(write_manifest());
    engine.restrict_workspace("/workspace");
    assert_eq!(
        engine.decide(&call("/workspace/src/main.rs")).kind,
        PolicyDecisionKind::Allow
    );
    assert_eq!(
        engine.decide(&call("/workspace/docs/readme.md")).kind,
        PolicyDecisionKind::Deny
    );
}

#[test]
fn unknown_or_denied_tools_fail_closed() {
    let mut engine = PolicyEngine::default();
    let unknown = ToolCall {
        id: "x".into(),
        name: "shell.exec".into(),
        arguments: json!({}),
    };
    assert_eq!(engine.decide(&unknown).kind, PolicyDecisionKind::Deny);
    engine.register(write_manifest());
    engine.deny_tool("file.write");
    assert_eq!(
        engine.decide(&call("/workspace/src/main.rs")).kind,
        PolicyDecisionKind::Deny
    );
}

#[test]
fn approval_is_not_an_allow() {
    let mut manifest = write_manifest();
    manifest.approval = ApprovalMode::Always;
    let mut engine = PolicyEngine::default();
    engine.register(manifest);
    let call = call("/workspace/src/main.rs");
    let decision = engine.decide(&call);
    assert_eq!(decision.kind, PolicyDecisionKind::RequireApproval);
    assert!(decision.into_grant(&call).is_err());
}

#[test]
fn batch_policy_parallelizes_independent_writes() {
    let mut engine = PolicyEngine::default();
    engine.register(write_manifest());
    let calls = vec![
        call("/workspace/src/a.rs"),
        ToolCall {
            id: "call-2".into(),
            ..call("/workspace/src/b.rs")
        },
    ];
    let plan = engine.resolve_batch(&PolicyContext::default(), &calls);
    assert_eq!(plan.stage_count(), 1);
    assert_eq!(plan.stages[0], vec!["call-1", "call-2"]);
}

#[test]
fn batch_policy_serializes_conflicting_read_after_write() {
    let mut engine = PolicyEngine::default();
    engine.register(write_manifest());
    let mut read = ToolManifest::new("file.read");
    read.capabilities.insert(Capability::FilesystemRead);
    read.effects.insert(Effect::Read);
    engine.register(read);
    let calls = vec![
        call("/workspace/src/a.rs"),
        ToolCall {
            id: "call-2".into(),
            name: "file.read".into(),
            arguments: serde_json::json!({"path":"/workspace/src/a.rs"}),
        },
    ];
    let plan = engine.resolve_batch(&PolicyContext::default(), &calls);
    assert_eq!(plan.stage_count(), 2);
    assert_eq!(plan.stages[1], vec!["call-2"]);
}

#[test]
fn context_budget_denies_the_batch_without_widening_policy() {
    let mut engine = PolicyEngine::default();
    engine.register(write_manifest());
    let context = PolicyContext {
        remaining_tool_calls: Some(0),
        ..PolicyContext::default()
    };
    let plan = engine.resolve_batch(&context, &[call("/workspace/src/a.rs")]);
    assert!(plan.is_empty());
    assert_eq!(plan.decisions[0].decision.kind, PolicyDecisionKind::Deny);
}

#[test]
fn approved_calls_keep_resource_dependencies_and_parallelism() {
    let mut engine = PolicyEngine::default();
    let mut manifest = write_manifest();
    manifest.approval = ApprovalMode::Always;
    engine.register(manifest);
    let calls = vec![
        call("/workspace/src/a.rs"),
        ToolCall {
            id: "b".into(),
            ..call("/workspace/src/b.rs")
        },
        ToolCall {
            id: "a-again".into(),
            ..call("/workspace/src/a.rs")
        },
    ];
    let plan = engine.resolve_batch(&PolicyContext::default(), &calls);
    assert!(plan.stages.is_empty());
    let approved = calls.iter().map(|call| call.id.clone()).collect::<Vec<_>>();
    assert_eq!(
        plan.stages_with_approvals(&approved),
        vec![vec!["call-1", "b"], vec!["a-again"]]
    );
    assert_eq!(plan.stages_with_approvals(&["b".into()]), vec![vec!["b"]]);
}

#[test]
fn denied_calls_cannot_be_scheduled_by_an_approval_id() {
    let engine = PolicyEngine::default();
    let plan = engine.resolve_batch(&PolicyContext::default(), &[call("/workspace/src/a.rs")]);
    assert!(plan.stages_with_approvals(&["call-1".into()]).is_empty());
}
