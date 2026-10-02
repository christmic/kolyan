//! No-network inventory and fixture/actual source separation checks.

use std::collections::BTreeSet;

use super::Dataset;

use futures_util::{StreamExt, stream};
use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentSelector,
    EnvironmentTool, ProviderFactory,
};
use kolyan_core::{ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::{
    ModelEventStream, ModelProvider, ModelRef, ModelRequest, ProviderFuture, ToolCall,
};
use kolyan_policy::{ApprovalEvidence, PolicyContext, PreparedGrant, ToolExecutionScope};
use kolyan_server::ExecutionRef;
use serde_json::json;
use std::sync::{Arc, Mutex};

use super::super::{data, evidence::Evidence, harness};

#[path = "../../common/tool_preparation.rs"]
#[allow(dead_code)] // Shared trusted fixture declarations; no native effects in unit selectors.
mod tool_preparation;
use super::{
    host::Providers,
    ports::{Executor, State},
};

struct Probe(Mutex<Vec<ModelRequest>>);
impl ModelProvider for Probe {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.0.lock().unwrap().push(request);
        Box::pin(async {
            Ok(Box::pin(stream::iter([
                Ok(kolyan_model::ModelEvent::Started),
                Ok(kolyan_model::ModelEvent::ReasoningDelta(
                    "actual-port-marker".into(),
                )),
                Err(kolyan_model::ProviderError::new(
                    kolyan_model::ProviderErrorKind::Transport,
                    kolyan_model::ProviderErrorPhase::Stream,
                    "unit transport marker",
                )),
            ])) as ModelEventStream)
        })
    }
}

#[tokio::test]
async fn actual_provider_builder_does_not_read_script_or_open_provider_early() {
    let mut dataset: Dataset =
        serde_json::from_str(include_str!("../../fixtures/agent/native_effects.json")).unwrap();
    dataset.frames.clear();
    dataset.parent_frames.clear();
    let dir = tempfile::tempdir().unwrap();
    let evidence = Arc::new(Evidence::new(&dir.path().join("actual.jsonl")));
    let state = State::new(
        "none".into(),
        dataset.write_arguments.clone(),
        evidence,
        dataset.gate_timeout_ms,
    );
    let model = ModelRef::new("fixture", "actual-port-probe");
    let permissions = AgentPermissions {
        tools: [EnvironmentTool::Write].into(),
        ..Default::default()
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "native-probe".into(),
        revision: "r1".into(),
        display_name: None,
        model: model.clone(),
        instructions: dataset.input.clone(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let mut catalog = AgentCatalog::new(1).unwrap();
    catalog.register(definition.clone()).unwrap();
    let snapshot = catalog
        .resolve(
            &AgentSelector::Named(definition.key()),
            "probe-instance",
            &permissions,
            &permissions,
        )
        .unwrap();
    let probe = Arc::new(Probe(Mutex::new(Vec::new())));
    let providers = Providers {
        live: Some(probe.clone()),
        dataset,
        state: state.clone(),
        ledger: kolyan_ledger::SqliteLedger::open(dir.path().join("ledger.sqlite")).unwrap(),
    };
    let provider = providers.build(&snapshot, &execution()).unwrap();
    assert!(probe.0.lock().unwrap().is_empty());
    assert_eq!(state.effects(), 0);
    let request = harness::request(&data::dataset().turns[0], &model, 8192);
    let events = provider
        .stream(request.clone())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(events.len(), 3);
    assert!(matches!(&events[0], Ok(kolyan_model::ModelEvent::Started)));
    assert!(
        matches!(&events[1], Ok(kolyan_model::ModelEvent::ReasoningDelta(text)) if text == "actual-port-marker")
    );
    assert!(
        matches!(&events[2], Err(error) if error.kind == kolyan_model::ProviderErrorKind::Transport
        && error.phase == kolyan_model::ProviderErrorPhase::Stream && error.message == "unit transport marker")
    );
    assert_eq!(*probe.0.lock().unwrap(), vec![request]);
    assert_eq!(state.effects(), 0);
}

fn execution() -> ExecutionRef {
    ExecutionRef {
        session_id: "unit-session".into(),
        execution_id: "unit-execution".into(),
        turn_id: "unit-turn".into(),
    }
}

struct NeverExecute;
impl ToolExecutor for NeverExecute {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { tool_preparation::prepare(call) })
    }
    fn execute_invocation(&self, _: ToolInvocation) -> ToolFuture<'_> {
        panic!("fault selection unit tests must never create an OS effect")
    }
}

fn invocation(call: ToolCall, snapshot: &str) -> ToolInvocation {
    let prepared = tool_preparation::prepare(call).unwrap();
    let e = execution();
    let scope = ToolExecutionScope {
        execution: kolyan_runtime::ExecutionKey {
            session_id: e.session_id,
            execution_id: e.execution_id,
            turn_id: e.turn_id,
        },
        step_id: "unit-step".into(),
        agent_snapshot_digest: Some(snapshot.into()),
    };
    let decision = tool_preparation::policy().decide_prepared(&prepared, &PolicyContext::default());
    let policy_revision = decision.policy_version.clone();
    let grant = PreparedGrant::issue(
        &prepared,
        decision,
        ApprovalEvidence::NotConfirmed,
        scope.clone(),
    )
    .unwrap();
    ToolInvocation {
        prepared,
        grant,
        scope,
        policy_revision,
        control: Default::default(),
        window: kolyan_core::ToolExecutionWindow::at_deadline(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        ),
    }
}

#[tokio::test]
async fn native_selector_rejects_wrong_actual_input_scope_and_snapshot_before_effect() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../../fixtures/agent/native_effects.json")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let state = State::new(
        "none".into(),
        dataset.write_arguments.clone(),
        Arc::new(Evidence::new(&dir.path().join("actual.jsonl"))),
        10000,
    );
    state.admit(execution()).unwrap();
    let digest = "a".repeat(64);
    let executor = Executor {
        inner: NeverExecute,
        state: state.clone(),
        execution: execution(),
        snapshot_digest: digest.clone(),
    };
    let call = ToolCall {
        id: "actual-model-id-not-native-write".into(),
        name: "file.write".into(),
        arguments: json!(dataset.write_arguments),
    };
    let mut wrong = call.clone();
    wrong.arguments["path"] = json!("outside.txt");
    assert!(
        executor
            .execute_invocation(invocation(wrong, &digest))
            .await
            .is_err()
    );
    assert!(
        executor
            .execute_invocation(invocation(call.clone(), &"b".repeat(64)))
            .await
            .is_err()
    );
    let mut foreign = invocation(call, &digest);
    foreign.scope.execution.execution_id = "foreign".into();
    assert!(executor.execute_invocation(foreign).await.is_err());
    assert_eq!(state.effects(), 0);
    assert!(state.selected_call().is_none());
    assert_eq!(state.refusals(), 3);
}

struct FailBeforeEffect;
impl ToolExecutor for FailBeforeEffect {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { tool_preparation::prepare(call) })
    }
    fn execute_invocation(&self, _: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async {
            Err(kolyan_core::ToolError::Failed {
                message: "unit port deliberately has no OS effect".into(),
            })
        })
    }
}

#[tokio::test]
async fn native_selector_retains_actual_generated_id_scope_and_never_overwrites_first_binding() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../../fixtures/agent/native_effects.json")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let state = State::new(
        "none".into(),
        dataset.write_arguments.clone(),
        Arc::new(Evidence::new(&dir.path().join("actual.jsonl"))),
        10000,
    );
    state.admit(execution()).unwrap();
    let digest = "a".repeat(64);
    let executor = Executor {
        inner: FailBeforeEffect,
        state: state.clone(),
        execution: execution(),
        snapshot_digest: digest.clone(),
    };
    let call = ToolCall {
        id: "provider-generated-not-script-id".into(),
        name: "file.write".into(),
        arguments: json!(dataset.write_arguments),
    };
    let first = invocation(call.clone(), &digest);
    let scope = first.scope.clone();
    assert!(executor.execute_invocation(first).await.is_err());
    assert_eq!(state.selected_call(), Some(call.clone()));
    assert_eq!(state.selected_scope(), Some(scope));
    let mut second = call.clone();
    second.id = "second-actual-call".into();
    assert!(
        executor
            .execute_invocation(invocation(second, &digest))
            .await
            .is_err()
    );
    assert_eq!(state.selected_call(), Some(call));
    assert_eq!(
        state.effects(),
        1,
        "Only the first unit port was entered; neither port creates an OS effect"
    );
    assert_eq!(state.refusals(), 1);
}

#[test]
fn native_fault_live_plan_has_every_configured_deployment_and_original_case() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../../fixtures/agent/native_effects.json")).unwrap();
    let deployments = super::super::deployments();
    let labels = deployments
        .iter()
        .flat_map(|deployment| {
            dataset.cases.iter().map(move |case| {
                format!(
                    "agent/native-effects/{}/{}/{}/{}",
                    deployment.family, deployment.surface, deployment.model, case.id
                )
            })
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(deployments.len(), 19);
    assert_eq!(dataset.cases.len(), 3);
    assert_eq!(labels.len(), 57);
    assert_eq!(dataset.write_arguments.path, "safe/native-proof.txt");
    assert_eq!(dataset.write_arguments.content, dataset.expected_content);
    assert_ne!(dataset.input, dataset.parent_input);
    assert_eq!(dataset.gate_timeout_ms, 10000);
    assert_eq!(dataset.drive_timeout_ms, 40000);
    assert_eq!(dataset.live_drive_timeout_ms, 600000);
}
