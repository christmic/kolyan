//! Host-bound discovery and pure preparation; no network or child execution.

use std::sync::Mutex;

use kolyan_model::{
    ModelProvider, ModelRequest, ProviderError, ProviderErrorKind, ProviderErrorPhase,
    ProviderFuture, ToolCall,
};
use serde::Deserialize;
use serde_json::json;

use super::*;
use crate::{AgentInvocationBinding, BindingContextKind, prepare_agent_invocation};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    limit: usize,
    children: usize,
    accepted: bool,
}

#[derive(Default)]
struct Probe(Mutex<Vec<ModelRequest>>);

impl ModelProvider for Probe {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.0.lock().unwrap().push(request);
        Box::pin(async {
            Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                ProviderErrorPhase::Open,
                "offline dispatch observation",
            ))
        })
    }
}

#[tokio::test]
async fn actual_host_child_bound_matches_schema_preparation_and_restoration() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("limits.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        let harness = Harness::new();
        let mut permissions = harness.runner.host.clone();
        permissions.delegation.allow_self = true;
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "limit-parent".into(),
            revision: "1".into(),
            display_name: None,
            model: kolyan_model::ModelRef::new("test-provider", "selected-model"),
            instructions: "Offline host-bound discovery fixture".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let snapshot = harness
            .runner
            .catalog
            .resolve(
                &AgentSelector::Inline(definition),
                "limits-instance",
                &permissions,
                &permissions,
            )
            .unwrap();
        let limits = InvokePrepareLimits {
            max_children: case.limit,
            max_parallel: 2,
            max_child_input_bytes: 1024,
            max_output_bytes: 65536,
            admission_timeout_ms: 1000,
        };
        let build = |limits: InvokePrepareLimits| {
            AgentRunner::new(
                harness.service.clone(),
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                harness.runner.catalog.clone(),
                permissions.clone(),
                (
                    Providers {
                        observations: harness.observations.clone(),
                        fail: false,
                        reject_context: false,
                        call_tool: false,
                    },
                    Tools(harness.observations.clone(), false),
                ),
                harness.runner.input_artifacts.clone(),
            )
            .unwrap()
            .with_delegation(AgentDelegationConfig {
                limits,
                approval: ApprovalMode::Never,
            })
            .unwrap()
        };
        let runner = build(limits.clone());
        let advertised = runner.delegation_definition(&snapshot).unwrap().unwrap();
        let restored = build(limits.clone())
            .delegation_definition(&snapshot)
            .unwrap()
            .unwrap();
        let children: Vec<_> = (0..case.children)
            .map(|_| {
                json!({"target":{"kind":"self_call"},"input":"Explicit private input",
                    "permissions":{"tools":[],"delegation":{
                        "named_targets":[],"allow_inline":false,"allow_self":false}}})
            })
            .collect();
        let call = ToolCall {
            id: case.id.clone(),
            name: AGENT_INVOKE_NAME.into(),
            arguments: json!({"parallel":false,"children":children}),
        };
        let binding = AgentInvocationBinding {
            task_id: "limits-task".into(),
            invocation_id: "root".into(),
            logical_session_id: "limits-session".into(),
            private_session_id: "limits-session".into(),
            context_kind: BindingContextKind::Root,
            snapshot: snapshot.clone(),
        };
        let execution = kolyan_server::ExecutionRef {
            session_id: "limits-session".into(),
            turn_id: "limits-turn".into(),
            execution_id: "limits-execution".into(),
        };
        let prepared = prepare_agent_invocation(
            call.clone(),
            &binding,
            &execution,
            &runner.catalog,
            &permissions,
            &limits,
        );
        let schema = jsonschema::validator_for(&advertised.input_schema)
            .unwrap()
            .is_valid(&call.arguments);
        let mut changed = limits.clone();
        changed.max_children = if case.limit == 1 { 2 } else { 1 };
        let expected = build(changed).delegation_definition(&snapshot).unwrap();
        let mut historical = harness.request(&case.id, false).turn.model_request;
        historical.tools = vec![advertised.clone()];
        let provider = AdvertisementProvider {
            inner: Probe::default(),
            expected,
        };
        let rejection = match provider.stream(historical.clone()).await {
            Ok(_) => panic!("changed host bound must reject before dispatch"),
            Err(error) => error,
        };
        rows.push(json!({"id":case.id,"source":"offline_host_discovery_and_pure_preparation",
            "limits":limits,"call":call,"advertisement":advertised,"restored":restored,
            "schema":schema,"prepared":prepared.is_ok(),"prepare_error":prepared.err().map(|e|e.to_string()),
            "historical":historical,"changed_host_error":{"phase":format!("{:?}",rejection.phase),
                "kind":format!("{:?}",rejection.kind),"message":rejection.message},
            "dispatches":provider.inner.0.lock().unwrap().len(),
            "effects":harness.observations.effects.lock().unwrap().len()}));
    }
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-host-child-bounds-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in &rows {
        writeln!(output, "{}", row).unwrap();
    }
    output.sync_all().unwrap();
    println!("AGENT_CHILD_BOUND_TRACE={}", path.display());
    for (row, case) in rows.iter().zip(&cases) {
        assert_eq!(row["advertisement"], row["restored"], "{}", case.id);
        assert_eq!(
            row["advertisement"]["input_schema"]["properties"]["children"]["maxItems"],
            case.limit
        );
        assert_eq!(row["schema"], case.accepted, "{}", case.id);
        assert_eq!(row["prepared"], case.accepted, "{}", case.id);
        assert_eq!(row["dispatches"], 0);
        assert_eq!(row["effects"], 0);
        assert_eq!(row["changed_host_error"]["phase"], "Validate");
    }
}
