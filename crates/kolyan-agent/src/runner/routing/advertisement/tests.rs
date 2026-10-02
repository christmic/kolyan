//! Discovery matrix exports exact schemas and examples before comparisons.

mod semantics;
mod skills;

use super::*;
use crate::{AgentDefinitionInput, AgentKey, AgentSnapshot, EnvironmentTool};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchCase {
    id: String,
    expected: bool,
    request: String,
    dispatches: usize,
}

#[derive(Default)]
struct DispatchProbe(std::sync::Mutex<Vec<ModelRequest>>);

impl ModelProvider for DispatchProbe {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.0.lock().unwrap().push(request);
        Box::pin(async {
            Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                ProviderErrorPhase::Open,
                "dispatch probe reached",
            ))
        })
    }
}

#[tokio::test]
async fn resumed_advertisements_dispatch_unchanged_or_reject_before_provider() {
    let cases: Vec<DispatchCase> =
        serde_json::from_str(include_str!("tests/dispatch.json")).unwrap();
    let exact = ToolDefinition {
        name: AGENT_INVOKE_NAME.into(),
        description: Some("Exact admitted revision".into()),
        input_schema: json!({"const":{"definition_id":"userchild-definition","revision":"r1"}}),
    };
    let harness = crate::runner::tests::support::Harness::new();
    let mut rows = Vec::new();
    for case in &cases {
        let mut request = harness.request(&case.id, false).turn.model_request;
        request.tools = match case.request.as_str() {
            "exact" => vec![exact.clone()],
            "none" => Vec::new(),
            "duplicate" => vec![exact.clone(), exact.clone()],
            "changed" => {
                let mut changed = exact.clone();
                changed.input_schema["const"]["revision"] = json!("r2");
                vec![changed]
            }
            other => panic!("unknown fixture request: {other}"),
        };
        // Round-trip the historical request just as a reconstructed checkpoint.
        let restored: ModelRequest =
            serde_json::from_value(serde_json::to_value(&request).unwrap()).unwrap();
        let provider = AdvertisementProvider {
            skills: None,
            inner: DispatchProbe::default(),
            expected: case.expected.then(|| exact.clone()),
        };
        let error = match provider.stream(restored).await {
            Ok(_) => panic!("probe must return its explicit diagnostic error"),
            Err(error) => error,
        };
        rows.push(json!({"id":case.id,"source":request,
            "dispatched":provider.inner.0.lock().unwrap().clone(),
            "error":{"kind":format!("{:?}",error.kind),"phase":format!("{:?}",error.phase),
                "message":error.message,"provider":error.provider,"status":error.status,"display":error.to_string()}}));
    }
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-discovery-dispatch-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in rows {
        writeln!(output, "{}", serde_json::to_string(&row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    println!("AGENT_DISCOVERY_DISPATCH_TRACE={}", path.display());
    let exported = std::fs::read_to_string(&path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        let dispatched = row["dispatched"].as_array().unwrap();
        assert_eq!(dispatched.len(), case.dispatches, "{}: {row}", case.id);
        if case.dispatches == 1 {
            assert_eq!(dispatched[0], row["source"], "no payload rewriting");
            assert_eq!(row["error"]["phase"], "Open");
        } else {
            assert_eq!(row["error"]["phase"], "Validate");
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    parent_named: bool,
    host_named: bool,
    catalog_revision: String,
    parent_self: bool,
    host_self: bool,
    parent_inline: bool,
    host_inline: bool,
    expected_named: bool,
    expected_self: bool,
    expected_inline: bool,
}

#[test]
fn discoverable_targets_are_exact_attenuated_and_restorable() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/discovery.json")).unwrap();
    let mut rows = Vec::new();
    let key = AgentKey::new("userchild-definition", "r1").unwrap();
    for case in &cases {
        let mut permissions = AgentPermissions {
            tools: [EnvironmentTool::Read, EnvironmentTool::Write].into(),
            ..Default::default()
        };
        if case.parent_named {
            permissions.delegation.named_targets.insert(key.clone());
        }
        permissions.delegation.allow_self = case.parent_self;
        permissions.delegation.allow_inline = case.parent_inline;
        let parent_definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "saved-parent".into(),
            revision: "r1".into(),
            display_name: None,
            model: kolyan_model::ModelRef::new("unit", "parent"),
            instructions: "Saved definition".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let snapshot =
            AgentSnapshot::new(parent_definition, "saved-instance".into(), permissions).unwrap();
        let mut host = AgentPermissions {
            tools: [EnvironmentTool::Read].into(),
            ..Default::default()
        };
        if case.host_named {
            host.delegation.named_targets.insert(key.clone());
        }
        host.delegation.allow_self = case.host_self;
        host.delegation.allow_inline = case.host_inline;
        let mut catalog = AgentCatalog::new(8).unwrap();
        let child = AgentDefinition::new(AgentDefinitionInput {
            definition_id: key.definition_id.clone(),
            revision: case.catalog_revision.clone(),
            display_name: Some("Not an authorization key".into()),
            model: kolyan_model::ModelRef::new("unit", "child"),
            instructions: "Child definition".into(),
            permissions: AgentPermissions {
                tools: [EnvironmentTool::Read].into(),
                ..Default::default()
            },
        })
        .unwrap();
        catalog.register(child.clone()).unwrap();
        let advertised = definition(&snapshot, &host, &catalog).unwrap();
        let restored_snapshot: AgentSnapshot =
            serde_json::from_value(serde_json::to_value(&snapshot).unwrap()).unwrap();
        let reconstructed = definition(&restored_snapshot, &host, &catalog.clone()).unwrap();
        let requested = AgentPermissions {
            tools: [EnvironmentTool::Read].into(),
            ..Default::default()
        };
        let inputs = [
            ("named", json!({"kind":"named","value":key})),
            ("self_call", json!({"kind":"self_call"})),
            ("inline", json!({"kind":"inline","value":child})),
        ];
        let mut examples = Vec::new();
        for (kind, target) in inputs {
            let arguments = json!({"parallel":false,"children":[{"target":target,"input":"Selected private input","permissions":requested}]});
            let schema = advertised
                .as_ref()
                .map(|definition| jsonschema::validator_for(&definition.input_schema).unwrap());
            let valid = schema
                .as_ref()
                .is_some_and(|validator| validator.is_valid(&arguments));
            let parent = crate::AgentInvocationBinding {
                task_id: "task".into(),
                invocation_id: "root".into(),
                logical_session_id: "session".into(),
                private_session_id: "session".into(),
                context_kind: crate::BindingContextKind::Root,
                snapshot: snapshot.clone(),
            };
            let prepared = crate::prepare_agent_invocation(
                kolyan_model::ToolCall {
                    id: format!("{kind}-call"),
                    name: AGENT_INVOKE_NAME.into(),
                    arguments: arguments.clone(),
                },
                &parent,
                &kolyan_server::ExecutionRef {
                    session_id: "session".into(),
                    turn_id: "turn".into(),
                    execution_id: "execution".into(),
                },
                &catalog,
                &host,
                &crate::InvokePrepareLimits {
                    max_children: 2,
                    max_parallel: 2,
                    max_child_input_bytes: 1024,
                    max_output_bytes: 4096,
                    admission_timeout_ms: 1000,
                },
            )
            .is_ok();
            examples
                .push(json!({"kind":kind,"input":arguments,"schema":valid,"prepared":prepared}));
        }
        let invented = json!({"parallel":false,"children":[{"target":{"kind":"named","value":{"definition_id":"child","revision":"r1"}},"input":"Selected input","permissions":requested}]});
        let missing = json!({"parallel":false,"children":[{"target":{"kind":"named","value":{"definition_id":key.definition_id}},"input":"Selected input","permissions":requested}]});
        let schema = advertised
            .as_ref()
            .map(|definition| jsonschema::validator_for(&definition.input_schema).unwrap());
        rows.push(json!({"id":case.id,"advertised":advertised,"reconstructed":reconstructed,"examples":examples,
            "invented_key_valid":schema.as_ref().is_some_and(|validator|validator.is_valid(&invented)),
            "missing_revision_valid":schema.as_ref().is_some_and(|validator|validator.is_valid(&missing))}));
    }
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-discovery-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in rows {
        writeln!(output, "{}", serde_json::to_string(&row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    println!("AGENT_DISCOVERY_TRACE={}", path.display());
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["advertised"], row["reconstructed"]);
        assert_eq!(
            row["advertised"].is_null(),
            !(case.expected_named || case.expected_self || case.expected_inline)
        );
        for (example, expected) in row["examples"].as_array().unwrap().iter().zip([
            case.expected_named,
            case.expected_self,
            case.expected_inline,
        ]) {
            assert_eq!(example["schema"], expected, "{}: {row}", case.id);
            assert_eq!(example["prepared"], expected, "{}: {row}", case.id);
        }
        assert_eq!(row["invented_key_valid"], false);
        assert_eq!(row["missing_revision_valid"], false);
    }
}
