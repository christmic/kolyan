//! Synthetic argument observations, not model output or OS effects.
use super::super::*;
use crate::{
    AgentDefinitionInput, AgentInvocationBinding, AgentKey, BindingContextKind, EnvironmentTool,
    InvokePrepareError, InvokePrepareLimits, prepare_agent_invocation,
};
use serde::Deserialize;
use std::{collections::BTreeMap, io::Write};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    contexts: BTreeMap<String, Context>,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    tools: Vec<EnvironmentTool>,
    definition_tools: Vec<EnvironmentTool>,
    host_tools: Vec<EnvironmentTool>,
    named: bool,
    self_call: bool,
    inline: bool,
    branches: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    context: String,
    target: String,
    permissions: Value,
    expected_schema: bool,
    expected_prepared: bool,
    expected_error: Option<String>,
}
fn permissions(context: &Context, host: bool) -> AgentPermissions {
    let mut permissions = AgentPermissions {
        tools: if host {
            &context.host_tools
        } else {
            &context.tools
        }
        .iter()
        .copied()
        .collect(),
        ..Default::default()
    };
    if context.named {
        permissions
            .delegation
            .named_targets
            .insert(AgentKey::new("child", "r1").unwrap());
    }
    permissions.delegation.allow_self = context.self_call;
    permissions.delegation.allow_inline = context.inline;
    permissions
}
fn observe(case: &Case, context: &Context) -> Value {
    let parent_permissions = permissions(context, false);
    let host = permissions(context, true);
    let mut definition_permissions = parent_permissions.clone();
    definition_permissions.tools = context.definition_tools.iter().copied().collect();
    let parent = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "parent".into(),
        revision: "r1".into(),
        display_name: None,
        model: kolyan_model::ModelRef::new("fixture", "model"),
        instructions: "Fixture parent".into(),
        permissions: definition_permissions,
    })
    .unwrap();
    let snapshot = AgentSnapshot::new(parent, "instance".into(), parent_permissions).unwrap();
    let child = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "child".into(),
        revision: "r1".into(),
        display_name: None,
        model: kolyan_model::ModelRef::new("fixture", "model"),
        instructions: "Fixture child".into(),
        permissions: AgentPermissions {
            tools: [EnvironmentTool::Read].into(),
            ..Default::default()
        },
    })
    .unwrap();
    let mut catalog = AgentCatalog::new(4).unwrap();
    catalog.register(child.clone()).unwrap();
    let advertised = definition(&snapshot, &host, &catalog).unwrap();
    let restored: AgentSnapshot =
        serde_json::from_value(serde_json::to_value(&snapshot).unwrap()).unwrap();
    let reconstructed = definition(&restored, &host, &catalog).unwrap();
    let target = match case.target.as_str() {
        "self_call" => json!({"kind":"self_call"}),
        "named" => json!({"kind":"named","value":child.key()}),
        "inline" => json!({"kind":"inline","value":child}),
        other => panic!("invalid fixture target: {other}"),
    };
    let arguments = json!({"children":[{"target":target,"input":"Fixture private input","permissions":case.permissions}],"parallel":false});
    let valid = advertised.as_ref().is_some_and(|tool| {
        jsonschema::validator_for(&tool.input_schema)
            .unwrap()
            .is_valid(&arguments)
    });
    let binding = AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: "root".into(),
        logical_session_id: "session".into(),
        private_session_id: "session".into(),
        context_kind: BindingContextKind::Root,
        snapshot,
    };
    let result = prepare_agent_invocation(
        kolyan_model::ToolCall {
            id: case.id.clone(),
            name: AGENT_INVOKE_NAME.into(),
            arguments: arguments.clone(),
        },
        &binding,
        &kolyan_server::ExecutionRef {
            session_id: "session".into(),
            turn_id: "turn".into(),
            execution_id: "execution".into(),
        },
        &catalog,
        &host,
        &InvokePrepareLimits {
            max_children: 2,
            max_parallel: 2,
            max_child_input_bytes: 1024,
            max_output_bytes: 4096,
            admission_timeout_ms: 1000,
        },
    );
    let error = result.as_ref().err().map(|error| match error {
        InvokePrepareError::Invalid(_) => "invalid",
        InvokePrepareError::Agent(AgentError::PermissionDenied) => "permission",
        _ => "other",
    });
    let branches: Vec<Value> = advertised
        .as_ref()
        .map(|tool| {
            tool.input_schema["properties"]["children"]["items"]["oneOf"]
                .as_array()
                .unwrap()
                .clone()
        })
        .unwrap_or_default();
    let kinds: Vec<_> = branches
        .iter()
        .map(|branch| branch["properties"]["target"]["properties"]["kind"]["const"].clone())
        .collect();
    json!({"id":case.id,"fixture_only":true,"snapshot":binding.snapshot,"host":host,"arguments":arguments,"advertised":advertised,
        "reconstructed":reconstructed,"branches":branches,"kinds":kinds,"schema_valid":valid,
        "prepared":result.is_ok(),"error_class":error,"prepare_error":result.err().map(|error|error.to_string())})
}
#[test]
fn semantic_declarations_preserve_strict_schema_and_real_preparation() {
    let data: Dataset = serde_json::from_str(include_str!("semantics.json")).unwrap();
    let rows: Vec<_> = data
        .cases
        .iter()
        .map(|case| observe(case, &data.contexts[&case.context]))
        .collect();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-advertisement-semantics-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut output = std::fs::File::create(&path).unwrap();
    for row in &rows {
        writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    println!("AGENT_ADVERTISEMENT_SEMANTICS={}", path.display());
    assert_eq!(data.schema_version, 1);
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), data.cases.len());
    for (line, case) in exported.lines().zip(&data.cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(
            row["schema_valid"], case.expected_schema,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["prepared"], case.expected_prepared,
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["error_class"],
            json!(case.expected_error),
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["kinds"],
            json!(data.contexts[&case.context].branches),
            "{}: {row}",
            case.id
        );
        assert_eq!(
            row["advertised"], row["reconstructed"],
            "stable reconstruction"
        );
        for branch in row["branches"].as_array().unwrap() {
            let properties = &branch["properties"]["permissions"]["properties"];
            for node in [
                &properties["tools"],
                &properties["delegation"]["properties"]["named_targets"],
            ] {
                assert_eq!(node["type"], "array");
                assert!(node["description"].as_str().unwrap().contains("[]"));
                if node["items"] == false {
                    assert_eq!(node["maxItems"], 0);
                    assert_eq!(node["examples"], json!([[]]));
                    for example in node["examples"].as_array().unwrap() {
                        assert!(jsonschema::validator_for(node).unwrap().is_valid(example));
                    }
                }
            }
            for name in ["allow_inline", "allow_self"] {
                let node = &properties["delegation"]["properties"][name];
                assert_eq!(node["type"], "boolean");
                if node.get("const").is_some() {
                    assert_eq!(node["const"], false);
                }
            }
            assert!(
                branch["properties"]["permissions"]["description"]
                    .as_str()
                    .unwrap()
                    .contains("Descendants cannot regain omitted tools")
            );
            assert!(
                branch["properties"]["target"]["description"]
                    .as_str()
                    .unwrap()
                    .contains("does not grant")
            );
            if branch["properties"]["target"]["properties"]["kind"]["const"] == "inline" {
                assert!(
                    branch["properties"]["target"]["properties"]["value"]["description"]
                        .as_str()
                        .unwrap()
                        .contains("not an execution grant")
                );
            }
        }
    }
}
