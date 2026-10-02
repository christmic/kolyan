//! Nested inline wire examples are synthetic inputs, never model or effect evidence.
use super::super::input_schema;
use super::{Mutation, mutate};
use crate::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentInvocationBinding, AgentInvokeInput,
    AgentSelector, BindingContextKind, EnvironmentTool, InvokePrepareError, InvokePrepareLimits,
    PreparedAgentInvocation, prepare_agent_invocation,
};
use kolyan_model::ToolCall;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    host_tools: Vec<EnvironmentTool>,
    source_case: String,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutations: Vec<Mutation>,
    schema: bool,
    decoded: bool,
    prepared: bool,
    static_tools: Option<Value>,
}
fn preparation(result: Result<PreparedAgentInvocation, InvokePrepareError>) -> Value {
    match result {
        Ok(plan) => json!({"accepted":true,"digest":plan.prepared().digest(),
            "static_tools":plan.children()[0].definition.permissions().tools}),
        Err(error) => json!({"accepted":false,"static_tools":null,"error":error.to_string()}),
    }
}
#[test]
fn inline_nested_arrays_remain_strict_static_and_restorable() {
    let data: Dataset = serde_json::from_str(include_str!("nested.json")).unwrap();
    let source: Value =
        serde_json::from_str(include_str!("../../../../invoke/tests/prepare.json")).unwrap();
    let inputs: Vec<AgentDefinitionInput> =
        serde_json::from_value(source["definitions"].clone()).unwrap();
    let original = AgentDefinition::new(inputs[0].clone()).unwrap();
    let mut catalog = AgentCatalog::new(8).unwrap();
    for input in inputs {
        catalog
            .register(AgentDefinition::new(input).unwrap())
            .unwrap();
    }
    let parent = AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: "root".into(),
        logical_session_id: "session".into(),
        private_session_id: "session".into(),
        context_kind: BindingContextKind::Root,
        snapshot: catalog
            .resolve(
                &AgentSelector::Named(original.key()),
                "nested-parent",
                original.permissions(),
                original.permissions(),
            )
            .unwrap(),
    };
    let restored: AgentInvocationBinding =
        serde_json::from_value(serde_json::to_value(&parent).unwrap()).unwrap();
    let mut host = original.permissions().clone();
    host.tools = data.host_tools.iter().copied().collect();
    let execution = kolyan_server::ExecutionRef {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "execution".into(),
    };
    let limits: InvokePrepareLimits = serde_json::from_value(source["limits"].clone()).unwrap();
    let source_case = source["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == data.source_case)
        .unwrap();
    let changes: Vec<Mutation> = serde_json::from_value(source_case["mutations"].clone()).unwrap();
    let schema = input_schema();
    let restored_schema: Value =
        serde_json::from_slice(&serde_json::to_vec(&schema).unwrap()).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let mut rows = Vec::new();
    for case in &data.cases {
        let mut call: ToolCall = serde_json::from_value(source["call"].clone()).unwrap();
        call.id = case.id.clone();
        mutate(&mut call.arguments, &changes);
        mutate(&mut call.arguments, &case.mutations);
        let decoded = serde_json::from_value::<AgentInvokeInput>(call.arguments.clone());
        let result = preparation(prepare_agent_invocation(
            call.clone(),
            &parent,
            &execution,
            &catalog,
            &host,
            &limits,
        ));
        let restored_result = preparation(prepare_agent_invocation(
            call.clone(),
            &restored,
            &execution,
            &catalog.clone(),
            &host,
            &limits,
        ));
        rows.push(
            json!({"event":"nested_case","id":case.id,"arguments":call.arguments,
            "schema":validator.is_valid(&call.arguments),"decoded":decoded.is_ok(),
            "decode_error":decoded.err().map(|error|error.to_string()),
            "preparation":result,"reconstructed_preparation":restored_result,"host":host}),
        );
    }
    let mut examples = Vec::new();
    let outer = &schema["properties"]["children"]["items"]["properties"]["permissions"];
    let nested = &schema["properties"]["children"]["items"]["properties"]["target"]["oneOf"][2]["properties"]
        ["value"]["properties"]["permissions"];
    for (location, permissions) in [("requested", outer), ("inline_static", nested)] {
        for (field, node) in [
            ("tools", &permissions["properties"]["tools"]),
            (
                "named_targets",
                &permissions["properties"]["delegation"]["properties"]["named_targets"],
            ),
        ] {
            let values = node["examples"].as_array().unwrap();
            let valid: Vec<_> = values
                .iter()
                .map(|example| jsonschema::validator_for(node).unwrap().is_valid(example))
                .collect();
            examples.push(json!({"location":location,"field":field,"schema":node,
                "examples_valid":valid,"permission_description":permissions["description"]}));
        }
    }
    let directory = tempfile::Builder::new()
        .prefix("kolyan-agent-nested-schema-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut output = std::fs::File::create(&path).unwrap();
    for row in &rows {
        writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    writeln!(
        output,
        "{}",
        json!({"event":"nested_schema_contract","schema":schema,
        "reconstructed_schema":restored_schema,"examples":examples})
    )
    .unwrap();
    output.sync_all().unwrap();
    println!("AGENT_NESTED_SCHEMA_TRACE={}", path.display());
    let exported = std::fs::read_to_string(&path).unwrap();
    let actual: Vec<Value> = exported
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(data.schema_version, 1);
    assert_eq!(actual.len(), data.cases.len() + 1);
    for (row, case) in actual.iter().zip(&data.cases) {
        assert_eq!(row["id"], case.id);
        assert_eq!(row["schema"], case.schema, "{row}");
        assert_eq!(row["decoded"], case.decoded, "{row}");
        assert_eq!(row["preparation"]["accepted"], case.prepared, "{row}");
        assert_eq!(
            row["preparation"]["static_tools"],
            json!(case.static_tools),
            "{row}"
        );
        assert_eq!(
            row["preparation"], row["reconstructed_preparation"],
            "{row}"
        );
    }
    let contract = actual.last().unwrap();
    assert_eq!(contract["schema"], contract["reconstructed_schema"]);
    for row in contract["examples"].as_array().unwrap() {
        assert_eq!(row["schema"]["type"], "array");
        assert_eq!(row["schema"]["examples"], json!([[]]));
        assert_eq!(row["examples_valid"], json!([true]));
        let description = row["schema"]["description"].as_str().unwrap();
        assert!(description.contains("[]"));
        assert!(description.contains("object"));
        assert!(description.contains("string"));
        assert!(
            row["permission_description"]
                .as_str()
                .unwrap()
                .contains("immutable static ceiling")
        );
        assert!(
            row["permission_description"]
                .as_str()
                .unwrap()
                .contains("Neither is a grant")
        );
    }
}
