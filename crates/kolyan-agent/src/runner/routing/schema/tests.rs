//! Schema examples reuse preparation input SSOT; expected boundary results are
//! independent data. Structural admission never substitutes permission checks.

mod nested;

use super::*;
use crate::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentInvocationBinding, AgentInvokeInput,
    AgentSelector, BindingContextKind, InvokePrepareLimits, prepare_agent_invocation,
};
use kolyan_model::ToolCall;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    source_case: String,
    mutations: Vec<Mutation>,
    schema: bool,
    decoded: bool,
    prepared: bool,
}
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Mutation {
    Set { pointer: String, value: Value },
    Remove { pointer: String },
}

fn mutate(input: &mut Value, changes: &[Mutation]) {
    for change in changes {
        let (pointer, value) = match change {
            Mutation::Set { pointer, value } => (pointer, Some(value)),
            Mutation::Remove { pointer } => (pointer, None),
        };
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        let object = input.pointer_mut(parent).unwrap().as_object_mut().unwrap();
        if let Some(value) = value {
            object.insert(key.into(), value.clone());
        } else {
            assert!(object.remove(key).is_some());
        }
    }
}

#[test]
fn complete_schema_examples_match_decode_and_preflight() {
    let source: Value =
        serde_json::from_str(include_str!("../../../invoke/tests/prepare.json")).unwrap();
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/examples.json")).unwrap();
    let definitions: Vec<AgentDefinitionInput> =
        serde_json::from_value(source["definitions"].clone()).unwrap();
    let original = AgentDefinition::new(definitions[0].clone()).unwrap();
    let mut catalog = AgentCatalog::new(8).unwrap();
    for definition in definitions {
        catalog
            .register(AgentDefinition::new(definition).unwrap())
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
                "schema-parent",
                original.permissions(),
                original.permissions(),
            )
            .unwrap(),
    };
    let execution = kolyan_server::ExecutionRef {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "execution".into(),
    };
    let limits: InvokePrepareLimits = serde_json::from_value(source["limits"].clone()).unwrap();
    let validator = jsonschema::validator_for(&input_schema()).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        let source_case = source["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == case.source_case)
            .unwrap();
        let mut call: ToolCall = serde_json::from_value(source["call"].clone()).unwrap();
        let changes: Vec<Mutation> =
            serde_json::from_value(source_case["mutations"].clone()).unwrap();
        mutate(&mut call.arguments, &changes);
        mutate(&mut call.arguments, &case.mutations);
        let decoded = serde_json::from_value::<AgentInvokeInput>(call.arguments.clone());
        let prepared = prepare_agent_invocation(
            call.clone(),
            &parent,
            &execution,
            &catalog,
            original.permissions(),
            &limits,
        );
        rows.push(json!({"id":case.id,"arguments":call.arguments,
            "schema":validator.is_valid(&call.arguments),"decoded":decoded.is_ok(),"prepared":prepared.is_ok(),
            "decode_error":decoded.err().map(|error|error.to_string()),
            "prepare_error":prepared.err().map(|error|error.to_string())}));
    }
    let directory = tempfile::tempdir().unwrap().keep();
    let path = directory.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in rows {
        writeln!(output, "{}", serde_json::to_string(&row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    eprintln!("Agent invoke schema: {}", path.display());
    let exported = std::fs::read_to_string(path).unwrap();
    assert_eq!(exported.lines().count(), cases.len());
    for (line, case) in exported.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["schema"], case.schema, "{} schema: {row}", case.id);
        assert_eq!(row["decoded"], case.decoded, "{} decode: {row}", case.id);
        assert_eq!(row["prepared"], case.prepared, "{} prepare: {row}", case.id);
    }
}
