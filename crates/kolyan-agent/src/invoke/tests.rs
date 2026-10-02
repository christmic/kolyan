//! Dataset-driven preparation evidence is exported before semantic comparisons.

use std::fs::File;
use std::io::{BufWriter, Write};

use kolyan_policy::{ApprovalMode, PolicyEngine};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::{AgentDefinitionInput, binding::BindingContextKind};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    definitions: Vec<AgentDefinitionInput>,
    limits: InvokePrepareLimits,
    call: ToolCall,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutations: Vec<Mutation>,
    expected: Expected,
    #[serde(default)]
    replace_catalog_parent: bool,
    #[serde(default)]
    deny_named_host: bool,
    execution_session: Option<String>,
    tool_name: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Mutation {
    Set { pointer: String, value: Value },
    Remove { pointer: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    category: String,
    roles: Option<Vec<InvocationRole>>,
    models: Option<Vec<String>>,
    parallel: Option<bool>,
}

#[test]
fn preparation_matrix_exports_then_compares_typed_intents_and_exact_claims() {
    let dataset: Dataset = serde_json::from_str(include_str!("tests/prepare.json")).unwrap();
    let original = AgentDefinition::new(dataset.definitions[0].clone()).unwrap();
    let mut original_catalog = AgentCatalog::new(8).unwrap();
    original_catalog.register(original.clone()).unwrap();
    let parent = AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: "root".into(),
        logical_session_id: "session".into(),
        private_session_id: "session".into(),
        context_kind: BindingContextKind::Root,
        snapshot: original_catalog
            .resolve(
                &AgentSelector::Named(original.key()),
                "host-parent-instance",
                original.permissions(),
                original.permissions(),
            )
            .unwrap(),
    };
    let mut rows = Vec::new();
    let mut outcomes = Vec::new();
    for case in &dataset.cases {
        let mut catalog = AgentCatalog::new(8).unwrap();
        for definition in &dataset.definitions {
            let mut definition = definition.clone();
            if case.replace_catalog_parent && definition.definition_id == "parent" {
                definition.instructions = "Replacement catalog instructions".into();
            }
            catalog
                .register(AgentDefinition::new(definition).unwrap())
                .unwrap();
        }
        let mut call = dataset.call.clone();
        for mutation in &case.mutations {
            mutate(&mut call.arguments, mutation);
        }
        if let Some(name) = &case.tool_name {
            call.name = name.clone();
        }
        let execution = ExecutionRef {
            session_id: case
                .execution_session
                .clone()
                .unwrap_or_else(|| "session".into()),
            turn_id: "turn".into(),
            execution_id: "execution".into(),
        };
        let mut host = original.permissions().clone();
        if case.deny_named_host {
            host.delegation.named_targets.clear();
        }
        let outcome = prepare_agent_invocation(
            call.clone(),
            &parent,
            &execution,
            &catalog,
            &host,
            &dataset.limits,
        );
        let actual = match &outcome {
            Ok(intent) => {
                json!({"category":"prepared","prepared":intent.prepared(),"children":intent.children(),"parallel":intent.parallel()})
            }
            Err(error) => json!({"category":category(error),"error":error.to_string()}),
        };
        rows.push(json!({"fixture_id":case.id,"call":call,"execution":execution,"host":host,"limits":dataset.limits,"parent":parent,"actual":actual}));
        outcomes.push(outcome);
    }
    let root = tempfile::Builder::new()
        .prefix("kolyan-invoke-prepare-")
        .tempdir()
        .unwrap()
        .keep();
    let trace = root.join("actual.jsonl");
    let mut writer = BufWriter::new(File::create(&trace).unwrap());
    for row in &rows {
        serde_json::to_writer(&mut writer, row).unwrap();
        writeln!(writer).unwrap();
    }
    writer.flush().unwrap();
    println!("AGENT_INVOKE_PREPARE_TRACE={}", trace.display());
    let exported: Vec<Value> = std::fs::read_to_string(&trace)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(exported, rows);
    let manifest = agent_invoke_manifest(ApprovalMode::Never);
    let mut policy = PolicyEngine::default();
    policy.register(manifest.clone());
    for ((case, outcome), row) in dataset.cases.iter().zip(outcomes).zip(exported) {
        assert_eq!(
            row["actual"]["category"], case.expected.category,
            "{}",
            case.id
        );
        if let Ok(intent) = outcome {
            assert_eq!(
                intent
                    .children()
                    .iter()
                    .map(|child| child.role)
                    .collect::<Vec<_>>(),
                *case.expected.roles.as_ref().unwrap(),
                "{}",
                case.id
            );
            assert_eq!(
                intent
                    .children()
                    .iter()
                    .map(|child| child.definition.model().model.clone())
                    .collect::<Vec<_>>(),
                *case.expected.models.as_ref().unwrap(),
                "{}",
                case.id
            );
            assert_eq!(
                Some(intent.parallel()),
                case.expected.parallel,
                "{}",
                case.id
            );
            assert_eq!(
                intent.prepared().claim().capabilities,
                manifest.capabilities
            );
            assert_eq!(intent.prepared().claim().effects, manifest.effects);
            assert_eq!(
                intent.prepared().execution_binding()["parent_snapshot_digest"],
                parent.snapshot.digest()
            );
            assert_eq!(
                intent.prepared().execution_binding()["execution"],
                row["execution"]
            );
            assert!(
                !intent
                    .prepared()
                    .claim()
                    .capabilities
                    .contains(&Capability::ProcessExecute)
            );
            assert!(intent.prepared().claim().resource.path.is_none());
            assert_eq!(
                intent.prepared().execution_binding()["limits"]["max_parallel"],
                1
            );
            assert_eq!(
                policy
                    .decide_prepared(intent.prepared(), &Default::default())
                    .kind,
                kolyan_policy::PolicyDecisionKind::Allow
            );
            for child in intent.prepared().execution_binding()["children"]
                .as_array()
                .unwrap()
            {
                assert!(child.get("instance_id").is_none());
            }
            for (index, child) in intent.children().iter().enumerate() {
                assert_eq!(
                    child.input,
                    row["call"]["arguments"]["children"][index]["input"]
                        .as_str()
                        .unwrap()
                );
                assert_eq!(
                    serde_json::to_value(&child.permissions).unwrap(),
                    row["call"]["arguments"]["children"][index]["permissions"]
                );
            }
            if case.replace_catalog_parent {
                assert_eq!(
                    intent.children()[0].definition.instructions(),
                    original.instructions()
                );
            }
        }
    }
}

fn mutate(value: &mut Value, mutation: &Mutation) {
    let pointer = match mutation {
        Mutation::Set { pointer, .. } | Mutation::Remove { pointer } => pointer,
    };
    let (parent, key) = pointer.rsplit_once('/').unwrap();
    let owner = value.pointer_mut(parent).unwrap();
    match mutation {
        Mutation::Set { value, .. } => {
            owner
                .as_object_mut()
                .unwrap()
                .insert(key.into(), value.clone());
        }
        Mutation::Remove { .. } => {
            assert!(owner.as_object_mut().unwrap().remove(key).is_some());
        }
    }
}

fn category(error: &InvokePrepareError) -> &'static str {
    match error {
        InvokePrepareError::Agent(AgentError::PermissionDenied) => "permission_denied",
        InvokePrepareError::Agent(AgentError::NotFound) => "not_found",
        _ => "invalid",
    }
}
