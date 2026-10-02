//! Actual Runner/Core policy/Runtime receipts and required artifact reads.
//! The network model is scripted; no live-model or native-tool acceptance claim.

mod adapters;
mod continuation;

use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    sync::{Arc, Mutex},
};

use kolyan_core::ToolErrorPolicy;
use kolyan_ledger::{LedgerStore, MemoryFactJournal, SqliteFactJournal};
use kolyan_model::{ContentBlock, ModelRef};
use kolyan_server::{
    ExecutionService, InstanceRegistry, SessionExecutionService, TaskCoordinator,
    TaskExecutionService,
};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde::Deserialize;
use serde_json::{Value, json};

use super::support::{Harness, Observations};
use crate::*;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Load,
    Malicious,
    WrongDigest,
    Answer,
    Corrupt,
    Child,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    named: bool,
    sqlite: bool,
    mode: Mode,
    revision: String,
    allow: bool,
    body: String,
    success: bool,
    requests: usize,
    loaded: usize,
}

async fn run(case: &Case, root: &std::path::Path) -> Value {
    fs::create_dir_all(root).unwrap();
    let harness = Harness::new();
    let journal = adapters::Journal {
        inner: if case.sqlite {
            Arc::new(SqliteFactJournal::open(root.join("facts.sqlite")).unwrap())
        } else {
            Arc::new(MemoryFactJournal::default())
        },
        streams: Arc::new(Mutex::new(Default::default())),
    };
    let journal_port = Arc::new(journal.clone());
    let verifier = Arc::new(AgentChildWaitVerifier::default());
    let service = Arc::new(TaskExecutionService::new(
        TaskCoordinator::new(journal.clone()),
        SessionExecutionService::new(
            ExecutionService::new(kolyan_ledger::InMemoryLedger::default(), NoopTraceSink)
                .with_external_wait_verifier(verifier.clone()),
            harness.service.sessions().sessions().clone(),
        ),
    ));
    let mut permissions = AgentPermissions::default();
    if matches!(case.mode, Mode::Child) {
        permissions
            .delegation
            .named_targets
            .insert(AgentKey::new("knowledge-child", "1").unwrap());
    }
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "knowledge-agent".into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef::new("fixture", "scripted"),
        instructions: "Use only authorized task knowledge.".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let mut agents = AgentCatalog::new(4).unwrap();
    agents.register(definition.clone()).unwrap();
    if matches!(case.mode, Mode::Child) {
        agents
            .register(
                AgentDefinition::new(AgentDefinitionInput {
                    definition_id: "knowledge-child".into(),
                    revision: "1".into(),
                    display_name: None,
                    model: definition.model().clone(),
                    instructions: "Load authorized knowledge only.".into(),
                    permissions: AgentPermissions::default(),
                })
                .unwrap(),
            )
            .unwrap();
    }
    let artifacts_root = root.join("skills");
    let artifacts =
        Arc::new(ArtifactStore::new(&artifacts_root, skills::MAX_BODY_BYTES as u64).unwrap());
    let catalog = SkillCatalog::new(
        journal_port.clone(),
        artifacts,
        "skills.consumer.fixture".into(),
        SkillLimits::default(),
    )
    .unwrap();
    let selected = catalog
        .register(
            SkillDescriptorInput {
                key: SkillKey::new("guide".into(), case.revision.clone()).unwrap(),
                title: "Knowledge guide".into(),
                description: "Untrusted task knowledge only".into(),
            },
            &case.body,
        )
        .unwrap();
    // An absent unselected body must never be touched by discovery, binding or load.
    let unselected = catalog
        .register(
            SkillDescriptorInput {
                key: SkillKey::new("unselected".into(), "1".into()).unwrap(),
                title: "Not selected".into(),
                description: "Must not read".into(),
            },
            "unselected body",
        )
        .unwrap();
    fs::remove_file(artifacts_root.join(&unselected.metadata().body().digest)).unwrap();
    if matches!(case.mode, Mode::Corrupt) {
        fs::write(
            artifacts_root.join(&selected.metadata().body().digest),
            b"changed",
        )
        .unwrap();
    }
    let policy = if case.allow {
        SkillAccessPolicy::new(
            "host.skills".into(),
            "1".into(),
            vec![SkillAccessRuleInput {
                agent: if matches!(case.mode, Mode::Child) {
                    AgentKey::new("knowledge-child", "1").unwrap()
                } else {
                    definition.key()
                },
                logical_session_id: "session".into(),
                task_id: Some(case.id.clone()),
                invocation_id: if matches!(case.mode, Mode::Child) {
                    None
                } else {
                    Some("root".into())
                },
                skills: [selected.metadata().descriptor().key.clone()].into(),
            }],
        )
        .unwrap()
    } else {
        SkillAccessPolicy::deny_all()
    };
    let runtime = Arc::new(SkillRuntime::new(catalog.clone(), policy));
    let observations = Arc::new(Observations::default());
    let mut runner = AgentRunner::new(
        service.clone(),
        InstanceRegistry::new(journal_port.clone(), "skills.host", 64).unwrap(),
        AgentInvocationBindingStore::new(journal_port),
        agents,
        permissions.clone(),
        (
            adapters::Factory {
                observations: observations.clone(),
                case: case.clone(),
            },
            adapters::environment(observations.clone()),
        ),
        Arc::new(ArtifactStore::new(root.join("inputs"), 16 * 1024 * 1024).unwrap()),
    )
    .unwrap()
    .with_skills(runtime)
    .with_tool_error_policy(if matches!(case.mode, Mode::Malicious) {
        ToolErrorPolicy::ContinueBatch
    } else {
        ToolErrorPolicy::FailTurn
    });
    if matches!(case.mode, Mode::Child) {
        runner = runner
            .with_delegation(AgentDelegationConfig {
                limits: InvokePrepareLimits {
                    max_children: 1,
                    max_parallel: 1,
                    max_child_input_bytes: 1024,
                    max_output_bytes: 65536,
                    admission_timeout_ms: 10000,
                },
                approval: kolyan_policy::ApprovalMode::Never,
            })
            .unwrap();
    }
    let runner = Arc::new(runner);
    verifier.attach(&runner).unwrap();
    let mut request = harness.request(&case.id, case.named);
    request.selector = if case.named {
        AgentSelector::Named(definition.key())
    } else {
        AgentSelector::Inline(definition)
    };
    request.requested_permissions = permissions;
    request.limits.max_tokens = None;
    request.limits.max_steps_per_turn = 4;
    request.turn.config.max_steps = 4;
    request.turn.model_request.max_output_tokens = Some(50);
    if matches!(case.mode, Mode::Child) {
        request.limits.max_depth = 2;
        request.limits.max_invocations = 2;
        request.limits.max_attempts = 2;
    }
    let mut template = request.turn.clone();
    template.model_request.messages.clear();
    template.model_request.tools.clear();
    let before = journal.records();
    let execution_id = request.execution.execution_id.clone();
    let result = runner.start(request).await;
    let result = if matches!(case.mode, Mode::Child) {
        match result {
            Ok(started) => {
                let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } =
                    &started.execution
                else {
                    panic!("child fixture requires durable external wait");
                };
                let owner = DelegationOwner {
                    task_id: case.id.clone(),
                    logical_session_id: "session".into(),
                    parent: started.task.attempts["attempt"].binding.clone(),
                    scope: suspension.checkpoint.scope.clone(),
                };
                match runner
                    .pump_agent_children(
                        owner,
                        suspension.checkpoint.checkpoint_id.clone(),
                        "delegate-call".into(),
                        template,
                    )
                    .await
                {
                    Ok(AgentChildrenPumpResult::Resumed(result)) => Ok(*result),
                    Ok(_) => Err(RunnerError::Host("child remained waiting".into())),
                    Err(error) => Err(RunnerError::Host(error.to_string())),
                }
            }
            Err(error) => Err(error),
        }
    } else {
        result
    };
    let outcome = match &result {
        Ok(result) => json!({"success":true,"snapshot":result.snapshot,"task":result.task}),
        Err(error) => json!({"success":false,"error":error.to_string()}),
    };
    let mut ledger = service
        .sessions()
        .execution()
        .server()
        .coordinator()
        .ledger()
        .execution_events_after(&execution_id, 0)
        .unwrap();
    if matches!(case.mode, Mode::Child) {
        for attempt in service
            .coordinator()
            .snapshot(&case.id)
            .unwrap()
            .attempts
            .values()
            .filter(|a| a.binding.execution.execution_id != execution_id)
        {
            ledger.extend(
                service
                    .sessions()
                    .execution()
                    .server()
                    .coordinator()
                    .ledger()
                    .execution_events_after(&attempt.binding.execution.execution_id, 0)
                    .unwrap(),
            );
        }
    }
    let input_proof = service
        .coordinator()
        .snapshot(&case.id)
        .ok()
        .and_then(|task| {
            task.invocations
                .get("root")
                .map(|i| i.definition.input_source.clone())
        });
    let mut origin = Value::Null;
    if let Some(source) = input_proof {
        let saved = runner
            .bindings
            .load(&case.id, "root", "session")
            .unwrap()
            .unwrap();
        let (_, document): (_, Value) = runner.load_input_document(&saved, &source).unwrap();
        origin = document;
    }
    let mut missing = origin.clone();
    missing.as_object_mut().unwrap().remove("skill_binding");
    let missing_rejected =
        serde_json::from_value::<crate::runner::input::RootInput>(missing.clone())
            .err()
            .map(|e| e.to_string());
    let mut child_sources = Vec::new();
    if matches!(case.mode, Mode::Child) {
        let task = service.coordinator().snapshot(&case.id).unwrap();
        for (id, invocation) in &task.invocations {
            if id == "root" {
                continue;
            }
            let saved = runner
                .bindings
                .load(&case.id, id, "session")
                .unwrap()
                .unwrap();
            let (_, document): (_, Value) = runner
                .load_input_document(&saved, &invocation.definition.input_source)
                .unwrap();
            let mut absent = document.clone();
            absent.as_object_mut().unwrap().remove("skill_binding");
            let error = serde_json::from_value::<crate::runner::input::ChildInput>(absent.clone())
                .err()
                .map(|e| e.to_string());
            child_sources.push(
                json!({"document":document,"missing_binding_document":absent,
                "missing_binding_error":error}),
            );
        }
    }
    json!({"id":case.id,"outcome":outcome,"body":case.body,"selected":selected,"unselected":unselected,
        "before":before,"journal":journal.records(),"ledger":ledger,"source_document":origin,
        "missing_binding_document":missing,"missing_binding_error":missing_rejected,"child_sources":child_sources,
        "requests":*observations.requests.lock().unwrap(),"environment_effects":*observations.effects.lock().unwrap()})
}

#[tokio::test]
async fn real_runner_skill_load_receipt_and_next_step_data_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("skills/cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skills-consumer-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut file = File::create(&path).unwrap();
    for case in &cases {
        let row = run(case, &root.join(&case.id)).await;
        serde_json::to_writer(&mut file, &row).unwrap();
        writeln!(file).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    println!("SKILLS_CONSUMER_ACTUAL={}", path.display());
    let actual: Vec<Value> = BufReader::new(File::open(&path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(actual.len(), cases.len());
    for (row, case) in actual.iter().zip(&cases) {
        assert_eq!(row["id"], case.id);
        assert_eq!(
            row["outcome"]["success"], case.success,
            "{}: {}",
            case.id, row["outcome"]
        );
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            case.requests,
            "{}",
            case.id
        );
        assert_eq!(row["environment_effects"], json!([]));
        if matches!(case.mode, Mode::Child) {
            let children = row["child_sources"].as_array().unwrap();
            assert_eq!(children.len(), 1);
            assert!(
                children[0]["missing_binding_error"]
                    .as_str()
                    .unwrap()
                    .contains("skill_binding")
            );
            assert_ne!(
                children[0]["document"]["skill_binding"],
                row["source_document"]["skill_binding"]
            );
        }
        assert!(row["source_document"].get("skill_binding").is_some());
        assert!(
            row["missing_binding_error"]
                .as_str()
                .unwrap()
                .contains("skill_binding")
        );
        let requests: Vec<kolyan_model::ModelRequest> =
            serde_json::from_value(row["requests"].clone()).unwrap();
        let loaded: Vec<_> = requests
            .iter()
            .flat_map(|r| &r.messages)
            .flat_map(|m| &m.content)
            .filter_map(|b| {
                if let ContentBlock::ToolResult { result } = b
                    && result.call_id == "skill-call"
                    && !result.is_error
                {
                    return Some(serde_json::from_str::<Value>(&result.content).unwrap());
                }
                None
            })
            .collect();
        let unique: std::collections::BTreeSet<_> = loaded.iter().map(Value::to_string).collect();
        assert_eq!(unique.len(), case.loaded, "{}", case.id);
        let completed = row["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| {
                event["kind"] == "tool_execution_completed"
                    && event["payload"]["call_id"] == "skill-call"
                    && event["payload"]["is_error"] == false
            })
            .count();
        assert_eq!(
            completed, case.loaded,
            "{} must have one actual completed Skill result",
            case.id
        );
        let receipts = row["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "effect_receipt")
            .count();
        if case.loaded == 1 {
            assert_eq!(
                receipts,
                if matches!(case.mode, Mode::Child) {
                    2
                } else {
                    1
                },
                "{} must have one actual effect receipt",
                case.id
            );
        }
        for value in loaded {
            assert_eq!(value["body"], case.body);
            assert_eq!(value["key"]["revision"], case.revision);
            assert_eq!(
                value["content_digest"],
                row["selected"]["metadata"]["content_digest"]
            );
        }
        if matches!(case.mode, Mode::Malicious) {
            assert!(requests[2].messages.iter().flat_map(|m| &m.content).any(|b| matches!(b, ContentBlock::ToolResult { result } if result.call_id == "forbidden-shell" && result.is_error)));
        }
    }
}
