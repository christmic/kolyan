//! Actual ArtifactStore/SQLite, policy preparations and grants; no network model.

use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    sync::Arc,
};

use kolyan_core::{ToolExecutor, ToolInvocation, ToolOutcome, TurnControl};
use kolyan_ledger::{FactJournal, SqliteFactJournal};
use kolyan_model::{ModelRef, ToolCall};
use kolyan_policy::{ApprovalEvidence, PolicyContext, PreparedGrant, ToolExecutionScope};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::{
    AgentDefinition, AgentDefinitionInput, AgentInvocationBinding, AgentInvocationBindingStore,
    AgentPermissions, AgentSnapshot, BindingContextKind, SkillAccessPolicy, SkillAccessRuleInput,
    SkillCatalog, SkillDescriptorInput, SkillKey, SkillLimits, SkillScope,
};

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    Execution,
    Snapshot,
    Grant,
    Policy,
    Output,
    Cancel,
    Revoke,
    Historical,
    MissingBody,
    Path,
    Revision,
    Tool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    expected: String,
}

fn outcome(result: Result<ToolOutcome, ToolError>) -> Value {
    match result {
        Ok(ToolOutcome::Completed(result)) => {
            json!({"kind":"completed","result":result,"complete_serialized_bytes":serde_json::to_vec(&result).unwrap().len()})
        }
        Ok(_) => json!({"kind":"unexpected_wait"}),
        Err(error) => {
            json!({"kind":match &error { ToolError::PolicyDenied { .. } => "denied", ToolError::Failed { .. } => "failed",
            ToolError::Cancelled => "cancelled", ToolError::Unavailable { .. } => "unavailable", _ => "other" },"error":error.to_string()})
        }
    }
}

async fn run(case: &Case, root: &std::path::Path) -> Value {
    fs::create_dir_all(root).unwrap();
    let journal = Arc::new(SqliteFactJournal::open(root.join("facts.sqlite")).unwrap());
    let artifacts_root = root.join("artifacts");
    let artifacts = Arc::new(
        kolyan_trace::ArtifactStore::new(&artifacts_root, super::super::MAX_BODY_BYTES as u64)
            .unwrap(),
    );
    let catalog = SkillCatalog::new(
        journal.clone(),
        artifacts,
        "executor.host".into(),
        SkillLimits::default(),
    )
    .unwrap();
    let body = "Exact UTF-8 🦀\n\u{0}\t\"tail\"\n\n";
    let selected = catalog
        .register(
            SkillDescriptorInput {
                key: SkillKey::new("guide".into(), "1".into()).unwrap(),
                title: "guide".into(),
                description: "Knowledge not authority".into(),
            },
            body,
        )
        .unwrap();
    let permissions = AgentPermissions::default();
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "agent".into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef::new("fixture", "not-opened"),
        instructions: "fixture".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let saved = AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: "root".into(),
        logical_session_id: "session".into(),
        private_session_id: "session".into(),
        context_kind: BindingContextKind::Root,
        snapshot: AgentSnapshot::new(definition.clone(), "instance".into(), permissions).unwrap(),
    };
    let ownership = AgentInvocationBindingStore::new(journal.clone())
        .save(&saved)
        .unwrap();
    let runtime = Arc::new(SkillRuntime::new(
        catalog.clone(),
        SkillAccessPolicy::new(
            "acl".into(),
            "1".into(),
            vec![SkillAccessRuleInput {
                agent: definition.key(),
                logical_session_id: "session".into(),
                task_id: Some("task".into()),
                invocation_id: Some("root".into()),
                skills: [selected.metadata().descriptor().key.clone()].into(),
            }],
        )
        .unwrap(),
    ));
    let ad = runtime
        .discover(&saved.snapshot, &SkillScope::from_binding(&saved).unwrap())
        .unwrap();
    let binding = runtime.bind(&ad, &ownership).unwrap();
    let execution = ExecutionRef {
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "execution".into(),
    };
    let mut policy = PolicyEngine::default();
    policy.register(super::super::skill_load_manifest());
    let policy = Arc::new(policy);
    let executor = SkillExecutor::new(
        runtime.clone(),
        binding.clone(),
        execution.clone(),
        policy.clone(),
    )
    .unwrap();
    let mut call = ToolCall {
        id: "call-\"escaped".into(),
        name: SKILL_LOAD_NAME.into(),
        arguments: json!({"skill_id":"guide","revision":"1", "content_digest":selected.metadata().content_digest()}),
    };
    match case.mutation {
        Mutation::Path => call.arguments["path"] = json!("/unauthorized"),
        Mutation::Revision => call.arguments["revision"] = json!("unknown"),
        Mutation::Tool => call.name = "unknown".into(),
        _ => {}
    }
    if matches!(case.mutation, Mutation::MissingBody) {
        fs::remove_file(artifacts_root.join(&selected.metadata().body().digest)).unwrap();
    }
    let prepared = executor.prepare(call.clone()).await;
    let mut row = json!({"id":case.id,"body":body,"input":call,"binding":binding,"selected":selected,"ownership":ownership,
        "prepared":prepared.as_ref().ok(),"policy_revision":policy.revision(),"before":journal.read(catalog.stream_id(),0,1024).unwrap()});
    let result = match prepared {
        Err(error) => outcome(Err(error)),
        Ok(prepared) => {
            if matches!(case.mutation, Mutation::MissingBody) {
                json!({"kind":"prepared"})
            } else {
                let mut scope = ToolExecutionScope {
                    execution: kolyan_runtime::ExecutionKey {
                        session_id: execution.session_id.clone(),
                        turn_id: execution.turn_id.clone(),
                        execution_id: execution.execution_id.clone(),
                    },
                    step_id: "turn-step-0".into(),
                    agent_snapshot_digest: Some(saved.snapshot.digest().into()),
                };
                let mut decision = policy.decide_prepared(&prepared, &PolicyContext::default());
                if matches!(case.mutation, Mutation::Output) {
                    decision.constraints.max_output_bytes = Some(128);
                }
                let mut grant_scope = scope.clone();
                if matches!(case.mutation, Mutation::Grant) {
                    grant_scope.execution.execution_id = "foreign".into();
                }
                let grant = PreparedGrant::issue(
                    &prepared,
                    decision,
                    ApprovalEvidence::NotConfirmed,
                    grant_scope,
                )
                .unwrap();
                let mut policy_revision = policy.revision();
                let control = TurnControl::default();
                match case.mutation {
                    Mutation::Execution => scope.execution.execution_id = "foreign".into(),
                    Mutation::Snapshot => scope.agent_snapshot_digest = Some("0".repeat(64)),
                    Mutation::Policy => policy_revision = "changed.policy".into(),
                    Mutation::Cancel => control.cancel(),
                    Mutation::Revoke | Mutation::Historical => {
                        catalog
                            .revoke(
                                &selected.metadata().descriptor().key,
                                selected.reference(),
                                "revoke.1",
                                "host revoked",
                            )
                            .unwrap();
                    }
                    _ => {}
                }
                row["grant"] = json!(grant);
                row["scope"] = json!(scope);
                if matches!(case.mutation, Mutation::Historical) {
                    let current = runtime
                        .validate_current(&binding)
                        .err()
                        .map(|error| error.to_string());
                    match SkillExecutor::new(runtime.clone(), binding, execution, policy) {
                        Ok(_) => json!({"kind":"historical","current_error":current}),
                        Err(error) => json!({"kind":"unexpected","error":error.to_string()}),
                    }
                } else {
                    outcome(
                        executor
                            .execute_invocation(ToolInvocation {
                                prepared,
                                grant,
                                scope,
                                policy_revision,
                                control,
                            })
                            .await,
                    )
                }
            }
        }
    };
    row["outcome"] = result;
    row["after"] = json!(journal.read(catalog.stream_id(), 0, 1024).unwrap());
    row
}

#[tokio::test]
async fn skill_adapter_scope_grant_limits_and_historical_restore_data_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skills-adapter-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut file = File::create(&path).unwrap();
    for case in &cases {
        serde_json::to_writer(&mut file, &run(case, &root.join(&case.id)).await).unwrap();
        writeln!(file).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    println!("SKILLS_ADAPTER_ACTUAL={}", path.display());
    let actual: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(actual.len(), cases.len());
    for (row, case) in actual.iter().zip(&cases) {
        assert_eq!(
            row["outcome"]["kind"], case.expected,
            "{}: {}",
            case.id, row["outcome"]
        );
        if matches!(case.mutation, Mutation::Historical) {
            assert!(row["outcome"]["current_error"].is_string());
        }
        if matches!(case.mutation, Mutation::None) {
            let loaded: Value =
                serde_json::from_str(row["outcome"]["result"]["content"].as_str().unwrap())
                    .unwrap();
            assert_eq!(loaded["body"], row["body"]);
            assert!(
                row["outcome"]["complete_serialized_bytes"]
                    .as_u64()
                    .unwrap()
                    <= MAX_TOOL_RESULT_BYTES as u64
            );
        }
    }
}
