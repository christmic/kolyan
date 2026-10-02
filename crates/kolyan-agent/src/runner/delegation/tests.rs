//! Real Task/Session/Runtime child driving with a module-only scripted Provider.

mod provider;

use std::fs::File;
use std::io::{BufWriter, Write};

use kolyan_core::TurnControl;
use kolyan_ledger::{LedgerEvent, LedgerEventKind};
use kolyan_model::{ModelRef, ToolCall};
use kolyan_policy::{ApprovalEvidence, ApprovalMode, PreparedGrant};
use kolyan_server::{
    CancellationPolicy, InstanceOwner, InvocationRole, TaskDefinition, TaskLimits,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::runner::tests::support::*;
use crate::{
    AgentDefinition, AgentDefinitionInput, AgentInvocationBinding, AgentPermissions, AgentSnapshot,
    BindingContextKind, agent_invoke_manifest, prepare_agent_invocation,
};
use provider::{Concurrency, Script, ScriptFactory};
use std::sync::atomic::Ordering;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    targets: Vec<Target>,
    parallel: bool,
    mutation: Mutation,
    max_invocations: u64,
    allowed: bool,
    model_outcome: Script,
    #[serde(default)]
    slots: Option<usize>,
    #[serde(default)]
    attested: bool,
    #[serde(default)]
    token_ceiling: Option<u64>,
    #[serde(default)]
    barrier: Option<usize>,
    #[serde(default = "default_parallel_bound")]
    prepared_parallel_bound: usize,
    #[serde(default = "default_peak")]
    expected_peak: usize,
}
fn default_parallel_bound() -> usize {
    2
}
fn default_peak() -> usize {
    1
}

struct ReadAttestedTools(Tools, bool);
impl EnvironmentToolFactory for ReadAttestedTools {
    type Executor = TestExecutor;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &kolyan_server::ExecutionRef,
    ) -> Result<crate::RunnerToolSet<TestExecutor>, crate::RunnerError> {
        self.0.build(snapshot, execution)
    }
    fn enforces_read_only_parallel(&self, snapshot: &AgentSnapshot) -> bool {
        self.1
            && snapshot
                .permissions()
                .tools
                .iter()
                .all(|tool| *tool == crate::EnvironmentTool::Read)
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Target {
    Named,
    Inline,
    #[serde(rename = "self")]
    SelfCall,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    Scope,
    Limits,
    Cancel,
}

#[tokio::test]
async fn admission_and_actual_child_drive_export_then_compare() {
    let mut cases: Vec<Case> = serde_json::from_str(include_str!("tests/admission.json")).unwrap();
    cases.extend(serde_json::from_str::<Vec<Case>>(include_str!("tests/parallel.json")).unwrap());
    let mut rows = Vec::new();
    for case in &cases {
        let harness = Harness::new();
        let parent_sentinel = format!("Parent-only history for {}", case.id);
        let mut history = harness.request("parent-history", false);
        history.turn.model_request.messages[0].content = vec![kolyan_model::ContentBlock::Text {
            text: parent_sentinel.clone(),
        }];
        harness.runner.start(history).await.unwrap();
        let parent_history = harness
            .service
            .sessions()
            .sessions()
            .load("session")
            .unwrap();
        harness.observations.requests.lock().unwrap().clear();
        let template_source = harness.request("template", false).turn;
        let concurrency = Arc::new(Concurrency {
            active: Default::default(),
            peak: Default::default(),
            barrier: case.barrier.map(tokio::sync::Barrier::new),
        });
        let mut runner = Arc::new(
            crate::AgentRunner::new(
                harness.service.clone(),
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                harness.runner.catalog.clone(),
                harness.runner.host.clone(),
                (
                    ScriptFactory {
                        observations: harness.observations.clone(),
                        script: case.model_outcome,
                        execution: harness.service.sessions().execution().clone(),
                        concurrency: concurrency.clone(),
                    },
                    ReadAttestedTools(Tools(harness.observations.clone(), false), case.attested),
                ),
                harness.runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        let task_id = case.id.clone();
        let child_permissions = AgentPermissions {
            tools: [crate::EnvironmentTool::Read].into(),
            ..Default::default()
        };
        let child = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "child".into(),
            revision: "1".into(),
            display_name: None,
            model: ModelRef::new("unit", "child-model"),
            instructions: "Child instruction only".into(),
            permissions: child_permissions.clone(),
        })
        .unwrap();
        let mut parent_permissions = child_permissions.clone();
        parent_permissions.delegation.allow_self = true;
        parent_permissions.delegation.allow_inline = true;
        parent_permissions
            .delegation
            .named_targets
            .insert(child.key());
        let parent_definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "parent".into(),
            revision: "1".into(),
            display_name: None,
            model: ModelRef::new("unit", "parent-model"),
            instructions: "Saved parent instructions".into(),
            permissions: parent_permissions.clone(),
        })
        .unwrap();
        let mutable = Arc::get_mut(&mut runner).unwrap();
        if let Some(bound) = case.slots {
            mutable.execution_budget = crate::AgentExecutionBudget::new(bound).unwrap();
        }
        mutable.host = parent_permissions.clone();
        mutable.catalog.register(child.clone()).unwrap();
        let instance = mutable
            .instances
            .reserve(InstanceOwner {
                logical_session_id: "session".into(),
                task_id: task_id.clone(),
                invocation_id: "parent".into(),
            })
            .unwrap();
        let snapshot =
            AgentSnapshot::new(parent_definition, instance.instance_id, parent_permissions)
                .unwrap();
        let saved = AgentInvocationBinding {
            task_id: task_id.clone(),
            invocation_id: "parent".into(),
            logical_session_id: "session".into(),
            private_session_id: "session".into(),
            context_kind: BindingContextKind::Root,
            snapshot: snapshot.clone(),
        };
        let ownership = mutable.bindings.save(&saved).unwrap();
        let original_input = template_source.model_request.clone();
        let inventory = mutable
            .routed_tool_set(&snapshot, &execution(&task_id), None)
            .unwrap()
            .definitions;
        let mut selected_input = original_input.clone();
        selected_input.model = snapshot.definition().model().clone();
        selected_input.system.push(kolyan_model::SystemInstruction {
            text: snapshot.definition().instructions().into(),
            cache: false,
        });
        selected_input.tools = inventory
            .iter()
            .filter(|definition| {
                definition.name == crate::AGENT_INVOKE_NAME
                    || crate::runner::tools::permits(&snapshot, &definition.name)
            })
            .cloned()
            .collect();
        let source = mutable
            .publish_input_document(
                &saved,
                kolyan_server::InvocationInputKind::Standalone,
                &crate::runner::input::RootInput {
                    skill_binding: None,
                    ownership: ownership.clone(),
                    execution: execution(&task_id),
                    requested_permissions: snapshot.permissions().clone(),
                    original_input,
                    selected_input,
                    inventory,
                },
                vec![ownership],
            )
            .unwrap();
        let input_source = kolyan_server::InvocationInputSource::Standalone {
            fact: source.reference,
        };
        let parent = AttemptBinding {
            attempt_id: "parent-attempt".into(),
            invocation_id: "parent".into(),
            execution: execution(&task_id),
            agent: snapshot.identity().clone(),
            constraints_digest: snapshot.digest().into(),
            input_source: input_source.clone(),
        };
        let coordinator = mutable.service.coordinator();
        coordinator
            .register_task(
                &format!("{task_id}/register"),
                TaskDefinition {
                    task_id: task_id.clone(),
                    objective: "Actual child execution".into(),
                    agent: parent.agent.clone(),
                    constraints_digest: parent.constraints_digest.clone(),
                    criteria: vec![kolyan_server::CompletionCriterion::ExecutionCompleted {
                        id: "parent-final".into(),
                        invocation_id: "parent".into(),
                    }],
                    limits: TaskLimits {
                        max_depth: 2,
                        max_invocations: case.max_invocations,
                        max_attempts: 5,
                        max_tokens: case.token_ceiling,
                        max_steps_per_turn: 3,
                    },
                    cancellation_policy: CancellationPolicy::AllInvocations,
                },
            )
            .unwrap();
        coordinator
            .admit_invocation(
                &task_id,
                &format!("{task_id}/parent-admitted"),
                kolyan_server::InvocationDefinition {
                    invocation_id: "parent".into(),
                    agent: parent.agent.clone(),
                    constraints_digest: parent.constraints_digest.clone(),
                    role: InvocationRole::Root,
                    parent_invocation_id: None,
                    dependencies: vec![],
                    input_source,
                },
            )
            .unwrap();
        coordinator
            .start_attempt(
                &task_id,
                &format!("{task_id}/parent-started"),
                parent.clone(),
            )
            .unwrap();
        // Explicit host orchestration fixture: the parent is admitted but not a
        // scripted automatic agent.invoke loop. Children use actual service runs.
        let id = format!("{}/task-binding", parent.execution.execution_id);
        harness.service.sessions().execution().server().coordinator().ledger().append(LedgerEvent {
            event_id:id.clone(),idempotency_key:id,cursor:0,
            turn_id:parent.execution.turn_id.clone(),execution_id:parent.execution.execution_id.clone(),kind:LedgerEventKind::ExecutionBound,
            payload:json!({"binding":kolyan_runtime::ExecutionBinding{task_id:task_id.clone(),invocation_id:parent.invocation_id.clone(),attempt_id:parent.attempt_id.clone(),session_id:parent.execution.session_id.clone(),turn_id:parent.execution.turn_id.clone(),execution_id:parent.execution.execution_id.clone()}}),
        }).unwrap();
        let owner = DelegationOwner {
            task_id:task_id.clone(),logical_session_id:"session".into(),parent:parent.clone(),
            scope:serde_json::from_value(json!({"execution":{"session_id":parent.execution.session_id,"turn_id":parent.execution.turn_id,"execution_id":parent.execution.execution_id},"step_id":"parent-step","agent_snapshot_digest":snapshot.digest()})).unwrap(),
        };
        let call = ToolCall {
            id: "invoke-call".into(),
            name: crate::AGENT_INVOKE_NAME.into(),
            arguments: json!({"parallel":case.parallel,"children":case.targets.iter().enumerate().map(|(index,target)| {
            let target = match target {
                Target::Named => json!({"kind":"named","value":child.key()}),
                Target::Inline => json!({"kind":"inline","value":child}),
                Target::SelfCall => json!({"kind":"self_call"}),
            };
            json!({"target":target,"input":format!("Explicit private input {index}"),"permissions":child_permissions})
        }).collect::<Vec<_>>()}),
        };
        let mut limits = InvokePrepareLimits {
            max_children: 8,
            max_parallel: case.prepared_parallel_bound,
            max_child_input_bytes: 4096,
            max_output_bytes: 32768,
            admission_timeout_ms: 10000,
        };
        let prepared = prepare_agent_invocation(
            call.clone(),
            &saved,
            &parent.execution,
            &runner.catalog,
            &runner.host,
            &limits,
        )
        .unwrap();
        let mut policy = PolicyEngine::default();
        policy.register(agent_invoke_manifest(ApprovalMode::Never));
        let policy = Arc::new(policy);
        let grant = PreparedGrant::issue(
            prepared.prepared(),
            policy.decide_prepared(prepared.prepared(), &Default::default()),
            ApprovalEvidence::NotConfirmed,
            owner.scope.clone(),
        )
        .unwrap();
        let issued = IssuedToolAuthority {
            prepared: prepared.prepared().clone(),
            grant: grant.clone(),
            scope: owner.scope.clone(),
            policy_revision: policy.revision(),
        };
        let control = TurnControl::default();
        let mut effect_scope = owner.scope.clone();
        match case.mutation {
            Mutation::None => {}
            Mutation::Scope => effect_scope.execution.execution_id = "foreign-execution".into(),
            Mutation::Limits => limits.max_output_bytes = 16384,
            Mutation::Cancel => control.cancel(),
        }
        let before = runner
            .recover_agent_child_wait(owner.clone(), issued.clone())
            .await
            .unwrap();
        let result = runner
            .admit_agent_children(
                owner.clone(),
                ToolInvocation {
                    prepared: issued.prepared.clone(),
                    grant,
                    scope: effect_scope,
                    policy_revision: policy.revision(),
                    control,
                    window: kolyan_core::ToolExecutionWindow::at_deadline(
                        std::time::Instant::now() + std::time::Duration::from_secs(30),
                    ),
                },
                limits,
                policy,
            )
            .await;
        let mut driven = Vec::new();
        let mut repeated = Vec::new();
        let mut recovery = None;
        let mut errors = Vec::new();
        let mut consumed = None;
        let mut consumed_again = None;
        let outcome = match result {
            Err(error) => json!({"allowed":false,"error":error.to_string()}),
            Ok(wait) => {
                let verified = match runner
                    .verify_agent_child_wait(owner.clone(), issued.clone(), wait.clone())
                    .await
                {
                    Ok(children) => json!(children),
                    Err(error) => {
                        errors.push(error.to_string());
                        json!({"error":error.to_string()})
                    }
                };
                recovery = match runner
                    .recover_agent_child_wait(owner.clone(), issued.clone())
                    .await
                {
                    Ok(wait) => wait,
                    Err(error) => {
                        errors.push(error.to_string());
                        None
                    }
                };
                let mut template = template_source;
                template.model_request.messages.clear();
                template.model_request.max_output_tokens = Some(50);
                let first = match runner
                    .drive_agent_children(
                        owner.clone(),
                        issued.clone(),
                        wait.clone(),
                        template.clone(),
                    )
                    .await
                {
                    Ok(results) => results,
                    Err(error) => {
                        errors.push(error.to_string());
                        Vec::new()
                    }
                };
                for item in first {
                    match item {
                        AgentChildDriveResult::Terminal {
                            result,
                            dispatch_error,
                        } => {
                            driven.push(json!({"terminal":result,"dispatch_error":dispatch_error}))
                        }
                        AgentChildDriveResult::Waiting { child, execution } => {
                            driven.push(
                                json!({"waiting":child,"execution":format!("{execution:?}")}),
                            );
                        }
                    }
                }
                let replay = match runner
                    .drive_agent_children(owner.clone(), issued.clone(), wait.clone(), template)
                    .await
                {
                    Ok(results) => results,
                    Err(error) => {
                        errors.push(error.to_string());
                        Vec::new()
                    }
                };
                for item in replay {
                    match item {
                        AgentChildDriveResult::Terminal {
                            result,
                            dispatch_error,
                        } => repeated
                            .push(json!({"terminal":result,"dispatch_error":dispatch_error})),
                        AgentChildDriveResult::Waiting { child, execution } => {
                            repeated.push(
                                json!({"waiting":child,"execution":format!("{execution:?}")}),
                            );
                        }
                    }
                }
                consumed = Some(
                    match runner
                        .consume_agent_children(owner.clone(), issued.clone(), wait.clone())
                        .await
                    {
                        Ok(result) => json!({"result":result}),
                        Err(error) => {
                            errors.push(error.to_string());
                            json!({"error":error.to_string()})
                        }
                    },
                );
                consumed_again = Some(
                    match runner
                        .consume_agent_children(owner.clone(), issued.clone(), wait.clone())
                        .await
                    {
                        Ok(result) => json!({"result":result}),
                        Err(error) => {
                            errors.push(error.to_string());
                            json!({"error":error.to_string()})
                        }
                    },
                );
                json!({"allowed":true,"wait":wait,"verified":verified})
            }
        };
        let state = runner.service.coordinator().snapshot(&task_id).unwrap();
        rows.push(json!({"fixture_id":case.id,"input":call,"peak_inflight":concurrency.peak.load(Ordering::SeqCst),"active_after_join":concurrency.active.load(Ordering::SeqCst),"parent_history":parent_history,"parent_sentinel":parent_sentinel,"owner":owner,"issued":issued,"before":before,"outcome":outcome,"errors":errors,"recovery":recovery,"driven":driven,"repeated":repeated,"consumed":consumed,"consumed_again":consumed_again,"task":state,"requests":*harness.observations.requests.lock().unwrap(),"facts":runner.service.coordinator().journal().read(&task_id,0,1024).unwrap()}));
    }
    let root = tempfile::Builder::new()
        .prefix("kolyan-agent-children-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut output = BufWriter::new(File::create(&path).unwrap());
    for row in &rows {
        serde_json::to_writer(&mut output, row).unwrap();
        writeln!(output).unwrap();
    }
    output.flush().unwrap();
    println!("AGENT_CHILD_ADMISSION_TRACE={}", path.display());
    let actual: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual, rows);
    for (case, row) in cases.iter().zip(actual) {
        assert!(row["before"].is_null());
        assert_eq!(
            row["outcome"]["allowed"], case.allowed,
            "{}: {row}",
            case.id
        );
        if case.allowed {
            assert_eq!(
                row["peak_inflight"], case.expected_peak,
                "{}: {row}",
                case.id
            );
            assert_eq!(row["active_after_join"], 0);
            assert!(
                serde_json::to_string(&row["parent_history"])
                    .unwrap()
                    .contains(row["parent_sentinel"].as_str().unwrap())
            );
            assert!(
                row["errors"].as_array().unwrap().is_empty(),
                "{}: {row}",
                case.id
            );
            assert_eq!(
                row["requests"].as_array().unwrap().len(),
                case.targets.len()
            );
            if case.model_outcome == Script::Completed {
                assert_eq!(row["driven"], row["repeated"]);
            }
            for (first, repeated) in row["driven"]
                .as_array()
                .unwrap()
                .iter()
                .zip(row["repeated"].as_array().unwrap())
            {
                assert_eq!(first["terminal"], repeated["terminal"]);
            }
            assert_eq!(row["consumed"], row["consumed_again"]);
            assert_eq!(
                row["consumed"]["result"]["is_error"],
                case.model_outcome != Script::Completed
            );
            assert_eq!(row["recovery"], row["outcome"]["wait"]);
            assert_eq!(row["driven"].as_array().unwrap().len(), case.targets.len());
            for result in row["driven"].as_array().unwrap() {
                assert_eq!(
                    result["terminal"]["outcome"]["status"],
                    serde_json::to_value(case.model_outcome).unwrap()
                );
            }
            let requests = row["requests"].as_array().unwrap();
            for index in 0..case.targets.len() {
                let request = if case.expected_peak > 1 {
                    let request_id = format!(
                        "{}-step-0",
                        row["driven"][index]["terminal"]["binding"]["execution"]["turn_id"]
                            .as_str()
                            .unwrap()
                    );
                    let matching: Vec<_> = requests
                        .iter()
                        .filter(|request| request["request_id"] == request_id)
                        .collect();
                    assert_eq!(
                        matching.len(),
                        1,
                        "exact child request identity: {}",
                        case.id
                    );
                    matching[0]
                } else {
                    &requests[index]
                };
                assert!(
                    !serde_json::to_string(request)
                        .unwrap()
                        .contains(row["parent_sentinel"].as_str().unwrap())
                );
                assert_eq!(request["messages"].as_array().unwrap().len(), 1);
                assert_eq!(
                    request["messages"][0]["content"].as_array().unwrap().len(),
                    1
                );
                assert_eq!(
                    request["messages"][0]["content"][0]["text"],
                    format!("Explicit private input {index}")
                );
            }
        } else {
            assert!(row["requests"].as_array().unwrap().is_empty());
            assert_eq!(row["task"]["invocations"].as_object().unwrap().len(), 1);
        }
    }
}
