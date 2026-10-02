//! Actual parent invoke creates source documents. Read faults target the exact
//! child document/parent chain; no schema error stands in for semantic corruption.

use super::*;
use crate::runner::input::tests::refusals::Journal;
use crate::runner::input::{InputBody, MAX_INPUT_DOCUMENT_BYTES, archive};
use kolyan_ledger::{FactJournal, LedgerStore};
use serde_json::Value;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    call: ToolCall,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    expected_error: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    TargetDefinition,
    ChildRole,
    ParentOwnership,
    ParentAttemptSource,
    ChildIndex,
    BodyUnknown,
}

#[tokio::test]
async fn child_source_semantic_corruption_exports_then_refuses_without_reentry() {
    let dataset: Dataset = serde_json::from_str(include_str!("input_source/cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-agent-child-source-refusals-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("AGENT_CHILD_SOURCE_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &dataset.cases {
        let harness = Harness::new();
        let verifier = Arc::new(AgentChildWaitVerifier::default());
        let service = Arc::new(TaskExecutionService::new(
            TaskCoordinator::new(harness.service.coordinator().journal().clone()),
            SessionExecutionService::new(
                ExecutionService::new(kolyan_ledger::InMemoryLedger::default(), NoopTraceSink)
                    .with_external_wait_verifier(verifier.clone()),
                harness.service.sessions().sessions().clone(),
            ),
        ));
        let mut permissions = harness.runner.host.clone();
        permissions.delegation.allow_self = true;
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "source-parent".into(),
            revision: "1".into(),
            display_name: None,
            model: kolyan_model::ModelRef::new("unit", "pump"),
            instructions: "Source proof fixture".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let factory = provider::Factory {
            observations: harness.observations.clone(),
            call: dataset.call.clone(),
            child_call: None,
            fault: None,
            execution_service: service.sessions().execution().clone(),
        };
        let runner = Arc::new(
            crate::AgentRunner::new(
                service.clone(),
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                harness.runner.catalog.clone(),
                permissions.clone(),
                (factory.clone(), Tools(harness.observations.clone(), false)),
                harness.runner.input_artifacts.clone(),
            )
            .unwrap()
            .with_delegation(AgentDelegationConfig {
                limits: crate::InvokePrepareLimits {
                    max_children: 2,
                    max_parallel: 2,
                    max_child_input_bytes: 1024,
                    max_output_bytes: 65536,
                    admission_timeout_ms: 10000,
                },
                approval: ApprovalMode::Never,
            })
            .unwrap(),
        );
        verifier.attach(&runner).unwrap();
        let mut request = harness.request(&case.id, false);
        request.selector = AgentSelector::Inline(definition);
        request.requested_permissions = permissions;
        request.limits.max_tokens = None;
        request.limits.max_invocations = 3;
        request.limits.max_attempts = 3;
        request.limits.max_steps_per_turn = 3;
        request.turn.config.max_steps = 3;
        let started = runner.start(request).await.unwrap();
        assert!(matches!(
            started.execution,
            kolyan_runtime::DurableTurnResult::Suspended { .. }
        ));
        let child = started
            .task
            .invocations
            .values()
            .filter(|inv| inv.definition.role != kolyan_server::InvocationRole::Root)
            .find(|inv| {
                let saved = harness
                    .bindings
                    .load(&case.id, &inv.definition.invocation_id, "session")
                    .unwrap()
                    .unwrap();
                let (_, document): (_, crate::runner::input::ChildInput) = runner
                    .load_input_document(&saved, &inv.definition.input_source)
                    .unwrap();
                document.child_index == 0
            })
            .unwrap();
        let source = child.definition.input_source.clone();
        let saved = harness
            .bindings
            .load(&case.id, &child.definition.invocation_id, "session")
            .unwrap()
            .unwrap();
        let journal = service.coordinator().journal().clone();
        let original = journal
            .read(&source.fact().stream_id, source.fact().position - 1, 1)
            .unwrap()
            .remove(0);
        let mut replacement = original.clone();
        let body: InputBody =
            serde_json::from_value(original.draft.payload["body"].clone()).unwrap();
        let bytes = runner
            .input_artifacts
            .read(&body.artifact, MAX_INPUT_DOCUMENT_BYTES as u64)
            .unwrap();
        let mut document: Value = serde_json::from_slice(&bytes).unwrap();
        let original_document = document.clone();
        match case.mutation {
            Mutation::None => {}
            Mutation::TargetDefinition => {
                document["intent"]["definition"]["definition_id"] =
                    json!("different-target-definition")
            }
            Mutation::ChildRole => document["intent"]["role"] = json!("Delegation"),
            Mutation::ParentOwnership => {
                let registered = journal.read(&case.id, 0, 1).unwrap().remove(0);
                let foreign = kolyan_ledger::FactRef {
                    stream_id: registered.stream_id,
                    position: registered.position,
                    fact_id: registered.draft.fact_id,
                };
                document["parent_ownership"] = serde_json::to_value(&foreign).unwrap();
                replacement.draft.causes[0] = foreign;
            }
            Mutation::ParentAttemptSource => {
                document["parent"]["parent"]["input_source"] =
                    serde_json::to_value(&source).unwrap()
            }
            Mutation::ChildIndex => document["child_index"] = json!(1),
            Mutation::BodyUnknown => replacement.draft.payload["body"]["unknown"] = json!(true),
        }
        if !matches!(case.mutation, Mutation::None | Mutation::BodyUnknown) {
            assert_ne!(
                original_document, document,
                "semantic mutation must change original evidence"
            );
            let changed = archive(runner.input_artifacts.as_ref(), &document).unwrap();
            replacement.draft.payload["body"] =
                serde_json::to_value(InputBody { artifact: changed }).unwrap();
        }
        let injected =
            Journal::injected(journal.clone(), source.fact().clone(), replacement.clone());
        let rebuilt_service = Arc::new(TaskExecutionService::new(
            TaskCoordinator::new(injected.clone()),
            SessionExecutionService::new(
                ExecutionService::new(
                    service
                        .sessions()
                        .execution()
                        .server()
                        .coordinator()
                        .ledger()
                        .clone(),
                    NoopTraceSink,
                ),
                service.sessions().sessions().clone(),
            ),
        ));
        let rebuilt = crate::AgentRunner::new(
            rebuilt_service,
            runner.instances.clone(),
            runner.bindings.clone(),
            crate::AgentCatalog::new(8).unwrap(),
            runner.host.clone(),
            (factory, Tools(harness.observations.clone(), false)),
            runner.input_artifacts.clone(),
        )
        .unwrap();
        let facts_before = journal.read(&case.id, 0, 1024).unwrap();
        let requests_before = harness.observations.requests.lock().unwrap().clone();
        let effects_before = harness.observations.effects.lock().unwrap().clone();
        let first = rebuilt
            .verify_child_input(&saved, &source)
            .err()
            .map(|error| error.to_string());
        let second = rebuilt
            .verify_child_input(&saved, &source)
            .err()
            .map(|error| error.to_string());
        let row = json!({"id":case.id,"original_source":original,"fault_source":replacement,"original_document":original_document,"fault_document":document,"first_error":first,"second_error":second,"facts_before":facts_before,"facts_after":journal.read(&case.id,0,1024).unwrap(),"requests_before":requests_before,"requests_after":harness.observations.requests.lock().unwrap().clone(),"effects_before":effects_before,"effects_after":harness.observations.effects.lock().unwrap().clone(),"writes":injected.write_count(),"ledger":service.sessions().execution().server().coordinator().ledger().execution_events_after(&started.task.attempts["attempt"].binding.execution.execution_id,0).unwrap()});
        writeln!(output, "{row}").unwrap();
        output.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), dataset.cases.len());
    for (line, case) in actual.lines().zip(dataset.cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["first_error"].is_string(), case.expected_error, "{row}");
        assert_eq!(
            row["second_error"].is_string(),
            case.expected_error,
            "{row}"
        );
        assert_eq!(row["writes"], 0);
        assert_eq!(row["facts_before"], row["facts_after"]);
        assert_eq!(row["requests_before"], row["requests_after"]);
        assert_eq!(row["effects_before"], row["effects_after"]);
    }
}
