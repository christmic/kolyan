//! Public preparation preserves input without registering or executing a Task.

use super::*;
use crate::runner::tests::support::{Harness, Provider, TestExecutor, Tools};
use kolyan_ledger::FactJournal;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    named: bool,
    mutation: Mutation,
    expected_retry_error: bool,
    expected_attempt_error: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    Input,
    Permissions,
    Revision,
    Inventory,
    Execution,
    AttemptSession,
    AttemptTurn,
    AttemptExecution,
}

struct NoProvider(Arc<AtomicUsize>);
impl ProviderFactory for NoProvider {
    type Provider = Provider;
    fn build(
        &self,
        _: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<crate::provider::ContextPreparingProvider<Provider>, RunnerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(RunnerError::Host(
            "preparation must not construct Provider".into(),
        ))
    }
}
struct Inventory {
    tools: Tools,
    changed: Arc<AtomicBool>,
}
impl EnvironmentToolFactory for Inventory {
    type Executor = TestExecutor;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<RunnerToolSet<TestExecutor>, RunnerError> {
        let mut set = self.tools.build(snapshot, execution)?;
        if self.changed.load(Ordering::SeqCst) {
            set.definitions[0]
                .description
                .get_or_insert_default()
                .push_str(" changed trusted inventory");
        }
        Ok(set)
    }
}

#[tokio::test]
async fn public_root_preparation_exports_before_asserting_exact_retry_and_no_execution() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-root-preparation-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("ROOT_PREPARATION_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let harness = Harness::new();
        let builds = Arc::new(AtomicUsize::new(0));
        let inventory_changed = Arc::new(AtomicBool::new(false));
        let runner = Arc::new(
            AgentRunner::new(
                harness.service.clone(),
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                harness.runner.catalog.clone(),
                harness.runner.host.clone(),
                (
                    NoProvider(builds.clone()),
                    Inventory {
                        tools: Tools(harness.observations.clone(), false),
                        changed: inventory_changed.clone(),
                    },
                ),
                harness.runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        let original = harness.request(&case.id, case.named);
        let first = runner
            .prepare_root_input(RootInputPreparationRequest {
                task_id: original.task_id.clone(),
                invocation_id: original.invocation_id.clone(),
                execution: original.execution.clone(),
                selector: original.selector,
                requested_permissions: original.requested_permissions,
                model_request: original.turn.model_request.clone(),
            })
            .await
            .unwrap();
        let mut retry = harness.request(&case.id, case.named);
        match case.mutation {
            Mutation::None => {}
            Mutation::AttemptSession | Mutation::AttemptTurn | Mutation::AttemptExecution => {}
            Mutation::Input => retry.turn.model_request.system[0]
                .text
                .push_str(" changed actual input"),
            Mutation::Permissions => retry.requested_permissions.tools.clear(),
            Mutation::Revision => {
                let mut definition: crate::AgentDefinitionInput =
                    first.snapshot.definition().clone().into();
                definition.revision = "different-revision".into();
                retry.selector =
                    AgentSelector::Inline(crate::AgentDefinition::new(definition).unwrap());
            }
            Mutation::Inventory => inventory_changed.store(true, Ordering::SeqCst),
            Mutation::Execution => retry.execution.execution_id.push_str("-different"),
        }
        let second = runner
            .prepare_root_input(RootInputPreparationRequest {
                task_id: retry.task_id,
                invocation_id: retry.invocation_id,
                execution: retry.execution,
                selector: retry.selector,
                requested_permissions: retry.requested_permissions,
                model_request: retry.turn.model_request,
            })
            .await;
        let source = &first.input_source;
        let saved = harness
            .bindings
            .load(&case.id, "root", "session")
            .unwrap()
            .unwrap();
        let typed = kolyan_server::InvocationInputSource::Standalone {
            fact: source.reference.clone(),
        };
        let mut attempt = kolyan_server::AttemptBinding {
            attempt_id: "attempt".into(),
            invocation_id: "root".into(),
            execution: original.execution.clone(),
            agent: first.snapshot.identity().clone(),
            constraints_digest: first.snapshot.digest().into(),
            input_source: typed.clone(),
        };
        match case.mutation {
            Mutation::AttemptSession => attempt.execution.session_id.push_str("-foreign"),
            Mutation::AttemptTurn => attempt.execution.turn_id.push_str("-foreign"),
            Mutation::AttemptExecution => attempt.execution.execution_id.push_str("-foreign"),
            _ => {}
        }
        let attempt_error = runner
            .verify_root_attempt_source(&saved, &attempt)
            .err()
            .map(|error| error.to_string());
        let (_, document): (_, crate::runner::input::RootInput) =
            harness.runner.load_input_document(&saved, &typed).unwrap();
        let row = json!({
            "id": case.id,
            "source": source,
            "document": document,
            "original_input": original.turn.model_request,
            "selected_input": first.selected_input,
            "retry_error": second.as_ref().err().map(ToString::to_string),
            "retry_source": second.as_ref().ok().map(|result| &result.input_source),
            "task_facts": harness.service.coordinator().journal().read(&case.id,0,100).unwrap(),
            "requests": harness.observations.requests.lock().unwrap().clone(),
            "effects": harness.observations.effects.lock().unwrap().clone(),
            "provider_builds": builds.load(Ordering::SeqCst),
            "attempt": attempt,
            "attempt_error": attempt_error,
        });
        writeln!(output, "{row}").unwrap();
        output.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), cases.len());
    for (line, case) in actual.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(
            row["retry_error"].is_string(),
            case.expected_retry_error,
            "{row}"
        );
        if !case.expected_retry_error {
            assert_eq!(row["source"], row["retry_source"]);
        }
        assert_eq!(row["document"]["original_input"], row["original_input"]);
        assert_eq!(row["document"]["selected_input"], row["selected_input"]);
        assert_eq!(row["task_facts"], json!([]));
        assert_eq!(row["requests"], json!([]));
        assert_eq!(row["effects"], json!([]));
        assert_eq!(row["provider_builds"], 0);
        assert_eq!(
            row["attempt_error"].is_string(),
            case.expected_attempt_error,
            "{row}"
        );
    }
}
