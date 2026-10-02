//! Inject read corruption only after real completion. A rebuilt Runner must
//! refuse closure without another model/tool call or coordination write.

use super::super::*;
use crate::runner::tests::support::{Harness, Providers, Tools};
use kolyan_ledger::{FactDraft, FactError, FactRecord, MemoryFactJournal};
use kolyan_server::{
    ExecutionService, SessionExecutionService, TaskCoordinator, TaskExecutionService,
};
use kolyan_trace::NoopTraceSink;
use serde_json::{Value, json};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    fault: Fault,
    expected_error: bool,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Fault {
    None,
    Missing,
    Substituted,
    Foreign,
    UnknownBody,
    UnknownDocument,
    OriginalInput,
    Schema,
    Noncritical,
    OptionalArtifact,
    MissingArtifact,
}

#[derive(Clone)]
pub(in crate::runner) struct Journal {
    inner: MemoryFactJournal,
    target: FactRef,
    replacement: Option<FactRecord>,
    missing: bool,
    writes: Arc<AtomicUsize>,
}
impl Journal {
    pub(in crate::runner) fn injected(
        inner: MemoryFactJournal,
        target: FactRef,
        replacement: FactRecord,
    ) -> Self {
        Self {
            inner,
            target,
            replacement: Some(replacement),
            missing: false,
            writes: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub(in crate::runner) fn write_count(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }
}
impl FactJournal for Journal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        if stream == self.target.stream_id && self.missing {
            return Ok(Vec::new());
        }
        let mut rows = self.inner.read(stream, after, limit)?;
        for row in &mut rows {
            if row.stream_id == self.target.stream_id
                && row.position == self.target.position
                && let Some(replacement) = &self.replacement
            {
                *row = replacement.clone();
            }
        }
        Ok(rows)
    }
    fn append(
        &self,
        stream: &str,
        expected: u64,
        facts: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.append(stream, expected, facts)
    }
}

#[tokio::test]
async fn reconstructed_source_refusals_export_all_evidence_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("refusals.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-agent-source-refusals-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("AGENT_SOURCE_REFUSAL_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let harness = Harness::new();
        let completed = harness
            .runner
            .start(harness.request(&case.id, false))
            .await
            .unwrap();
        let original = harness.service.coordinator().journal().clone();
        let snapshot = original.read(&case.id, 0, 1024).unwrap();
        let source = &completed.task.invocations["root"].definition.input_source;
        let target = source.fact().clone();
        let source_record = original
            .read(&target.stream_id, target.position - 1, 1)
            .unwrap()
            .remove(0);
        let mut replacement = source_record.clone();
        match case.fault {
            Fault::None | Fault::Missing => {}
            Fault::Substituted => {
                replacement.draft.fact_id = format!("{}-substituted", replacement.draft.fact_id)
            }
            Fault::Foreign => replacement.draft.payload["scope"]["task_id"] = json!("foreign-task"),
            Fault::UnknownBody => replacement.draft.payload["body"]["unknown"] = json!(true),
            Fault::Schema => replacement.draft.schema_version = 2,
            Fault::Noncritical => replacement.draft.critical = false,
            Fault::OptionalArtifact => {
                replacement.draft.payload["body"]["artifact"]["retention"] = json!("optional")
            }
            Fault::MissingArtifact => {
                replacement.draft.payload["body"]["artifact"]["digest"] = json!("0".repeat(64))
            }
            Fault::UnknownDocument | Fault::OriginalInput => {
                let body: InputBody =
                    serde_json::from_value(replacement.draft.payload["body"].clone()).unwrap();
                let bytes = harness
                    .runner
                    .input_artifacts
                    .read(&body.artifact, MAX_INPUT_DOCUMENT_BYTES as u64)
                    .unwrap();
                let mut document: Value = serde_json::from_slice(&bytes).unwrap();
                let before_document = document.clone();
                match case.fault {
                    Fault::UnknownDocument => document["unknown"] = json!(true),
                    Fault::OriginalInput => {
                        document["original_input"]["messages"][0]["content"][0]["text"] =
                            json!(case.id)
                    }
                    _ => unreachable!(),
                }
                assert_ne!(
                    before_document, document,
                    "mutation must change actual evidence"
                );
                let artifact = archive(harness.runner.input_artifacts.as_ref(), &document).unwrap();
                replacement.draft.payload["body"] =
                    serde_json::to_value(InputBody { artifact }).unwrap();
            }
        }
        let writes = Arc::new(AtomicUsize::new(0));
        let journal = Journal {
            inner: original.clone(),
            target,
            replacement: Some(replacement.clone()),
            missing: matches!(case.fault, Fault::Missing),
            writes: writes.clone(),
        };
        let service = Arc::new(TaskExecutionService::new(
            TaskCoordinator::new(journal),
            SessionExecutionService::new(
                ExecutionService::new(
                    harness
                        .service
                        .sessions()
                        .execution()
                        .server()
                        .coordinator()
                        .ledger()
                        .clone(),
                    NoopTraceSink,
                ),
                harness.service.sessions().sessions().clone(),
            ),
        ));
        let rebuilt = Arc::new(
            AgentRunner::new(
                service,
                harness.runner.instances.clone(),
                harness.bindings.clone(),
                crate::AgentCatalog::new(8).unwrap(),
                harness.runner.host.clone(),
                (
                    Providers {
                        observations: harness.observations.clone(),
                        fail: false,
                        reject_context: false,
                        call_tool: false,
                    },
                    Tools(harness.observations.clone(), false),
                ),
                harness.runner.input_artifacts.clone(),
            )
            .unwrap(),
        );
        let before_requests = harness.observations.requests.lock().unwrap().clone();
        let before_effects = harness.observations.effects.lock().unwrap().clone();
        let request = crate::TaskFinalizationRequest {
            task_id: case.id.clone(),
            logical_session_id: "session".into(),
            root_invocation_id: "root".into(),
            root_attempt_id: "attempt".into(),
            policy: crate::TaskFinalizationPolicy::AllInvocationsSuccessful,
        };
        let first = rebuilt.finalize_task(request.clone()).await;
        let second = rebuilt.finalize_task(request).await;
        let row = json!({"id":case.id,"original_source":source_record,"read_fault_source":replacement,
            "first_error":first.err().map(|error|error.to_string()),"second_error":second.err().map(|error|error.to_string()),
            "facts_before":snapshot,"facts_after":original.read(&case.id,0,1024).unwrap(),
            "requests_before":before_requests,"requests_after":harness.observations.requests.lock().unwrap().clone(),
            "effects_before":before_effects,"effects_after":harness.observations.effects.lock().unwrap().clone(),"writes":writes.load(Ordering::SeqCst),
            "ledger":harness.service.sessions().execution().server().coordinator().ledger().execution_events_after(&completed.task.attempts["attempt"].binding.execution.execution_id,0).unwrap(),
        });
        writeln!(output, "{row}").unwrap();
        output.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), cases.len());
    for (line, case) in actual.lines().zip(cases) {
        let row: Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["first_error"].is_string(), case.expected_error, "{row}");
        assert_eq!(
            row["second_error"].is_string(),
            case.expected_error,
            "{row}"
        );
        assert_eq!(row["facts_before"], row["facts_after"]);
        assert_eq!(row["requests_before"], row["requests_after"]);
        assert_eq!(row["effects_before"], row["effects_after"]);
        assert_eq!(row["writes"], 0);
    }
}
