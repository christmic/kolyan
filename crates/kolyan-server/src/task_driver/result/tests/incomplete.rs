//! Completed lifecycle notifications do not upgrade an unsuccessful Turn.

use super::*;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    reason: String,
    notification: bool,
    conflict: Option<String>,
    accepted: bool,
}

fn fixture(
    row: &Row,
) -> (
    TaskCoordinator<MemoryFactJournal>,
    InMemoryLedger,
    AttemptBinding,
) {
    let original_journal = MemoryFactJournal::default();
    let (original, source, binding) = case(original_journal.clone(), false);
    let ledger = InMemoryLedger::default();
    for mut event in source.events_after(0).unwrap() {
        if event.kind == LedgerEventKind::StepCompleted {
            let mut step: kolyan_core::StepResult =
                serde_json::from_value(event.payload["step"].clone()).unwrap();
            step.outcome = if row.reason == "Refused" {
                kolyan_core::StepOutcome::Refused
            } else {
                kolyan_core::StepOutcome::Incomplete
            };
            step.response.stop_reason = StopReason::MaxOutputTokens;
            event.payload = json!({"step_id":step.step_id,"step":step});
        } else if event.event_id == "terminal" {
            event.kind = LedgerEventKind::TurnCompleted;
            event.payload = json!({"reason":row.reason});
        }
        event.cursor = 0;
        ledger.append(event).unwrap();
    }
    if row.notification {
        append(
            &ledger,
            "notification",
            LedgerEventKind::TurnCompleted,
            json!({"outcome":format!("{} {{ response: diagnostic only }}", row.reason)}),
        );
    }
    match row.conflict.as_deref() {
        None => (),
        Some("success") => {
            append(
                &ledger,
                "contradiction",
                LedgerEventKind::TurnCompleted,
                json!({"reason":"FinalAnswer"}),
            );
        }
        Some("cancel") => {
            append(
                &ledger,
                "contradiction",
                LedgerEventKind::TurnCancelled,
                json!(null),
            );
        }
        Some("execution_cancel") => {
            append(
                &ledger,
                "contradiction",
                LedgerEventKind::ExecutionCancelled,
                json!(null),
            );
        }
        Some(other) => panic!("unknown contradiction {other}"),
    }
    // Construct a new fixture journal; never mutate the source journal or a live fact.
    let journal = MemoryFactJournal::default();
    for record in original.records("task").unwrap() {
        for cause in &record.draft.causes {
            if cause.stream_id != "task" {
                copy_causes(
                    &original_journal,
                    &journal,
                    &cause.stream_id,
                    cause.position,
                );
            }
        }
        let mut draft = record.draft;
        if draft.kind == "task.attempt_observed" {
            let TaskEvent::AttemptObserved(mut observation) =
                serde_json::from_value(draft.payload).unwrap()
            else {
                panic!("fixture observation differs")
            };
            observation.outcome = AttemptOutcome::Failed {
                reason: "Turn did not produce an admitted final answer".into(),
                safe_to_retry: false,
            };
            draft.payload = json!(TaskEvent::AttemptObserved(observation));
        }
        journal
            .append("task", record.position - 1, vec![draft])
            .unwrap();
    }
    (TaskCoordinator::new(journal), ledger, binding)
}

fn copy_causes(source: &MemoryFactJournal, target: &MemoryFactJournal, stream: &str, through: u64) {
    let head = target
        .read(stream, 0, 1024)
        .unwrap()
        .last()
        .map_or(0, |r| r.position);
    for record in source.read(stream, head, 1024).unwrap() {
        if record.position > through {
            break;
        }
        for cause in &record.draft.causes {
            copy_causes(source, target, &cause.stream_id, cause.position);
        }
        target
            .append(stream, record.position - 1, vec![record.draft])
            .unwrap();
    }
}

#[test]
fn historical_unsuccessful_completion_exports_before_comparison_without_mutation() {
    let rows: Vec<Row> = serde_json::from_str(include_str!("incomplete.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-history-incomplete-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    let mut observations = Vec::new();
    for row in rows {
        let (coordinator, ledger, binding) = fixture(&row);
        let before = coordinator.records("task").unwrap();
        let physical_before = ledger.events_after(0).unwrap();
        let first = load_for(
            &coordinator,
            &ledger,
            "task",
            &binding,
            4096,
            ResultRead::Historical,
        );
        let second = load_for(
            &coordinator,
            &ledger,
            "task",
            &binding,
            4096,
            ResultRead::Historical,
        );
        let first = first.map_err(|error| error.to_string());
        let second = second.map_err(|error| error.to_string());
        let unchanged = before == coordinator.records("task").unwrap()
            && physical_before == ledger.events_after(0).unwrap();
        serde_json::to_writer(
            &mut file,
            &json!({"id":row.id,"first":first,"second":second,
            "journal_unchanged":unchanged,"physical":physical_before}),
        )
        .unwrap();
        writeln!(file).unwrap();
        file.sync_all().unwrap();
        observations.push((row, first, second, unchanged));
    }
    eprintln!("historical incomplete evidence: {}", path.display());
    for (row, first, second, unchanged) in observations {
        assert_eq!(first.is_ok(), row.accepted, "{}: {first:?}", row.id);
        assert_eq!(first, second, "{}: idempotency", row.id);
        assert!(unchanged, "{}: read mutated facts", row.id);
        if let Ok(result) = first {
            assert!(
                matches!(result.outcome, VerifiedTaskOutcome::Failed { .. }),
                "{}: unsuccessful Turn cannot become successful",
                row.id
            );
        }
    }
}
