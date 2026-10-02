//! Real Server/Runtime journal-to-ledger publications with injected write gaps.
//! This uses a deterministic host adapter, not Sagan's unfinished Agent adapter.
use super::super::*;
use super::support::*;
use crate::{ConsumedResult, ExecutionService, SessionService};
use kolyan_core::{ExternalResolution, ResumeInput, TurnExecutor};
use kolyan_ledger::FactJournal;
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_trace::NoopTraceSink;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

type Service = TaskExecutionService<
    kolyan_ledger::MemoryFactJournal,
    FaultLedger,
    NoopTraceSink,
    FileSessionStore,
>;
fn service(host: &Host, store: &FileSessionStore) -> Service {
    let execution = ExecutionService::new(host.ledger.clone(), NoopTraceSink)
        .with_external_wait_verifier(Arc::new(host.clone()));
    TaskExecutionService::new(
        host.coordinator.clone(),
        SessionExecutionService::new(execution, SessionService::new(store.clone())),
    )
}
fn executor(
    coordinator: &TaskCoordinator<kolyan_ledger::MemoryFactJournal>,
    models: &Arc<AtomicUsize>,
    tools: &Arc<AtomicUsize>,
) -> TurnExecutor<Provider, Delegate> {
    TurnExecutor::with_tools(
        Provider {
            delegate: true,
            calls: models.clone(),
        },
        Delegate {
            coordinator: coordinator.clone(),
            calls: tools.clone(),
        },
    )
    .with_policy_engine(policy())
}
async fn waiting(
    host: &Host,
    store: &FileSessionStore,
    models: &Arc<AtomicUsize>,
    tools: &Arc<AtomicUsize>,
) -> kolyan_core::TurnSuspension {
    store.create("session-root").unwrap();
    store.create("session-child").unwrap();
    let current = service(host, store);
    let (_, outcome) = current
        .run(
            "task",
            binding("root"),
            executor(&host.coordinator, models, tools),
            request(&binding("root")),
        )
        .await
        .unwrap();
    let DurableTurnResult::Suspended { suspension, .. } = outcome else {
        panic!("parent should await durable child")
    };
    assert_eq!(suspension.waiting.external_waits.len(), 1);
    let (_, child) = current
        .run(
            "task",
            binding("child"),
            TurnExecutor::new(Provider {
                delegate: false,
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            request(&binding("child")),
        )
        .await
        .unwrap();
    assert!(matches!(child, DurableTurnResult::Completed(..)));
    let state = host.coordinator.snapshot("task").unwrap();
    let AttemptOutcome::Completed { evidence } = &state.attempts[&binding("child").attempt_id]
        .observation
        .as_ref()
        .unwrap()
        .outcome
    else {
        panic!("successful child")
    };
    host.coordinator
        .consume_child_result(
            "task",
            "consume-child-once",
            "root",
            ConsumedResult {
                child_invocation_id: "child".into(),
                completion_fact: state.invocations["child"].completion_fact.clone().unwrap(),
                evidence: evidence.clone(),
            },
        )
        .unwrap();
    *suspension
}

#[tokio::test]
async fn consumed_proof_recovers_missing_receipt_and_merged_publication_without_reconsumption() {
    for fault in [
        LedgerEventKind::EffectReceipt,
        LedgerEventKind::TurnCheckpointMerged,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = FileSessionStore::new(directory.path()).unwrap();
        let host = Host {
            coordinator: coordinator(),
            ledger: FaultLedger::default(),
        };
        let models = Arc::new(AtomicUsize::new(0));
        let tools = Arc::new(AtomicUsize::new(0));
        let suspension = waiting(&host, &store, &models, &tools).await;
        let current = service(&host, &store);
        let proof = current
            .load_verified_consumed_result("task", &binding("root"), &binding("child"), 65536)
            .unwrap()
            .unwrap();
        let encoded = serde_json::to_vec(&proof).unwrap().len();
        assert!(
            current
                .load_verified_consumed_result(
                    "task",
                    &binding("root"),
                    &binding("child"),
                    encoded - 1
                )
                .is_err()
        );
        let resolutions = ResumeInput::ExternalResolved(vec![ExternalResolution {
            call_id: call().id,
            wait: wait(),
            result: host.result().unwrap(),
        }]);
        *host.ledger.failure.lock().unwrap() = Some(fault);
        assert!(
            current
                .resume(
                    "task",
                    binding("root"),
                    &suspension.checkpoint.checkpoint_id,
                    resolutions.clone(),
                    executor(&host.coordinator, &models, &tools)
                )
                .await
                .is_err()
        );
        assert_eq!(tools.load(Ordering::SeqCst), 1);
        assert_eq!(models.load(Ordering::SeqCst), 1);
        let rows = host.coordinator.journal().read("task", 0, 1024).unwrap();
        assert_eq!(
            rows.iter()
                .filter(|row| row.draft.kind == "task.result_consumed")
                .count(),
            1
        );
        drop(current);
        let rebuilt = service(&host, &store);
        // The same durable consumption fact is read again; no consume call.
        assert_eq!(
            rebuilt
                .load_verified_consumed_result("task", &binding("root"), &binding("child"), 65536)
                .unwrap()
                .unwrap(),
            proof
        );
        let result = rebuilt
            .sessions()
            .resume(
                executor(&host.coordinator, &models, &tools),
                "session-root",
                "execution-root",
                &suspension.checkpoint.checkpoint_id,
                resolutions,
            )
            .await
            .unwrap();
        assert!(matches!(result, DurableTurnResult::Completed(..)));
        assert_eq!(tools.load(Ordering::SeqCst), 1);
        assert_eq!(models.load(Ordering::SeqCst), 2);
        let rows = host.coordinator.journal().read("task", 0, 1024).unwrap();
        assert_eq!(
            rows.iter()
                .filter(|row| row.draft.kind == "task.result_consumed")
                .count(),
            1
        );
        let events = host
            .ledger
            .execution_events_after("execution-root", 0)
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
                .count(),
            1
        );
        assert!(
            events
                .iter()
                .any(|event| event.kind == LedgerEventKind::TurnCheckpointMerged)
        );
    }
}

#[tokio::test]
async fn consumed_root_cancel_and_foreign_attempt_refuse_late_resolution_without_new_facts() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(directory.path()).unwrap();
    let host = Host {
        coordinator: coordinator(),
        ledger: FaultLedger::default(),
    };
    let models = Arc::new(AtomicUsize::new(0));
    let tools = Arc::new(AtomicUsize::new(0));
    let suspension = waiting(&host, &store, &models, &tools).await;
    let current = service(&host, &store);
    let mut foreign = binding("root");
    foreign.execution.session_id = "foreign".into();
    assert!(
        current
            .load_verified_consumed_result("task", &foreign, &binding("child"), 65536)
            .is_err()
    );
    current
        .cancel("task", "cancel-root", "host cancelled")
        .unwrap();
    let ledger_before = host.ledger.events_after(0).unwrap();
    let journal_before = host.coordinator.journal().read("task", 0, 1024).unwrap();
    assert!(
        current
            .load_verified_consumed_result("task", &binding("root"), &binding("child"), 65536)
            .is_err()
    );
    let result = kolyan_model::ToolResult {
        call_id: call().id,
        content: "late unverified result".into(),
        is_error: false,
    };
    assert!(
        current
            .sessions()
            .resume(
                executor(&host.coordinator, &models, &tools),
                "session-root",
                "execution-root",
                &suspension.checkpoint.checkpoint_id,
                ResumeInput::ExternalResolved(vec![ExternalResolution {
                    call_id: call().id,
                    wait: wait(),
                    result
                }])
            )
            .await
            .is_err()
    );
    assert_eq!(host.ledger.events_after(0).unwrap(), ledger_before);
    assert_eq!(
        host.coordinator.journal().read("task", 0, 1024).unwrap(),
        journal_before
    );
    assert_eq!(tools.load(Ordering::SeqCst), 1);
    assert_eq!(models.load(Ordering::SeqCst), 1);
}
