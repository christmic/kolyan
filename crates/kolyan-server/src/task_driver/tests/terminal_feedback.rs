//! Actual Runtime child terminal facts, not invented successful completion.
use super::super::*;
use super::support::*;
use crate::{ConsumedTerminalResult, ExecutionService, SessionService, TerminalResultDisposition};
use kolyan_core::{ExternalResolution, ResumeInput, TurnExecutor};
use kolyan_ledger::FactJournal;
use kolyan_model::{
    ModelRequest, ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture,
};
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_trace::NoopTraceSink;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct StoppedProvider {
    execution: ExecutionService<FaultLedger, NoopTraceSink>,
    cancelled: bool,
}
impl ModelProvider for StoppedProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        if self.cancelled {
            self.execution.cancel(&binding("child").execution).unwrap();
            // Server cancellation is a durable boundary request, not the
            // Provider's transport cancellation port. Return control so the
            // actual Runtime boundary must observe the committed cancellation.
            Box::pin(async move {
                Ok(Box::pin(futures_util::stream::iter(vec![
                    Ok(kolyan_model::ModelEvent::Started),
                    Ok(kolyan_model::ModelEvent::Completed(
                        kolyan_model::ModelResponse {
                            id: "cancelled-child-response".into(),
                            model: request.model,
                            content: vec![kolyan_model::ContentBlock::Text {
                                text: "must not become successful completion".into(),
                            }],
                            structured_output: None,
                            stop_reason: kolyan_model::StopReason::EndTurn,
                            usage: kolyan_model::TokenUsage::default(),
                            metadata: serde_json::Value::Null,
                        },
                    )),
                ])) as kolyan_model::ModelEventStream)
            })
        } else {
            Box::pin(async {
                Err(ProviderError::new(
                    ProviderErrorKind::Unavailable,
                    ProviderErrorPhase::Open,
                    "child failed\u{0000}without a result",
                ))
            })
        }
    }
}

#[tokio::test]
async fn terminal_feedback_failed_and_cancelled_child_survives_publication_commit_gaps() {
    for (cancelled, fault) in [
        (false, LedgerEventKind::EffectReceipt),
        (true, LedgerEventKind::EffectReceipt),
        (false, LedgerEventKind::TurnCheckpointMerged),
        (true, LedgerEventKind::TurnCheckpointMerged),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = FileSessionStore::new(directory.path()).unwrap();
        store.create("session-root").unwrap();
        store.create("session-child").unwrap();
        let host = Host {
            coordinator: coordinator(),
            ledger: FaultLedger::default(),
        };
        let execution = ExecutionService::new(host.ledger.clone(), NoopTraceSink)
            .with_external_wait_verifier(Arc::new(host.clone()));
        let service = TaskExecutionService::new(
            host.coordinator.clone(),
            SessionExecutionService::new(execution.clone(), SessionService::new(store)),
        );
        let models = Arc::new(AtomicUsize::new(0));
        let tools = Arc::new(AtomicUsize::new(0));
        let parent_executor = || {
            TurnExecutor::with_tools(
                Provider {
                    delegate: true,
                    calls: models.clone(),
                },
                Delegate {
                    coordinator: host.coordinator.clone(),
                    calls: tools.clone(),
                },
            )
            .with_policy_engine(policy())
        };
        let (_, outcome) = service
            .run(
                "task",
                binding("root"),
                parent_executor(),
                request(&binding("root")),
            )
            .await
            .unwrap();
        let DurableTurnResult::Suspended { suspension, .. } = outcome else {
            panic!("expected wait")
        };
        let _child_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            service.run(
                "task",
                binding("child"),
                TurnExecutor::new(StoppedProvider {
                    execution,
                    cancelled,
                }),
                request(&binding("child")),
            ),
        )
        .await
        .expect("child cancellation or failure must stop the Runtime");
        if cancelled {
            assert!(_child_result.is_err());
            let events = host
                .ledger
                .execution_events_after("execution-child", 0)
                .unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.kind == LedgerEventKind::ExecutionCancelled)
                    .count(),
                1
            );
            assert!(
                events
                    .iter()
                    .any(|event| event.kind == LedgerEventKind::TurnCancelled)
            );
            assert!(!events.iter().any(|event| matches!(
                event.kind,
                LedgerEventKind::TurnCompleted
                    | LedgerEventKind::TurnFailed
                    | LedgerEventKind::EffectStarted
            )));
        }
        let proof = service
            .load_verified_result("task", &binding("child"), 65536)
            .unwrap();
        let disposition = match &proof.outcome {
            VerifiedTaskOutcome::Failed { reason } if !cancelled => {
                TerminalResultDisposition::Failed {
                    reason: reason.clone(),
                }
            }
            VerifiedTaskOutcome::Cancelled { reason } if cancelled => {
                TerminalResultDisposition::Cancelled {
                    reason: reason.clone(),
                }
            }
            other => panic!("wrong actual child outcome: {other:?}"),
        };
        let state = host.coordinator.snapshot("task").unwrap();
        assert!(state.invocations["child"].completion_fact.is_none());
        let observed = state.attempts[&binding("child").attempt_id]
            .observation
            .as_ref()
            .unwrap();
        host.coordinator
            .consume_terminal_result(
                "task",
                "terminal-consume",
                "root",
                ConsumedTerminalResult {
                    parent: binding("root"),
                    child: binding("child"),
                    terminal_fact: proof.terminal_fact,
                    source: observed.source.clone(),
                    disposition,
                },
            )
            .unwrap();
        let consumed = service
            .load_verified_consumed_result("task", &binding("root"), &binding("child"), 65536)
            .unwrap()
            .unwrap();
        let size = serde_json::to_vec(&consumed).unwrap().len();
        assert!(
            service
                .load_verified_consumed_result(
                    "task",
                    &binding("root"),
                    &binding("child"),
                    size - 1
                )
                .is_err()
        );
        let resolved = host.result().unwrap();
        assert!(resolved.is_error);
        let input = ResumeInput::ExternalResolved(vec![ExternalResolution {
            call_id: call().id,
            wait: wait(),
            result: resolved,
        }]);
        *host.ledger.failure.lock().unwrap() = Some(fault);
        assert!(
            service
                .resume(
                    "task",
                    binding("root"),
                    &suspension.checkpoint.checkpoint_id,
                    input.clone(),
                    parent_executor()
                )
                .await
                .is_err()
        );
        assert_eq!(
            service
                .load_verified_consumed_result("task", &binding("root"), &binding("child"), 65536)
                .unwrap()
                .unwrap(),
            consumed
        );
        let resumed = service
            .sessions()
            .resume(
                parent_executor(),
                "session-root",
                "execution-root",
                &suspension.checkpoint.checkpoint_id,
                input,
            )
            .await
            .unwrap();
        assert!(matches!(resumed, DurableTurnResult::Completed(..)));
        assert_eq!(models.load(Ordering::SeqCst), 2);
        assert_eq!(tools.load(Ordering::SeqCst), 1);
        assert_eq!(
            host.coordinator
                .journal()
                .read("task", 0, 1024)
                .unwrap()
                .iter()
                .filter(|row| row.draft.kind == "task.terminal_result_consumed")
                .count(),
            1
        );
        assert_eq!(
            host.ledger
                .execution_events_after("execution-root", 0)
                .unwrap()
                .iter()
                .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
                .count(),
            1
        );
        assert!(
            host.coordinator
                .complete_task("task", "not-success", vec![])
                .is_err()
        );
    }
}
