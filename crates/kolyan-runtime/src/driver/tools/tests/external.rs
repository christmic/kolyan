use super::*;
use crate::{ExternalRecoveryFuture, ExternalVerificationFuture};
use kolyan_core::{ExternalWait, IssuedToolAuthority};

struct SavedAdmission {
    issued: IssuedToolAuthority,
    wait: ExternalWait,
    refuse: bool,
    reads: AtomicUsize,
}

impl ExternalWaitVerifier for SavedAdmission {
    fn verify_wait(&self, context: ExternalWaitContext) -> ExternalVerificationFuture<'_> {
        Box::pin(async move {
            if self.refuse || context.issued != self.issued || context.wait != self.wait {
                return Err(ToolError::PolicyDenied {
                    message: "unproven admission".into(),
                });
            }
            context.validate(&self.issued.scope)
        })
    }

    fn verify_result(
        &self,
        _: ExternalWaitContext,
        _: ToolResult,
    ) -> ExternalVerificationFuture<'_> {
        Box::pin(async {
            Err(ToolError::PolicyDenied {
                message: "no terminal proof".into(),
            })
        })
    }

    fn recover_wait(&self, issued: IssuedToolAuthority) -> ExternalRecoveryFuture<'_> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if issued != self.issued {
                return Err(ToolError::PolicyDenied {
                    message: "foreign authority".into(),
                });
            }
            Ok(Some(self.wait.clone()))
        })
    }
}

fn entered(ledger: &InMemoryLedger) -> PreparedEvidence {
    let invocation = invocation_for(call());
    let bound = PreparedEvidence::new(
        invocation.prepared,
        invocation.grant,
        invocation.scope,
        &invocation.policy_revision,
        &key(),
    )
    .unwrap();
    for (suffix, kind, payload) in [
        (
            "prepared",
            LedgerEventKind::EffectPrepared,
            bound.prepared_payload().unwrap(),
        ),
        (
            "authorized",
            LedgerEventKind::EffectAuthorized,
            bound.authorized_payload().unwrap(),
        ),
        (
            "started",
            LedgerEventKind::EffectStarted,
            bound.started_payload(),
        ),
    ] {
        let id = format!("{}/{suffix}", bound.prefix());
        ledger
            .append(LedgerEvent {
                event_id: id.clone(),
                idempotency_key: id,
                execution_id: "e".into(),
                turn_id: "t".into(),
                cursor: 0,
                kind,
                payload,
            })
            .unwrap();
    }
    bound
}

fn host(bound: &PreparedEvidence, refuse: bool) -> Arc<SavedAdmission> {
    Arc::new(SavedAdmission {
        issued: bound.issued(),
        wait: ExternalWait {
            wait_id: "durable-wait".into(),
            kind: "fixture.admitted".into(),
            schema_version: 1,
            binding: json!({"admission":"durable-child"}),
        },
        refuse,
        reads: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn admitted_external_work_is_recovered_without_reexecuting_the_tool() {
    let ledger = InMemoryLedger::default();
    let bound = entered(&ledger);
    let verifier = host(&bound, false);
    let count = Arc::new(AtomicUsize::new(0));
    let tools = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()))
        .with_wait_verifier(verifier.clone());
    let expected = ToolOutcome::AwaitingExternal(verifier.wait.clone());
    assert_eq!(
        tools
            .execute_invocation(invocation_for(call()))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(verifier.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        ledger
            .event_by_id(&format!("{}/waiting", bound.prefix()))
            .unwrap()
            .unwrap()
            .payload,
        bound.wait_payload(&verifier.wait).unwrap()
    );
    drop(tools);
    let rebuilt = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()))
        .with_wait_verifier(verifier.clone());
    assert_eq!(
        rebuilt
            .execute_invocation(invocation_for(call()))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(verifier.reads.load(Ordering::SeqCst), 1);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        ledger
            .event_by_id(&format!("{}/receipt", bound.prefix()))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn absent_or_unverified_external_proof_is_uncertain_not_tool_feedback() {
    for refuse in [false, true] {
        let ledger = InMemoryLedger::default();
        let bound = entered(&ledger);
        let count = Arc::new(AtomicUsize::new(0));
        let tools = DurableTools::new(ledger.clone(), key(), CountingTool(count.clone()));
        let tools = if refuse {
            tools.with_wait_verifier(host(&bound, true))
        } else {
            tools
        };
        assert!(matches!(
            tools.execute_invocation(invocation_for(call())).await,
            Err(ToolError::Uncertain { .. })
        ));
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(
            ledger
                .event_by_id(&format!("{}/waiting", bound.prefix()))
                .unwrap()
                .is_none()
        );
        assert!(
            ledger
                .event_by_id(&format!("{}/receipt", bound.prefix()))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            ledger
                .event_by_id(&format!("{}/uncertain", bound.prefix()))
                .unwrap()
                .unwrap()
                .kind,
            LedgerEventKind::EffectUncertain
        );
    }
}
