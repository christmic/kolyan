//! Scripted trusted host port. This is not a native-hook acceptance test.

use super::*;

pub(super) fn binding(context: &EffectHookContext) -> Value {
    json!({"issued":context.issued(),"effect_id":context.effect_id(),"input_digest":context.input_digest()})
}

pub(super) struct Port {
    pub(super) mode: String,
    pub(super) store: Store,
    pub(super) journal: Arc<Journal>,
    pub(super) calls: Arc<Mutex<Vec<Value>>>,
    pub(super) after_entered: Arc<tokio::sync::Notify>,
}

impl Port {
    fn record(&self, phase: &str, context: &EffectHookContext) {
        self.calls.lock().unwrap().push(json!({
            "phase":phase,"context":binding(context),
            "deadline":format!("{:?}",context.window().deadline()),
            "remaining_ns":context.window().remaining().as_nanos().to_string(),
            "control_cancelled":context.control().is_cancelled(),
            "ledger_at_call":self.store.execution_events_after("e",0).unwrap()
        }));
    }

    fn persist(
        &self,
        kind: &str,
        payload: Value,
        causes: Vec<kolyan_ledger::FactRef>,
    ) -> Result<FactRecord, EffectHookError> {
        let records = self
            .journal
            .read("scripted-effect-hook", 0, 64)
            .map_err(host)?;
        let head = records.last().map_or(0, |record| record.position);
        self.journal
            .append(
                "scripted-effect-hook",
                head,
                vec![FactDraft {
                    fact_id: format!("fixture-hook-{kind}-{head}"),
                    subject: kolyan_ledger::FactSubject {
                        kind: "test.effect".into(),
                        id: "e".into(),
                    },
                    kind: kind.into(),
                    schema_version: 1,
                    critical: true,
                    causes,
                    payload,
                }],
            )
            .map_err(host)?
            .into_iter()
            .next()
            .ok_or_else(|| host("no appended fact"))
    }
}

impl EffectHookPort for Port {
    fn before_effect(
        &self,
        context: EffectHookContext,
    ) -> EffectHookFuture<'_, EffectHookDecision> {
        Box::pin(async move {
            self.record("before", &context);
            self.persist("fixture.hook.before", binding(&context), vec![])?;
            match self.mode.as_str() {
                "deny" => Ok(EffectHookDecision::Denied {
                    reason: "host hook denied".into(),
                }),
                "empty_deny" => Ok(EffectHookDecision::Denied {
                    reason: String::new(),
                }),
                "before_host" => Err(host("explicit hook parse/storage failure")),
                "before_cancel" => Err(EffectHookError::Cancelled),
                "before_pending" => std::future::pending().await,
                "cancel_then_continue" => {
                    context.control().cancel();
                    Ok(EffectHookDecision::Continue)
                }
                _ => Ok(EffectHookDecision::Continue),
            }
        })
    }

    fn after_receipt(
        &self,
        context: EffectHookContext,
        receipt: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        Box::pin(async move {
            self.record("after", &context);
            if self
                .store
                .event_by_id(&receipt.event().event_id)
                .map_err(host)?
                .as_ref()
                != Some(receipt.event())
            {
                return Err(host("observer received a noncommitted receipt"));
            }
            let payload = json!({"context":binding(&context),"receipt":receipt.event(),"result":receipt.result()});
            let intent = self.persist("fixture.hook.after_started", payload.clone(), vec![])?;
            self.after_entered.notify_one();
            match self.mode.as_str() {
                "after_host" => return Err(host("explicit observer protocol failure")),
                "after_cancel" => return Err(EffectHookError::Cancelled),
                "after_pending" | "after_drop" => return std::future::pending().await,
                _ => {}
            }
            let mut payload = payload;
            if self.mode == "replay_foreign" {
                // Deliberately foreign proof, not a modified target receipt.
                payload["context"]["issued"]["scope"]["execution"]["session_id"] = json!("foreign");
            }
            self.persist(
                "fixture.hook.after_completed",
                payload,
                vec![kolyan_ledger::FactRef {
                    stream_id: intent.stream_id,
                    position: intent.position,
                    fact_id: intent.draft.fact_id,
                }],
            )?;
            Ok(())
        })
    }

    fn verify_observation(
        &self,
        context: EffectHookContext,
        receipt: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        Box::pin(async move {
            self.record("verify", &context);
            let records = self
                .journal
                .read("scripted-effect-hook", 0, 64)
                .map_err(host)?;
            let expected = json!({"context":binding(&context),"receipt":receipt.event(),"result":receipt.result()});
            let intent = records
                .iter()
                .find(|record| record.draft.kind == "fixture.hook.after_started")
                .ok_or_else(|| incomplete("missing observation intent"))?;
            let completion = records
                .iter()
                .find(|record| record.draft.kind == "fixture.hook.after_completed")
                .ok_or_else(|| incomplete("observation never completed"))?;
            let cause = kolyan_ledger::FactRef {
                stream_id: intent.stream_id.clone(),
                position: intent.position,
                fact_id: intent.draft.fact_id.clone(),
            };
            if intent.draft.payload != expected
                || completion.draft.payload != expected
                || completion.position <= intent.position
                || completion.draft.causes != vec![cause]
            {
                return Err(incomplete("foreign or malformed observation proof"));
            }
            Ok(())
        })
    }
}

fn incomplete(message: &str) -> EffectHookError {
    EffectHookError::Incomplete {
        message: message.into(),
    }
}

pub(super) fn host(error: impl std::fmt::Display) -> EffectHookError {
    EffectHookError::Host {
        message: error.to_string(),
    }
}
