use kolyan_ledger::{
    FactDraft, FactJournal, FactRef, FactSubject, FileLedger, LedgerStore, SqliteFactJournal,
};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, TokenUsage, ToolCall,
};
use kolyan_policy::{ApprovalMode, PathScope, PolicyEngine};
use kolyan_runtime::effect_hooks::{
    CommittedEffectReceipt, EffectHookContext, EffectHookDecision, EffectHookError,
    EffectHookFuture, EffectHookPort,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub(super) struct Provider {
    pub(super) call: ToolCall,
    pub(super) requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.requests.lock().unwrap().push(request.clone());
        let received_result = request.messages.iter().any(|m| {
            m.content
                .iter()
                .any(|b| matches!(b, ContentBlock::ToolResult { .. }))
        });
        let response = ModelResponse {
            id: request.request_id.clone(),
            model: request.model,
            content: if received_result {
                vec![ContentBlock::Text {
                    text: "fixture final answer".into(),
                }]
            } else {
                vec![ContentBlock::ToolCall {
                    call: self.call.clone(),
                }]
            },
            structured_output: None,
            stop_reason: if received_result {
                StopReason::EndTurn
            } else {
                StopReason::ToolUse
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

pub(super) fn policy(root: &Path, approval: bool) -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    let mut manifest = kolyan_tools::RestrictedFileTool::tool_manifests()
        .into_iter()
        .find(|m| m.tool_name == "file.write")
        .unwrap();
    manifest.path_scopes = vec![PathScope::new(root.join("safe").to_string_lossy())];
    manifest.approval = if approval {
        ApprovalMode::Always
    } else {
        ApprovalMode::Never
    };
    policy.register(manifest);
    policy.restrict_workspace(root.join("safe").to_string_lossy());
    Arc::new(policy)
}

pub(super) struct Hooks {
    pub(super) mode: String,
    pub(super) ledger: FileLedger,
    pub(super) file: PathBuf,
    pub(super) calls: Arc<Mutex<Vec<Value>>>,
    pub(super) journal: SqliteFactJournal,
}

impl Hooks {
    fn observation(context: &EffectHookContext, receipt: &CommittedEffectReceipt) -> Value {
        json!({"issued":context.issued(),"effect_id":context.effect_id(),
            "input_digest":context.input_digest(),"receipt":receipt.event(),"result":receipt.result()})
    }

    fn verify_saved(&self, payload: &Value) -> Result<(), EffectHookError> {
        let rows = self
            .journal
            .read("fixture.observations", 0, 64)
            .map_err(|error| EffectHookError::Host {
                message: error.to_string(),
            })?;
        if rows.len() != 2
            || rows[0].draft.kind != "fixture.observer.started"
            || rows[1].draft.kind != "fixture.observer.completed"
            || rows.iter().any(|r| &r.draft.payload != payload)
            || rows[1].draft.causes
                != vec![FactRef {
                    stream_id: rows[0].stream_id.clone(),
                    position: rows[0].position,
                    fact_id: rows[0].draft.fact_id.clone(),
                }]
        {
            return Err(EffectHookError::Incomplete {
                message: "fixture observation proof incomplete".into(),
            });
        }
        Ok(())
    }

    fn record(
        &self,
        phase: &str,
        context: &EffectHookContext,
        receipt: Option<&CommittedEffectReceipt>,
    ) {
        self.calls.lock().unwrap().push(json!({
            "phase":phase,"issued":context.issued(),"effect_id":context.effect_id(),
            "input_digest":context.input_digest(),"deadline":format!("{:?}",context.window().deadline()),
            "remaining_ms":u64::try_from(context.window().remaining().as_millis()).unwrap(),
            "cancelled":context.control().is_cancelled(),"file_exists":self.file.exists(),
            "receipt":receipt.map(|r|r.event()),"result":receipt.and_then(|r|r.result().as_ref().ok()),
            "ledger_at_call":self.ledger.execution_events_after("e",0).unwrap(),
        }));
    }
}

impl EffectHookPort for Hooks {
    fn before_effect(
        &self,
        context: EffectHookContext,
    ) -> EffectHookFuture<'_, EffectHookDecision> {
        Box::pin(async move {
            self.record("before", &context, None);
            match self.mode.as_str() {
                "deny" => Ok(EffectHookDecision::Denied {
                    reason: "fixture policy denies entry".into(),
                }),
                "before_host" => Err(EffectHookError::Host {
                    message: "fixture hook unavailable".into(),
                }),
                "before_cancel" => {
                    context.control().cancel();
                    Ok(EffectHookDecision::Continue)
                }
                "before_pending" => std::future::pending().await,
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
            self.record("after", &context, Some(&receipt));
            assert_eq!(
                self.ledger
                    .event_by_id(&receipt.event().event_id)
                    .unwrap()
                    .as_ref(),
                Some(receipt.event())
            );
            let payload = Self::observation(&context, &receipt);
            let started = self
                .journal
                .append(
                    "fixture.observations",
                    0,
                    vec![FactDraft {
                        fact_id: "fixture-observer-started".into(),
                        subject: FactSubject {
                            kind: "test.effect".into(),
                            id: context.effect_id().into(),
                        },
                        kind: "fixture.observer.started".into(),
                        schema_version: 1,
                        critical: true,
                        causes: vec![],
                        payload: payload.clone(),
                    }],
                )
                .map_err(|error| EffectHookError::Host {
                    message: error.to_string(),
                })?;
            if self.mode == "after_host" {
                return Err(EffectHookError::Host {
                    message: "fixture observer failed".into(),
                });
            }
            self.journal
                .append(
                    "fixture.observations",
                    1,
                    vec![FactDraft {
                        fact_id: "fixture-observer-completed".into(),
                        subject: started[0].draft.subject.clone(),
                        kind: "fixture.observer.completed".into(),
                        schema_version: 1,
                        critical: true,
                        causes: vec![FactRef {
                            stream_id: started[0].stream_id.clone(),
                            position: started[0].position,
                            fact_id: started[0].draft.fact_id.clone(),
                        }],
                        payload: payload.clone(),
                    }],
                )
                .map_err(|error| EffectHookError::Host {
                    message: error.to_string(),
                })?;
            self.verify_saved(&payload)
        })
    }
    fn verify_observation(
        &self,
        context: EffectHookContext,
        receipt: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        Box::pin(async move {
            self.record("verify", &context, Some(&receipt));
            self.verify_saved(&Self::observation(&context, &receipt))
        })
    }
}
