//! Scripted preparation, Provider and governed tools; no remote requests.

use super::*;

pub(super) struct Hook {
    pub(super) action: String,
    pub(super) source_path: std::path::PathBuf,
    pub(super) store: FileSessionStore,
    pub(super) seen: Arc<Mutex<Vec<Value>>>,
    pub(super) returned: Arc<Mutex<Vec<ModelRequest>>>,
}

impl TurnPreparationHook for Hook {
    fn prepare<'a>(
        &'a self,
        execution: &'a ExecutionRef,
        session_version: u64,
        request: &'a TurnRequest,
    ) -> TurnPreparationFuture<'a> {
        let observation = json!({"execution":execution,"session_version":session_version,
            "request":turn_value(request)});
        self.seen.lock().unwrap().push(observation.clone());
        Box::pin(async move {
            // Test-owned artifact publication; production code never writes these.
            let path = if self.action == "write_failure" {
                self.source_path.join("missing-parent/source.json")
            } else {
                self.source_path.clone()
            };
            std::fs::write(path, serde_json::to_vec(&observation).unwrap())
                .map_err(StorageError::from)?;
            match self.action.as_str() {
                "pending" | "drop" => std::future::pending::<()>().await,
                "delay" => tokio::time::sleep(Duration::from_millis(25)).await,
                "blocking" => std::thread::sleep(Duration::from_millis(20)),
                "reject" => {
                    return Err(PreparationFailure::Rejected {
                        reason: "saved source rejected".into(),
                    }
                    .into());
                }
                "conflict" => {
                    self.store.append_turn(
                        "s",
                        SessionTurn {
                            turn_id: "concurrent".into(),
                            execution_id: "concurrent-e".into(),
                            status: SessionTurnStatus::Completed,
                        },
                        vec![text("concurrent history")],
                    )?;
                }
                _ => {}
            }
            let mut selected = request.model_request.clone();
            if let Some(field) = self.action.strip_prefix("field:") {
                let mut value = serde_json::to_value(&selected).unwrap();
                value[field] = match field {
                    "request_id" => json!("forged"),
                    "model" => json!({"provider":"other","model":"other"}),
                    "system" | "tools" => json!([]),
                    "tool_choice" => json!("none"),
                    "max_output_tokens" => json!(1),
                    "extensions" => json!({"changed":true}),
                    _ => Value::Null,
                };
                selected = serde_json::from_value(value).unwrap();
            } else {
                match self.action.as_str() {
                    "reduce" | "approval" | "resume" | "recovery" => {
                        selected.messages.drain(1..6);
                    }
                    "insert" => selected.messages.insert(1, text("invented")),
                    "reorder" => selected.messages.swap(0, 1),
                    "modify" => selected.messages[1] = text("changed"),
                    "duplicate" => selected.messages.insert(0, selected.messages[0].clone()),
                    "remove_tail" => {
                        selected.messages.pop();
                    }
                    "change_tail" => *selected.messages.last_mut().unwrap() = text("changed tail"),
                    "messages_bound" => selected.messages = vec![text("extra"); 4097],
                    "blocks_bound" => {
                        selected.messages[0].content =
                            vec![ContentBlock::Text { text: "x".into() }; 16385]
                    }
                    "bytes_bound" => selected.extensions = json!("x".repeat(16 * 1024 * 1024)),
                    _ => {}
                }
            }
            self.returned.lock().unwrap().push(selected.clone());
            Ok(selected)
        })
    }
}

pub(super) struct Provider {
    pub(super) seen: Arc<Mutex<Vec<ModelRequest>>>,
    pub(super) opened_at: Arc<Mutex<Vec<u64>>>,
    pub(super) approval: bool,
    pub(super) prepared: PreparedCall,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.opened_at.lock().unwrap().push(now_ms());
        let mut seen = self.seen.lock().unwrap();
        let first = seen.is_empty();
        seen.push(request.clone());
        let content = if self.approval && first {
            vec![ContentBlock::ToolCall {
                call: self.prepared.call().clone(),
            }]
        } else {
            vec![ContentBlock::Text {
                text: "completed 🦀".into(),
            }]
        };
        let response = ModelResponse {
            id: "response".into(),
            model: request.model,
            content,
            structured_output: None,
            stop_reason: if self.approval && first {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
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

pub(super) struct Tools {
    pub(super) prepared: PreparedCall,
    pub(super) calls: Arc<AtomicUsize>,
}
impl ToolExecutor for Tools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        let prepared = self.prepared.clone();
        Box::pin(async move {
            if prepared.call() != &call {
                return Err(kolyan_core::ToolError::Unavailable { name: call.name });
            }
            Ok(prepared)
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        let calls = self.calls.clone();
        Box::pin(async move {
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &invocation.policy_revision,
                    &invocation.scope,
                )
                .map_err(|error| kolyan_core::ToolError::PolicyDenied {
                    message: error.to_string(),
                })?;
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "committed".into(),
                is_error: false,
            }))
        })
    }
}
