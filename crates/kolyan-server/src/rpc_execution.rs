//! Transport-neutral execution handlers. Fact persistence and Session commits
//! are delegated to the same services used by local callers.

use std::sync::Arc;

use kolyan_core::{ToolExecutor, TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::LedgerStore;
use kolyan_model::{ContentBlock, Message, MessageRole, ModelProvider, ModelRequest};
use kolyan_storage::SessionStore;
use kolyan_trace::TraceSink;
use serde_json::{Value, json};

use crate::{ExecutionRef, RpcRequest, RpcResponse, SessionExecutionService, rpc_error};

pub struct ExecutionRpc<L, S, SS, P, T> {
    service: SessionExecutionService<L, S, SS>,
    executor: Arc<dyn Fn() -> TurnExecutor<P, T> + Send + Sync>,
    template: ModelRequest,
    config: TurnConfig,
}

impl<L, S, SS, P, T> ExecutionRpc<L, S, SS, P, T>
where
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
    P: ModelProvider,
    T: ToolExecutor,
{
    pub fn new(
        service: SessionExecutionService<L, S, SS>,
        executor: impl Fn() -> TurnExecutor<P, T> + Send + Sync + 'static,
        template: ModelRequest,
        config: TurnConfig,
    ) -> Self {
        Self {
            service,
            executor: Arc::new(executor),
            template,
            config,
        }
    }

    pub async fn handle_json(&self, input: &str) -> String {
        let response = match serde_json::from_str::<RpcRequest>(input) {
            Ok(request) if request.jsonrpc == "2.0" => {
                let result = self.dispatch(&request).await;
                match result {
                    Ok(result) => RpcResponse {
                        jsonrpc: "2.0".into(),
                        id: request.id,
                        result: Some(result),
                        error: None,
                    },
                    Err((code, message)) => rpc_error(request.id, code, message),
                }
            }
            Ok(request) => rpc_error(request.id, -32600, "jsonrpc must be 2.0"),
            Err(error) => rpc_error(Value::Null, -32700, error.to_string()),
        };
        serde_json::to_string(&response).expect("RPC response")
    }

    async fn dispatch(&self, request: &RpcRequest) -> Result<Value, (i32, String)> {
        let params = &request.params;
        if request.method == "server.info" {
            return Ok(json!({"protocol_version":1}));
        }
        if request.method.starts_with("session.") {
            let id = string(params, "session_id")?;
            let session = match request.method.as_str() {
                "session.create" => self.service.sessions().create(id).map_err(failed)?,
                "session.load" => self.service.sessions().load(id).map_err(failed)?,
                "session.reconcile" => self
                    .service
                    .reconcile(id, string(params, "execution_id")?)
                    .map_err(failed)?,
                _ => return Err((-32601, "unknown method".into())),
            };
            return Ok(json!(session));
        }
        if !matches!(
            request.method.as_str(),
            "execution.start"
                | "execution.approve"
                | "execution.cancel"
                | "execution.status"
                | "execution.events"
        ) {
            return Err((-32601, "unknown method".into()));
        }
        let key = ExecutionRef {
            session_id: string(params, "session_id")?.into(),
            turn_id: string(params, "turn_id")?.into(),
            execution_id: string(params, "execution_id")?.into(),
        };
        let ledger = self.service.execution().server().coordinator().ledger();
        if request.method != "execution.start" {
            let session = self
                .service
                .sessions()
                .load(&key.session_id)
                .map_err(failed)?;
            if !session
                .turns
                .iter()
                .any(|turn| turn.turn_id == key.turn_id && turn.execution_id == key.execution_id)
            {
                return Err((-32602, "execution does not belong to Session Turn".into()));
            }
        }
        match request.method.as_str() {
            "execution.start" => {
                let input = string(params, "input")?;
                let mut model_request = self.template.clone();
                model_request.messages = vec![Message {
                    role: MessageRole::User,
                    content: vec![ContentBlock::Text { text: input.into() }],
                }];
                let request = TurnRequest {
                    turn_id: key.turn_id.clone(),
                    model_request,
                    config: self.config,
                };
                let result = self
                    .service
                    .start(
                        (self.executor)(),
                        request,
                        &key.session_id,
                        &key.execution_id,
                    )
                    .await
                    .map_err(failed)?;
                Ok(result_value(result))
            }
            "execution.approve" => {
                let result = self
                    .service
                    .resume(
                        (self.executor)(),
                        &key.session_id,
                        &key.execution_id,
                        string(params, "approval_id")?,
                    )
                    .await
                    .map_err(failed)?;
                Ok(result_value(result))
            }
            "execution.cancel" => {
                self.service.cancel(&key).map_err(failed)?;
                Ok(json!({"cancellation_requested":true}))
            }
            "execution.status" => {
                let state = self.service.state(&key.execution_id).map_err(failed)?;
                let events = ledger.events_after(0).map_err(failed)?;
                let cancelled = events.iter().any(|event| {
                    event.execution_id == key.execution_id
                        && event.kind == kolyan_ledger::LedgerEventKind::ExecutionCancelled
                });
                let stopped = events.iter().any(|event| {
                    event.execution_id == key.execution_id
                        && matches!(
                            event.kind,
                            kolyan_ledger::LedgerEventKind::TurnCancelled
                                | kolyan_ledger::LedgerEventKind::TurnCompleted
                                | kolyan_ledger::LedgerEventKind::TurnFailed
                                | kolyan_ledger::LedgerEventKind::TurnTimedOut
                        )
                });
                let state = if cancelled && !stopped {
                    json!("Cancelling")
                } else {
                    json!(state)
                };
                Ok(
                    json!({"state":state, "cancellation_requested":cancelled, "execution_stopped":stopped}),
                )
            }
            "execution.events" => {
                let cursor = match params.get("after_cursor") {
                    None => 0,
                    Some(value) => value
                        .as_u64()
                        .ok_or((-32602, "after_cursor must be unsigned".into()))?,
                };
                let events = ledger
                    .events_after(cursor)
                    .map_err(failed)?
                    .into_iter()
                    .filter(|event| event.execution_id == key.execution_id)
                    .take(1000)
                    .collect::<Vec<_>>();
                let next = events.last().map_or(cursor, |event| event.cursor);
                Ok(json!({"events":events, "next_cursor":next}))
            }
            _ => unreachable!("validated method"),
        }
    }
}

fn result_value(result: kolyan_runtime::DurableTurnResult) -> Value {
    match result {
        kolyan_runtime::DurableTurnResult::Completed(execution, _) => json!({
            "state":"Completed", "end_reason":format!("{:?}", execution.result.end_reason),
            "steps":execution.result.steps,
        }),
        kolyan_runtime::DurableTurnResult::AwaitingApproval { approval, .. } => json!({
            "state":"Suspended", "approval":approval,
        }),
    }
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, (i32, String)> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or((-32602, format!("{key} must be a nonempty string")))
}

fn failed(error: impl std::fmt::Display) -> (i32, String) {
    (-32000, error.to_string())
}
