use super::engine::RunState;
use super::*;
use kolyan_policy::BatchExecutionPlan;
use std::collections::HashMap;

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    pub(super) async fn dispatch_pending(
        &self,
        state: &RunState,
        control: &TurnControl,
        plan: Option<&BatchExecutionPlan>,
        events: &EventEmitter,
    ) -> Result<Vec<ToolResult>, TurnError> {
        let pending = state.pending.as_ref().expect("pending batch");
        let mut grants = HashMap::new();
        let mut results = Vec::new();
        let stages = match plan {
            Some(plan) => {
                for item in &plan.decisions {
                    let call = pending.batch.call(&item.call_id).expect("planned call");
                    if item.decision.kind == PolicyDecisionKind::Deny {
                        events.emit(TurnEvent::ToolCallRequested {
                            turn_id: state.turn_id.clone(),
                            call: call.clone(),
                        });
                        let error = ToolError::PolicyDenied {
                            message: item.decision.reason.clone(),
                        };
                        results.push(ToolDispatchResult {
                            call_id: call.id.clone(),
                            result: Err(error),
                        });
                    } else {
                        let grant = if item.decision.kind == PolicyDecisionKind::RequireApproval {
                            if !pending.approved.contains(&call.id) {
                                return Err(TurnError::InvalidRequest {
                                    message: "missing invocation approval".into(),
                                });
                            }
                            item.decision.clone().into_approved_grant(call)
                        } else {
                            item.decision.clone().into_grant(call)
                        };
                        grants.insert(
                            call.id.clone(),
                            grant.map_err(|error| TurnError::InvalidRequest {
                                message: error.to_string(),
                            })?,
                        );
                    }
                }
                plan.stages_with_approvals(&pending.approved)
            }
            None => vec![
                pending
                    .batch
                    .calls()
                    .iter()
                    .map(|call| call.id.clone())
                    .collect(),
            ],
        };
        for stage in stages {
            let calls = stage
                .iter()
                .map(|id| pending.batch.call(id).expect("scheduled call"));
            let mut stage_results = Vec::new();
            match state.dispatch.mode {
                ToolDispatchMode::Serial => {
                    for call in calls {
                        let dispatch = self
                            .dispatch_call(
                                state,
                                control,
                                call,
                                grants.get(&call.id).cloned(),
                                events,
                            )
                            .await;
                        let dispatch = match dispatch {
                            Ok(dispatch) => dispatch,
                            Err(error) => {
                                for result in &stage_results {
                                    emit_dispatch_result(state, result, events);
                                }
                                return Err(error);
                            }
                        };
                        if state.dispatch.on_error == ToolErrorPolicy::FailTurn
                            && let Err(error) = &dispatch.result
                        {
                            for result in &stage_results {
                                emit_dispatch_result(state, result, events);
                            }
                            emit_dispatch_result(state, &dispatch, events);
                            return Err(error.clone().into());
                        }
                        stage_results.push(dispatch);
                    }
                }
                ToolDispatchMode::Parallel => {
                    let futures = calls.map(|call| {
                        self.dispatch_call(
                            state,
                            control,
                            call,
                            grants.get(&call.id).cloned(),
                            events,
                        )
                    });
                    let mut admission_error = None;
                    for result in join_all(futures).await {
                        match result {
                            Ok(result) => stage_results.push(result),
                            Err(error) => {
                                admission_error.get_or_insert(error);
                            }
                        }
                    }
                    if let Some(error) = admission_error {
                        for result in &stage_results {
                            emit_dispatch_result(state, result, events);
                        }
                        return Err(error);
                    }
                    if state.dispatch.on_error == ToolErrorPolicy::FailTurn
                        && let Some(error) = stage_results
                            .iter()
                            .find_map(|result| result.result.as_ref().err())
                    {
                        for result in &stage_results {
                            emit_dispatch_result(state, result, events);
                        }
                        return Err(error.clone().into());
                    }
                }
            }
            for result in &stage_results {
                emit_dispatch_result(state, result, events);
            }
            results.extend(stage_results);
        }
        pending.batch.validate_results(&results)?;
        let mut ordered = Vec::with_capacity(results.len());
        for call in pending.batch.calls() {
            let dispatch = results
                .iter()
                .find(|result| result.call_id == call.id)
                .expect("validated result");
            let result = match &dispatch.result {
                Ok(result) => result.clone(),
                Err(error) => {
                    // Denied calls never entered the dispatch boundary.
                    if plan.is_some_and(|plan| {
                        plan.decisions.iter().any(|item| {
                            item.call_id == call.id
                                && item.decision.kind == PolicyDecisionKind::Deny
                        })
                    }) {
                        events.emit(TurnEvent::ToolExecutionFailed {
                            turn_id: state.turn_id.clone(),
                            call_id: call.id.clone(),
                            name: call.name.clone(),
                            error: error.clone(),
                        });
                    }
                    let result = ToolResult {
                        call_id: call.id.clone(),
                        content: error.to_string(),
                        is_error: true,
                    };
                    events.emit(TurnEvent::ToolResult {
                        turn_id: state.turn_id.clone(),
                        result: result.clone(),
                    });
                    result
                }
            };
            ordered.push(result);
        }
        Ok(ordered)
    }

    async fn dispatch_call(
        &self,
        state: &RunState,
        control: &TurnControl,
        call: &ToolCall,
        grant: Option<ExecutionGrant>,
        events: &EventEmitter,
    ) -> Result<ToolDispatchResult, TurnError> {
        self.admit(
            state,
            control,
            TurnBoundaryKind::Tool {
                step_id: state.step_id(),
                call_id: call.id.clone(),
            },
        )
        .await?;
        state.check(control)?;
        if !state
            .pending
            .as_ref()
            .expect("pending batch")
            .approved
            .contains(&call.id)
        {
            events.emit(TurnEvent::ToolCallRequested {
                turn_id: state.turn_id.clone(),
                call: call.clone(),
            });
        }
        events.emit(TurnEvent::ToolExecutionStarted {
            turn_id: state.turn_id.clone(),
            call_id: call.id.clone(),
            name: call.name.clone(),
        });
        let execute = async {
            match grant {
                Some(grant) => {
                    self.tool_executor
                        .execute_with_grant(call.clone(), grant)
                        .await
                }
                None => self.tool_executor.execute(call.clone()).await,
            }
        };
        let tool_limit = state.tool_timeout.map(|timeout| Instant::now() + timeout);
        let deadline = match (state.deadline, tool_limit) {
            (Some(turn), Some(tool)) => Some(turn.min(tool)),
            (turn, tool) => turn.or(tool),
        };
        let result = tokio::select! {
            biased;
            _ = control.wait_cancelled() => Err(ToolError::Cancelled),
            result = async {
                match deadline {
                    Some(limit) => tokio::time::timeout_at(limit.into(), execute).await.unwrap_or(Err(ToolError::TimedOut)),
                    None => execute.await,
                }
            } => result,
        };
        match &result {
            Ok(result) if result.call_id != call.id => {
                return Err(ToolError::InvalidBatch {
                    message: format!(
                        "tool result call id mismatch: expected {}, got {}",
                        call.id, result.call_id
                    ),
                }
                .into());
            }
            _ => {}
        }
        let dispatch = ToolDispatchResult {
            call_id: call.id.clone(),
            result,
        };
        if let Err(error) = state.check(control) {
            emit_dispatch_result(state, &dispatch, events);
            return Err(error);
        }
        Ok(dispatch)
    }
}

fn emit_dispatch_result(state: &RunState, result: &ToolDispatchResult, events: &EventEmitter) {
    let call = state
        .pending
        .as_ref()
        .expect("pending tools")
        .batch
        .call(&result.call_id)
        .expect("dispatched call");
    match &result.result {
        Ok(result) => events.emit(TurnEvent::ToolResult {
            turn_id: state.turn_id.clone(),
            result: result.clone(),
        }),
        Err(error) => events.emit(TurnEvent::ToolExecutionFailed {
            turn_id: state.turn_id.clone(),
            call_id: call.id.clone(),
            name: call.name.clone(),
            error: error.clone(),
        }),
    }
}
