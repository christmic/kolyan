//! Fresh preparation and policy for Ready calls. Historical authority does not
//! enter preparation or new grant issuance. Completed feedback is charged too.

use super::engine::RunState;
use super::*;
use kolyan_policy::BatchExecutionPlan;

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    pub(super) async fn initialize_pending(
        &self,
        state: &mut RunState,
        control: &TurnControl,
        events: &EventEmitter,
    ) -> Result<(), TurnError> {
        if self.policy_engine.is_none() {
            return Err(TurnError::InvalidRequest {
                message: "tool execution requires a trusted policy engine".into(),
            });
        }
        let scope = self.tool_scope(&state.turn_id, &state.step_id())?;
        let calls = ToolCallBatch::try_from(tool_calls(
            &state.steps.last().expect("completed Step").response.content,
        ))?
        .into_calls();
        if state
            .tool_calls_used
            .checked_add(calls.len())
            .is_none_or(|requested| {
                state
                    .config
                    .max_tool_calls
                    .is_some_and(|limit| requested > limit)
            })
        {
            return Err(TurnError::ToolBudgetExceeded);
        }
        if let Some(policy) = &self.policy_engine
            && let Some(call) = calls.iter().find(|call| {
                policy.has_no_progress(
                    call,
                    &state.model_request.messages[state.input_message_count..],
                )
            })
        {
            return Err(TurnError::NoProgress {
                tool_name: call.name.clone(),
            });
        }
        let mut entries = Vec::new();
        for call in calls {
            match self.prepare_ready(state, control, &call).await {
                Ok(prepared) => entries.push(CheckpointCall {
                    call,
                    prepared: Some(prepared),
                    charged: false,
                    state: CheckpointCallState::Ready,
                }),
                Err(error) => {
                    let feedback = self.prepare_failure(state, &call, error, events)?;
                    entries.push(CheckpointCall {
                        call,
                        prepared: None,
                        charged: true,
                        state: CheckpointCallState::Completed {
                            result: feedback,
                            issued: None,
                        },
                    });
                }
            }
        }
        let charged = entries.iter().filter(|entry| entry.charged).count();
        let stages = vec![entries.iter().map(|entry| entry.call.id.clone()).collect()];
        state.pending = Some(
            TurnCheckpoint::reconstruct(
                CheckpointReconstruction {
                    scope: scope.clone(),
                    model_request: state.model_request.clone(),
                    input_message_count: state.input_message_count,
                    steps: state.steps.clone(),
                    calls: entries,
                    stages,
                    stage_index: 0,
                    approvals: Vec::new(),
                    budget: CheckpointBudget {
                        max_steps: state.config.max_steps,
                        max_tool_calls: state.config.max_tool_calls,
                        deadline_at_ms: state.deadline_at_ms,
                        tool_timeout_ms: state
                            .tool_timeout
                            .map(|timeout| timeout.as_millis() as u64),
                        prior_tool_calls_used: state.tool_calls_used,
                        tool_calls_used: state.tool_calls_used + charged,
                    },
                    dispatch: state.dispatch,
                },
                &scope,
            )
            .map_err(checkpoint_error)?,
        );
        Ok(())
    }

    pub(super) async fn prepare_ready(
        &self,
        state: &RunState,
        control: &TurnControl,
        call: &ToolCall,
    ) -> Result<PreparedCall, ToolError> {
        state.check(control).map_err(|error| match error {
            TurnError::Cancelled => ToolError::Cancelled,
            _ => ToolError::TimedOut,
        })?;
        let future = self.tool_executor.prepare(call.clone());
        let deadline = [
            state.deadline,
            state.tool_timeout.map(|timeout| Instant::now() + timeout),
        ]
        .into_iter()
        .flatten()
        .min();
        let prepared = tokio::select! {
            biased;
            _ = control.wait_cancelled() => return Err(ToolError::Cancelled),
            result = async {
                match deadline {
                    Some(limit) => tokio::time::timeout_at(limit.into(), future).await.unwrap_or(Err(ToolError::TimedOut)),
                    None => future.await,
                }
            } => result?,
        };
        prepared
            .validate()
            .map_err(|error| ToolError::InvalidBatch {
                message: error.to_string(),
            })?;
        if prepared.call() != call {
            return Err(ToolError::InvalidBatch {
                message: "preparation changed the original model call".into(),
            });
        }
        Ok(prepared)
    }

    fn prepare_failure(
        &self,
        state: &RunState,
        call: &ToolCall,
        error: ToolError,
        events: &EventEmitter,
    ) -> Result<ToolResult, TurnError> {
        events.emit(TurnEvent::ToolCallRequested {
            turn_id: state.turn_id.clone(),
            call: call.clone(),
        })?;
        events.emit(TurnEvent::ToolExecutionFailed {
            turn_id: state.turn_id.clone(),
            call_id: call.id.clone(),
            name: call.name.clone(),
            error: error.clone(),
        })?;
        if state.dispatch.on_error == ToolErrorPolicy::FailTurn
            || matches!(
                error,
                ToolError::Uncertain { .. }
                    | ToolError::Cancelled
                    | ToolError::TimedOut
                    | ToolError::InvalidBatch { .. }
            )
        {
            return Err(error.into());
        }
        let feedback = ToolResult {
            call_id: call.id.clone(),
            content: error.to_string(),
            is_error: true,
        };
        events.emit(TurnEvent::ToolResult {
            turn_id: state.turn_id.clone(),
            result: feedback.clone(),
        })?;
        Ok(feedback)
    }

    pub(super) async fn refresh_ready(
        &self,
        state: &mut RunState,
        control: &TurnControl,
        events: &EventEmitter,
    ) -> Result<BatchExecutionPlan, TurnError> {
        let ready: Vec<_> = state
            .pending
            .as_ref()
            .expect("checkpoint")
            .calls
            .iter()
            .filter(|item| matches!(item.state, CheckpointCallState::Ready))
            .map(|item| item.call.clone())
            .collect();
        for call in ready {
            let current = match self.prepare_ready(state, control, &call).await {
                Ok(current) => current,
                Err(error) => {
                    let feedback = self.prepare_failure(state, &call, error, events)?;
                    let checkpoint = state.pending.as_mut().expect("checkpoint");
                    let item = checkpoint
                        .calls
                        .iter_mut()
                        .find(|item| item.call.id == call.id)
                        .expect("Ready call");
                    item.prepared = None;
                    item.charged = true;
                    item.state = CheckpointCallState::Completed {
                        result: feedback,
                        issued: None,
                    };
                    checkpoint
                        .approvals
                        .retain(|approval| approval.prepared.call().id != call.id);
                    continue;
                }
            };
            let checkpoint = state.pending.as_mut().expect("checkpoint");
            if checkpoint.approvals.iter().any(|approval| {
                approval.prepared.call().id == call.id
                    && approval.evidence_id.is_some()
                    && approval.prepared != current
            }) {
                return Err(TurnError::InvalidRequest {
                    message: "approval preparation has changed".into(),
                });
            }
            checkpoint
                .calls
                .iter_mut()
                .find(|item| item.call.id == call.id)
                .expect("Ready call")
                .prepared = Some(current);
        }
        state.check(control)?;
        let policy = self
            .policy_engine
            .as_ref()
            .ok_or_else(|| TurnError::InvalidRequest {
                message: "tool execution requires a trusted policy engine".into(),
            })?;
        let checkpoint = state.pending.as_mut().expect("checkpoint");
        checkpoint.budget.tool_calls_used = checkpoint.budget.prior_tool_calls_used
            + checkpoint.calls.iter().filter(|item| item.charged).count();
        let preparations: Vec<_> = checkpoint
            .calls
            .iter()
            .filter(|item| matches!(item.state, CheckpointCallState::Ready))
            .map(|item| item.prepared.clone().expect("Ready preparation"))
            .collect();
        let plan = policy
            .resolve_prepared_batch(
                &PolicyContext {
                    turn_id: Some(state.turn_id.clone()),
                    remaining_tool_calls: state
                        .config
                        .max_tool_calls
                        .map(|limit| limit.saturating_sub(checkpoint.budget.tool_calls_used)),
                    ..Default::default()
                },
                &preparations,
            )
            .map_err(|error| TurnError::InvalidRequest {
                message: error.to_string(),
            })?;
        for planned in &plan.decisions {
            let item = checkpoint
                .calls
                .iter_mut()
                .find(|item| item.call.id == planned.call_id)
                .expect("planned call");
            let prepared = item.prepared.as_ref().expect("planned preparation");
            let saved = checkpoint
                .approvals
                .iter()
                .find(|approval| approval.prepared.call().id == item.call.id);
            if saved.is_some_and(|approval| {
                approval.evidence_id.is_some()
                    && (approval.policy_revision != planned.decision.policy_version
                        || planned.decision.kind != PolicyDecisionKind::RequireApproval)
            }) {
                return Err(TurnError::InvalidRequest {
                    message: "approval is stale under the current policy".into(),
                });
            }
            match planned.decision.kind {
                PolicyDecisionKind::Deny => {
                    let error = ToolError::PolicyDenied {
                        message: planned.decision.reason.clone(),
                    };
                    events.emit(TurnEvent::ToolCallRequested {
                        turn_id: state.turn_id.clone(),
                        call: item.call.clone(),
                    })?;
                    events.emit(TurnEvent::ToolExecutionFailed {
                        turn_id: state.turn_id.clone(),
                        call_id: item.call.id.clone(),
                        name: item.call.name.clone(),
                        error: error.clone(),
                    })?;
                    if state.dispatch.on_error == ToolErrorPolicy::FailTurn {
                        return Err(error.into());
                    }
                    let result = ToolResult {
                        call_id: item.call.id.clone(),
                        content: error.to_string(),
                        is_error: true,
                    };
                    events.emit(TurnEvent::ToolResult {
                        turn_id: state.turn_id.clone(),
                        result: result.clone(),
                    })?;
                    item.charged = true;
                    item.state = CheckpointCallState::Completed {
                        result,
                        issued: None,
                    };
                    checkpoint
                        .approvals
                        .retain(|approval| approval.prepared.call().id != item.call.id);
                }
                PolicyDecisionKind::RequireApproval => {
                    if saved.is_none_or(|approval| {
                        approval.prepared != *prepared
                            || approval.policy_revision != planned.decision.policy_version
                    }) {
                        checkpoint
                            .approvals
                            .retain(|approval| approval.prepared.call().id != item.call.id);
                        let approval_id = checkpoint::host_identity(
                            "approval",
                            &(
                                &checkpoint.scope,
                                &item.call.id,
                                prepared.digest(),
                                &planned.decision.policy_version,
                            ),
                        );
                        checkpoint.approvals.push(CheckpointApproval {
                            approval_id,
                            reason: planned.decision.reason.clone(),
                            prepared: prepared.clone(),
                            scope: checkpoint.scope.clone(),
                            policy_revision: planned.decision.policy_version.clone(),
                            evidence_id: None,
                            expires_at_ms: None,
                        });
                        events.emit(TurnEvent::ToolCallRequested {
                            turn_id: state.turn_id.clone(),
                            call: item.call.clone(),
                        })?;
                        events.emit(TurnEvent::ApprovalRequested {
                            turn_id: state.turn_id.clone(),
                            call_id: item.call.id.clone(),
                            name: item.call.name.clone(),
                        })?;
                    }
                }
                _ => {
                    checkpoint
                        .approvals
                        .retain(|approval| approval.prepared.call().id != item.call.id);
                }
            }
        }
        // All IDs only express potential dependency stages, never approval proof.
        let all: Vec<_> = plan
            .decisions
            .iter()
            .map(|item| item.call_id.clone())
            .collect();
        let mut stages = if state.dispatch.mode == ToolDispatchMode::Serial {
            checkpoint
                .calls
                .iter()
                .filter(|item| matches!(item.state, CheckpointCallState::Ready))
                .map(|item| vec![item.call.id.clone()])
                .collect::<Vec<_>>()
        } else {
            plan.stages_with_approvals(&all)
        };
        let completed: Vec<_> = checkpoint
            .calls
            .iter()
            .filter(|item| matches!(item.state, CheckpointCallState::Completed { .. }))
            .map(|item| item.call.id.clone())
            .collect();
        checkpoint.stage_index = usize::from(!completed.is_empty());
        if !completed.is_empty() {
            stages.insert(0, completed);
        }
        checkpoint.stages = stages;
        checkpoint.budget.tool_calls_used = checkpoint.budget.prior_tool_calls_used
            + checkpoint.calls.iter().filter(|item| item.charged).count();
        checkpoint
            .validate(&checkpoint.scope)
            .map_err(checkpoint_error)?;
        if let Some(recorder) = &self.event_recorder {
            recorder.record_checkpoint(checkpoint)?;
        }
        Ok(plan)
    }
}
