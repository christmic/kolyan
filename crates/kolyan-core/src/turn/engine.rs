use super::*;

pub(super) struct PendingTools {
    pub batch: ToolCallBatch,
    pub assistant_content: Vec<ContentBlock>,
    pub approved: Vec<String>,
}

pub(super) struct RunState {
    pub turn_id: String,
    pub model_request: ModelRequest,
    pub steps: Vec<StepResult>,
    pub config: TurnConfig,
    pub deadline: Option<Instant>,
    pub deadline_at_ms: Option<u64>,
    pub tool_calls_used: usize,
    pub dispatch: ToolDispatchPolicy,
    pub tool_timeout: Option<Duration>,
    pub pending: Option<PendingTools>,
    resumed: bool,
    resume_boundary: Option<String>,
}

impl RunState {
    pub fn new(
        request: TurnRequest,
        dispatch: ToolDispatchPolicy,
        tool_timeout: Option<Duration>,
    ) -> Result<Self, TurnError> {
        validate_request(&request)?;
        Ok(Self {
            turn_id: request.turn_id,
            model_request: request.model_request,
            steps: Vec::new(),
            config: request.config,
            deadline: request.config.deadline.map(|limit| Instant::now() + limit),
            deadline_at_ms: request
                .config
                .deadline
                .map(|limit| now_ms().saturating_add(limit.as_millis() as u64)),
            tool_calls_used: 0,
            dispatch,
            tool_timeout,
            pending: None,
            resumed: false,
            resume_boundary: None,
        })
    }

    pub fn restore(approval: &ApprovalRequest) -> Result<Self, TurnError> {
        let c = &approval.continuation;
        let invalid = || TurnError::InvalidRequest {
            message: "inconsistent approval checkpoint".into(),
        };
        if approval
            .expires_at_ms
            .is_some_and(|limit| limit <= now_ms())
        {
            return Err(TurnError::InvalidRequest {
                message: "approval checkpoint has expired".into(),
            });
        }
        let last = c.steps.last().ok_or_else(invalid)?;
        let batch = ToolCallBatch::try_from(c.pending_calls.clone())?;
        let call = batch.call(&c.call_id).ok_or_else(invalid)?;
        let step_id = format!("{}-step-{}", c.turn_id, c.steps.len() - 1);
        if approval.turn_id != c.turn_id
            || approval.call_id != c.call_id
            || approval.tool_name != c.tool_name
            || c.tool_name != call.name
            || c.next_step_index != c.steps.len()
            || c.max_steps < c.steps.len()
            || c.max_steps == 0
            || last.step_id != step_id
            || c.steps
                .iter()
                .enumerate()
                .any(|(index, step)| step.step_id != format!("{}-step-{index}", c.turn_id))
            || last.outcome != StepOutcome::ToolCalls
            || c.assistant_content != last.response.content
            || c.pending_calls != tool_calls(&last.response.content)
            || c.args_fingerprint != serde_json::to_string(&call.arguments).expect("JSON arguments")
            || approval.approval_id != format!("{step_id}-approval-{}", call.id)
            || c.continuation_id != format!("continuation-{}", approval.approval_id)
            || c.approved_call_ids.contains(&call.id)
            || c.approved_call_ids.iter().collect::<HashSet<_>>().len() != c.approved_call_ids.len()
            || c.approved_call_ids
                .iter()
                .any(|id| batch.call(id).is_none())
        {
            return Err(invalid());
        }
        Ok(Self {
            turn_id: c.turn_id.clone(),
            model_request: c.model_request.clone(),
            steps: c.steps.clone(),
            config: TurnConfig {
                max_steps: c.max_steps,
                max_tool_calls: c.max_tool_calls,
                deadline: None,
            },
            deadline: deadline_instant(c.deadline_at_ms),
            deadline_at_ms: c.deadline_at_ms,
            tool_calls_used: c.tool_calls_used,
            dispatch: c.tool_dispatch,
            tool_timeout: c.tool_timeout_ms.map(Duration::from_millis),
            pending: Some(PendingTools {
                batch,
                assistant_content: c.assistant_content.clone(),
                approved: c.approved_call_ids.clone(),
            }),
            resumed: true,
            resume_boundary: Some(approval.approval_id.clone()),
        })
    }

    pub fn step_id(&self) -> String {
        format!(
            "{}-step-{}",
            self.turn_id,
            self.steps.len().saturating_sub(1)
        )
    }

    pub fn check(&self, control: &TurnControl) -> Result<(), TurnError> {
        if control.is_cancelled() {
            return Err(TurnError::Cancelled);
        }
        if self.deadline.is_some_and(|limit| limit <= Instant::now()) {
            return Err(TurnError::TimedOut);
        }
        Ok(())
    }

    fn checkpoint(
        &self,
        call: &ToolCall,
        reason: String,
        policy_version: String,
    ) -> ApprovalRequest {
        let pending = self.pending.as_ref().expect("pending approval batch");
        let approval_id = format!("{}-approval-{}", self.step_id(), call.id);
        ApprovalRequest {
            approval_id: approval_id.clone(),
            turn_id: self.turn_id.clone(),
            call_id: call.id.clone(),
            tool_name: call.name.clone(),
            reason,
            state: ApprovalState::Pending,
            expires_at_ms: None,
            continuation: TurnContinuation {
                continuation_id: format!("continuation-{approval_id}"),
                approval_id,
                turn_id: self.turn_id.clone(),
                model_request: self.model_request.clone(),
                assistant_content: pending.assistant_content.clone(),
                pending_calls: pending.batch.calls().to_vec(),
                steps: self.steps.clone(),
                max_steps: self.config.max_steps,
                next_step_index: self.steps.len(),
                call_id: call.id.clone(),
                tool_name: call.name.clone(),
                args_fingerprint: serde_json::to_string(&call.arguments).expect("JSON arguments"),
                policy_version,
                approved_call_ids: pending.approved.clone(),
                max_tool_calls: self.config.max_tool_calls,
                tool_calls_used: self.tool_calls_used,
                deadline_at_ms: self.deadline_at_ms,
                tool_dispatch: self.dispatch,
                tool_timeout_ms: self.tool_timeout.map(|limit| limit.as_millis() as u64),
            },
        }
    }
}

impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    pub(super) async fn admit(
        &self,
        state: &RunState,
        control: &TurnControl,
        kind: TurnBoundaryKind,
    ) -> Result<(), TurnError> {
        state.check(control)?;
        if let Some(gate) = &self.boundary_control {
            let admission = gate.admit(TurnBoundary {
                turn_id: state.turn_id.clone(),
                kind,
            });
            tokio::select! {
                biased;
                _ = control.wait_cancelled() => return Err(TurnError::Cancelled),
                result = async {
                    match state.deadline {
                        Some(limit) => tokio::time::timeout_at(limit.into(), admission).await.map_err(|_| TurnError::TimedOut)?,
                        None => admission.await,
                    }
                } => result?,
            }
        }
        Ok(())
    }

    pub(super) fn validate_approval_policy(
        &self,
        approval: &ApprovalRequest,
    ) -> Result<(), TurnError> {
        let policy = self
            .policy_engine
            .as_ref()
            .ok_or_else(|| TurnError::InvalidRequest {
                message: "approval resume requires a policy engine".into(),
            })?;
        let c = &approval.continuation;
        for call in &c.pending_calls {
            if call.id == c.call_id || c.approved_call_ids.contains(&call.id) {
                let decision = policy.decide_with_context(
                    call,
                    &PolicyContext {
                        turn_id: Some(c.turn_id.clone()),
                        ..Default::default()
                    },
                );
                if decision.kind != PolicyDecisionKind::RequireApproval
                    || decision.policy_version != c.policy_version
                {
                    return Err(TurnError::InvalidRequest {
                        message: "approval is stale under the current policy".into(),
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) async fn run(
        &self,
        mut state: RunState,
        control: TurnControl,
        suspend: bool,
        queue: Option<EventQueue>,
    ) -> Result<ResumableTurn, TurnError> {
        let events = EventEmitter::new(queue);
        if !state.resumed {
            events.emit(TurnEvent::Started {
                turn_id: state.turn_id.clone(),
            });
        }
        match self.drive(&mut state, &control, suspend, &events).await {
            Ok(DriveExit::Suspended(approval)) => Ok(ResumableTurn::AwaitingApproval(approval)),
            Ok(DriveExit::Completed(result)) => {
                Ok(ResumableTurn::Completed(Box::new(TurnExecution {
                    result: *result,
                    events: events.into_events(),
                })))
            }
            Err(error) => {
                // Do not wait on the control adapter again after cancellation,
                // timeout or adapter failure. Runtime commits this terminal result.
                events.emit(terminal_event(&state.turn_id, &error));
                Err(error)
            }
        }
    }

    async fn drive(
        &self,
        state: &mut RunState,
        control: &TurnControl,
        suspend: bool,
        events: &EventEmitter,
    ) -> Result<DriveExit, TurnError> {
        if let Some(approval_id) = state.resume_boundary.take() {
            self.admit(
                state,
                control,
                TurnBoundaryKind::ResumeApproval { approval_id },
            )
            .await?;
        }
        loop {
            state.check(control)?;
            if state.pending.is_some()
                && let Some(approval) = self.handle_pending(state, control, suspend, events).await?
            {
                return Ok(DriveExit::Suspended(Box::new(approval)));
            }
            if state.steps.len() >= state.config.max_steps {
                return self
                    .complete(
                        state,
                        control,
                        events,
                        TurnOutcome::MaxSteps,
                        TurnEndReason::MaxSteps,
                    )
                    .await;
            }
            let step_id = format!("{}-step-{}", state.turn_id, state.steps.len());
            self.admit(
                state,
                control,
                TurnBoundaryKind::Step {
                    step_id: step_id.clone(),
                },
            )
            .await?;
            state.model_request.request_id = step_id.clone();
            events.emit(TurnEvent::StepStarted {
                turn_id: state.turn_id.clone(),
                step_id: step_id.clone(),
            });
            let step_control = StepControl::default();
            let step_future = self.step_executor.execute_with_control(
                StepRequest {
                    step_id,
                    model_request: state.model_request.clone(),
                    options: StepExecutionOptions {
                        deadline: state.deadline,
                        ..Default::default()
                    },
                },
                step_control.clone(),
            );
            let step = tokio::select! {
                biased;
                _ = control.wait_cancelled() => { step_control.cancel(); return Err(TurnError::Cancelled); },
                result = async {
                    match state.deadline {
                        Some(limit) => tokio::time::timeout_at(limit.into(), step_future).await.map_err(|_| TurnError::TimedOut)?.map_err(map_step_error),
                        None => step_future.await.map_err(map_step_error),
                    }
                } => result?,
            };
            state.steps.push(step.clone());
            events.emit(TurnEvent::StepCompleted {
                turn_id: state.turn_id.clone(),
                step: step.clone(),
            });
            if step.outcome != StepOutcome::ToolCalls {
                let (outcome, reason) = outcome_from_step(&step);
                return self.complete(state, control, events, outcome, reason).await;
            }
            let batch = ToolCallBatch::try_from(tool_calls(&step.response.content))?;
            state.pending = Some(PendingTools {
                batch,
                assistant_content: step.response.content,
                approved: Vec::new(),
            });
        }
    }

    async fn complete(
        &self,
        state: &RunState,
        control: &TurnControl,
        events: &EventEmitter,
        outcome: TurnOutcome,
        reason: TurnEndReason,
    ) -> Result<DriveExit, TurnError> {
        self.admit(state, control, TurnBoundaryKind::Terminal { reason })
            .await?;
        events.emit(TurnEvent::Completed {
            turn_id: state.turn_id.clone(),
            outcome: outcome.clone(),
        });
        Ok(DriveExit::Completed(Box::new(TurnResult {
            turn_id: state.turn_id.clone(),
            outcome,
            end_reason: reason,
            steps: state.steps.clone(),
        })))
    }

    async fn handle_pending(
        &self,
        state: &mut RunState,
        control: &TurnControl,
        suspend: bool,
        events: &EventEmitter,
    ) -> Result<Option<ApprovalRequest>, TurnError> {
        let pending = state.pending.as_ref().expect("pending tools");
        if state
            .config
            .max_tool_calls
            .is_some_and(|limit| pending.batch.len() > limit.saturating_sub(state.tool_calls_used))
        {
            return Err(TurnError::ToolBudgetExceeded);
        }
        let plan = self.policy_engine.as_ref().map(|policy| {
            policy.resolve_batch(
                &PolicyContext {
                    turn_id: Some(state.turn_id.clone()),
                    remaining_tool_calls: state
                        .config
                        .max_tool_calls
                        .map(|limit| limit.saturating_sub(state.tool_calls_used)),
                    ..Default::default()
                },
                pending.batch.calls(),
            )
        });
        if let Some(plan) = &plan {
            if state.dispatch.on_error == ToolErrorPolicy::FailTurn
                && let Some(denied) = plan
                    .decisions
                    .iter()
                    .find(|item| item.decision.kind == PolicyDecisionKind::Deny)
            {
                let call = pending.batch.call(&denied.call_id).expect("planned call");
                let error = ToolError::PolicyDenied {
                    message: denied.decision.reason.clone(),
                };
                events.emit(TurnEvent::ToolCallRequested {
                    turn_id: state.turn_id.clone(),
                    call: call.clone(),
                });
                events.emit(TurnEvent::ToolExecutionFailed {
                    turn_id: state.turn_id.clone(),
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    error: error.clone(),
                });
                return Err(error.into());
            }
            for item in &plan.decisions {
                let pending = state.pending.as_ref().expect("pending tools");
                if item.decision.kind != PolicyDecisionKind::RequireApproval
                    || pending.approved.contains(&item.call_id)
                {
                    continue;
                }
                let call = pending
                    .batch
                    .call(&item.call_id)
                    .expect("planned call")
                    .clone();
                let checkpoint = state.checkpoint(
                    &call,
                    item.decision.reason.clone(),
                    item.decision.policy_version.clone(),
                );
                self.admit(
                    state,
                    control,
                    TurnBoundaryKind::AwaitingApproval {
                        approval_id: checkpoint.approval_id.clone(),
                    },
                )
                .await?;
                events.emit(TurnEvent::ToolCallRequested {
                    turn_id: state.turn_id.clone(),
                    call: call.clone(),
                });
                events.emit(TurnEvent::ApprovalRequested {
                    turn_id: state.turn_id.clone(),
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                });
                if suspend {
                    return Ok(Some(checkpoint));
                }
                tokio::select! {
                    biased;
                    _ = control.wait_cancelled() => return Err(TurnError::Cancelled),
                    result = async {
                        let approval = control.wait_for_tool_approval(&call.name);
                        match state.deadline {
                            Some(limit) => tokio::time::timeout_at(limit.into(), approval).await.map_err(|_| TurnError::TimedOut),
                            None => { approval.await; Ok(()) },
                        }
                    } => result?,
                }
                self.admit(
                    state,
                    control,
                    TurnBoundaryKind::ResumeApproval {
                        approval_id: checkpoint.approval_id,
                    },
                )
                .await?;
                state
                    .pending
                    .as_mut()
                    .expect("pending tools")
                    .approved
                    .push(call.id);
            }
        }
        let results = self
            .dispatch_pending(state, control, plan.as_ref(), events)
            .await?;
        state.check(control)?;
        let pending = state.pending.take().expect("pending tools");
        state.tool_calls_used += pending.batch.len();
        append_tool_context(
            &mut state.model_request.messages,
            &pending.assistant_content,
            results,
        );
        state.model_request.tool_choice = ToolChoice::Auto;
        Ok(None)
    }
}

enum DriveExit {
    Suspended(Box<ApprovalRequest>),
    Completed(Box<TurnResult>),
}

#[cfg(test)]
mod tests;
