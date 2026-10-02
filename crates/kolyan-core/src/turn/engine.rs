//! Model-loop advancement over a portable checkpoint; no suspended tool future.
use super::*;

pub(super) struct RunState {
    pub turn_id: String,
    pub model_request: ModelRequest,
    pub input_message_count: usize,
    pub steps: Vec<StepResult>,
    pub config: TurnConfig,
    pub deadline: Option<Instant>,
    pub deadline_at_ms: Option<u64>,
    pub tool_calls_used: usize,
    pub dispatch: ToolDispatchPolicy,
    pub tool_timeout: Option<Duration>,
    pub pending: Option<TurnCheckpoint>,
    resumed: bool,
    resume_boundary: Option<String>,
}
impl RunState {
    pub fn with_deadline(
        request: TurnRequest,
        dispatch: ToolDispatchPolicy,
        tool_timeout: Option<Duration>,
        deadline: TurnDeadline,
    ) -> Result<Self, TurnError> {
        validate_request(&request)?;
        deadline.validate_duration(request.config.deadline)?;
        Ok(Self {
            turn_id: request.turn_id,
            input_message_count: request.model_request.messages.len(),
            model_request: request.model_request,
            steps: Vec::new(),
            config: request.config,
            deadline: deadline.instant(),
            deadline_at_ms: deadline.deadline_at_ms(),
            tool_calls_used: 0,
            dispatch,
            tool_timeout,
            pending: None,
            resumed: false,
            resume_boundary: None,
        })
    }
    pub fn restore_with_deadline(
        checkpoint: &TurnCheckpoint,
        scope: &ToolExecutionScope,
        deadline: TurnDeadline,
    ) -> Result<Self, TurnError> {
        checkpoint.validate(scope).map_err(checkpoint_error)?;
        deadline.validate_duration(None)?;
        if deadline.deadline_at_ms() != checkpoint.budget.deadline_at_ms {
            return Err(TurnError::InvalidRequest {
                message: "restored deadline differs from the checkpoint cutoff".into(),
            });
        }
        Ok(Self {
            turn_id: scope.execution.turn_id.clone(),
            model_request: checkpoint.model_request.clone(),
            input_message_count: checkpoint.input_message_count,
            steps: checkpoint.steps.clone(),
            config: TurnConfig {
                max_steps: checkpoint.budget.max_steps,
                max_tool_calls: checkpoint.budget.max_tool_calls,
                deadline: None,
            },
            deadline: deadline.instant(),
            deadline_at_ms: checkpoint.budget.deadline_at_ms,
            tool_calls_used: checkpoint.budget.prior_tool_calls_used,
            dispatch: checkpoint.dispatch,
            tool_timeout: checkpoint.budget.tool_timeout_ms.map(Duration::from_millis),
            pending: Some(checkpoint.clone()),
            resumed: true,
            resume_boundary: Some(checkpoint.checkpoint_id.clone()),
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
}
impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    pub(super) fn new_run_state(&self, request: TurnRequest) -> Result<RunState, TurnError> {
        let deadline =
            TurnDeadline::capture(request.config.deadline, self.absolute_deadline_at_ms)?;
        RunState::with_deadline(request, self.tool_dispatch, self.tool_timeout, deadline)
    }
    pub(super) fn validate_resume_scope(
        &self,
        expected: &ToolExecutionScope,
    ) -> Result<(), TurnError> {
        if self.tool_scope(&expected.execution.turn_id, &expected.step_id)? != *expected {
            return Err(TurnError::InvalidRequest {
                message: "checkpoint belongs to another admitted execution scope".into(),
            });
        }
        Ok(())
    }
    pub(super) fn tool_scope(
        &self,
        turn_id: &str,
        step_id: &str,
    ) -> Result<ToolExecutionScope, TurnError> {
        let key = self
            .execution_key
            .clone()
            .ok_or_else(|| TurnError::InvalidRequest {
                message: "tool execution requires an admitted execution key".into(),
            })?;
        if key.turn_id != turn_id {
            return Err(TurnError::InvalidRequest {
                message: "execution key belongs to another Turn".into(),
            });
        }
        let scope = ToolExecutionScope {
            execution: key,
            step_id: step_id.into(),
            agent_snapshot_digest: self.agent_snapshot_digest.clone(),
        };
        scope.validate().map_err(|e| TurnError::InvalidRequest {
            message: e.to_string(),
        })?;
        Ok(scope)
    }
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
    pub(super) async fn run(
        &self,
        mut state: RunState,
        control: TurnControl,
        suspend: bool,
        queue: Option<EventQueue>,
    ) -> Result<ResumableTurn, TurnError> {
        let events = EventEmitter::new(queue, self.event_recorder.clone());
        if !state.resumed {
            events.emit(TurnEvent::Started {
                turn_id: state.turn_id.clone(),
            })?;
        }
        match self.drive(&mut state, &control, suspend, &events).await {
            Ok(DriveExit::Suspended(suspension)) => Ok(ResumableTurn::Suspended(suspension)),
            Ok(DriveExit::Completed(result)) => {
                Ok(ResumableTurn::Completed(Box::new(TurnExecution {
                    result: *result,
                    events: events.into_events(),
                })))
            }
            Err(error) => {
                // No readmission after cancellation or fatal uncertainty. Runtime
                // owns recovery evidence, never a fabricated completion receipt.
                if let Err(recording) = events.emit(terminal_event(&state.turn_id, &error)) {
                    if matches!(error, TurnError::Tool(ToolError::Uncertain { .. })) {
                        return Err(ToolError::Uncertain {
                            message: format!(
                                "{error}; terminal recording also failed: {recording}"
                            ),
                        }
                        .into());
                    }
                    return Err(recording);
                }
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
        if let Some(checkpoint_id) = state.resume_boundary.take() {
            self.admit(
                state,
                control,
                TurnBoundaryKind::ResumeCheckpoint { checkpoint_id },
            )
            .await?;
        }
        loop {
            state.check(control)?;
            if state.pending.is_some()
                && let Some(waiting) = self.handle_pending(state, control, suspend, events).await?
            {
                return Ok(DriveExit::Suspended(Box::new(waiting)));
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
            })?;
            if let Some(recorder) = &self.event_recorder {
                recorder.record_request(&state.model_request)?;
            }
            let step_control = StepControl::default();
            let future = self.step_executor.execute_with_control(
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
                        Some(limit) => tokio::time::timeout_at(limit.into(), future).await.map_err(|_| TurnError::TimedOut)?.map_err(map_step_error),
                        None => future.await.map_err(map_step_error),
                    }
                } => result?,
            };
            state.steps.push(step.clone());
            events.emit(TurnEvent::StepCompleted {
                turn_id: state.turn_id.clone(),
                step: step.clone(),
            })?;
            if step.outcome != StepOutcome::ToolCalls {
                let (outcome, reason) = outcome_from_step(&step);
                return self.complete(state, control, events, outcome, reason).await;
            }
            self.initialize_pending(state, control, events).await?;
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
        })?;
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
    ) -> Result<Option<TurnSuspension>, TurnError> {
        loop {
            state.check(control)?;
            let checkpoint = state.pending.as_ref().expect("pending checkpoint");
            if checkpoint
                .calls
                .iter()
                .any(|item| matches!(item.state, CheckpointCallState::AwaitingExternal { .. }))
            {
                return self.suspend_pending(state, control).await.map(Some);
            }
            if checkpoint
                .calls
                .iter()
                .all(|item| matches!(item.state, CheckpointCallState::Completed { .. }))
            {
                let checkpoint = state.pending.take().expect("complete batch");
                let batch = ToolCallBatch::try_from(
                    checkpoint
                        .calls
                        .iter()
                        .map(|item| item.call.clone())
                        .collect::<Vec<_>>(),
                )?;
                let paired: Vec<_> = checkpoint
                    .calls
                    .iter()
                    .map(|item| {
                        let CheckpointCallState::Completed { result, .. } = &item.state else {
                            unreachable!("checked complete")
                        };
                        ToolDispatchResult {
                            call_id: item.call.id.clone(),
                            result: Ok(result.clone()),
                        }
                    })
                    .collect();
                batch.validate_results(&paired)?;
                state.tool_calls_used = checkpoint.budget.tool_calls_used;
                let content = checkpoint
                    .steps
                    .last()
                    .expect("completed Step")
                    .response
                    .content
                    .clone();
                let results = checkpoint
                    .calls
                    .into_iter()
                    .map(|item| {
                        if let CheckpointCallState::Completed { result, .. } = item.state {
                            result
                        } else {
                            unreachable!("checked complete")
                        }
                    })
                    .collect();
                append_tool_context(&mut state.model_request.messages, &content, results);
                return Ok(None);
            }
            let plan = self.refresh_ready(state, control, events).await?;
            let summary = {
                let checkpoint = state.pending.as_ref().expect("refreshed checkpoint");
                checkpoint
                    .suspension_summary(&checkpoint.scope)
                    .map_err(checkpoint_error)?
            };
            if !suspend && let Some(approval) = summary.approvals.first() {
                self.admit(
                    state,
                    control,
                    TurnBoundaryKind::AwaitingApproval {
                        approval_id: approval.approval_id.clone(),
                    },
                )
                .await?;
                let wait = control.wait_for_tool_approval(&approval.tool_name);
                tokio::select! {
                    biased;
                    _ = control.wait_cancelled() => return Err(TurnError::Cancelled),
                    result = async {
                        match state.deadline {
                            Some(limit) => tokio::time::timeout_at(limit.into(), wait).await.map_err(|_| TurnError::TimedOut),
                            None => { wait.await; Ok(()) },
                        }
                    } => result?,
                }
                self.admit(
                    state,
                    control,
                    TurnBoundaryKind::ResumeApproval {
                        approval_id: approval.approval_id.clone(),
                    },
                )
                .await?;
                let checkpoint = state.pending.as_mut().expect("approval checkpoint");
                let saved = checkpoint
                    .approvals
                    .iter_mut()
                    .find(|saved| saved.approval_id == approval.approval_id)
                    .expect("derived approval");
                saved.evidence_id = Some(approval.approval_id.clone());
                continue;
            }
            let progressed = self
                .dispatch_current_stage(state, control, &plan, events)
                .await?;
            if !progressed {
                return self.suspend_pending(state, control).await.map(Some);
            }
        }
    }
    async fn suspend_pending(
        &self,
        state: &RunState,
        control: &TurnControl,
    ) -> Result<TurnSuspension, TurnError> {
        let checkpoint = state.pending.as_ref().expect("waiting checkpoint");
        let suspension = TurnSuspension::from_checkpoint(checkpoint.clone(), &checkpoint.scope)
            .map_err(checkpoint_error)?;
        if suspension.waiting.approvals.is_empty() && suspension.waiting.external_waits.is_empty() {
            return Err(TurnError::InvalidRequest {
                message: "tool stage has no runnable call or waiting reason".into(),
            });
        }
        for approval in &suspension.waiting.approvals {
            self.admit(
                state,
                control,
                TurnBoundaryKind::AwaitingApproval {
                    approval_id: approval.approval_id.clone(),
                },
            )
            .await?;
        }
        if !suspension.waiting.external_waits.is_empty() {
            self.admit(
                state,
                control,
                TurnBoundaryKind::AwaitingExternal {
                    checkpoint_id: checkpoint.checkpoint_id.clone(),
                    wait_ids: suspension
                        .waiting
                        .external_waits
                        .iter()
                        .map(|item| item.wait.wait_id.clone())
                        .collect(),
                },
            )
            .await?;
        }
        Ok(suspension)
    }
}
enum DriveExit {
    Suspended(Box<TurnSuspension>),
    Completed(Box<TurnResult>),
}
#[cfg(test)]
mod tests;
