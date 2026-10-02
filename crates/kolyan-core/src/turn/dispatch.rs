//! Bounded short tool invocations and collection of the current independent
//! stage. Uncertainty is fatal even under ordinary error-feedback policy.
use super::engine::RunState;
use super::*;
use kolyan_policy::BatchExecutionPlan;

struct DispatchObservation {
    call: ToolCall,
    issued: IssuedToolAuthority,
    outcome: Result<ToolOutcome, ToolError>,
}
impl<P: ModelProvider, T: ToolExecutor> TurnExecutor<P, T> {
    pub(super) async fn dispatch_current_stage(
        &self,
        state: &mut RunState,
        control: &TurnControl,
        plan: &BatchExecutionPlan,
        events: &EventEmitter,
    ) -> Result<bool, TurnError> {
        let checkpoint = state.pending.as_ref().expect("checkpoint");
        let Some(stage) = checkpoint.stages.get(checkpoint.stage_index) else {
            return Ok(true);
        };
        let pending_approval = checkpoint
            .approvals
            .iter()
            .any(|approval| approval.evidence_id.is_none());
        let mut invocations = Vec::new();
        for id in stage {
            let item = checkpoint
                .calls
                .iter()
                .find(|item| &item.call.id == id)
                .expect("stage member");
            if !matches!(item.state, CheckpointCallState::Ready) {
                continue;
            }
            let planned = plan
                .decisions
                .iter()
                .find(|decision| &decision.call_id == id)
                .expect("Ready decision");
            if planned.decision.kind == PolicyDecisionKind::RequireApproval && pending_approval {
                // Preserve batch-wide approval semantics: one confirmation does
                // not start required effects while another approval is pending.
                continue;
            }
            let evidence = if planned.decision.kind == PolicyDecisionKind::RequireApproval {
                let saved = checkpoint
                    .approvals
                    .iter()
                    .find(|approval| approval.prepared.call().id == *id)
                    .expect("required approval");
                if saved.expires_at_ms.is_some_and(|expiry| expiry <= now_ms()) {
                    return Err(TurnError::InvalidRequest {
                        message: "approval has expired".into(),
                    });
                }
                ApprovalEvidence::Confirmed {
                    scope: checkpoint.scope.clone(),
                    prepared_digest: saved.prepared.digest().into(),
                    policy_revision: saved.policy_revision.clone(),
                    evidence_id: saved.evidence_id.clone().expect("confirmed approval"),
                }
            } else {
                ApprovalEvidence::NotConfirmed
            };
            let prepared = item.prepared.clone().expect("Ready preparation");
            let grant = PreparedGrant::issue(
                &prepared,
                planned.decision.clone(),
                evidence,
                checkpoint.scope.clone(),
            )
            .map_err(|error| TurnError::InvalidRequest {
                message: error.to_string(),
            })?;
            let issued = IssuedToolAuthority {
                prepared,
                grant,
                scope: checkpoint.scope.clone(),
                policy_revision: planned.decision.policy_version.clone(),
            };
            issued
                .validate(&checkpoint.scope)
                .map_err(checkpoint_error)?;
            invocations.push((item.call.clone(), issued));
        }
        if invocations.is_empty() {
            return Ok(false);
        }
        match state.dispatch.mode {
            ToolDispatchMode::Serial => {
                for (call, issued) in invocations {
                    let observation = self
                        .dispatch_call(state, control, call, issued, events)
                        .await?;
                    let waiting =
                        matches!(observation.outcome, Ok(ToolOutcome::AwaitingExternal(_)));
                    self.apply_observation(state, observation, events)?;
                    state.check(control)?;
                    if waiting {
                        break;
                    }
                }
            }
            ToolDispatchMode::Parallel => {
                let futures = invocations
                    .into_iter()
                    .map(|(call, issued)| self.dispatch_call(state, control, call, issued, events));
                let observations = join_all(futures).await;
                let mut fatal = None;
                // Drain every already admitted short invocation before halting.
                // Their independent Runtime receipts are never discarded/replayed.
                for observation in observations {
                    let result = match observation {
                        Ok(observation) => self.apply_observation(state, observation, events),
                        Err(error) => Err(error),
                    };
                    if let Err(error) = result {
                        if matches!(error, TurnError::Tool(ToolError::Uncertain { .. })) {
                            fatal = Some(error);
                        } else {
                            fatal.get_or_insert(error);
                        }
                    }
                }
                if let Some(error) = fatal {
                    return Err(error);
                }
                state.check(control)?;
            }
        }
        let checkpoint = state.pending.as_mut().expect("checkpoint");
        checkpoint.budget.tool_calls_used = checkpoint.budget.prior_tool_calls_used
            + checkpoint.calls.iter().filter(|item| item.charged).count();
        checkpoint
            .validate(&checkpoint.scope)
            .map_err(checkpoint_error)?;
        Ok(true)
    }

    async fn dispatch_call(
        &self,
        state: &RunState,
        control: &TurnControl,
        call: ToolCall,
        issued: IssuedToolAuthority,
        events: &EventEmitter,
    ) -> Result<DispatchObservation, TurnError> {
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
        let announced = state
            .pending
            .as_ref()
            .expect("checkpoint")
            .approvals
            .iter()
            .any(|approval| approval.prepared.call().id == call.id);
        if !announced {
            events.emit(TurnEvent::ToolCallRequested {
                turn_id: state.turn_id.clone(),
                call: call.clone(),
            })?;
        }
        events.emit(TurnEvent::ToolExecutionStarted {
            turn_id: state.turn_id.clone(),
            call_id: call.id.clone(),
            name: call.name.clone(),
        })?;
        let grant_timeout =
            issued
                .grant
                .constraints()
                .timeout_ms
                .ok_or_else(|| TurnError::InvalidRequest {
                    message: "issued timeout is missing".into(),
                })?;
        let deadline = [
            state.deadline,
            state.tool_timeout.map(|timeout| Instant::now() + timeout),
            Some(Instant::now() + Duration::from_millis(grant_timeout)),
        ]
        .into_iter()
        .flatten()
        .min()
        .expect("mandatory grant timeout");
        let future = self.tool_executor.execute_invocation(ToolInvocation {
            prepared: issued.prepared.clone(),
            grant: issued.grant.clone(),
            scope: issued.scope.clone(),
            policy_revision: issued.policy_revision.clone(),
            control: control.clone(),
            window: ToolExecutionWindow::at_deadline(deadline),
        });
        let outcome = tokio::select! {
            biased;
            _ = control.wait_cancelled() => Err(ToolError::Cancelled),
            result = tokio::time::timeout_at(deadline.into(), future) => result.unwrap_or(Err(ToolError::TimedOut)),
        };
        Ok(DispatchObservation {
            call,
            issued,
            outcome,
        })
    }

    fn apply_observation(
        &self,
        state: &mut RunState,
        observation: DispatchObservation,
        events: &EventEmitter,
    ) -> Result<(), TurnError> {
        let DispatchObservation {
            call,
            issued,
            outcome,
        } = observation;
        let completed = match outcome {
            Ok(ToolOutcome::Completed(result)) => {
                issued.validate_result(&result).map_err(|error| {
                    TurnError::Tool(ToolError::InvalidBatch {
                        message: error.to_string(),
                    })
                })?;
                events.emit(TurnEvent::ToolResult {
                    turn_id: state.turn_id.clone(),
                    result: result.clone(),
                })?;
                CheckpointCallState::Completed {
                    result,
                    issued: Some(issued),
                }
            }
            Ok(ToolOutcome::AwaitingExternal(wait)) => {
                if let Err(error) = wait.validate() {
                    let error = ToolError::Uncertain {
                        message: format!(
                            "executor returned invalid external wait after admission: {error}"
                        ),
                    };
                    let recording = events.emit(TurnEvent::ToolExecutionFailed {
                        turn_id: state.turn_id.clone(),
                        call_id: call.id,
                        name: call.name,
                        error: error.clone(),
                    });
                    if let Err(recording) = recording {
                        return Err(ToolError::Uncertain {
                            message: format!("{error}; failure recording also failed: {recording}"),
                        }
                        .into());
                    }
                    return Err(error.into());
                }
                events.emit(TurnEvent::ToolAwaitingExternal {
                    turn_id: state.turn_id.clone(),
                    call_id: call.id.clone(),
                    wait: wait.clone(),
                })?;
                CheckpointCallState::AwaitingExternal { wait, issued }
            }
            Err(error) => {
                let recording = events.emit(TurnEvent::ToolExecutionFailed {
                    turn_id: state.turn_id.clone(),
                    call_id: call.id.clone(),
                    name: call.name,
                    error: error.clone(),
                });
                if matches!(error, ToolError::Uncertain { .. }) {
                    if let Err(recording) = recording {
                        return Err(ToolError::Uncertain {
                            message: format!("{error}; failure recording also failed: {recording}"),
                        }
                        .into());
                    }
                    return Err(error.into());
                }
                recording?;
                if matches!(error, ToolError::Cancelled) {
                    return Err(TurnError::Cancelled);
                }
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
                let result = ToolResult {
                    call_id: call.id.clone(),
                    content: error.to_string(),
                    is_error: true,
                };
                issued.validate_result(&result).map_err(|error| {
                    TurnError::Tool(ToolError::InvalidBatch {
                        message: error.to_string(),
                    })
                })?;
                events.emit(TurnEvent::ToolResult {
                    turn_id: state.turn_id.clone(),
                    result: result.clone(),
                })?;
                CheckpointCallState::Completed {
                    result,
                    issued: Some(issued),
                }
            }
        };
        let item = state
            .pending
            .as_mut()
            .expect("checkpoint")
            .calls
            .iter_mut()
            .find(|item| item.call.id == call.id)
            .expect("observed call");
        item.state = completed;
        item.charged = true;
        Ok(())
    }
}
