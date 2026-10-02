//! Immutable Session input, projection and recoverable commit coordination.

use super::*;

impl<L, S, SS> SessionExecutionService<L, S, SS>
where
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    pub fn new(execution: ExecutionService<L, S>, sessions: SessionService<SS>) -> Self {
        Self {
            execution,
            sessions,
            context_policy: kolyan_storage::SessionContextPolicy::ConversationOnly,
            preparation_hook: None,
        }
    }

    /// Select context projection for new Turns; resumptions retain the policy
    /// captured with their immutable input boundary.
    pub fn with_context_policy(mut self, policy: kolyan_storage::SessionContextPolicy) -> Self {
        self.context_policy = policy;
        self
    }

    pub fn with_preparation_hook(mut self, hook: Arc<dyn TurnPreparationHook>) -> Self {
        self.preparation_hook = Some(hook);
        self
    }

    pub fn execution(&self) -> &ExecutionService<L, S> {
        &self.execution
    }

    pub fn sessions(&self) -> &SessionService<SS> {
        &self.sessions
    }

    /// Repair stopped Session projections from existing durable facts. Active
    /// attempts retain ownership of their commit; this never drives execution.
    pub fn load_reconciled(&self, session_id: &str) -> Result<SessionRecord, ServerError> {
        let session = self.sessions.load(session_id)?;
        let coordinator = self.execution.server.coordinator();
        for turn in &session.turns {
            if coordinator.is_active(&turn.execution_id) {
                continue;
            }
            let facts = coordinator
                .ledger()
                .execution_events_after(&turn.execution_id, 0)
                .map_err(CoordinatorError::from)?;
            let state = self.execution.state(&turn.execution_id)?;
            let key = ExecutionRef {
                session_id: session_id.into(),
                turn_id: turn.turn_id.clone(),
                execution_id: turn.execution_id.clone(),
            };
            let suspended = state == ExecutionState::Suspended
                && suspension::current_suspension(&key, &facts)?.is_some();
            let status = match terminal_status(&facts) {
                Some(status) => Some(status),
                None if self.verified_execution_cancellation(&key)? => {
                    Some(SessionTurnStatus::Cancelled)
                }
                None => suspended.then_some(SessionTurnStatus::Suspended),
            };
            if let Some(status) = status
                && (status != turn.status
                    || !facts.iter().any(|event| {
                        event.kind == LedgerEventKind::SessionCommitted
                            && event.payload["status"] == json!(status)
                    }))
            {
                self.reconcile(session_id, &turn.execution_id)?;
            }
        }
        Ok(self.sessions.load(session_id)?)
    }

    pub async fn start<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        mut request: TurnRequest,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Result<DurableTurnResult, ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let deadline = kolyan_core::TurnDeadline::capture(
            request.config.deadline,
            executor.absolute_deadline_at_ms(),
        )?;
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let turn_id = request.turn_id.clone();
        let current_messages = request.model_request.messages.clone();
        let session = self.sessions.load(&session_id)?;
        let mut contextual_messages = match self.context_policy {
            kolyan_storage::SessionContextPolicy::ConversationOnly => session.messages,
            kolyan_storage::SessionContextPolicy::FullTrajectory => session.context_messages,
        };
        contextual_messages.extend(current_messages.clone());
        request.model_request.messages = contextual_messages;
        if let Some(hook) = &self.preparation_hook {
            preparation::prepare_turn(
                hook.as_ref(),
                &ExecutionRef {
                    session_id: session_id.clone(),
                    turn_id: turn_id.clone(),
                    execution_id: execution_id.clone(),
                },
                session.version,
                &mut request,
                current_messages.len(),
                &deadline,
            )
            .await?;
        }
        if deadline
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
        {
            return Err(PreparationFailure::DeadlineExpired.into());
        }
        self.sessions.store.begin_turn_with_projection(
            &session_id,
            SessionTurn {
                turn_id: turn_id.clone(),
                execution_id: execution_id.clone(),
                status: SessionTurnStatus::Running,
            },
            session.version,
            current_messages.clone(),
            self.context_policy,
        )?;

        let result = self
            .execution
            .start_with_deadline(
                executor,
                request,
                session_id.clone(),
                execution_id,
                deadline,
            )
            .await;
        self.commit_result(&session_id, &turn_id, current_messages, result)
    }

    pub async fn resume<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        checkpoint_id: &str,
        input: ResumeInput,
    ) -> Result<DurableTurnResult, ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let suspension = self
            .execution
            .load_suspension(&execution_id, checkpoint_id)?;
        let turn_id = suspension.checkpoint.scope.execution.turn_id.clone();
        let pending_messages =
            self.resume_messages(&session_id, &execution_id, &suspension, false)?;
        let result = self
            .execution
            .resume(
                executor,
                session_id.clone(),
                execution_id,
                checkpoint_id,
                input,
            )
            .await;
        self.commit_result(&session_id, &turn_id, pending_messages, result)
    }

    pub async fn resume_approval<P: ModelProvider, T: ToolExecutor>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        approval_id: &str,
    ) -> Result<DurableTurnResult, ServerError> {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let suspension = self.execution.load_current_suspension(&execution_id)?;
        let turn_id = suspension.checkpoint.scope.execution.turn_id.clone();
        let pending_messages =
            self.resume_messages(&session_id, &execution_id, &suspension, false)?;
        let result = self
            .execution
            .resume_approval(executor, &session_id, &execution_id, approval_id)
            .await;
        self.commit_result(&session_id, &turn_id, pending_messages, result)
    }

    /// Recover only the persisted merge/drive gap, retaining immutable input.
    pub async fn resume_committed<P: ModelProvider, T: ToolExecutor>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        checkpoint_id: &str,
    ) -> Result<DurableTurnResult, ServerError> {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let saved = self
            .execution
            .load_suspension(&execution_id, checkpoint_id)?;
        let turn_id = saved.checkpoint.scope.execution.turn_id.clone();
        let pending_messages = self.resume_messages(&session_id, &execution_id, &saved, true)?;
        let result = self
            .execution
            .resume_committed(executor, &session_id, &execution_id, checkpoint_id)
            .await;
        self.commit_result(&session_id, &turn_id, pending_messages, result)
    }

    fn resume_messages(
        &self,
        session_id: &str,
        execution_id: &str,
        suspension: &TurnSuspension,
        committed_recovery: bool,
    ) -> Result<Vec<Message>, ServerError> {
        let turn_id = &suspension.checkpoint.scope.execution.turn_id;
        let session = self.sessions.load(session_id)?;
        let turn = session
            .turns
            .iter()
            .find(|turn| &turn.turn_id == turn_id && turn.execution_id == execution_id)
            .ok_or_else(|| {
                StorageError::Conflict("suspension does not belong to Session".into())
            })?;
        if suspension.checkpoint.scope.execution.session_id != session_id {
            return Err(StorageError::Conflict("foreign suspension Session".into()).into());
        }
        if turn.status != SessionTurnStatus::Suspended
            && !(committed_recovery && turn.status == SessionTurnStatus::Running)
        {
            return Err(StorageError::Conflict("Session Turn is not suspended".into()).into());
        }
        let input = session
            .inputs
            .get(turn_id)
            .ok_or_else(|| StorageError::Conflict("missing immutable Turn input".into()))?;
        let history = match input.context_policy {
            kolyan_storage::SessionContextPolicy::ConversationOnly => &session.messages,
            kolyan_storage::SessionContextPolicy::FullTrajectory => &session.context_messages,
        };
        if history.len() != input.history_len {
            return Err(StorageError::Conflict(
                "Session history changed while Turn was suspended".into(),
            )
            .into());
        }
        let mut expected = history.clone();
        expected.extend(input.messages.clone());
        // Restore the already admitted selection, never rerun the host hook or
        // rebuild an unselected checkpoint from current Session history.
        let admitted = kolyan_runtime::verified_execution_input(
            self.execution.server.coordinator().ledger(),
            &kolyan_runtime::ExecutionKey {
                session_id: session_id.into(),
                turn_id: turn_id.clone(),
                execution_id: execution_id.into(),
            },
            16 * 1024 * 1024,
        )?;
        let mut full = admitted.model_request.clone();
        full.messages = expected;
        preparation::validate_selection(&full, &admitted.model_request, input.messages.len())?;
        if suspension.checkpoint.input_message_count != admitted.model_request.messages.len()
            || !suspension
                .checkpoint
                .model_request
                .messages
                .starts_with(&admitted.model_request.messages)
        {
            return Err(StorageError::Conflict(
                "checkpoint input differs from original Turn".into(),
            )
            .into());
        }
        Ok(input.messages.clone())
    }

    /// Reject exactly the current persisted approval without invoking model or
    /// tool code. Local admission and an exact durable decision exclude racing
    /// approval; no permanent resume claim can strand a checkpoint on restart.
    /// A crash after the terminal fact is recoverable through reconcile.
    pub fn deny<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        execution: &ExecutionRef,
        approval_id: &str,
    ) -> Result<(), ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let session = self.sessions.load(&execution.session_id)?;
        if !session.turns.iter().any(|turn| {
            turn.turn_id == execution.turn_id
                && turn.execution_id == execution.execution_id
                && turn.status == SessionTurnStatus::Suspended
        }) || self.execution.state(&execution.execution_id)? != ExecutionState::Suspended
        {
            return Err(
                StorageError::Conflict("Turn is not suspended under this Session".into()).into(),
            );
        }
        let ledger = self.execution.server.coordinator().ledger();
        let suspension = self
            .execution
            .load_current_suspension(&execution.execution_id)?;
        if suspension.checkpoint.scope.execution.turn_id != execution.turn_id {
            return Err(StorageError::Conflict("approval Turn mismatch".into()).into());
        }
        executor
            .with_execution_key(suspension.checkpoint.scope.execution.clone())
            .reject_approval(suspension.clone(), approval_id, "denied by user")
            .map_err(RuntimeError::from)?;
        self.execution
            .server
            .coordinator()
            .admit(execution.clone(), AdmissionKind::Resume)?;
        let _guard = ExecutionGuard {
            server: self.execution.server.clone(),
            execution_id: execution.execution_id.clone(),
        };
        suspension::record_approval_decision(ledger, execution, &suspension, approval_id, "deny")?;
        // Terminal persistence is atomic against cancellation. No side effect
        // is authorized by this decision, even if the process stops afterwards.
        let event_id = format!("{}/approval/{approval_id}/denied", execution.execution_id);
        ledger
            .append_unless_cancelled(LedgerEvent {
                event_id: event_id.clone(),
                turn_id: execution.turn_id.clone(),
                execution_id: execution.execution_id.clone(),
                cursor: 0,
                kind: LedgerEventKind::TurnFailed,
                idempotency_key: event_id,
                payload: json!({"reason":"ApprovalRejected", "approval_id":approval_id}),
            })
            .map_err(CoordinatorError::from)?;
        self.commit_session(
            &execution.session_id,
            &execution.turn_id,
            SessionTurnStatus::Failed,
            Vec::new(),
        )
    }

    pub fn cancel(&self, execution: &ExecutionRef) -> Result<(), ServerError> {
        let state = self.execution.state(&execution.execution_id)?;
        self.execution.cancel(execution)?;
        if state == ExecutionState::Suspended {
            self.execution.server.coordinator().append_once(
                execution,
                "turn-cancelled-at-suspension",
                LedgerEventKind::TurnCancelled,
                json!({"boundary":"suspension"}),
            )?;
            self.commit_session(
                &execution.session_id,
                &execution.turn_id,
                SessionTurnStatus::Cancelled,
                Vec::new(),
            )?;
        }
        Ok(())
    }

    pub fn state(&self, execution_id: &str) -> Result<ExecutionState, ServerError> {
        self.execution.state(execution_id)
    }

    /// Reapply a durable Session commit without rerunning the model or tools.
    pub fn reconcile(
        &self,
        session_id: &str,
        execution_id: &str,
    ) -> Result<SessionRecord, ServerError> {
        let session = self.sessions.load(session_id)?;
        let turn = session
            .turns
            .iter()
            .find(|turn| turn.execution_id == execution_id)
            .ok_or_else(|| StorageError::NotFound(execution_id.into()))?;
        let ledger = self.execution.server.coordinator.ledger();
        let events = ledger
            .execution_events_after(execution_id, 0)
            .map_err(CoordinatorError::from)?;
        let state = self.execution.state(execution_id)?;
        let status = match terminal_status(&events) {
            Some(status) => status,
            None if self.verified_execution_cancellation(&ExecutionRef {
                session_id: session_id.into(),
                turn_id: turn.turn_id.clone(),
                execution_id: execution_id.into(),
            })? =>
            {
                SessionTurnStatus::Cancelled
            }
            None if state == ExecutionState::Suspended
                && suspension::current_suspension(
                    &ExecutionRef {
                        session_id: session_id.into(),
                        turn_id: turn.turn_id.clone(),
                        execution_id: execution_id.into(),
                    },
                    &events,
                )?
                .is_some() =>
            {
                SessionTurnStatus::Suspended
            }
            _ => {
                return Err(StorageError::Conflict(
                    "execution has no recoverable Session commit".into(),
                )
                .into());
            }
        };
        if let Some(intent) = events.iter().rev().find(|event| {
            event.kind == LedgerEventKind::SessionCommitPrepared
                && event.payload["session_id"] == session_id
                && event.payload["status"] == json!(status)
        }) {
            let messages = serde_json::from_value(intent.payload["messages"].clone())
                .map_err(StorageError::from)?;
            self.commit_session(session_id, &turn.turn_id, status, messages)?;
        } else {
            let mut messages = Vec::new();
            if status == SessionTurnStatus::Completed {
                messages = session
                    .inputs
                    .get(&turn.turn_id)
                    .ok_or_else(|| StorageError::Conflict("missing immutable Turn input".into()))?
                    .messages
                    .clone();
                let event = events
                    .iter()
                    .rev()
                    .find(|event| event.kind == LedgerEventKind::StepCompleted)
                    .ok_or_else(|| {
                        StorageError::Conflict("completed execution has no Step fact".into())
                    })?;
                let step: kolyan_core::StepResult =
                    serde_json::from_value(event.payload["step"].clone())
                        .map_err(StorageError::from)?;
                if step.outcome != kolyan_core::StepOutcome::ToolCalls {
                    messages.push(Message {
                        role: MessageRole::Assistant,
                        content: step.response.content,
                    });
                }
            }
            self.commit_session(session_id, &turn.turn_id, status, messages)?;
        }
        Ok(self.sessions.load(session_id)?)
    }

    fn commit_session(
        &self,
        session_id: &str,
        turn_id: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
    ) -> Result<(), ServerError> {
        let session = self.sessions.load(session_id)?;
        let turn = session
            .turns
            .iter()
            .find(|turn| turn.turn_id == turn_id)
            .ok_or_else(|| StorageError::NotFound(turn_id.into()))?;
        let execution = ExecutionRef {
            session_id: session_id.into(),
            turn_id: turn_id.into(),
            execution_id: turn.execution_id.clone(),
        };
        let coordinator = self.execution.server.coordinator();
        let mut context = Vec::new();
        if status == SessionTurnStatus::Completed {
            context = session
                .inputs
                .get(turn_id)
                .ok_or_else(|| StorageError::Conflict("missing immutable Turn input".into()))?
                .messages
                .clone();
            for event in coordinator
                .ledger()
                .execution_events_after(&execution.execution_id, 0)
                .map_err(CoordinatorError::from)?
                .into_iter()
                .filter(|event| {
                    event
                        .event_id
                        .starts_with(&format!("{}/turn-event/", execution.execution_id))
                })
            {
                match event.kind {
                    LedgerEventKind::StepCompleted => {
                        let step: kolyan_core::StepResult =
                            serde_json::from_value(event.payload["step"].clone())
                                .map_err(StorageError::from)?;
                        context.push(Message {
                            role: MessageRole::Assistant,
                            content: step.response.content,
                        });
                    }
                    LedgerEventKind::ToolExecutionCompleted => {
                        let result = serde_json::from_value(event.payload["result"].clone())
                            .map_err(StorageError::from)?;
                        context.push(Message {
                            role: MessageRole::User,
                            content: vec![kolyan_model::ContentBlock::ToolResult { result }],
                        });
                    }
                    _ => {}
                }
            }
        }
        coordinator.append_once(
            &execution,
            &format!("session/{status:?}/prepared"),
            LedgerEventKind::SessionCommitPrepared,
            json!({"session_id": session_id, "status": status, "messages": messages, "context_messages": context}),
        )?;
        self.sessions
            .store
            .update_turn_with_context(session_id, turn_id, status, messages, context)?;
        coordinator.append_once(
            &execution,
            &format!("session/{status:?}/committed"),
            LedgerEventKind::SessionCommitted,
            json!({"session_id": session_id, "status": status}),
        )?;
        Ok(())
    }

    fn commit_result(
        &self,
        session_id: &str,
        turn_id: &str,
        input_messages: Vec<Message>,
        result: Result<DurableTurnResult, ServerError>,
    ) -> Result<DurableTurnResult, ServerError> {
        match result {
            Ok(DurableTurnResult::Suspended {
                suspension,
                trajectory,
            }) => {
                self.commit_session(
                    session_id,
                    turn_id,
                    SessionTurnStatus::Suspended,
                    Vec::new(),
                )?;
                Ok(DurableTurnResult::Suspended {
                    suspension,
                    trajectory,
                })
            }
            Ok(DurableTurnResult::Completed(execution, trajectory)) => {
                let messages = completed_messages(&execution, input_messages);
                self.commit_session(session_id, turn_id, SessionTurnStatus::Completed, messages)?;
                Ok(DurableTurnResult::Completed(execution, trajectory))
            }
            Err(error) => {
                // Rejected duplicate attempts are not execution failures. Only
                // durable terminal facts may close the original Session Turn.
                let session = self.sessions.load(session_id)?;
                let turn = session
                    .turns
                    .iter()
                    .find(|turn| turn.turn_id == turn_id)
                    .ok_or_else(|| StorageError::NotFound(turn_id.into()))?;
                let events = self
                    .execution
                    .server
                    .coordinator()
                    .ledger()
                    .execution_events_after(&turn.execution_id, 0)
                    .map_err(CoordinatorError::from)?;
                let status = match terminal_status(&events) {
                    Some(status) => Some(status),
                    None if self.verified_execution_cancellation(&ExecutionRef {
                        session_id: session_id.into(),
                        turn_id: turn_id.into(),
                        execution_id: turn.execution_id.clone(),
                    })? =>
                    {
                        Some(SessionTurnStatus::Cancelled)
                    }
                    None => None,
                };
                if let Some(status @ (SessionTurnStatus::Failed | SessionTurnStatus::Cancelled)) =
                    status
                {
                    self.commit_session(session_id, turn_id, status, Vec::new())?;
                }
                Err(error)
            }
        }
    }

    /// Authenticate cancellation of this exact registered Session Turn, not an
    /// error string or a fabricated Core terminal. Completed facts win elsewhere.
    fn verified_execution_cancellation(
        &self,
        execution: &ExecutionRef,
    ) -> Result<bool, ServerError> {
        let id = format!("{}/execution-cancelled", execution.execution_id);
        let ledger = self.execution.server.coordinator().ledger();
        let Some(event) = ledger.event_by_id(&id).map_err(CoordinatorError::from)? else {
            return Ok(false);
        };
        if event.event_id != id
            || event.idempotency_key != id
            || event.kind != LedgerEventKind::ExecutionCancelled
            || event.execution_id != execution.execution_id
            || event.turn_id != execution.turn_id
            || !event.payload.is_null()
        {
            return Err(CoordinatorError::Ledger(kolyan_ledger::LedgerError::Conflict(id)).into());
        }
        let session = self.sessions.load(&execution.session_id)?;
        if !session.turns.iter().any(|turn| {
            turn.turn_id == execution.turn_id && turn.execution_id == execution.execution_id
        }) {
            return Err(StorageError::Conflict(
                "cancellation differs from registered Session Turn".into(),
            )
            .into());
        }
        Ok(self.execution.state(&execution.execution_id)? == ExecutionState::Cancelled)
    }
}

fn terminal_status(events: &[LedgerEvent]) -> Option<SessionTurnStatus> {
    events.iter().find_map(|event| match event.kind {
        LedgerEventKind::TurnCompleted => Some(SessionTurnStatus::Completed),
        LedgerEventKind::TurnCancelled => Some(SessionTurnStatus::Cancelled),
        LedgerEventKind::TurnFailed | LedgerEventKind::TurnTimedOut => {
            Some(SessionTurnStatus::Failed)
        }
        _ => None,
    })
}

fn completed_messages(execution: &TurnExecution, input: Vec<Message>) -> Vec<Message> {
    let mut messages = input;
    let response = match &execution.result.outcome {
        TurnOutcome::FinalAnswer { response }
        | TurnOutcome::Refused { response }
        | TurnOutcome::Incomplete { response } => Some(response),
        TurnOutcome::Rejected { .. } | TurnOutcome::Expired { .. } | TurnOutcome::MaxSteps => None,
    };
    if let Some(response) = response {
        messages.push(Message {
            role: MessageRole::Assistant,
            content: response.content.clone(),
        });
    }
    messages
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod deadline_tests;
