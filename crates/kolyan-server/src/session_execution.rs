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
        }
    }

    /// Select context projection for new Turns; resumptions retain the policy
    /// captured with their immutable input boundary.
    pub fn with_context_policy(mut self, policy: kolyan_storage::SessionContextPolicy) -> Self {
        self.context_policy = policy;
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
            let suspension = facts
                .iter()
                .rev()
                .find(|event| event.kind == LedgerEventKind::ExecutionSuspended);
            let status = terminal_status(&facts).or_else(|| {
                (state == ExecutionState::Suspended
                    && facts.iter().any(|event| {
                        event.kind == LedgerEventKind::ApprovalRequested
                            && event.payload.get("continuation").is_some()
                            && suspension.is_some_and(|suspended| {
                                suspended.payload["approval_id"] == event.payload["approval_id"]
                            })
                    }))
                .then_some(SessionTurnStatus::Suspended)
            });
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
            .start(executor, request, session_id.clone(), execution_id)
            .await;
        self.commit_result(&session_id, &turn_id, current_messages, result)
    }

    pub async fn resume<P, T>(
        &self,
        executor: TurnExecutor<P, T>,
        session_id: impl Into<String>,
        execution_id: impl Into<String>,
        approval_id: &str,
    ) -> Result<DurableTurnResult, ServerError>
    where
        P: ModelProvider,
        T: ToolExecutor,
    {
        let session_id = session_id.into();
        let execution_id = execution_id.into();
        let approval = self.execution.load_approval(&execution_id, approval_id)?;
        let session = self.sessions.load(&session_id)?;
        let turn = session
            .turns
            .iter()
            .find(|turn| turn.turn_id == approval.turn_id && turn.execution_id == execution_id)
            .ok_or_else(|| StorageError::Conflict("approval does not belong to Session".into()))?;
        if turn.status != SessionTurnStatus::Suspended {
            return Err(StorageError::Conflict("Session Turn is not suspended".into()).into());
        }
        let input = session
            .inputs
            .get(&approval.turn_id)
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
        if !approval
            .continuation
            .model_request
            .messages
            .starts_with(&expected)
        {
            return Err(StorageError::Conflict(
                "checkpoint input differs from original Turn".into(),
            )
            .into());
        }
        let pending_messages = input.messages.clone();
        let result = self
            .execution
            .resume(executor, session_id.clone(), execution_id, approval_id)
            .await;
        self.commit_result(&session_id, &approval.turn_id, pending_messages, result)
    }

    /// Reject exactly the current persisted approval without invoking model or
    /// tool code. The shared resume claim prevents approve/deny double decisions.
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
        let events = ledger
            .execution_events_after(&execution.execution_id, 0)
            .map_err(CoordinatorError::from)?;
        let current = events.iter().rev().find(|event| {
            event.execution_id == execution.execution_id
                && event.kind == LedgerEventKind::ApprovalRequested
        });
        if !current.is_some_and(|event| event.payload["approval_id"] == approval_id) {
            return Err(StorageError::Conflict("approval is not current".into()).into());
        }
        let approval = self
            .execution
            .load_approval(&execution.execution_id, approval_id)?;
        if approval.turn_id != execution.turn_id {
            return Err(StorageError::Conflict("approval Turn mismatch".into()).into());
        }
        executor
            .reject_approval(approval, approval_id, "denied by user")
            .map_err(RuntimeError::from)?;
        if !ledger
            .claim(&format!(
                "{}/attempt/resume/{approval_id}",
                execution.execution_id
            ))
            .map_err(CoordinatorError::from)?
        {
            return Err(StorageError::Conflict("approval already decided".into()).into());
        }
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
        self.execution.server.coordinator().append_once(
            execution,
            &format!("approval/{approval_id}/resolved"),
            LedgerEventKind::ApprovalResolved,
            json!({"approval_id":approval_id, "decision":"deny"}),
        )?;
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
                "turn-cancelled-at-approval",
                LedgerEventKind::TurnCancelled,
                json!({"boundary":"approval"}),
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
            None if state == ExecutionState::Suspended
                && events.iter().any(|event| {
                    event.kind == LedgerEventKind::ApprovalRequested
                        && event.payload.get("continuation").is_some()
                }) =>
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
            Ok(DurableTurnResult::AwaitingApproval {
                approval,
                trajectory,
            }) => {
                self.commit_session(
                    session_id,
                    turn_id,
                    SessionTurnStatus::Suspended,
                    Vec::new(),
                )?;
                Ok(DurableTurnResult::AwaitingApproval {
                    approval,
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
                if let Some(status @ (SessionTurnStatus::Failed | SessionTurnStatus::Cancelled)) =
                    terminal_status(&events)
                {
                    self.commit_session(session_id, turn_id, status, Vec::new())?;
                }
                Err(error)
            }
        }
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
