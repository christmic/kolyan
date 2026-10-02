//! Blocking commit sequence. No child may be driven before the final exact fact.

use kolyan_ledger::{FactDraft, FactSubject};
use kolyan_model::{ContentBlock, Message, MessageRole};
use kolyan_server::{InstanceOwner, InvocationDefinition, PrivateContextService};

use super::*;
use crate::{
    AgentInvocationBinding, AgentSnapshot, BindingContextKind, child_private_session_id,
    prepare_agent_invocation,
};

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    pub(super) fn admit_children_blocking(
        &self,
        owner: DelegationOwner,
        invocation: ToolInvocation,
        limits: InvokePrepareLimits,
        policy: Arc<PolicyEngine>,
    ) -> Result<ExternalWait, ToolError> {
        let (parent, parent_fact) = self.validate_child_owner(&owner)?;
        if invocation.scope != owner.scope || invocation.policy_revision != policy.revision() {
            return Err(denied("delegation scope or current policy differs"));
        }
        invocation
            .grant
            .validate(&invocation.prepared, &policy.revision(), &owner.scope)
            .map_err(denied)?;
        let fresh = prepare_agent_invocation(
            invocation.prepared.call().clone(),
            &parent,
            &owner.parent.execution,
            &self.catalog,
            &self.host,
            &limits,
        )
        .map_err(denied)?;
        if fresh.prepared() != &invocation.prepared {
            return Err(denied("delegation preparation changed"));
        }
        let issued = IssuedToolAuthority {
            prepared: invocation.prepared,
            grant: invocation.grant,
            scope: invocation.scope,
            policy_revision: invocation.policy_revision,
        };
        issued.validate(&owner.scope).map_err(denied)?;
        let coordinate = admission_coordinate(&owner, &issued)?;
        if let Some(existing) = self.load_child_admission(&coordinate)? {
            self.verify_child_admission(&owner, &issued, &existing)?;
            return wait(coordinate);
        }
        if invocation.control.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let task = self
            .service
            .coordinator()
            .snapshot(&owner.task_id)
            .map_err(denied)?;
        let count = fresh.children().len() as u64;
        if (task.invocations.len() as u64)
            .checked_add(count)
            .is_none_or(|n| n > task.definition.limits.max_invocations)
            || (task.attempts.len() as u64)
                .checked_add(count)
                .is_none_or(|n| n > task.definition.limits.max_attempts)
            || task.definition.limits.max_tokens.is_some_and(|limit| {
                task.usage.unreported_steps > 0
                    || task.usage.total().is_none_or(|used| used >= limit)
            })
        {
            return Err(denied(
                "shared child invocation/attempt/token budget exhausted",
            ));
        }
        let contexts = PrivateContextService::new(
            self.service.sessions().sessions().clone(),
            Arc::new(JournalPort(self.service.clone())),
            Arc::new(self.bindings.clone()),
        );
        let mut children = Vec::new();
        let mut causes = vec![parent_fact.clone()];
        for (index, intent) in fresh.children().iter().enumerate() {
            // Any interruption after the first effect is uncertain unless the
            // complete admission fact is committed. Never silently retry it.
            if invocation.control.is_cancelled() {
                return Err(uncertain("cancelled during child admission"));
            }
            let key =
                crate::digest(&("kolyan.agent.child.v1", &coordinate, index)).map_err(denied)?;
            let invocation_id = format!("agent.child.{key}");
            let instance = self
                .instances
                .reserve(InstanceOwner {
                    logical_session_id: owner.logical_session_id.clone(),
                    task_id: owner.task_id.clone(),
                    invocation_id: invocation_id.clone(),
                })
                .map_err(uncertain)?;
            let snapshot = AgentSnapshot::new(
                intent.definition.clone(),
                instance.instance_id,
                intent.permissions.clone(),
            )
            .map_err(uncertain)?;
            let saved = AgentInvocationBinding {
                task_id: owner.task_id.clone(),
                invocation_id: invocation_id.clone(),
                logical_session_id: owner.logical_session_id.clone(),
                private_session_id: child_private_session_id(
                    &owner.logical_session_id,
                    &owner.task_id,
                    &invocation_id,
                )
                .map_err(uncertain)?,
                context_kind: BindingContextKind::Child,
                snapshot,
            };
            let binding_fact = self.bindings.save(&saved).map_err(uncertain)?;
            let context_owner = PrivateContextOwner {
                logical_session_id: owner.logical_session_id.clone(),
                task_id: owner.task_id.clone(),
                invocation_id: invocation_id.clone(),
                private_session_id: saved.private_session_id.clone(),
                snapshot_digest: saved.snapshot.digest().into(),
            };
            let initialized = contexts
                .initialize(&context_owner, &binding_fact, projection(&intent.input))
                .map_err(uncertain)?;
            let initialization_digest = initialized
                .initialization
                .ok_or_else(|| uncertain("private initialization is absent"))?
                .binding_digest;
            let initialization = contexts
                .load_verified_initialization(
                    &context_owner,
                    &binding_fact,
                    super::super::input::MAX_INPUT_DOCUMENT_BYTES,
                )
                .map_err(uncertain)?;
            let skill_binding = self
                .prepare_skills(&saved, &binding_fact)
                .map_err(uncertain)?;
            let input = self
                .publish_input_document(
                    &saved,
                    kolyan_server::InvocationInputKind::Derived,
                    &super::super::input::ChildInput {
                        skill_binding: skill_binding.as_ref().map(|b| b.reference().clone()),
                        parent: owner.clone(),
                        parent_ownership: parent_fact.clone(),
                        issued: issued.clone(),
                        child_index: index,
                        intent: intent.clone(),
                        ownership: binding_fact.clone(),
                        context_owner: context_owner.clone(),
                        initialization: initialization.initialization_fact.clone(),
                        initialization_digest: initialization_digest.clone(),
                        initial_messages: initialization.initialization.messages,
                    },
                    super::super::skills::causes(
                        vec![
                            parent_fact.clone(),
                            binding_fact.clone(),
                            initialization.initialization_fact,
                        ],
                        skill_binding.as_ref().map(|b| b.reference()),
                    ),
                )
                .map_err(uncertain)?;
            let input_source = kolyan_server::InvocationInputSource::Derived {
                fact: input.reference,
            };
            let admitted_fact = format!("{invocation_id}/admitted");
            let admitted = self
                .service
                .coordinator()
                .admit_invocation(
                    &owner.task_id,
                    &admitted_fact,
                    InvocationDefinition {
                        invocation_id: invocation_id.clone(),
                        agent: saved.snapshot.identity().clone(),
                        constraints_digest: saved.snapshot.digest().into(),
                        role: intent.role,
                        parent_invocation_id: Some(owner.parent.invocation_id.clone()),
                        dependencies: vec![],
                        input_source: input_source.clone(),
                    },
                )
                .map_err(uncertain)?;
            causes.extend([
                binding_fact.clone(),
                instance.fact.clone(),
                FactRef {
                    stream_id: owner.task_id.clone(),
                    position: admitted.position,
                    fact_id: admitted_fact,
                },
            ]);
            children.push(AdmittedAgentChild {
                binding_fact,
                instance_fact: instance.fact,
                context_owner,
                initialization_digest,
                attempt: AttemptBinding {
                    attempt_id: format!("agent.attempt.{key}"),
                    invocation_id,
                    execution: kolyan_server::ExecutionRef {
                        session_id: saved.private_session_id,
                        turn_id: format!("agent.turn.{key}"),
                        execution_id: format!("agent.execution.{key}"),
                    },
                    agent: saved.snapshot.identity().clone(),
                    constraints_digest: saved.snapshot.digest().into(),
                    input_source,
                },
            });
            self.save_execution_link(&coordinate, children.last().expect("just appended child"))?;
        }
        let serialized = !self.parallel_eligible(&owner, &children, fresh.parallel())?;
        let admission = Admission {
            owner,
            parent_binding_fact: parent_fact,
            issued,
            children,
            parallel_requested: fresh.parallel(),
            serialized,
        };
        let payload = serde_json::to_value(&admission).map_err(uncertain)?;
        if serde_json::to_vec(&payload).map_err(uncertain)?.len() > MAX_ADMISSION_BYTES {
            return Err(uncertain("child admission payload exceeds bound"));
        }
        let draft = FactDraft {
            fact_id: coordinate.fact_id.clone(),
            subject: FactSubject {
                kind: WAIT_KIND.into(),
                id: coordinate.fact_id.clone(),
            },
            kind: ADMISSION_KIND.into(),
            schema_version: 1,
            critical: true,
            causes,
            payload,
        };
        self.service
            .coordinator()
            .journal()
            .append(&coordinate.stream_id, 0, vec![draft])
            .map_err(uncertain)?;
        let saved = self
            .load_child_admission(&coordinate)?
            .ok_or_else(|| uncertain("committed admission is missing"))?;
        if saved != admission {
            return Err(uncertain("committed admission differs"));
        }
        self.verify_child_admission(&admission.owner, &admission.issued, &saved)?;
        wait(coordinate)
    }

    pub(super) fn validate_child_owner(
        &self,
        owner: &DelegationOwner,
    ) -> Result<(AgentInvocationBinding, FactRef), ToolError> {
        let saved = self.inspect_child_owner(owner)?;
        let state = self
            .service
            .coordinator()
            .snapshot(&owner.task_id)
            .map_err(denied)?;
        let parent = &state.invocations[&owner.parent.invocation_id];
        let attempt = &state.attempts[&owner.parent.attempt_id];
        if state.state.is_terminal()
            || parent.cancellation_requested
            || attempt.cancellation_requested
            || !matches!(
                parent.state,
                kolyan_server::InvocationState::Running
                    | kolyan_server::InvocationState::Suspended
                    | kolyan_server::InvocationState::RecoveryRequired
            )
        {
            return Err(denied(
                "parent lifecycle does not authorize new work or consumption",
            ));
        }
        Ok(saved)
    }

    // Immutable inspection is not active-parent authority. Only the existing
    // child's lifecycle entry point may use it after RootOnly cancellation.
    pub(super) fn inspect_child_owner(
        &self,
        owner: &DelegationOwner,
    ) -> Result<(AgentInvocationBinding, FactRef), ToolError> {
        let result = self.inspect_historical_child_owner(owner)?;
        self.validate_current_permissions(&result.0.snapshot)?;
        Ok(result)
    }

    pub(super) fn inspect_historical_child_owner(
        &self,
        owner: &DelegationOwner,
    ) -> Result<(AgentInvocationBinding, FactRef), ToolError> {
        owner.scope.validate().map_err(denied)?;
        let (saved, fact) = self
            .bindings
            .load_with_reference(
                &owner.task_id,
                &owner.parent.invocation_id,
                &owner.logical_session_id,
            )
            .map_err(denied)?
            .ok_or_else(|| denied("parent Agent binding is absent"))?;
        let state = self
            .service
            .coordinator()
            .snapshot(&owner.task_id)
            .map_err(denied)?;
        let parent = state
            .invocations
            .get(&owner.parent.invocation_id)
            .ok_or_else(|| denied("parent is not admitted"))?;
        let attempt = state
            .attempts
            .get(&owner.parent.attempt_id)
            .ok_or_else(|| denied("parent attempt is absent"))?;
        let key = &owner.scope.execution;
        if parent.attempts.last() != Some(&owner.parent.attempt_id)
            || parent.definition.agent != owner.parent.agent
            || parent.definition.constraints_digest != owner.parent.constraints_digest
            || attempt.binding != owner.parent
            || saved.private_session_id != owner.parent.execution.session_id
            || saved.snapshot.identity() != &owner.parent.agent
            || saved.snapshot.digest() != owner.parent.constraints_digest
            || key.session_id != owner.parent.execution.session_id
            || key.turn_id != owner.parent.execution.turn_id
            || key.execution_id != owner.parent.execution.execution_id
            || owner.scope.agent_snapshot_digest.as_deref() != Some(saved.snapshot.digest())
        {
            return Err(denied(
                "parent scope, attempt, snapshot or lifecycle differs",
            ));
        }
        Ok((saved, fact))
    }

    pub(super) fn load_child_admission(
        &self,
        reference: &FactRef,
    ) -> Result<Option<Admission>, ToolError> {
        let rows = self
            .service
            .coordinator()
            .journal()
            .read(&reference.stream_id, 0, 2)
            .map_err(denied)?;
        let [record] = rows.as_slice() else {
            return if rows.is_empty() {
                Ok(None)
            } else {
                Err(denied("extra child admission facts"))
            };
        };
        if record.stream_id != reference.stream_id
            || record.position != reference.position
            || record.draft.fact_id != reference.fact_id
            || record.draft.subject.kind != WAIT_KIND
            || record.draft.subject.id != reference.fact_id
            || record.draft.kind != ADMISSION_KIND
            || record.draft.schema_version != 1
            || !record.draft.critical
            || serde_json::to_vec(&record.draft.payload)
                .map_err(denied)?
                .len()
                > MAX_ADMISSION_BYTES
        {
            return Err(denied("child admission coordinate or schema differs"));
        }
        let admission: Admission =
            serde_json::from_value(record.draft.payload.clone()).map_err(denied)?;
        if !record.draft.causes.contains(&admission.parent_binding_fact)
            || admission.children.iter().any(|child| {
                !record.draft.causes.contains(&child.binding_fact)
                    || !record.draft.causes.contains(&child.instance_fact)
            })
        {
            return Err(denied("child admission causal ownership is missing"));
        }
        Ok(Some(admission))
    }

    pub(super) fn verify_child_admission(
        &self,
        owner: &DelegationOwner,
        issued: &IssuedToolAuthority,
        admission: &Admission,
    ) -> Result<(), ToolError> {
        self.validate_child_owner(owner)?;
        self.inspect_child_admission(owner, issued, admission)
    }

    pub(super) fn inspect_child_admission(
        &self,
        owner: &DelegationOwner,
        issued: &IssuedToolAuthority,
        admission: &Admission,
    ) -> Result<(), ToolError> {
        self.inspect_child_owner(owner)?;
        self.inspect_historical_child_admission(owner, issued, admission)?;
        for child in &admission.children {
            let saved = self
                .bindings
                .load(
                    &owner.task_id,
                    &child.attempt.invocation_id,
                    &owner.logical_session_id,
                )
                .map_err(denied)?
                .ok_or_else(|| denied("child saved owner is absent"))?;
            self.verify_child_input(&saved, &child.attempt.input_source)
                .map_err(denied)?;
        }
        Ok(())
    }

    pub(super) fn inspect_historical_child_admission(
        &self,
        owner: &DelegationOwner,
        issued: &IssuedToolAuthority,
        admission: &Admission,
    ) -> Result<(), ToolError> {
        issued.validate(&owner.scope).map_err(denied)?;
        let (_, parent_fact) = self.inspect_historical_child_owner(owner)?;
        if &admission.owner != owner
            || &admission.issued != issued
            || admission.parent_binding_fact != parent_fact
            || (!admission.serialized && !admission.parallel_requested)
            || admission.children.is_empty()
            || admission.children.len() > 8
        {
            return Err(denied("child admission authority differs"));
        }
        let task = self
            .service
            .coordinator()
            .snapshot(&owner.task_id)
            .map_err(denied)?;
        for (index, child) in admission.children.iter().enumerate() {
            use kolyan_server::PrivateContextOwnershipVerifier;
            self.bindings
                .verify_owner(&child.context_owner, &child.binding_fact)
                .map_err(denied)?;
            let definition = &task
                .invocations
                .get(&child.attempt.invocation_id)
                .ok_or_else(|| denied("child task admission is absent"))?
                .definition;
            let saved = self
                .bindings
                .load(
                    &owner.task_id,
                    &child.attempt.invocation_id,
                    &owner.logical_session_id,
                )
                .map_err(denied)?
                .ok_or_else(|| denied("child saved owner is absent"))?;
            let (source, _): (_, super::super::input::ChildInput) = self
                .load_input_document(&saved, &child.attempt.input_source)
                .map_err(denied)?;
            let input = self
                .verify_historical_child_input(&saved, &child.attempt.input_source)
                .map_err(denied)?;
            let initialized = PrivateContextService::new(
                self.service.sessions().sessions().clone(),
                Arc::new(JournalPort(self.service.clone())),
                Arc::new(self.bindings.clone()),
            )
            .load_verified_initialization(
                &child.context_owner,
                &child.binding_fact,
                super::super::input::MAX_INPUT_DOCUMENT_BYTES,
            )
            .map_err(denied)?;
            if source.envelope.kind != kolyan_server::InvocationInputKind::Derived
                || source.causes
                    != super::super::skills::causes(
                        vec![
                            parent_fact.clone(),
                            child.binding_fact.clone(),
                            initialized.initialization_fact.clone(),
                        ],
                        input.skill_binding.as_ref(),
                    )
                || input.parent != *owner
                || input.parent_ownership != parent_fact
                || input.issued != *issued
                || input.child_index != index
                || input.ownership != child.binding_fact
                || input.context_owner != child.context_owner
                || input.initialization != initialized.initialization_fact
                || input.initialization_digest != child.initialization_digest
                || input.initial_messages != initialized.initialization.messages
                || input.initial_messages != projection(&input.intent.input)
                || input.intent.definition != *saved.snapshot.definition()
                || input.intent.permissions != *saved.snapshot.permissions()
                || input.intent.role != definition.role
                || definition.input_source != child.attempt.input_source
            {
                return Err(denied(
                    "child source differs from actual invocation and initialization",
                ));
            }
            let session = self
                .service
                .sessions()
                .sessions()
                .load(&child.attempt.execution.session_id)
                .map_err(denied)?;
            if child.context_owner.task_id != owner.task_id
                || child.context_owner.logical_session_id != owner.logical_session_id
                || child.context_owner.invocation_id != child.attempt.invocation_id
                || child.context_owner.private_session_id != child.attempt.execution.session_id
                || child.context_owner.snapshot_digest != child.attempt.constraints_digest
                || definition.parent_invocation_id.as_deref() != Some(&owner.parent.invocation_id)
                || definition.agent != child.attempt.agent
                || definition.constraints_digest != child.attempt.constraints_digest
                || session
                    .initialization
                    .as_ref()
                    .map(|value| &value.binding_digest)
                    != Some(&child.initialization_digest)
            {
                return Err(denied("child graph or private initialization differs"));
            }
        }
        Ok(())
    }
}

// Forward the existing Task journal without introducing another backing store.
struct JournalPort<J, L, S, SS>(Arc<kolyan_server::TaskExecutionService<J, L, S, SS>>);
impl<J, L, S, SS> FactJournal for JournalPort<J, L, S, SS>
where
    J: FactJournal,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone,
    SS: SessionStore + Clone,
{
    fn read(
        &self,
        stream: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        self.0.coordinator().journal().read(stream, after, limit)
    }
    fn append(
        &self,
        stream: &str,
        expected: u64,
        facts: Vec<FactDraft>,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        self.0
            .coordinator()
            .journal()
            .append(stream, expected, facts)
    }
}

fn projection(input: &str) -> Vec<Message> {
    vec![Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text { text: input.into() }],
    }]
}
