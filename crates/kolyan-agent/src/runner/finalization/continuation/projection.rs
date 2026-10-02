//! Explicit pre-execution source archive; finalization only reads and verifies it.
//! Projection remains the shared context operation, not another topology role.

use super::*;
use crate::context::{
    ContextPolicy, ContextProjectionPlan, ContextTokenCounter, ProjectedContext, project_context,
};
use kolyan_ledger::FactRef;
use kolyan_model::{ModelDescriptor, ModelRequest};
use kolyan_server::{VerifiedHistoricalContext, VerifiedPrivateContextInitialization};

use serde::{Deserialize, Serialize};

/// Configure the actual host counter, never a replayed/fabricated trusted count.
#[derive(Clone)]
pub struct ContinuationProjectionConfig {
    pub counter: Arc<dyn ContextTokenCounter + Send + Sync>,
}

/// Host-selected source policy and current input before the successor executes.
/// Full predecessor history is obtained through Server's frozen proof reader.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationProjectionRequest {
    pub task_id: String,
    pub logical_session_id: String,
    pub invocation_id: String,
    pub predecessor_invocation_id: String,
    pub current_input: ModelRequest,
    pub descriptor: ModelDescriptor,
    pub source_bounds: ContextPolicy,
    pub plan: ContextProjectionPlan,
    pub target_policy: ContextPolicy,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    #[serde(deserialize_with = "crate::runner::skills::required_binding")]
    skill_binding: Option<FactRef>,
    request: ContinuationProjectionRequest,
    source: ModelRequest,
    projected: ProjectedContext,
    proof: Proof,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proof {
    ownership: FactRef,
    initialization: FactRef,
    predecessor: kolyan_server::AttemptBinding,
    terminal: FactRef,
    prepared: ExecutionEvidence,
    committed: ExecutionEvidence,
}

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    /// Prepare and publish an exact source before successor invocation admission.
    /// This issues no grant and starts no execution. The caller must pass the
    /// returned source into InvocationAdmitted; a published candidate alone is
    /// never execution authority. Retained artifacts may survive interrupted preparation.
    pub async fn prepare_continuation_input(
        self: &Arc<Self>,
        request: ContinuationProjectionRequest,
    ) -> Result<kolyan_server::VerifiedInvocationInputSource, RunnerError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || runner.record_projection(request))
            .await
            .map_err(|error| invalid(format!("projection recorder failed: {error}")))?
    }

    fn record_projection(
        &self,
        request: ContinuationProjectionRequest,
    ) -> Result<kolyan_server::VerifiedInvocationInputSource, RunnerError> {
        let config = self
            .continuation_projection
            .as_ref()
            .ok_or_else(|| invalid("host projection config is absent"))?;
        let task = self.service.coordinator().snapshot(&request.task_id)?;
        if task.state.is_terminal() || task.invocations.contains_key(&request.invocation_id) {
            return Err(invalid(
                "Continuation source must precede successor invocation admission",
            ));
        }
        let predecessor_id = &request.predecessor_invocation_id;
        let predecessor_binding = task
            .invocations
            .get(predecessor_id)
            .and_then(|inv| inv.attempts.last())
            .and_then(|id| task.attempts.get(id))
            .ok_or_else(|| invalid("projection predecessor attempt absent"))?
            .binding
            .clone();
        let terminal = self.service.load_verified_historical_result(
            &request.task_id,
            &predecessor_binding,
            1024 * 1024,
        )?;
        let history = self.service.load_verified_historical_context(
            &request.task_id,
            &HistoricalContextRequest {
                binding: predecessor_binding,
                terminal_fact: terminal.terminal_fact,
                prepared: self.context_endpoint(&terminal.binding.execution, "prepared")?,
                committed: self.context_endpoint(&terminal.binding.execution, "committed")?,
                max_bytes: MAX_CONTEXT_BYTES,
            },
        )?;
        let (saved, ownership) = self
            .bindings
            .load_with_reference(
                &request.task_id,
                &request.invocation_id,
                &request.logical_session_id,
            )?
            .ok_or_else(|| invalid("projection owner absent"))?;
        self.validate_current_permissions(&saved.snapshot)
            .map_err(|error| invalid(error.to_string()))?;
        if saved.context_kind != BindingContextKind::Child
            || request.current_input.model != *saved.snapshot.definition().model()
        {
            return Err(invalid("projection model or invocation owner differs"));
        }
        let owner = PrivateContextOwner {
            logical_session_id: saved.logical_session_id.clone(),
            task_id: saved.task_id.clone(),
            invocation_id: saved.invocation_id.clone(),
            private_session_id: saved.private_session_id.clone(),
            snapshot_digest: saved.snapshot.digest().into(),
        };
        let contexts = PrivateContextService::new(
            self.service.sessions().sessions().clone(),
            Arc::new(ContextJournal(self.service.clone())),
            Arc::new(self.bindings.clone()),
        );
        let initialization = contexts
            .load_verified_initialization(&owner, &ownership, MAX_CONTEXT_BYTES)
            .map_err(|error| invalid(error.to_string()))?;
        let skill_binding = self.prepare_skills(&saved, &ownership)?;
        let expected_skill = skill_binding
            .as_ref()
            .and_then(crate::skills::skill_load_definition);
        let mut schemas = request
            .current_input
            .tools
            .iter()
            .filter(|d| d.name == crate::skills::SKILL_LOAD_NAME);
        if schemas.next() != expected_skill.as_ref() || schemas.next().is_some() {
            return Err(invalid(
                "Continuation input must contain the exact prepared Skill schema before selecting context",
            ));
        }
        let mut source = request.current_input.clone();
        source.messages = history.messages.clone();
        source
            .messages
            .extend(request.current_input.messages.clone());
        let projected = project_context(
            &source,
            &request.descriptor,
            &request.source_bounds,
            &request.plan,
            &request.target_policy,
            config.counter.as_ref(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        selected_matches_initialization(&request, &projected, &initialization)?;
        let proof = Proof {
            ownership: ownership.clone(),
            initialization: initialization.initialization_fact.clone(),
            predecessor: history.binding,
            terminal: history.terminal_fact.clone(),
            prepared: history.prepared,
            committed: history.committed,
        };
        let body = Body {
            skill_binding: skill_binding.as_ref().map(|b| b.reference().clone()),
            request,
            source,
            projected,
            proof,
        };
        self.publish_input_document(
            &saved,
            kolyan_server::InvocationInputKind::Derived,
            &body,
            crate::runner::skills::causes(
                vec![
                    ownership,
                    initialization.initialization_fact,
                    history.terminal_fact,
                ],
                body.skill_binding.as_ref(),
            ),
        )
    }

    pub(super) fn verify_continuation_projection(
        &self,
        request: &TaskFinalizationRequest,
        invocation_id: &str,
        initialization: &VerifiedPrivateContextInitialization,
        history: &VerifiedHistoricalContext,
        successor: &VerifiedTaskResult,
    ) -> Result<bool, RunnerError> {
        let saved = self
            .bindings
            .load(&request.task_id, invocation_id, &request.logical_session_id)?
            .ok_or_else(|| invalid("Continuation owner is absent"))?;
        let (source, body): (_, Body) =
            self.load_input_document(&saved, &successor.binding.input_source)?;
        let proof = &body.proof;
        if source.envelope.kind != kolyan_server::InvocationInputKind::Derived
            || source.causes
                != crate::runner::skills::causes(
                    vec![
                        initialization.ownership_fact.clone(),
                        initialization.initialization_fact.clone(),
                        history.terminal_fact.clone(),
                    ],
                    body.skill_binding.as_ref(),
                )
            || proof.ownership != initialization.ownership_fact
            || proof.initialization != initialization.initialization_fact
            || proof.predecessor != history.binding
            || proof.terminal != history.terminal_fact
            || proof.prepared != history.prepared
            || proof.committed != history.committed
        {
            return Err(invalid("Continuation source provenance coordinates differ"));
        }
        let binding =
            self.verify_skill_origin(&saved, body.skill_binding.as_ref(), &source.causes)?;
        verify_skill_schema(&body.request.current_input, binding.as_ref())?;
        let config = self
            .continuation_projection
            .as_ref()
            .ok_or_else(|| invalid("host projection config is absent"))?;
        if body.request.task_id != request.task_id
            || body.request.logical_session_id != request.logical_session_id
            || body.request.invocation_id != invocation_id
            || body.request.predecessor_invocation_id != history.binding.invocation_id
        {
            return Err(invalid("foreign continuation source artifact"));
        }
        let mut expected = body.request.current_input.clone();
        expected.messages = history.messages.clone();
        expected
            .messages
            .extend(body.request.current_input.messages.clone());
        if body.source != expected {
            return Err(invalid(
                "projection source differs from frozen predecessor and current input",
            ));
        }
        let actual = project_context(
            &body.source,
            &body.request.descriptor,
            &body.request.source_bounds,
            &body.request.plan,
            &body.request.target_policy,
            config.counter.as_ref(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        if actual != body.projected {
            return Err(invalid("projection plan/provenance changed"));
        }
        selected_matches_initialization(&body.request, &actual, initialization)?;
        let key = kolyan_runtime::ExecutionKey {
            session_id: successor.binding.execution.session_id.clone(),
            turn_id: successor.binding.execution.turn_id.clone(),
            execution_id: successor.binding.execution.execution_id.clone(),
        };
        let input = kolyan_runtime::verified_execution_input(
            self.service
                .sessions()
                .execution()
                .server()
                .coordinator()
                .ledger(),
            &key,
            MAX_CONTEXT_BYTES,
        )
        .map_err(|error| invalid(error.to_string()))?;
        if input.agent_snapshot_digest.as_deref()
            != Some(successor.binding.constraints_digest.as_str())
            || input.model_request != body.projected.prepared.request
        {
            return Err(invalid(
                "successor admission differs from exact selected input",
            ));
        }
        Ok(true)
    }
    pub(in crate::runner) fn verify_continuation_origin(
        &self,
        saved: &crate::AgentInvocationBinding,
        definition: &kolyan_server::InvocationDefinition,
    ) -> Result<(), RunnerError> {
        let (source, body): (_, Body) =
            self.load_input_document(saved, &definition.input_source)?;
        let predecessor_id = definition
            .parent_invocation_id
            .as_ref()
            .ok_or_else(|| invalid("Continuation predecessor is absent"))?;
        if !definition.dependencies.contains(predecessor_id)
            || body.request.predecessor_invocation_id != *predecessor_id
            || body.request.task_id != saved.task_id
            || body.request.logical_session_id != saved.logical_session_id
            || body.request.invocation_id != saved.invocation_id
            || source.envelope.kind != kolyan_server::InvocationInputKind::Derived
        {
            return Err(invalid(
                "Continuation input origin differs from admitted topology",
            ));
        }
        let task = self.service.coordinator().snapshot(&saved.task_id)?;
        let predecessor = task
            .invocations
            .get(predecessor_id)
            .and_then(|invocation| invocation.attempts.last())
            .and_then(|attempt| task.attempts.get(attempt))
            .ok_or_else(|| invalid("Continuation predecessor attempt is absent"))?;
        let terminal = self.service.load_verified_historical_result(
            &saved.task_id,
            &predecessor.binding,
            1024 * 1024,
        )?;
        let history = self.service.load_verified_historical_context(
            &saved.task_id,
            &HistoricalContextRequest {
                binding: predecessor.binding.clone(),
                terminal_fact: terminal.terminal_fact,
                prepared: self.context_endpoint(&predecessor.binding.execution, "prepared")?,
                committed: self.context_endpoint(&predecessor.binding.execution, "committed")?,
                max_bytes: MAX_CONTEXT_BYTES,
            },
        )?;
        let (_, ownership) = self
            .bindings
            .load_with_reference(
                &saved.task_id,
                &saved.invocation_id,
                &saved.logical_session_id,
            )?
            .ok_or_else(|| invalid("Continuation input ownership is absent"))?;
        let owner = PrivateContextOwner {
            logical_session_id: saved.logical_session_id.clone(),
            task_id: saved.task_id.clone(),
            invocation_id: saved.invocation_id.clone(),
            private_session_id: saved.private_session_id.clone(),
            snapshot_digest: saved.snapshot.digest().into(),
        };
        let initialization = PrivateContextService::new(
            self.service.sessions().sessions().clone(),
            Arc::new(ContextJournal(self.service.clone())),
            Arc::new(self.bindings.clone()),
        )
        .load_verified_initialization(&owner, &ownership, MAX_CONTEXT_BYTES)
        .map_err(|error| invalid(error.to_string()))?;
        if source.causes
            != crate::runner::skills::causes(
                vec![
                    ownership.clone(),
                    initialization.initialization_fact.clone(),
                    history.terminal_fact.clone(),
                ],
                body.skill_binding.as_ref(),
            )
            || body.proof.ownership != ownership
            || body.proof.initialization != initialization.initialization_fact
            || body.proof.predecessor != history.binding
            || body.proof.terminal != history.terminal_fact
            || body.proof.prepared != history.prepared
            || body.proof.committed != history.committed
        {
            return Err(invalid("Continuation source coordinates differ"));
        }
        let binding =
            self.verify_skill_origin(saved, body.skill_binding.as_ref(), &source.causes)?;
        verify_skill_schema(&body.request.current_input, binding.as_ref())?;
        let mut expected = body.request.current_input.clone();
        expected.messages = history.messages;
        expected
            .messages
            .extend(body.request.current_input.messages.clone());
        if expected != body.source
            || body.request.current_input.model != *saved.snapshot.definition().model()
        {
            return Err(invalid(
                "Continuation source differs from frozen predecessor input",
            ));
        }
        let config = self
            .continuation_projection
            .as_ref()
            .ok_or_else(|| invalid("host projection config is absent"))?;
        let actual = project_context(
            &body.source,
            &body.request.descriptor,
            &body.request.source_bounds,
            &body.request.plan,
            &body.request.target_policy,
            config.counter.as_ref(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        if actual != body.projected {
            return Err(invalid("Continuation source plan/provenance changed"));
        }
        selected_matches_initialization(&body.request, &actual, &initialization)
    }
}

fn verify_skill_schema(
    request: &ModelRequest,
    binding: Option<&crate::VerifiedSkillBinding>,
) -> Result<(), RunnerError> {
    let expected = binding.and_then(crate::skills::skill_load_definition);
    let mut schemas = request
        .tools
        .iter()
        .filter(|d| d.name == crate::skills::SKILL_LOAD_NAME);
    if schemas.next() != expected.as_ref() || schemas.next().is_some() {
        return Err(invalid(
            "Continuation Skill schema differs from saved exact binding",
        ));
    }
    Ok(())
}

fn selected_matches_initialization(
    request: &ContinuationProjectionRequest,
    projected: &ProjectedContext,
    initialization: &VerifiedPrivateContextInitialization,
) -> Result<(), RunnerError> {
    let mut expected = request.current_input.clone();
    expected.messages = initialization.initialization.messages.clone();
    expected
        .messages
        .extend(request.current_input.messages.clone());
    if projected.prepared.request != expected {
        return Err(invalid(
            "selected context differs from original initialization/current input",
        ));
    }
    Ok(())
}
