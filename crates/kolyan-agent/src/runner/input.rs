//! Agent-owned immutable input documents. Server alone publishes source facts
//! and validates their generic scope. An artifact is evidence, never authority.

use kolyan_core::IssuedToolAuthority;
use kolyan_ledger::{FactJournal, FactRef, LedgerStore};
use kolyan_model::{Message, ModelRequest};
use kolyan_server::{
    InvocationInputEnvelope, InvocationInputKind, InvocationInputScope, InvocationInputSource,
    PrivateContextOwner, VerifiedInvocationInputSource,
};
use kolyan_storage::SessionStore;
use kolyan_trace::{ArtifactRef, ArtifactStore, Retention, TraceSink};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{AgentRunner, DelegationOwner, EnvironmentToolFactory, ProviderFactory, RunnerError};
use crate::{AgentInvocationBinding, ResolvedChildIntent};

pub(super) const MAX_INPUT_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InputBody {
    pub artifact: ArtifactRef,
}

/// Full host input before Agent assembly, plus the exact assembled current input.
/// Session history and Task output clipping are verified at their owning layers;
/// this document does not pretend those later operations have already occurred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RootInput {
    #[serde(deserialize_with = "super::skills::required_binding")]
    pub skill_binding: Option<FactRef>,
    pub ownership: FactRef,
    pub execution: kolyan_server::ExecutionRef,
    pub requested_permissions: crate::AgentPermissions,
    pub original_input: ModelRequest,
    pub selected_input: ModelRequest,
    pub inventory: Vec<kolyan_model::ToolDefinition>,
}

/// The actual authorized call and resolved child, not an owner-only placeholder.
/// Do not reference the final children-admitted receipt: it is published later.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChildInput {
    #[serde(deserialize_with = "super::skills::required_binding")]
    pub skill_binding: Option<FactRef>,
    pub parent: DelegationOwner,
    pub parent_ownership: FactRef,
    pub issued: IssuedToolAuthority,
    pub child_index: usize,
    pub intent: ResolvedChildIntent,
    pub ownership: FactRef,
    pub context_owner: PrivateContextOwner,
    pub initialization: FactRef,
    pub initialization_digest: String,
    pub initial_messages: Vec<Message>,
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
    pub(super) fn publish_input_document<D: Serialize>(
        &self,
        saved: &AgentInvocationBinding,
        kind: InvocationInputKind,
        document: &D,
        causes: Vec<FactRef>,
    ) -> Result<VerifiedInvocationInputSource, RunnerError> {
        let artifact = archive(self.input_artifacts.as_ref(), document)?;
        self.service
            .coordinator()
            .publish_invocation_input_source(
                InvocationInputEnvelope {
                    kind,
                    scope: input_scope(saved),
                    body: serde_json::to_value(InputBody { artifact })
                        .map_err(|error| invalid(error.to_string()))?,
                },
                causes,
            )
            .map_err(|error| invalid(error.to_string()))
    }

    pub(super) fn load_input_document<D: serde::de::DeserializeOwned>(
        &self,
        saved: &AgentInvocationBinding,
        source: &InvocationInputSource,
    ) -> Result<(VerifiedInvocationInputSource, D), RunnerError> {
        let proof = self
            .service
            .coordinator()
            .load_verified_invocation_input_source(
                source,
                &input_scope(saved),
                MAX_INPUT_DOCUMENT_BYTES,
            )
            .map_err(|error| invalid(error.to_string()))?;
        let body: InputBody = serde_json::from_value(proof.envelope.body.clone())
            .map_err(|error| invalid(error.to_string()))?;
        if body.artifact.retention != Retention::Required {
            return Err(invalid("invocation input artifact is not required"));
        }
        let bytes = self
            .input_artifacts
            .read(&body.artifact, MAX_INPUT_DOCUMENT_BYTES as u64)
            .map_err(|error| invalid(error.to_string()))?;
        let document =
            serde_json::from_slice(&bytes).map_err(|error| invalid(error.to_string()))?;
        Ok((proof, document))
    }

    pub(super) fn verify_root_input(
        &self,
        saved: &AgentInvocationBinding,
        source: &InvocationInputSource,
    ) -> Result<(), RunnerError> {
        let (proof, input): (_, RootInput) = self.load_input_document(saved, source)?;
        let (_, ownership) = self
            .bindings
            .load_with_reference(
                &saved.task_id,
                &saved.invocation_id,
                &saved.logical_session_id,
            )?
            .ok_or_else(|| invalid("root input owner disappeared"))?;
        let mut selected = input.original_input.clone();
        input.requested_permissions.validate()?;
        saved
            .snapshot
            .permissions()
            .require_subset_of(&input.requested_permissions)?;
        selected.model = saved.snapshot.definition().model().clone();
        selected.system.push(kolyan_model::SystemInstruction {
            text: saved.snapshot.definition().instructions().into(),
            cache: false,
        });
        let environment_inventory = input
            .inventory
            .iter()
            .filter(|definition| {
                definition.name != crate::AGENT_INVOKE_NAME
                    && definition.name != crate::skills::SKILL_LOAD_NAME
            })
            .cloned()
            .collect::<Vec<_>>();
        super::tools::validate_inventory(&saved.snapshot, &environment_inventory)?;
        if input
            .inventory
            .iter()
            .filter(|definition| definition.name == crate::AGENT_INVOKE_NAME)
            .count()
            > 1
        {
            return Err(invalid(
                "frozen invocation inventory has duplicate delegate schemas",
            ));
        }
        selected.tools = input
            .inventory
            .iter()
            .filter(|definition| {
                definition.name == crate::AGENT_INVOKE_NAME
                    || definition.name == crate::skills::SKILL_LOAD_NAME
                    || super::tools::permits(&saved.snapshot, &definition.name)
            })
            .cloned()
            .collect();
        if proof.envelope.kind != InvocationInputKind::Standalone
            || &input.requested_permissions != saved.snapshot.permissions()
            || input.execution.session_id != saved.logical_session_id
            || proof.causes
                != super::skills::causes(vec![ownership.clone()], input.skill_binding.as_ref())
            || input.ownership != ownership
            || !input.original_input.tools.is_empty()
            || selected != input.selected_input
            || input
                .selected_input
                .max_output_tokens
                .is_none_or(|limit| limit == 0)
        {
            return Err(invalid(
                "standalone source differs from actual root input and ownership",
            ));
        }
        let binding =
            self.verify_skill_origin(saved, input.skill_binding.as_ref(), &proof.causes)?;
        let expected_skill = binding
            .as_ref()
            .and_then(crate::skills::skill_load_definition);
        let mut skill_schemas = input
            .inventory
            .iter()
            .filter(|d| d.name == crate::skills::SKILL_LOAD_NAME);
        if skill_schemas.next() != expected_skill.as_ref() || skill_schemas.next().is_some() {
            return Err(invalid("root Skill schema differs from saved selection"));
        }
        Ok(())
    }

    pub(super) fn verify_child_input(
        &self,
        saved: &AgentInvocationBinding,
        source: &InvocationInputSource,
    ) -> Result<ChildInput, RunnerError> {
        let (proof, input): (_, ChildInput) = self.load_input_document(saved, source)?;
        let (_, ownership) = self
            .bindings
            .load_with_reference(
                &saved.task_id,
                &saved.invocation_id,
                &saved.logical_session_id,
            )?
            .ok_or_else(|| invalid("child input owner disappeared"))?;
        input
            .issued
            .validate(&input.parent.scope)
            .map_err(|error| invalid(error.to_string()))?;
        let actual: crate::AgentInvokeInput =
            serde_json::from_value(input.issued.prepared.call().arguments.clone())
                .map_err(|error| invalid(error.to_string()))?;
        let original = actual
            .children
            .get(input.child_index)
            .ok_or_else(|| invalid("child input index outside actual invoke"))?;
        let (parent, parent_ownership) = self
            .bindings
            .load_with_reference(
                &saved.task_id,
                &input.parent.parent.invocation_id,
                &saved.logical_session_id,
            )?
            .ok_or_else(|| invalid("derived source parent binding is absent"))?;
        let task = self.service.coordinator().snapshot(&saved.task_id)?;
        let admitted = task
            .invocations
            .get(&saved.invocation_id)
            .ok_or_else(|| invalid("derived source child is not admitted"))?;
        let parent_attempt = task
            .attempts
            .get(&input.parent.parent.attempt_id)
            .ok_or_else(|| invalid("derived source parent attempt is absent"))?;
        let parent_invocation = task
            .invocations
            .get(&parent.invocation_id)
            .ok_or_else(|| invalid("derived source parent admission is absent"))?;
        if input.parent_ownership != parent_ownership
            || parent_attempt.binding != input.parent.parent
            || parent_invocation.definition.input_source != input.parent.parent.input_source
            || parent.snapshot.identity() != &input.parent.parent.agent
            || parent.snapshot.digest() != input.parent.parent.constraints_digest
            || parent.private_session_id != input.parent.parent.execution.session_id
            || admitted.definition.parent_invocation_id.as_deref()
                != Some(parent.invocation_id.as_str())
            || admitted.definition.role != input.intent.role
        {
            return Err(invalid("derived source parent identity/admission differs"));
        }
        let prepared = crate::invoke::verify_saved_agent_invocation(
            &input.issued.prepared,
            &parent,
            &input.parent.parent.execution,
            &self.host,
        )
        .map_err(|error| invalid(error.to_string()))?;
        if prepared.children().get(input.child_index) != Some(&input.intent) {
            return Err(invalid(
                "derived source target/role/definition differs from exact invoke",
            ));
        }
        let initialized = kolyan_server::PrivateContextService::new(
            self.service.sessions().sessions().clone(),
            Arc::new(super::finalization::continuation::ContextJournal(
                self.service.clone(),
            )),
            Arc::new(self.bindings.clone()),
        )
        .load_verified_initialization(&input.context_owner, &ownership, MAX_INPUT_DOCUMENT_BYTES)
        .map_err(|error| invalid(error.to_string()))?;
        if proof.envelope.kind != InvocationInputKind::Derived
            || proof.causes
                != super::skills::causes(
                    vec![
                        input.parent_ownership.clone(),
                        ownership.clone(),
                        input.initialization.clone(),
                    ],
                    input.skill_binding.as_ref(),
                )
            || input.ownership != ownership
            || input.parent.task_id != saved.task_id
            || input.parent.logical_session_id != saved.logical_session_id
            || input.issued.prepared.call().name != crate::AGENT_INVOKE_NAME
            || input.issued.scope.execution.session_id != input.parent.parent.execution.session_id
            || input.issued.scope.execution.turn_id != input.parent.parent.execution.turn_id
            || input.issued.scope.execution.execution_id
                != input.parent.parent.execution.execution_id
            || input.intent.input != original.input
            || input.intent.permissions != original.permissions
            || input.intent.definition != *saved.snapshot.definition()
            || input.intent.permissions != *saved.snapshot.permissions()
            || input.context_owner.task_id != saved.task_id
            || input.context_owner.invocation_id != saved.invocation_id
            || input.context_owner.logical_session_id != saved.logical_session_id
            || input.context_owner.private_session_id != saved.private_session_id
            || input.context_owner.snapshot_digest != saved.snapshot.digest()
            || input.initialization != initialized.initialization_fact
            || input.initialization_digest != initialized.initialization.binding_digest
            || input.initial_messages != initialized.initialization.messages
            || input.initial_messages
                != vec![Message {
                    role: kolyan_model::MessageRole::User,
                    content: vec![kolyan_model::ContentBlock::Text {
                        text: original.input.clone(),
                    }],
                }]
        {
            return Err(invalid(
                "derived child source differs from actual invoke input and owner",
            ));
        }
        self.verify_skill_origin(saved, input.skill_binding.as_ref(), &proof.causes)?;
        Ok(input)
    }

    pub(super) fn verify_root_attempt_source(
        &self,
        saved: &AgentInvocationBinding,
        binding: &kolyan_server::AttemptBinding,
    ) -> Result<(), RunnerError> {
        self.verify_root_input(saved, &binding.input_source)?;
        let (_, origin): (_, RootInput) = self.load_input_document(saved, &binding.input_source)?;
        if origin.execution != binding.execution {
            return Err(invalid(
                "root attempt substituted prepared execution coordinates",
            ));
        }
        Ok(())
    }

    pub(super) fn verify_root_execution_input(
        &self,
        saved: &AgentInvocationBinding,
        binding: &kolyan_server::AttemptBinding,
    ) -> Result<(), RunnerError> {
        self.verify_root_attempt_source(saved, binding)?;
        let (_, origin): (_, RootInput) = self.load_input_document(saved, &binding.input_source)?;
        let key = kolyan_runtime::ExecutionKey {
            session_id: binding.execution.session_id.clone(),
            turn_id: binding.execution.turn_id.clone(),
            execution_id: binding.execution.execution_id.clone(),
        };
        let input = kolyan_runtime::verified_execution_input(
            self.service
                .sessions()
                .execution()
                .server()
                .coordinator()
                .ledger(),
            &key,
            MAX_INPUT_DOCUMENT_BYTES,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let session = self
            .service
            .sessions()
            .sessions()
            .load(&binding.execution.session_id)
            .map_err(|error| invalid(error.to_string()))?;
        let segment = session
            .inputs
            .get(&binding.execution.turn_id)
            .ok_or_else(|| invalid("root original Session input is absent"))?;
        if segment.messages != origin.selected_input.messages
            || input.model_request.messages.get(segment.history_len..)
                != Some(segment.messages.as_slice())
            || input.agent_snapshot_digest.as_deref() != Some(saved.snapshot.digest())
        {
            return Err(invalid(
                "standalone source differs from immutable Runtime input",
            ));
        }
        let mut expected = origin.selected_input;
        expected.messages = input.model_request.messages.clone();
        // Task owns finite-budget clipping. It may reduce, never expand, the
        // explicit host ceiling. Every other neutral request field must match.
        if input.model_request.max_output_tokens.is_none_or(|limit| {
            limit == 0
                || expected
                    .max_output_tokens
                    .is_none_or(|ceiling| limit > ceiling)
        }) {
            return Err(invalid("root admission expands the source output ceiling"));
        }
        expected.max_output_tokens = input.model_request.max_output_tokens;
        if expected != input.model_request {
            return Err(invalid(
                "root assembled input differs from immutable Runtime admission",
            ));
        }
        Ok(())
    }
}

pub(super) fn input_scope(saved: &AgentInvocationBinding) -> InvocationInputScope {
    InvocationInputScope {
        task_id: saved.task_id.clone(),
        invocation_id: saved.invocation_id.clone(),
        agent: saved.snapshot.identity().clone(),
        constraints_digest: saved.snapshot.digest().into(),
    }
}

pub(super) fn archive<D: Serialize>(
    store: &ArtifactStore,
    document: &D,
) -> Result<ArtifactRef, RunnerError> {
    let mut writer = BoundedDocument { bytes: Vec::new() };
    serde_json::to_writer(&mut writer, document).map_err(|error| invalid(error.to_string()))?;
    store
        .put(&writer.bytes, Retention::Required)
        .map_err(|error| invalid(error.to_string()))
}

struct BoundedDocument {
    bytes: Vec<u8>,
}
impl std::io::Write for BoundedDocument {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|size| size > MAX_INPUT_DOCUMENT_BYTES)
        {
            return Err(std::io::Error::other(
                "invocation input document exceeds host ceiling",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> RunnerError {
    RunnerError::Host(message.into())
}

#[cfg(test)]
pub(super) mod tests;
